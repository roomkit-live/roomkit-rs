//! The ACP agent channel (RFC §6.4): an external coding agent as a room's
//! intelligence channel, Python's `ACPChannel`.

mod cancellation;
mod client;
mod config;
mod connector;
mod preparation;
mod sessions;
mod turn;

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, ErrorRecord, EventDirection, EventSource,
    EventType, InboundMessage, Metadata, ResponseMetadata, RoomContext, RoomEvent, RoomId,
    TextContent,
};
use roomkit::{Channel, ChannelError, ClassedError, ResponseItem, current_turn_host};
use roomkit_core::rules::edits::opens_no_turn;
use roomkit_core::rules::instructions::is_standalone;
use roomkit_core::rules::transcript::{is_blank, mark_instruction};
use serde_json::{Value, json};

use self::connector::{Connector, PROTOCOL_VERSION};

/// The room messages a channel declares it reads: Python's `room_history`
/// default.
const ROOM_HISTORY: usize = 20;
use self::turn::{Ask, TurnStream};
use crate::env::absolute_path;
use crate::error::AcpError;
use crate::stdio::{StdioAcpTransport, StdioAcpTransportOptions};
use crate::text::acp_event_text;
use crate::transport::AcpTransport;

/// What [`AcpChannel::new`] takes beside the channel's id and its
/// directory: Python's keywords, with their defaults. `command` spawns the
/// agent here, `transport` reaches one that runs elsewhere: one of the two,
/// never both.
pub struct AcpChannelOptions<T = StdioAcpTransport> {
    /// The program and its arguments starting the agent, run directly,
    /// never through a shell: a [`StdioAcpTransport`].
    pub command: Option<Vec<String>>,
    /// How to reach an agent this channel does not spawn.
    pub transport: Option<T>,
    /// Variables added to a spawned agent's environment (a
    /// [`StdioAcpTransportOptions`]'s); refused with a `transport`.
    pub env: Option<BTreeMap<String, String>>,
    /// Names of the host's variables a spawned agent is given (a
    /// [`StdioAcpTransportOptions`]'s); refused with a `transport`.
    pub inherit_env: Option<Vec<String>>,
    /// The ACP authentication method the channel authenticates with, once
    /// connected.
    pub authentication_method: Option<String>,
    /// Initial string or boolean option values, applied before each session is prompted.
    pub session_config: Option<Metadata>,
}

impl<T> Default for AcpChannelOptions<T> {
    fn default() -> Self {
        Self {
            command: None,
            transport: None,
            env: None,
            inherit_env: None,
            authentication_method: None,
            session_config: None,
        }
    }
}

impl<T> fmt::Debug for AcpChannelOptions<T> {
    /// The program and the names of the variables, never the arguments nor
    /// the values, which may hold secrets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcpChannelOptions")
            .field(
                "program",
                &self.command.as_ref().and_then(|command| command.first()),
            )
            .field("transport", &self.transport.is_some())
            .field(
                "env",
                &self.env.as_ref().map(|env| env.keys().collect::<Vec<_>>()),
            )
            .field("inherit_env", &self.inherit_env)
            .field("authentication_method", &self.authentication_method)
            .field(
                "session_config",
                &self
                    .session_config
                    .as_ref()
                    .map(|config| config.keys().collect::<Vec<_>>()),
            )
            .finish()
    }
}

/// A room's intelligence channel speaking to an external coding agent over
/// ACP (RFC §6.4): Python's `ACPChannel`, RoomKit the ACP client.
///
/// One connection to the agent is opened at the channel's first turn, and
/// one ACP session per room, at the room's first turn, kept for the
/// room's next ones. A room's turns run one after the other in its
/// session; different rooms' run at once on the one connection. Each
/// message or instruction the channel reads that opens a turn is prompted
/// in the room's session, tagged with its event's id; what the agent says
/// streams back as the turn's text, and the turn's record says how the
/// prompt ended (`acp.protocol_version`, `prompt_returned`, `stop_reason`
/// when it is not `end_turn`, `interrupted` when it never returned).
///
/// A clone is the same channel. A channel spawning its agent has the type
/// `AcpChannel`; one reaching an
/// agent through a transport of its own, `AcpChannel<ThatTransport>`, as
/// an AI channel's type carries its provider (HITL 1bd56827):
///
/// ```no_run
/// use roomkit::model::ChannelId;
/// use roomkit_acp::{AcpChannel, AcpChannelOptions};
///
/// # fn build() -> Result<(), Box<dyn std::error::Error>> {
/// let options = AcpChannelOptions {
///     command: Some(vec!["claude-agent-acp".into()]),
///     ..AcpChannelOptions::default()
/// };
/// let claude: AcpChannel = AcpChannel::new(ChannelId::new("claude")?, "/srv/project", options)?;
/// # Ok(())
/// # }
/// ```
pub struct AcpChannel<T: AcpTransport = StdioAcpTransport> {
    shared: Arc<Shared<T>>,
}

/// What the channel's turns share.
struct Shared<T: AcpTransport> {
    channel_id: ChannelId,
    cwd: PathBuf,
    connector: Connector<T>,
    /// Each room's turn lock: one turn of a room at a time.
    rooms: Mutex<HashMap<RoomId, Arc<tokio::sync::Mutex<()>>>>,
    active: AtomicUsize,
    preparations: Mutex<HashMap<RoomId, Arc<preparation::Preparation>>>,
}

impl<T: AcpTransport> Shared<T> {
    fn room_lock(&self, room_id: &RoomId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.rooms
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(room_id.clone())
                .or_default(),
        )
    }

    /// A turn in flight, counted until it is dropped.
    fn active(self: &Arc<Self>) -> Active<T> {
        self.active.fetch_add(1, Ordering::AcqRel);
        Active(Arc::clone(self))
    }
}

/// A turn counted in the channel's `active_turns`.
struct Active<T: AcpTransport>(Arc<Shared<T>>);

impl<T: AcpTransport> Drop for Active<T> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T: AcpTransport> AcpChannel<T> {
    /// The channel `channel_id` in `cwd`, an absolute path (`~` read as the
    /// host's `HOME`) declared to every session and, for a spawned agent,
    /// the process's own directory: Python's `ACPChannel(channel_id,
    /// command=None, *, transport=None, cwd, ...)`.
    ///
    /// # Errors
    ///
    /// [`AcpError::Value`], in Python's words: both `command` and
    /// `transport`, or neither; `env` or `inherit_env` with a `transport`;
    /// a relative `cwd`; a command [`StdioAcpTransport::new`] refuses. A
    /// `command` for a channel whose transport type is not
    /// [`StdioAcpTransport`] is refused too: only that transport spawns.
    pub fn new(
        channel_id: ChannelId,
        cwd: impl AsRef<Path>,
        options: AcpChannelOptions<T>,
    ) -> Result<Self, AcpError> {
        let cwd = absolute_path(cwd.as_ref(), "cwd")?;
        let AcpChannelOptions {
            command,
            transport,
            env,
            inherit_env,
            authentication_method,
            session_config,
        } = options;
        let setup = config::validate(session_config.unwrap_or_default())?;
        let transport = match (command, transport) {
            (Some(_), Some(_)) => {
                return Err(AcpError::Value(
                    "command spawns the agent here and transport reaches one that is already \
                     running: pass one, not both"
                        .to_owned(),
                ));
            }
            (None, None) => {
                return Err(AcpError::Value(
                    "pass command to spawn the agent, or transport to reach one".to_owned(),
                ));
            }
            (None, Some(transport)) => {
                if env.is_some() || inherit_env.is_some() {
                    return Err(AcpError::Value(
                        "env and inherit_env configure the default subprocess spawn; a custom \
                         transport carries its own environment"
                            .to_owned(),
                    ));
                }
                transport
            }
            (Some(command), None) => {
                let spawned = StdioAcpTransport::new(
                    command,
                    &cwd,
                    StdioAcpTransportOptions { env, inherit_env },
                )?;
                let spawned: Box<dyn Any> = Box::new(spawned);
                *spawned.downcast::<T>().map_err(|_other| {
                    AcpError::Value(
                        "command spawns the agent with StdioAcpTransport: a channel of another \
                         transport takes transport"
                            .to_owned(),
                    )
                })?
            }
        };
        Ok(Self {
            shared: Arc::new(Shared {
                connector: Connector::new(
                    transport,
                    authentication_method,
                    channel_id.clone(),
                    setup,
                ),
                channel_id,
                cwd,
                rooms: Mutex::new(HashMap::new()),
                active: AtomicUsize::new(0),
                preparations: Mutex::default(),
            }),
        })
    }

    /// The source of the channel's answers.
    fn source(&self) -> EventSource {
        EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.shared.channel_id.clone(), ChannelType::AI)
        }
    }

    /// The ACP session of `room_id`, none before the room's first turn
    /// opened it. Python's `session_id`.
    pub fn session_id(&self, room_id: &RoomId) -> Option<String> {
        self.shared.connector.session_of(room_id)
    }
}

impl<T: AcpTransport> Clone for AcpChannel<T> {
    /// The same channel: its connection, its sessions, its turns.
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: AcpTransport> fmt::Debug for AcpChannel<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcpChannel")
            .field("channel_id", &self.shared.channel_id)
            .field("cwd", &self.shared.cwd)
            .field("active_turns", &self.active_turns())
            .finish_non_exhaustive()
    }
}

impl<T: AcpTransport> Channel for AcpChannel<T> {
    fn channel_id(&self) -> ChannelId {
        self.shared.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    /// The room messages the channel reads, its catch-up's window (RFC
    /// §19.3.2): Python's `room_history`, 20 by default.
    fn recent_events_window(&self) -> usize {
        ROOM_HISTORY
    }

    /// The registered turns in flight: Python's `active_turns`. A finished
    /// recovery leaves this count before its terminal session cleanup,
    /// while that cleanup still holds the room.
    fn active_turns(&self) -> usize {
        self.shared.active.load(Ordering::Acquire)
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            media_types: [ChannelMediaType::Text, ChannelMediaType::Rich].into(),
            supports_rich_text: true,
            ..ChannelCapabilities::default()
        }
    }

    /// The connection and its agent, without the command's arguments:
    /// Python's `info`.
    fn info(&self) -> Metadata {
        let (connected, agent, sessions) = self.shared.connector.report();
        let mut info = Metadata::new();
        info.insert(
            "transport".to_owned(),
            json!(self.shared.connector.transport.name()),
        );
        info.insert("protocol_version".to_owned(), json!(PROTOCOL_VERSION));
        // No SDK speaks for the channel: its connection is this crate's
        // (HITL c7b3c7a3).
        info.insert("sdk_version".to_owned(), Value::Null);
        info.insert("connected".to_owned(), json!(connected));
        info.insert("agent".to_owned(), agent.unwrap_or(Value::Null));
        info.insert("session_count".to_owned(), json!(sessions));
        info.insert("active_turns".to_owned(), json!(self.active_turns()));
        info
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Err(
            "ACP intelligence channels do not accept inbound messages".into(),
        ))
    }

    /// Prompts the room's session with `event` (RFC §6.4), the turn's
    /// stream read as the agent answers; nothing for the channel's own
    /// events, an event that opens no turn (RFC §10.3) or one whose text
    /// is blank.
    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if event.source.channel_id == self.shared.channel_id || opens_no_turn(&event) {
            return Ok(ChannelOutput::empty());
        }
        let mut text = acp_event_text(&event);
        if is_blank(&text) {
            return Ok(ChannelOutput::empty());
        }
        if event.r#type == EventType::Instruction {
            // A standalone instruction's session of its own comes with
            // parity stream L's lot 8.
            if is_standalone(&event) {
                return Ok(ChannelOutput::empty());
            }
            // The application's direction, never a participant's line (RFC
            // §10.1.1 step 6).
            text = mark_instruction(&text);
        }
        let record = ResponseMetadata::new();
        record.insert("acp", json!({ "protocol_version": PROTOCOL_VERSION }));
        let host = current_turn_host();
        let stream = TurnStream::new(
            Arc::clone(&self.shared),
            Ask {
                room_id: context.room.id.clone(),
                event_id: event.id.to_string(),
                text,
                record: record.clone(),
                host: host.clone(),
            },
        );
        match host {
            // Within the kit, the turn is one stream, read as it is written.
            Some(host) => Ok(ChannelOutput {
                response_metadata: record,
                ..host.respond_with_stream(Box::pin(stream))
            }),
            // Outside one, it is read whole, as an AI channel's is.
            None => buffered(stream, &event, self.source(), record).await,
        }
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    /// Run a configuration verb on this room's local session (RFC §10.6.3).
    async fn channel_call(
        &self,
        room_id: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        self.call_session(&room_id, &verb, &arguments).await
    }

    fn room_state(&self, room_id: &RoomId) -> Result<Metadata, ChannelError> {
        Ok(self.shared.connector.room_state(room_id))
    }

    fn restore_room_state(&self, room_id: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        self.shared.connector.restore_room_state(room_id, &state);
        Ok(())
    }

    /// Closes the rooms' sessions where the agent takes `session/close`,
    /// then the transport: Python's `close()`.
    async fn close(&self) {
        self.shared.connector.close().await;
    }
}

/// A turn read whole, outside a kit: its text one message answering
/// `trigger`, the turn's record on it, as an AI channel's turn is read
/// outside one (HITL c2e5eb96); a failure with nothing said is the turn's
/// error, one after some text the output's.
async fn buffered<T: AcpTransport>(
    mut stream: TurnStream<T>,
    trigger: &RoomEvent,
    source: EventSource,
    record: ResponseMetadata,
) -> Result<ChannelOutput, ChannelError> {
    let mut said = String::new();
    let mut failure = None;
    while let Some(item) = std::future::poll_fn(|cx| {
        futures_core::Stream::poll_next(std::pin::Pin::new(&mut stream), cx)
    })
    .await
    {
        match item {
            Ok(ResponseItem::Text(text)) => said.push_str(&text),
            Ok(_) => {}
            Err(error) => failure = Some(error),
        }
    }
    drop(stream);
    if is_blank(&said) {
        return match failure {
            Some(failure) => Err(failure),
            None => Ok(ChannelOutput {
                response_metadata: record,
                ..ChannelOutput::empty()
            }),
        };
    }
    let mut message = RoomEvent::at(
        trigger.id.clone(),
        trigger.room_id.clone(),
        EventType::Message,
        source,
        TextContent::new(said),
        trigger.created_at,
    );
    message.metadata = record.to_metadata();
    let error = failure.map(|failure| match failure.downcast_ref::<ClassedError>() {
        Some(classed) => ErrorRecord {
            attributes: Box::new(classed.attributes().clone()),
            ..ErrorRecord::new(classed.class(), classed.error().to_string())
        },
        None => ErrorRecord::new("Exception", failure.to_string()),
    });
    Ok(ChannelOutput {
        responded: true,
        response_events: vec![message],
        response_metadata: record,
        error,
        ..ChannelOutput::empty()
    })
}

#[cfg(test)]
mod tests;
