//! The ACP agent channel (RFC §6.4): an external coding agent as a room's
//! intelligence channel, Python's `ACPChannel`.

mod cancellation;
mod client;
mod config;
mod connector;
mod context;
mod detachment;
mod identity;
mod permissions;
mod preparation;
mod sessions;
mod steering;
mod tools;
mod turn;
mod updates;

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
use roomkit::{Channel, ChannelError, ClassedError, ResponseItem, TurnRecord, current_turn_host};
use roomkit_core::rules::edits::opens_no_turn;
use roomkit_core::rules::instructions::is_standalone;
use roomkit_core::rules::transcript::{is_blank, mark_instruction};
use serde_json::{Value, json};

use self::connector::{Connector, PROTOCOL_VERSION};

/// The room messages a channel declares it reads: Python's `room_history`
/// default.
const ROOM_HISTORY: i64 = 20;
use self::turn::{Ask, TurnStream};
use crate::context::AcpContextContributor;
use crate::env::{absolute_path, absolute_paths};
use crate::error::AcpError;
use crate::mcp::McpServers;
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
    /// Additional absolute directories declared to every session (RFC Appendix A.9.1).
    /// These are paths on the agent's machine; they do not change `cwd`.
    pub additional_directories: Option<Vec<PathBuf>>,
    /// Fixed or per-room MCP descriptors declared on every session opening (RFC §6.4).
    pub mcp_servers: Option<McpServers>,
    /// Publish a bounded thought preview to room subscribers; false by default.
    pub publish_thinking_text: bool,
    /// Optional permission and report handler; absent rejects every permission.
    pub external_tool_handler: Option<Arc<dyn roomkit_ai::ExternalToolHandler>>,
    /// Missed room messages carried into a prompt; 20 by default, zero disables it.
    pub room_history: i64,
    /// Optional async host blocks, resolved separately for every eligible turn.
    pub context_contributor: Option<AcpContextContributor>,
    /// The agent's display name; absent by default.
    pub name: Option<String>,
    /// The agent's role; absent by default.
    pub role: Option<String>,
    /// What the agent does; absent by default.
    pub description: Option<String>,
    /// Standing application instructions sent until a session's first normal RPC return.
    pub instructions: Option<String>,
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
            additional_directories: None,
            mcp_servers: None,
            publish_thinking_text: false,
            external_tool_handler: None,
            room_history: ROOM_HISTORY,
            context_contributor: None,
            name: None,
            role: None,
            description: None,
            instructions: None,
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
                "external_tool_handler",
                &self.external_tool_handler.is_some(),
            )
            .field("mcp_servers", &self.mcp_servers)
            .field("room_history", &self.room_history)
            .field("context_contributor", &self.context_contributor)
            .field("name", &self.name.is_some())
            .field("role", &self.role.is_some())
            .field("description", &self.description.is_some())
            .field("instructions", &self.instructions.is_some())
            .field("publish_thinking_text", &self.publish_thinking_text)
            .field(
                "additional_directories",
                &self.additional_directories.as_ref().map(Vec::len),
            )
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
    publish_thinking_text: bool,
    room_history: usize,
    context_contributor: Option<AcpContextContributor>,
    name: Option<String>,
    role: Option<String>,
    description: Option<String>,
    instructions: Option<String>,
    connector: Arc<Connector<T>>,
    /// Each room's turn lock: one turn of a room at a time.
    rooms: Mutex<HashMap<RoomId, Arc<tokio::sync::Mutex<()>>>>,
    active: AtomicUsize,
    closings: Mutex<tokio::task::JoinSet<()>>,
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
            additional_directories,
            mcp_servers,
            publish_thinking_text,
            external_tool_handler,
            room_history,
            context_contributor,
            name,
            role,
            description,
            instructions,
            env,
            inherit_env,
            authentication_method,
            session_config,
        } = options;
        let room_history = context::history_count(room_history)?;
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
        let additional_directories =
            absolute_paths(additional_directories, "additional_directories")?;
        Ok(Self {
            shared: Arc::new(Shared {
                connector: Arc::new(Connector::new(
                    transport,
                    authentication_method,
                    channel_id.clone(),
                    setup,
                    additional_directories,
                    mcp_servers,
                    external_tool_handler,
                )),
                channel_id,
                cwd,
                publish_thinking_text,
                room_history,
                context_contributor,
                name,
                role,
                description,
                instructions: instructions.filter(|value| !is_blank(value)),
                rooms: Mutex::new(HashMap::new()),
                active: AtomicUsize::new(0),
                closings: Mutex::default(),
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
    fn name(&self) -> Option<String> {
        self.shared.name.clone()
    }

    fn role(&self) -> Option<String> {
        self.shared.role.clone()
    }

    fn description(&self) -> Option<String> {
        self.shared.description.clone()
    }

    fn external_tool_bridge(&self) -> Option<roomkit::external_tools::ExternalToolBridge> {
        self.shared
            .connector
            .handler
            .handler
            .as_ref()
            .map(|handler| handler.state().bridge())
    }

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
        self.shared.room_history
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
    /// events, an event that opens no turn (RFC §10.3), or an event with
    /// no text, host content or Discussion notes.
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
        let blocks =
            context::contributed_blocks(self.shared.context_contributor.as_ref(), &context, &event)
                .await;
        let notes = context::notes(&event);
        if is_blank(&text) && blocks.is_empty() && notes.is_none() {
            return Ok(ChannelOutput::empty());
        }
        if event.r#type == EventType::Instruction {
            // The application's direction, never a participant's line (RFC
            // §10.1.1 step 6).
            text = mark_instruction(&text);
        }
        let standalone = is_standalone(&event);
        let record = turn::response_metadata(standalone);
        let host = current_turn_host();
        let stream = TurnStream::new(
            Arc::clone(&self.shared),
            Ask {
                standalone,
                room_id: context.room.id.clone(),
                event_id: event.id.to_string(),
                input: context::Input {
                    context,
                    trigger: event.clone(),
                    text,
                    blocks,
                    notes,
                },
                record: record.clone(),
                host: host.clone(),
                answers_room: updates::answers_room(&event),
            },
        );
        // Capture the record when the stream runs, along with the delivery
        // context its tool reports already carry. Never borrow a parent's.
        let scope = TurnRecord::default();
        scope.adopt(record.clone());
        let stream = scope.scope_stream(stream);
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

    async fn steer_turn_async(
        &self,
        directive: &roomkit::model::SteeringDirective,
        turn_id: &str,
    ) -> Result<Option<usize>, ChannelError> {
        self.steer(directive, turn_id).await.map_err(turn::raised)
    }

    fn room_state(&self, room_id: &RoomId) -> Result<Metadata, ChannelError> {
        Ok(self.shared.connector.room_state(room_id))
    }

    fn restore_room_state(&self, room_id: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        self.shared.connector.restore_room_state(room_id, &state);
        Ok(())
    }

    /// Forget room settings immediately and close its session after its turn (RFC §6.4).
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "detachment records local state without waiting for the agent"
    )]
    async fn on_room_detached(&self, room_id: RoomId) -> Result<(), ChannelError> {
        self.detach_room(room_id);
        Ok(())
    }

    /// Closes the rooms' sessions where the agent takes `session/close`,
    /// then the transport: Python's `close()`.
    async fn close(&self) {
        let _closings = detachment::ClosingTasks(&self.shared);
        self.shared.connector.close().await;
    }
}

/// A turn read whole, outside a kit: its text one message answering
/// `trigger`, the turn's record on it, as an AI channel's turn is read
/// outside one (HITL c2e5eb96); a failure with nothing said is the turn's
/// error, one after some text the output's.
async fn buffered<S: futures_core::Stream<Item = Result<ResponseItem, ChannelError>> + Unpin>(
    mut stream: S,
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
