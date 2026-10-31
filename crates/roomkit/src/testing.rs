//! Channels for tests and examples: a transport that records what it
//! delivers, and an agent that echoes what it is asked; a kit whose clock
//! and ids a test sets; and a tap on the messages a kit's turn backend
//! carries.
//!
//! The Python implementation's tests patch its clock and its `uuid4`; the
//! sans-IO core takes them as an input instead, which
//! [`kit_with_environment`] gives (step 5, decision 5).

use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, EventDirection, EventSource, EventType, RoomContext, RoomEvent,
    TextContent,
};
use roomkit_core::ops::ChannelDeclaration;
use roomkit_core::rules::transcoding::text_of;

pub use roomkit_core::ops::{Environment, SequentialEnvironment};

use crate::channel::{Channel, ChannelError};
use crate::store::ConversationStore;
use crate::turns::{Carried, TurnTimeouts};
use crate::{RoomKit, RoomKitOptions};

/// A kit built with `options` whose runs read `environment` instead of the
/// system clock and random ids: a deterministic clock and sequential ids,
/// for a reproducible test or example.
pub fn kit_with_environment<S: ConversationStore>(
    options: RoomKitOptions<S>,
    environment: impl Environment + Send + Sync + 'static,
) -> RoomKit<S> {
    RoomKit::with_environment(options, Arc::new(environment))
}

/// Waits until `kit` runs nothing in the background: no observer hook
/// running after the fact (RFC §9.3), and no inbound call running, whether
/// its caller waits for it, was answered at its decision (RFC §10.1 step
/// 18) or stopped waiting, and no direct call, a lifecycle change, a direct
/// change of a stored event or `commit_event`, each running on a task of
/// its own. What one of them starts, an observer its delivery fires, a call
/// its channel makes, is waited for too. A test seam: the Python
/// implementation's kit has no such call.
///
/// Work that keeps coming keeps the kit busy: `idle` returns at a moment
/// nothing runs. Awaited from the kit's own work, a hook or a channel, it
/// waits for itself: a test awaits it once its calls returned.
#[doc(hidden)]
pub async fn idle<S>(kit: &RoomKit<S>) {
    kit.inner.tasks.idle().await;
}

/// Sets the bounds both sides of `kit`'s turns keep (RFC §10.6.8), which
/// the Python implementation's kit takes no keyword for: a test seam. The
/// RFC's defaults unless set: 10 seconds to accept a turn, a heartbeat each
/// way every 2 seconds, 10 seconds of silence before a side is taken for
/// lost, 5 seconds for a cancelled turn to end. A turn already submitted
/// keeps the bounds it started with.
#[doc(hidden)]
pub fn set_turn_timeouts<S>(kit: &RoomKit<S>, timeouts: TurnTimeouts) {
    kit.inner.turns.set_timeouts(timeouts.bounded());
}

/// A delivery a [`RecordingTransport`] made.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivery {
    /// The event delivered, as transcoded for the transport.
    pub event: RoomEvent,
    /// Its text, as a person on the transport would read it.
    pub text: String,
}

/// What [`RecordingTransport::new`] takes beside the channel's id and
/// type, Python's `RecordingTransport` keywords.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordingTransportOptions {
    /// Python `capabilities=None`: what the transport declares (RFC §5.8),
    /// a text channel's when `None`. One that edits and deletes natively
    /// receives an edit or a delete as it is (RFC §6.6).
    pub capabilities: Option<ChannelCapabilities>,
}

/// A text transport that records every event delivered to it: Python's
/// `roomkit.testing.RecordingTransport(channel_id, channel_type, *,
/// capabilities=None)`.
///
/// Cloning it shares the record, so a test keeps a clone to read what the
/// registered one delivered. Its destination is its record, so it needs no
/// recipient: its declaration names no recipient key, whatever its type
/// (RFC §10.2 step 3d). It addresses its senders as its type does: an SMS
/// recording transport writes them in E.164 (RFC §5.7).
#[derive(Clone, Debug)]
pub struct RecordingTransport {
    declaration: ChannelDeclaration,
    deliveries: Arc<Mutex<Vec<Delivery>>>,
}

impl RecordingTransport {
    /// A bidirectional text transport of `channel_type` named `channel_id`,
    /// declaring the capabilities `options` names.
    pub fn new(
        channel_id: ChannelId,
        channel_type: ChannelType,
        options: RecordingTransportOptions,
    ) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel_id,
            channel_type,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.provider_name = Some("RecordingTransport".to_owned());
        declaration.recipient_key = None;
        if let Some(capabilities) = options.capabilities {
            declaration.capabilities = capabilities;
        }
        Self {
            declaration,
            deliveries: Arc::default(),
        }
    }

    /// The channel's id, Python's `channel_id`.
    pub fn channel_id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// What the transport delivered so far, in order.
    pub fn deliveries(&self) -> Vec<Delivery> {
        self.deliveries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The texts the transport delivered so far, in order.
    pub fn texts(&self) -> Vec<String> {
        self.deliveries()
            .into_iter()
            .map(|delivery| delivery.text)
            .collect()
    }
}

impl Channel for RecordingTransport {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let text = text_of(&event.content);
        self.deliveries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Delivery { event, text });
        ready(Ok(ChannelOutput::default()))
    }
}

/// What [`EchoAgent::new`] takes beside the channel's id and its reply,
/// Python's `EchoAgent` keywords.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EchoAgentOptions {
    /// Python `quote=False`: the reply is followed by the text of the event
    /// it answers.
    pub quote: bool,
}

/// An agent that answers every event it is asked with `reply`, followed by
/// the event's text when `quote` is set: Python's
/// `roomkit.testing.EchoAgent(channel_id, reply, *, quote=False)`.
#[derive(Clone, Debug)]
pub struct EchoAgent {
    declaration: ChannelDeclaration,
    reply: String,
    quote: bool,
}

impl EchoAgent {
    /// An agent named `channel_id` answering `reply`, quoting the event it
    /// answers after it when `options` says so.
    pub fn new(channel_id: ChannelId, reply: impl Into<String>, options: EchoAgentOptions) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        declaration.provider_name = Some("EchoAgent".to_owned());
        Self {
            declaration,
            reply: reply.into(),
            quote: options.quote,
        }
    }

    /// The channel's id, Python's `channel_id`.
    pub fn channel_id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// The agent's answer to `event`.
    fn reply_to(&self, event: &RoomEvent) -> RoomEvent {
        let text = if self.quote {
            format!("{}{}", self.reply, text_of(&event.content))
        } else {
            self.reply.clone()
        };
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.declaration.channel_id.clone(), ChannelType::AI)
        };
        // The pipeline gives every response a fresh id, its room and its
        // depth (gap G-024), and makes it answer the event that asked: the id
        // written here is a placeholder.
        RoomEvent::at(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            source,
            TextContent::new(text),
            event.created_at,
        )
    }
}

impl Channel for EchoAgent {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput {
            responded: true,
            response_events: vec![self.reply_to(&event)],
            ..ChannelOutput::empty()
        }))
    }
}

/// A tap on a kit's turn backend (RFC §10.6.3): every message the turns
/// submitted after it was set carry, as each is serialized, in the order
/// they cross. Cloning it shares the record.
///
/// The messages are the backend's own: a test or a tool reads them to see
/// a turn cross, never to build on. A request is written whole for the
/// tap, its events included, which the in-memory backend hands over as
/// they are (RFC §10.6.1).
///
/// It keeps every message, unbounded: set it for a test, not for a kit's
/// life.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct TurnTap {
    messages: Arc<Mutex<Vec<TappedMessage>>>,
}

/// One message a [`TurnTap`] saw.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TappedMessage {
    /// Its family: `request`, `frame` (executor to owner), `control`
    /// (owner to executor), `channel_control`, `room_call` (a host verb
    /// after its turn) or `room_call_result`.
    pub family: &'static str,
    /// The message as it crossed, in the serialized form of RFC §1.4: a
    /// frame or a control message in its numbered envelope.
    pub json: String,
}

impl TurnTap {
    /// Taps `kit`'s turn backend, for the turns submitted from now on, in
    /// place of any tap set before.
    pub fn on<S: ConversationStore>(kit: &RoomKit<S>) -> Self {
        let tap = Self::default();
        let record = Arc::clone(&tap.messages);
        kit.inner
            .turns
            .set_tap(Arc::new(move |carried: Carried, json: &str| {
                record
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(TappedMessage {
                        family: carried.name(),
                        json: json.to_owned(),
                    });
            }));
        tap
    }

    /// The messages seen so far, in order.
    pub fn messages(&self) -> Vec<TappedMessage> {
        self.messages
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
