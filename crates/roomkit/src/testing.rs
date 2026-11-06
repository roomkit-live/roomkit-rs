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
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventDirection, EventSource, EventType, InboundMessage, ParticipantId,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_core::rules::addresses::{NormalizeOptions, addressing_of};
use roomkit_core::rules::transcoding::text_of;

pub use roomkit_core::ops::{Environment, SequentialEnvironment};

use crate::channel::{Channel, ChannelError};
use crate::store::ConversationStore;
use crate::turns::{Carried, TurnTimeouts};
use crate::{FrameworkEvent, RoomKit, RoomKitOptions};

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
/// waits for itself: a test awaits it once its calls returned. Each
/// discussion's driver looks at its stored queue first, which another
/// process sharing the store may have changed, and its turns are waited
/// for.
#[doc(hidden)]
pub async fn idle<S>(kit: &RoomKit<S>) {
    // A discussion's driver looks first at what another process may have
    // stored meanwhile (RFC §19.7.5 rule 16).
    kit.inner.discussions.rouse_all();
    loop {
        kit.inner.tasks.idle().await;
        // A discussion's driver is no task of the kit's: at rest it looks at
        // its queue once a second, for ever.
        if kit.inner.discussions.settled() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
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

/// How many calls wait for the lock of the room `room_id` in `kit` now: the
/// conformance runner's `wait_for_lock` step, which knows calls started in
/// the background meet only once they wait for it. A test seam: the Python
/// implementation's kit has no such call.
#[doc(hidden)]
pub fn room_lock_waiters<S>(kit: &RoomKit<S>, room_id: &RoomId) -> usize {
    kit.inner.locks.waiting(room_id)
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
    channel_id: ChannelId,
    channel_type: ChannelType,
    capabilities: Option<ChannelCapabilities>,
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
        Self {
            channel_id,
            channel_type,
            capabilities: options.capabilities,
            deliveries: Arc::default(),
        }
    }

    /// The channel's id, Python's `channel_id`, without the trait in
    /// scope.
    pub fn channel_id(&self) -> &ChannelId {
        &self.channel_id
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
    fn channel_id(&self) -> ChannelId {
        self.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.channel_type.clone()
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities.clone().unwrap_or_default()
    }

    fn provider_name(&self) -> Option<String> {
        Some("RecordingTransport".to_owned())
    }

    /// Its senders written as its type writes them, as Python's:
    /// an SMS recording transport's in E.164 (RFC §5.7).
    fn normalize_address(&self, address: &str) -> String {
        if address.is_empty() {
            return address.to_owned();
        }
        addressing_of(&self.channel_type).normalize(address, NormalizeOptions::default())
    }

    /// The event of `message` in `context`'s room, as Python's builds it:
    /// its type, its content and its idempotency key, from its sender.
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let mut source = EventSource::new(self.channel_id.clone(), self.channel_type.clone());
        source.participant_id = ParticipantId::new(message.sender_id.clone()).ok();
        let mut event = RoomEvent::new(context.room.id.clone(), source, message.content);
        event.r#type = message.event_type;
        event.idempotency_key = message.idempotency_key;
        ready(Ok(event))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let text = recorded_text(&event.content);
        self.deliveries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Delivery { event, text });
        ready(Ok(ChannelOutput::default()))
    }
}

/// The text a recording transport keeps of `content`, as Python's
/// `content_text(content) or ""` reads it: a text's, a rich content's, a
/// media's label, a composite's parts; none of a location, a notice, a
/// template, an edit, a delete or a tool call.
fn recorded_text(content: &EventContent) -> String {
    match content {
        EventContent::Text(_)
        | EventContent::Rich(_)
        | EventContent::Media(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::Composite(_) => text_of(content),
        EventContent::Location(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_)
        | _ => String::new(),
    }
}

/// What [`EchoAgent::new`] takes beside the channel's id and its reply,
/// Python's `EchoAgent` keywords.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
    channel_id: ChannelId,
    reply: String,
    quote: bool,
}

impl EchoAgent {
    /// An agent named `channel_id` answering `reply`, quoting the event it
    /// answers after it when `options` says so.
    pub fn new(channel_id: ChannelId, reply: impl Into<String>, options: EchoAgentOptions) -> Self {
        Self {
            channel_id,
            reply: reply.into(),
            quote: options.quote,
        }
    }

    /// The channel's id, Python's `channel_id`, without the trait in
    /// scope.
    pub fn channel_id(&self) -> &ChannelId {
        &self.channel_id
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
            ..EventSource::new(self.channel_id.clone(), ChannelType::AI)
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
    fn channel_id(&self) -> ChannelId {
        self.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn provider_name(&self) -> Option<String> {
        Some("EchoAgent".to_owned())
    }

    /// Nothing sends an echo agent a message: it answers in its room.
    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err(
            "An EchoAgent answers in its room; nothing sends it a message".into(),
        ))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
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

/// The framework events of the given types a kit emits from the moment it
/// is set, in the order they are emitted (RFC §8.2), through
/// [`RoomKit::on`]: for a test, which reads them once the call returned,
/// or once [`idle`] did for the kit's background work. Cloning it shares
/// the record.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct EventTap {
    events: Arc<tokio::sync::watch::Sender<Vec<FrameworkEvent>>>,
}

impl EventTap {
    /// Taps `kit`'s framework events of `types`.
    pub fn on<S: ConversationStore>(kit: &RoomKit<S>, types: &[&str]) -> Self {
        let tap = Self::default();
        for event_type in types {
            let record = Arc::clone(&tap.events);
            // A closed kit refuses the tap, and emits nothing it could note.
            let tapped = kit.on(*event_type, move |event| {
                record.send_modify(|events| events.push(event));
                ready(())
            });
            if tapped.is_err() {
                break;
            }
        }
        tap
    }

    /// The events tapped so far.
    pub fn events(&self) -> Vec<FrameworkEvent> {
        self.events.borrow().clone()
    }

    /// The types of the events tapped so far.
    pub fn types(&self) -> Vec<String> {
        self.events()
            .into_iter()
            .map(|event| event.r#type)
            .collect()
    }

    /// The first event tapped that `wanted` admits, once there is one: a
    /// test bounds the wait with its own timeout.
    pub async fn wait_for(&self, wanted: impl Fn(&FrameworkEvent) -> bool) -> FrameworkEvent {
        let mut changes = self.events.subscribe();
        loop {
            let found = changes
                .borrow_and_update()
                .iter()
                .find(|event| wanted(event))
                .cloned();
            if let Some(event) = found {
                return event;
            }
            // The tap holds the sender, so a change always may come.
            if changes.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
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
