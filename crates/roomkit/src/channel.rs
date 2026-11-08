//! Channels: everything that interacts with a room (RFC §4.2, §6.1).

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use roomkit_core::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, EventContent, EventSource, InboundMessage,
    Metadata, ParticipantId, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::addresses::Addressing;
use roomkit_core::rules::transcript::is_blank;

use crate::steering::SteeringDirective;
use crate::stream::{DeliveryStream, StreamItem};

/// What a channel's call failed with. The pipeline records it as the error's
/// message; it never reaches a model or another channel.
pub type ChannelError = Box<dyn Error + Send + Sync>;

/// What the room's owner knows of a channel without its object (RFC
/// §10.6.2): Python's `ChannelDeclaration(*, channel_id, channel_type=AI,
/// category=INTELLIGENCE, capabilities=<an AI channel's>,
/// recent_events_window=50, serializes_room_turns=False,
/// steers_room_turns=False)`, its kind, what it can do, the history it
/// reads and how it takes a room's turns. A field left out reads as an AI
/// channel built with its defaults.
///
/// ```
/// use roomkit::ChannelDeclaration;
/// use roomkit::model::{ChannelCategory, ChannelId};
///
/// let declaration = ChannelDeclaration::new(ChannelId::new("ai")?);
/// assert_eq!(declaration.category, ChannelCategory::Intelligence);
/// assert_eq!(declaration.recent_events_window, 50);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelDeclaration {
    /// The channel's id.
    pub channel_id: ChannelId,
    /// Its type: `ai` by default.
    pub channel_type: ChannelType,
    /// What it does: `intelligence` by default.
    pub category: ChannelCategory,
    /// What it can do: an AI channel's by default, text, rich content and
    /// media.
    pub capabilities: ChannelCapabilities,
    /// Whether its turns declare the tools the room's owner serves (RFC
    /// §10.6.5): `true` by default; a channel's own declaration says
    /// `false`, an AI channel's `true`.
    pub takes_owner_tools: bool,
    /// How many of the room's latest events its turn reads: 50 by default.
    pub recent_events_window: usize,
    /// Whether its turns in a room run one after the other, in the order
    /// they were asked, which the room's owner keeps (RFC §6.4): `false` by
    /// default.
    pub serializes_room_turns: bool,
    /// Whether a message for a room where its turn runs is handed to that
    /// turn, read at its next stop, rather than waiting a turn of its own
    /// (RFC §6.4, §21.3): `false` by default. It implies
    /// `serializes_room_turns`.
    pub steers_room_turns: bool,
    /// Who the agent is, for an agent that says so (Python's `Agent`):
    /// `None` by default.
    pub identity: Option<AgentIdentity>,
}

/// Who an agent is (Python's `roomkit.AgentIdentity`): what its
/// declaration tells the kit and the strategies of the rooms it serves.
/// Every field is unset by default, but `auto_greet`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentIdentity {
    /// Its display name, distinct from its channel id and its role.
    pub name: Option<String>,
    /// What it does, in a line: the identity block's `Role:`.
    pub role: Option<String>,
    /// A short label of what it does: the block's `Description:`.
    pub description: Option<String>,
    /// What it answers and what it leaves: the block's `Scope:`.
    pub scope: Option<String>,
    /// What it says first, once a session with it is ready.
    pub greeting: Option<String>,
    /// The language it answers in: the block's `Language:`.
    pub language: Option<String>,
    /// Its own instructions in concise form, for an orchestrator to frame
    /// its tasks with.
    pub purpose: Option<String>,
    /// The voice it speaks with.
    pub voice: Option<String>,
    /// Whether it greets a session once ready: `true` by default.
    pub auto_greet: bool,
}

impl Default for AgentIdentity {
    fn default() -> Self {
        Self {
            name: None,
            role: None,
            description: None,
            scope: None,
            greeting: None,
            language: None,
            purpose: None,
            voice: None,
            auto_greet: true,
        }
    }
}

impl ChannelDeclaration {
    /// The declaration of the channel `channel_id`, Python's defaults for
    /// the rest.
    pub fn new(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            channel_type: ChannelType::AI,
            category: ChannelCategory::Intelligence,
            capabilities: ai_channel_capabilities(),
            takes_owner_tools: true,
            recent_events_window: 50,
            serializes_room_turns: false,
            steers_room_turns: false,
            identity: None,
        }
    }
}

/// What an AI channel can do (RFC §6.4): text, rich content and media,
/// each media read by the channel itself, as Python's
/// `ai_channel_capabilities()` gives.
pub(crate) fn ai_channel_capabilities() -> ChannelCapabilities {
    ChannelCapabilities {
        media_types: [
            ChannelMediaType::Text,
            ChannelMediaType::Rich,
            ChannelMediaType::Media,
        ]
        .into(),
        supports_rich_text: true,
        supports_media: true,
        ..ChannelCapabilities::default()
    }
}

/// A channel (RFC §6.1): a transport carrying messages to and from people or
/// systems, or an intelligence channel answering events. Python's
/// `roomkit.Channel`: what a Python channel tells by its constructor, its
/// class attributes, its properties and its overridable methods, a Rust
/// channel tells by the methods of the same names, each with Python's
/// default.
///
/// Implement the methods with `async fn`. A channel says its id and its
/// type, how it turns a message it received into an event
/// ([`handle_inbound`](Channel::handle_inbound)) and how it delivers one
/// ([`deliver`](Channel::deliver)), as Python's abstract methods require;
/// an intelligence channel implements [`on_event`](Channel::on_event), each
/// call of which is a turn.
///
/// ```
/// use roomkit::{Channel, ChannelError, default_inbound_event};
/// use roomkit::model::{
///     ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext,
///     RoomEvent,
/// };
///
/// struct Console {
///     channel_id: ChannelId,
/// }
///
/// impl Channel for Console {
///     fn channel_id(&self) -> ChannelId {
///         self.channel_id.clone()
///     }
///
///     fn channel_type(&self) -> ChannelType {
///         ChannelType::WEBSOCKET
///     }
///
///     async fn handle_inbound(
///         &self,
///         message: InboundMessage,
///         context: RoomContext,
///     ) -> Result<RoomEvent, ChannelError> {
///         Ok(default_inbound_event(self, &message, &context))
///     }
///
///     async fn deliver(
///         &self,
///         event: RoomEvent,
///         _binding: ChannelBinding,
///         _context: RoomContext,
///     ) -> Result<ChannelOutput, ChannelError> {
///         let _shown = Self::extract_text(&event);
///         Ok(ChannelOutput::empty())
///     }
/// }
/// ```
pub trait Channel: Send + Sync + 'static {
    /// Python's `channel_id`, set by its constructor.
    fn channel_id(&self) -> ChannelId;

    /// Python's class attribute `channel_type`: the channel's type, an open
    /// name (RFC §6.2).
    fn channel_type(&self) -> ChannelType;

    /// Python's class attribute `category`: what the channel does,
    /// `TRANSPORT` by default.
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    /// Python's class attribute `direction`: what it can do physically,
    /// `BIDIRECTIONAL` by default.
    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    /// What the channel supports (RFC §6.3): text alone by default.
    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities::default()
    }

    /// The provider or backend behind it, for event attribution (RFC
    /// §5.2): none by default.
    fn provider_name(&self) -> Option<String> {
        None
    }

    /// What the channel says of itself, for a host to show: nothing by
    /// default.
    fn info(&self) -> Metadata {
        Metadata::new()
    }

    /// `address` in the channel's form (RFC §5.7), as written by default.
    /// The kit reads every inbound sender through it before routing (RFC
    /// §10.1 step 1), and the correspondent an attach or a binding metadata
    /// update names.
    fn normalize_address(&self, address: &str) -> String {
        address.to_owned()
    }

    /// The binding metadata key the channel delivers to (RFC §5.7,
    /// Appendix A): none by default, a channel that delivers to a
    /// destination of its own. Routing reads a binding's recipient under it
    /// (RFC §10.4), whatever
    /// [`recipient_address`](Channel::recipient_address) answers there when
    /// that answers none.
    fn recipient_key(&self) -> Option<String> {
        None
    }

    /// The recipient `binding` delivers to, in the channel's form: none by
    /// default. It names the binding's conversation as much as its
    /// `participant_id` does: routing admits no one else through it (RFC
    /// §5.7, §10.4). Routing takes this answer first; when it is none, a
    /// value under [`recipient_key`](Channel::recipient_key) still names the
    /// conversation.
    fn recipient_address(&self, _binding: &ChannelBinding) -> Option<String> {
        None
    }

    /// The binding metadata naming `address` as the channel's recipient:
    /// none by default. An attach and a binding metadata update write a
    /// binding's recipient by it.
    fn recipient_metadata(&self, _address: &str) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// The conversation `message` belongs to, which routing addresses it by
    /// (RFC §10.4): its sender by default.
    fn conversation_address(&self, message: &InboundMessage) -> String {
        message.sender_id.clone()
    }

    /// The binding metadata a reply to `message` goes by: none by default.
    /// The kit records it on the binding of a routed sender, where the
    /// binding holds no value (RFC §10.1 step 2).
    fn reply_metadata(&self, _message: &InboundMessage) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// How many of the room's latest events the channel's turn reads: none
    /// by default.
    fn recent_events_window(&self) -> usize {
        0
    }

    /// Python's class attribute `sender_is_participant`: whether the
    /// channel names its own senders, its inbound `sender_id` a room's
    /// `Participant.id` rather than an address, which no identity resolver
    /// is asked about (RFC §11.6). `false` by default: a channel reading
    /// its sender off the wire, or handed it by the integrator, carries an
    /// address.
    fn sender_is_participant(&self) -> bool {
        false
    }

    /// Whether the channel, a transport, takes a streamed response as it is
    /// written ([`deliver_stream`](Channel::deliver_stream), RFC §6.1):
    /// `false` by default.
    fn supports_streaming_delivery(&self) -> bool {
        false
    }

    /// Whether it takes one in `room_id`: as
    /// [`supports_streaming_delivery`](Channel::supports_streaming_delivery)
    /// says by default. The kit asks it of every transport a response's rows
    /// reach, at each stream in the room, whatever
    /// [`supports_streaming_delivery`](Channel::supports_streaming_delivery)
    /// says: a channel may stream once a client connects.
    fn supports_streaming_delivery_for(&self, _room_id: &RoomId) -> bool {
        self.supports_streaming_delivery()
    }

    /// Applies `directive` to the loops of the turn `turn_id` the channel
    /// runs (RFC §10.6.8 `steer`): how many it reached, or `None` for a
    /// channel without steering, whose owner then cuts the turn on a
    /// `Cancel`. `None` by default.
    ///
    /// For the workspace's AI channel, as the Python implementation's
    /// executor reads a channel's own `steer()`: outside the public API.
    #[doc(hidden)]
    fn steer_turn(&self, _directive: &SteeringDirective, _turn_id: &str) -> Option<usize> {
        None
    }

    /// The names of the tools the channel serves itself, which a tool the
    /// kit sets up on it may not take (RFC §21.1): none by default.
    ///
    /// For the workspace's AI channel, as the Python implementation's
    /// owner reads a channel's own tool registry: outside the public API.
    #[doc(hidden)]
    fn own_tool_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Whether the channel's turns in a room run one after the other, in
    /// the order they were asked, each reading the answers before it (RFC
    /// §6.4): by default, whether it steers them
    /// ([`steers_room_turns`](Channel::steers_room_turns)), which implies
    /// it; otherwise a room's turns run side by side.
    fn serializes_room_turns(&self) -> bool {
        self.steers_room_turns()
    }

    /// How many turns the channel is running right now: what a caller
    /// retiring the channel needs to know, since a channel taken out of the
    /// kit may still answer a turn that holds it. Python's `active_turns`;
    /// 0 by default, a channel that counts none.
    fn active_turns(&self) -> usize {
        0
    }

    /// Whether a message for a room where the channel's turn runs is handed
    /// to that turn, which reads it at its next stop (between two rounds of
    /// tool calls, or once it has answered), rather than a turn of its own
    /// (RFC §6.4, §21.3). Such a channel serializes its room turns. `false`
    /// by default.
    fn steers_room_turns(&self) -> bool {
        false
    }

    /// What the room's owner knows of the channel (RFC §10.6.2): its id,
    /// type, category, capabilities, history window and how it takes a
    /// room's turns; it takes no tool the owner serves, as Python's base
    /// channel declares.
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration {
            channel_id: self.channel_id(),
            channel_type: self.channel_type(),
            category: self.category(),
            capabilities: self.capabilities(),
            takes_owner_tools: false,
            recent_events_window: self.recent_events_window(),
            serializes_room_turns: self.serializes_room_turns(),
            steers_room_turns: self.steers_room_turns(),
            identity: None,
        }
    }

    /// The plain text of `event`'s content: a text's body, else empty, as
    /// Python's static `extract_text` reads it.
    fn extract_text(event: &RoomEvent) -> String
    where
        Self: Sized,
    {
        if let EventContent::Text(text) = &event.content {
            text.body.clone()
        } else {
            String::new()
        }
    }

    /// Converts a message the channel received into a room event (RFC
    /// §6.1). The pipeline then gives it its id (RFC §8.1) and its source's
    /// channel, and fills the message's fields the channel left unset: a
    /// field the channel read off its wire stands, its type (but an
    /// instruction's), visibility, address, thread, idempotency key, depth
    /// and instant (RFC §10.1 step 4). [`default_inbound_event`] builds the
    /// event a channel that keeps no data of its own parses.
    ///
    /// A channel that receives no message refuses it here with an error,
    /// before anything enters the room: an AI agent does
    /// (`roomkit_ai::AiChannel`), since an event under its id is read as its
    /// own words.
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send;

    /// Reacts to a room event the channel may read (RFC §6.1). For an
    /// intelligence channel, this is its turn: the events it returns in a
    /// responding output re-enter the room. The default does nothing.
    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        async { Ok(ChannelOutput::empty()) }
    }

    /// Establishes what a new binding claims exists (RFC §6.1
    /// `on_room_attached`): the kit's attach calls it with the binding it
    /// stored, under the room's lock, before the attach is recorded or
    /// announced, its `ON_CHANNEL_ATTACHED` hooks included. An inbound
    /// message's automatic attach calls it too.
    ///
    /// An error refuses the attach: the kit puts back the binding it
    /// replaced, or removes it, and the attach fails with
    /// [`RoomKitError::Channel`](crate::RoomKitError::Channel), what the
    /// channel raised its message. An intelligence channel's runs on its
    /// executor, which logs its error and keeps the binding (RFC §10.6.3).
    ///
    /// The default does nothing.
    fn on_room_attached(
        &self,
        _room_id: RoomId,
        _binding: ChannelBinding,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        async { Ok(()) }
    }

    /// Lets go of what [`on_room_attached`](Channel::on_room_attached)
    /// established, and of everything the channel keeps for the room (RFC
    /// §6.1 `on_room_detached`): the kit's detach calls it once the binding
    /// is removed and the detach recorded, under the room's lock, before its
    /// `ON_CHANNEL_DETACHED` hooks run.
    ///
    /// An error undoes nothing: the detach is announced all the same, then
    /// fails with [`RoomKitError::Channel`](crate::RoomKitError::Channel).
    /// An intelligence channel's runs on its executor, which logs its error
    /// (RFC §10.6.3). A channel no longer registered is not told.
    ///
    /// The default does nothing.
    fn on_room_detached(
        &self,
        _room_id: RoomId,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        async { Ok(()) }
    }

    /// Takes `event`, committed in `room_id`, as a message the channel's
    /// memory is handed without a turn: a discussion's (RFC §19.7.5 rule 3),
    /// through the turn backend, whose executor logs what it raises (RFC
    /// §10.6.3). Python's `on_room_event`.
    ///
    /// The default does nothing.
    fn on_room_event(
        &self,
        _room_id: RoomId,
        _event: RoomEvent,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        async { Ok(()) }
    }

    /// Pushes an event to the channel's external recipient (RFC §6.1).
    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send;

    /// Delivers a response as it is written (RFC §6.1 `deliver_stream`), for
    /// a channel that says it takes one in the room
    /// ([`supports_streaming_delivery_for`](Channel::supports_streaming_delivery_for)):
    /// `text_stream` yields the response's text as it comes, each piece of
    /// the model's reasoning as [`StreamItem::Thinking`], what each generation
    /// used as [`StreamItem::Usage`], each row once the store holds it, with its id and its index, and last how the turn's
    /// loop ended as [`StreamItem::LoopEnd`], before the response's failure when it fails (RFC §6.4); the channel is never
    /// delivered those rows again. `event` is the empty event the
    /// response renders under: its source the agent, answering the event the
    /// response answers. The channel sets the pace: returning before the
    /// stream's end, or dropping it, stops the response, the text it was
    /// handed stored marked `cancelled` (RFC §12.2 step 13s). The text is
    /// shown before `BEFORE_BROADCAST` judges the row it ends in.
    ///
    /// A delivery cancelled, a held task cut or the kit's close (RFC §10.1
    /// "Explicit delivery cancellation", §23.3, §12.10.4) ends the stream of
    /// a channel reading it: what the channel does past that end stands for
    /// Python's `finally`, which a future dropped could not run, and
    /// [`DeliveryStream::cut`] tells that end from the response's own. It runs
    /// to its end, however long: the cancel's caller waits for it within its
    /// own timeout; the kit's close waits for it within its grace, then drops
    /// it. The read does not wait for it to store what the cut turn leaves:
    /// the room's next turn may start meanwhile, a second `deliver_stream` of
    /// this channel in this room, and [`deliver`](Channel::deliver) may be
    /// handed the end of a call meanwhile. An error it then returns is
    /// `ON_ERROR`'s, what the cut stored standing, and its caller's unless
    /// the kit's close or a held task's cut left it to end alone.
    /// A channel past the stream already is dropped where it waits, with no
    /// async cleanup: one that must release what it holds does so in a `Drop`.
    ///
    /// The default is Python's: it reads the text to the stream's end, then
    /// makes one [`deliver`](Channel::deliver) call with `event` holding the
    /// text joined; a response that fails delivers the text it read, if
    /// any, before the failure is returned; a delivery cancelled delivers
    /// nothing.
    fn deliver_stream(
        &self,
        text_stream: DeliveryStream,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        async move {
            let mut text_stream = text_stream;
            let (text, failure) = buffered(&mut text_stream).await;
            if text_stream.cut() {
                return Ok(ChannelOutput::empty());
            }
            let Some(failure) = failure else {
                return self.deliver(with_text(event, text), binding, context).await;
            };
            if !is_blank(&text) {
                self.deliver(with_text(event, text), binding, context)
                    .await?;
            }
            Err(failure)
        }
    }

    /// Releases what the channel holds (RFC §6.1 `close`): the kit's
    /// [`close`](crate::RoomKit::close) calls it once for each channel
    /// registered then, in turn, once the work under way ended or was cut.
    /// A call it makes into the kit is refused, the kit closing. It must
    /// not wait without bound on anything the channel does not own.
    ///
    /// The default releases nothing.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// The handler the channel's tools that ask a person keep their
    /// requests in, which the kit hands its `ON_USER_INPUT_REQUIRED` hooks
    /// when it registers the channel, for the requests made outside its
    /// turns (RFC §9.3): none by default.
    ///
    /// For the workspace's AI channel, as the Python implementation's kit
    /// wires an AI channel's handler in `register_channel`: outside the
    /// public API.
    #[doc(hidden)]
    fn human_input_handler(&self) -> Option<crate::human_input::HumanInputHandler> {
        None
    }

    /// Takes `registration`, the token of the channel's registration on
    /// its [`human_input_handler`](Self::human_input_handler), which the
    /// kit takes once it registered the channel, as Python's
    /// `_wire_ai_channel` registers it: its close names it. Nothing by
    /// default.
    #[doc(hidden)]
    fn human_input_registered(&self, registration: u64) {
        let _ = registration;
    }
}

/// The event `channel`, keeping no data of its own, parses from `message`
/// in `context`'s room, as Python's `super().handle_inbound(message,
/// context)` on a transport builds it: sourced from `channel`, its type and
/// its provider, its sender named on the source as its `participant_id`,
/// with the message's type, content, provider data, idempotency key and
/// metadata. The pipeline gives it its id and fills the message's other
/// fields it leaves unset; what a channel writes over it stands (RFC §10.1
/// step 4).
pub fn default_inbound_event(
    channel: &(impl Channel + ?Sized),
    message: &InboundMessage,
    context: &RoomContext,
) -> RoomEvent {
    let mut source = EventSource::new(channel.channel_id(), channel.channel_type());
    source.provider = channel.provider_name();
    source.participant_id = ParticipantId::new(message.sender_id.clone()).ok();
    source.external_id.clone_from(&message.external_id);
    source.raw_payload.clone_from(&message.raw_payload);
    source
        .provider_message_id
        .clone_from(&message.provider_message_id);
    let mut event = RoomEvent::new(context.room.id.clone(), source, message.content.clone());
    event.r#type = message.event_type;
    event.idempotency_key.clone_from(&message.idempotency_key);
    event.metadata.clone_from(&message.metadata);
    event
}

/// The text of `stream`, read to its end, and the failure it ended on.
async fn buffered(stream: &mut DeliveryStream) -> (String, Option<ChannelError>) {
    let mut text = String::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(StreamItem::Text(chunk)) => text.push_str(&chunk),
            Ok(
                StreamItem::Row(_)
                | StreamItem::Thinking(_)
                | StreamItem::Usage(_)
                | StreamItem::LoopEnd(_),
            ) => {}
            Err(failure) => return (text, Some(failure)),
        }
    }
    (text, None)
}

/// `event` holding `text`, as Python's `_buffered` writes it; none when it
/// is blank, which says nothing, as the room holds no row of it (RFC §6.4).
fn with_text(mut event: RoomEvent, text: String) -> RoomEvent {
    let text = if is_blank(&text) { String::new() } else { text };
    event.content = TextContent::new(text).into();
    event
}

/// A boxed, sendable future, as a channel or a router behind a trait object
/// returns it.
pub(crate) type BoxFuture<'call, T> = Pin<Box<dyn Future<Output = T> + Send + 'call>>;

/// A [`Channel`] behind a trait object, so the kit holds channels of any
/// type: its calls, and the synchronous members a router reads.
pub(crate) trait DynChannel: Send + Sync {
    fn normalize_address(&self, address: &str) -> String;

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String>;

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String>;

    fn conversation_address(&self, message: &InboundMessage) -> String;

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String>;

    fn supports_streaming_delivery_for(&self, room_id: &RoomId) -> bool;

    fn steer_turn(&self, directive: &SteeringDirective, turn_id: &str) -> Option<usize>;

    fn own_tool_names(&self) -> Vec<String>;

    fn declaration(&self) -> ChannelDeclaration;

    fn serializes_room_turns(&self) -> bool;

    fn steers_room_turns(&self) -> bool;

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<RoomEvent, ChannelError>>;

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>>;

    fn on_room_attached(
        &self,
        room_id: RoomId,
        binding: ChannelBinding,
    ) -> BoxFuture<'_, Result<(), ChannelError>>;

    fn on_room_detached(&self, room_id: RoomId) -> BoxFuture<'_, Result<(), ChannelError>>;

    fn on_room_event(
        &self,
        room_id: RoomId,
        event: RoomEvent,
    ) -> BoxFuture<'_, Result<(), ChannelError>>;

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>>;

    fn deliver_stream(
        &self,
        text_stream: DeliveryStream,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>>;

    fn close(&self) -> BoxFuture<'_, ()>;

    fn human_input_registered(&self, registration: u64);
}

impl<C: Channel> DynChannel for C {
    fn normalize_address(&self, address: &str) -> String {
        Channel::normalize_address(self, address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        Channel::recipient_address(self, binding)
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        Channel::recipient_metadata(self, address)
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        Channel::conversation_address(self, message)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        Channel::reply_metadata(self, message)
    }

    fn supports_streaming_delivery_for(&self, room_id: &RoomId) -> bool {
        Channel::supports_streaming_delivery_for(self, room_id)
    }

    fn steer_turn(&self, directive: &SteeringDirective, turn_id: &str) -> Option<usize> {
        Channel::steer_turn(self, directive, turn_id)
    }

    fn own_tool_names(&self) -> Vec<String> {
        Channel::own_tool_names(self)
    }

    fn declaration(&self) -> ChannelDeclaration {
        Channel::declaration(self)
    }

    fn serializes_room_turns(&self) -> bool {
        Channel::serializes_room_turns(self)
    }

    fn steers_room_turns(&self) -> bool {
        Channel::steers_room_turns(self)
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<RoomEvent, ChannelError>> {
        Box::pin(Channel::handle_inbound(self, message, context))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>> {
        Box::pin(Channel::on_event(self, event, binding, context))
    }

    fn on_room_attached(
        &self,
        room_id: RoomId,
        binding: ChannelBinding,
    ) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(Channel::on_room_attached(self, room_id, binding))
    }

    fn on_room_detached(&self, room_id: RoomId) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(Channel::on_room_detached(self, room_id))
    }

    fn on_room_event(
        &self,
        room_id: RoomId,
        event: RoomEvent,
    ) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(Channel::on_room_event(self, room_id, event))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>> {
        Box::pin(Channel::deliver(self, event, binding, context))
    }

    fn deliver_stream(
        &self,
        text_stream: DeliveryStream,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>> {
        Box::pin(Channel::deliver_stream(
            self,
            text_stream,
            event,
            binding,
            context,
        ))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(Channel::close(self))
    }

    fn human_input_registered(&self, registration: u64) {
        Channel::human_input_registered(self, registration);
    }
}

/// The configuration the core keeps of `channel`, read from its methods as
/// it is registered (RFC §10.6.2).
pub(crate) fn config_of<C: Channel>(channel: &C) -> ChannelConfig {
    let mut config = ChannelConfig::new(
        channel.channel_id(),
        channel.channel_type(),
        channel.category(),
        channel.direction(),
    );
    config.capabilities = channel.capabilities();
    config.provider_name = channel.provider_name();
    config.recipient_key = channel.recipient_key();
    config.supports_streaming_delivery = channel.supports_streaming_delivery();
    config.recent_events_window = channel.recent_events_window();
    config.sender_is_participant = channel.sender_is_participant();
    // The channel's own methods write and read its addresses, and the core
    // compares what they wrote as written, as Python's framework does (RFC
    // §5.7, RMK-1129).
    config.addressing = Addressing::AsWritten;
    config.chat_key = None;
    config.country_code = None;
    config
}

#[cfg(test)]
mod blank_tests {
    use roomkit_core::model::{ChannelId, ChannelType, EventSource, RoomId};

    use super::*;

    // Python: tests/test_streamed_response_failure.py::test_a_buffering_channel_delivers_no_text_for_a_blank_stream
    #[test]
    fn a_buffering_channel_delivers_no_text_for_a_blank_stream() {
        let event = RoomEvent::new(
            RoomId::new("room").unwrap(),
            EventSource::new(ChannelId::new("ai").unwrap(), ChannelType::AI),
            TextContent::new(""),
        );
        let blank = with_text(event.clone(), "\n\n".to_owned());
        assert_eq!(blank.content, TextContent::new("").into());
        let said = with_text(event, "Hi\n".to_owned());
        assert_eq!(said.content, TextContent::new("Hi\n").into());
    }
}
