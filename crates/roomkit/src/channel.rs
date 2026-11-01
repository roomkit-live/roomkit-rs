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
use roomkit_core::rules::addresses::{
    Addressing, ChannelAddressing, ChannelAddressingOptions, CountryCode,
};

use crate::stream::{DeliveryStream, StreamItem};

/// What a channel's call failed with. The pipeline records it as the error's
/// message; it never reaches a model or another channel.
pub type ChannelError = Box<dyn Error + Send + Sync>;

/// What the room's owner knows of a channel without its object (RFC
/// §10.6.2): Python's `ChannelDeclaration(*, channel_id, channel_type=AI,
/// category=INTELLIGENCE, capabilities=<an AI channel's>,
/// recent_events_window=50)`, its kind, what it can do and the history it
/// reads. A field left out reads as an AI channel built with its defaults.
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
    /// How many of the room's latest events its turn reads: 50 by default.
    pub recent_events_window: usize,
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
            recent_events_window: 50,
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
    fn normalize_address(&self, address: &str) -> String {
        address.to_owned()
    }

    /// The binding metadata key the channel delivers to (RFC §5.7,
    /// Appendix A): none by default, a channel that delivers to a
    /// destination of its own.
    fn recipient_key(&self) -> Option<String> {
        None
    }

    /// The recipient `binding` delivers to, in the channel's form: none by
    /// default.
    fn recipient_address(&self, _binding: &ChannelBinding) -> Option<String> {
        None
    }

    /// The binding metadata naming `address` as the channel's recipient:
    /// none by default.
    fn recipient_metadata(&self, _address: &str) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// The conversation `message` belongs to, which routing addresses it by
    /// (RFC §10.4): its sender by default.
    fn conversation_address(&self, message: &InboundMessage) -> String {
        message.sender_id.clone()
    }

    /// The binding metadata a reply to `message` goes by: none by default.
    fn reply_metadata(&self, _message: &InboundMessage) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// How many of the room's latest events the channel's turn reads: none
    /// by default.
    fn recent_events_window(&self) -> usize {
        0
    }

    /// Whether the channel, a transport, takes a streamed response as it is
    /// written ([`deliver_stream`](Channel::deliver_stream), RFC §6.1):
    /// `false` by default.
    fn supports_streaming_delivery(&self) -> bool {
        false
    }

    /// Whether it takes one in `room_id`: as
    /// [`supports_streaming_delivery`](Channel::supports_streaming_delivery)
    /// says by default.
    fn supports_streaming_delivery_for(&self, _room_id: &RoomId) -> bool {
        self.supports_streaming_delivery()
    }

    /// How the channel's addresses are written, and the country code its
    /// phone numbers written without one take (RFC §5.7), as the core
    /// compares the addresses its bindings name: as written by default,
    /// with no chat and the channel's own recipient key. A transport
    /// channel gives its declaration.
    ///
    /// For the workspace's channels, which carry an addressing
    /// declaration: outside the public API.
    #[doc(hidden)]
    fn address_config(&self) -> (ChannelAddressing, Option<CountryCode>) {
        let addressing = ChannelAddressing::new(
            Addressing::AsWritten,
            ChannelAddressingOptions {
                recipient_key: self.recipient_key(),
                ..ChannelAddressingOptions::default()
            },
        );
        (addressing, None)
    }

    /// What the room's owner knows of the channel (RFC §10.6.2): its id,
    /// type, category, capabilities and history window.
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration {
            channel_id: self.channel_id(),
            channel_type: self.channel_type(),
            category: self.category(),
            capabilities: self.capabilities(),
            recent_events_window: self.recent_events_window(),
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

    /// Pushes an event to the channel's external recipient (RFC §6.1).
    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send;

    /// Delivers a response as it is written (RFC §6.1 `deliver_stream`), for
    /// a channel that
    /// [`supports_streaming_delivery`](Channel::supports_streaming_delivery):
    /// `text_stream` yields the response's text as it comes and each row
    /// once the store holds it, with its id and its index; the channel is
    /// never delivered those rows again. `event` is the empty event the
    /// response renders under: its source the agent, answering the event the
    /// response answers. The channel sets the pace: returning before the
    /// stream's end, or dropping it, stops the response, the text it was
    /// handed stored marked `cancelled` (RFC §12.2 step 13s). The text is
    /// shown before `BEFORE_BROADCAST` judges the row it ends in.
    ///
    /// The kit's close drops this future, as the Python implementation
    /// cancels its stream readers: the close cuts every turn as it starts.
    /// The future is polled once more first, so that it takes what it was
    /// handed, then dropped where it waits, with no async cleanup: a target
    /// that must release what it holds does so in a `Drop`.
    ///
    /// The default is Python's: it reads the text to the stream's end, then
    /// makes one [`deliver`](Channel::deliver) call with `event` holding the
    /// text joined; a response that fails delivers the text it read, if
    /// any, before the failure is returned.
    fn deliver_stream(
        &self,
        text_stream: DeliveryStream,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        async move {
            let (text, failure) = buffered(text_stream).await;
            let Some(failure) = failure else {
                return self.deliver(with_text(event, text), binding, context).await;
            };
            if !text.is_empty() {
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
async fn buffered(mut stream: DeliveryStream) -> (String, Option<ChannelError>) {
    let mut text = String::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(StreamItem::Text(chunk)) => text.push_str(&chunk),
            Ok(StreamItem::Row(_)) => {}
            Err(failure) => return (text, Some(failure)),
        }
    }
    (text, None)
}

/// `event` holding `text`, as Python's `_buffered` writes it.
fn with_text(mut event: RoomEvent, text: String) -> RoomEvent {
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
}

impl<C: Channel> DynChannel for C {
    fn normalize_address(&self, address: &str) -> String {
        Channel::normalize_address(self, address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        Channel::recipient_address(self, binding)
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
    let (addressing, country_code) = channel.address_config();
    config.addressing = addressing.addressing;
    config.chat_key = addressing.chat_key;
    config.country_code = country_code;
    config
}
