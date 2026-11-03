//! A transport channel driven by its configuration (RFC §6.1, §5.7):
//! Python's `roomkit.TransportChannel`.

use std::collections::BTreeMap;
use std::future::{Future, ready};

use roomkit_core::model::{
    ChannelBinding, ChannelCapabilities, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventSource, InboundMessage, Metadata, ParticipantId, RoomContext, RoomEvent,
};
use roomkit_core::rules::addresses::{
    ChannelAddressing, CountryCode, NormalizeOptions, addressing_of, written_recipient,
};
use serde_json::Value;

use crate::channel::{Channel, ChannelError};
use crate::telemetry::{
    Attr, KitTelemetry, OpenSpan, SpanKind, StartSpanOptions, current_records_sensitive_data,
    current_telemetry,
};

/// A transport channel of any type, driven by configuration rather than a
/// type of its own: Python's `TransportChannel(channel_id, channel_type, *,
/// provider=None, capabilities=None, addressing=None, country_code=None,
/// defaults=None, requires_recipient=True)`. It turns what it receives into
/// events with the message's provider data, and writes and compares its
/// addresses as its addressing declaration says (RFC §5.7): its type's
/// unless it names another.
///
/// It has no provider in 0.x: [`deliver`](Channel::deliver) delivers
/// nothing, as Python's does without one. A channel that delivers somewhere
/// is a [`Channel`] of its own.
///
/// ```
/// use roomkit::model::{ChannelId, ChannelType};
/// use roomkit::{Channel, CountryCode, TransportChannel, TransportChannelOptions};
///
/// // An SMS number whose numbers written without a country code are French.
/// let sms = TransportChannel::new(
///     ChannelId::new("sms")?,
///     ChannelType::SMS,
///     TransportChannelOptions {
///         country_code: Some(CountryCode::new("+33")?),
///         ..Default::default()
///     },
/// );
/// assert_eq!(sms.normalize_address("06 12 34 56 78"), "+33612345678");
/// assert_eq!(sms.recipient_key().as_deref(), Some("phone_number"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug)]
pub struct TransportChannel {
    channel_id: ChannelId,
    channel_type: ChannelType,
    capabilities: ChannelCapabilities,
    addressing: ChannelAddressing,
    country_code: Option<CountryCode>,
    defaults: Metadata,
}

/// What [`TransportChannel::new`] takes beside its id and its type:
/// Python's keywords, with their defaults.
#[derive(Clone, Debug, PartialEq)]
pub struct TransportChannelOptions {
    /// What the channel supports: text alone by default.
    pub capabilities: Option<ChannelCapabilities>,
    /// How its addresses are written, its recipient key, its chat key and
    /// whether replies go to the sender: its type's by default
    /// ([`addressing_of`]).
    pub addressing: Option<ChannelAddressing>,
    /// On a channel addressed by phone number, the country code a number
    /// written without one takes; without it such a number is kept as
    /// written.
    pub country_code: Option<CountryCode>,
    /// The default values a provider's send takes, which the channel's
    /// [`info`](Channel::info) shows when set.
    pub defaults: Option<Metadata>,
    /// Whether a delivery needs a recipient in the binding's metadata:
    /// `true` by default. With no provider in 0.x, nothing reads it.
    pub requires_recipient: bool,
}

impl Default for TransportChannelOptions {
    fn default() -> Self {
        Self {
            capabilities: None,
            addressing: None,
            country_code: None,
            defaults: None,
            requires_recipient: true,
        }
    }
}

impl TransportChannel {
    /// The transport `channel_id` of `channel_type`, as `options` say.
    pub fn new(
        channel_id: ChannelId,
        channel_type: ChannelType,
        options: TransportChannelOptions,
    ) -> Self {
        let addressing = options
            .addressing
            .unwrap_or_else(|| addressing_of(&channel_type));
        Self {
            channel_id,
            channel_type,
            capabilities: options.capabilities.unwrap_or_default(),
            addressing,
            country_code: options.country_code,
            defaults: options.defaults.unwrap_or_default(),
        }
    }

    /// The channel's addressing declaration (RFC §5.7), Python's
    /// `addressing` property.
    pub fn addressing(&self) -> &ChannelAddressing {
        &self.addressing
    }

    /// The chat `message` was posted in, when the channel answers chats.
    fn chat_of(&self, message: &InboundMessage) -> Option<String> {
        let key = self.addressing.chat_key.as_deref()?;
        written_recipient(&message.metadata, key)
    }
}

impl Channel for TransportChannel {
    fn channel_id(&self) -> ChannelId {
        self.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.channel_type.clone()
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities.clone()
    }

    /// The defaults that are set.
    fn info(&self) -> Metadata {
        self.defaults
            .iter()
            .filter(|(_, value)| !value.is_null())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn normalize_address(&self, address: &str) -> String {
        if address.is_empty() {
            return address.to_owned();
        }
        let options = NormalizeOptions {
            country_code: self.country_code.clone(),
        };
        self.addressing.normalize(address, options)
    }

    /// The addressing's recipient key, when a binding's recipient is a
    /// conversation's address.
    fn recipient_key(&self) -> Option<String> {
        self.addressing
            .names_conversations()
            .then(|| self.addressing.recipient_key.clone())
            .flatten()
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        let key = self.recipient_key()?;
        let recipient = written_recipient(&binding.metadata, &key)?;
        Some(self.normalize_address(&recipient))
    }

    /// `{recipient_key: address}`, when the recipient is a conversation's
    /// address.
    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        match self.recipient_key() {
            Some(key) if !address.is_empty() => BTreeMap::from([(key, address.to_owned())]),
            _ => BTreeMap::new(),
        }
    }

    /// The chat the message carries, else its sender; no address on a
    /// channel whose replies go to a chat only (RFC §5.7).
    fn conversation_address(&self, message: &InboundMessage) -> String {
        if let Some(chat) = self.chat_of(message) {
            return chat;
        }
        if self.addressing.chat_key.is_some() && !self.addressing.replies_to_sender {
            return String::new();
        }
        message.sender_id.clone()
    }

    /// The chat the message carries, then its sender when replies go to
    /// the sender, so that a room replies where it is found.
    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        if let Some(chat) = self.chat_of(message) {
            return self.recipient_metadata(&chat);
        }
        if self.addressing.replies_to_sender {
            return self.recipient_metadata(&message.sender_id);
        }
        BTreeMap::new()
    }

    fn address_config(&self) -> (ChannelAddressing, Option<CountryCode>) {
        (self.addressing.clone(), self.country_code.clone())
    }

    /// The event of `message` in `context`'s room, as Python's builds it:
    /// its type, its content, its sender, its provider data, its
    /// idempotency key and its metadata; an SMS message with media is
    /// sourced as `MMS`.
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let span = self.handle_inbound_span(&context);
        let channel_type = if self.channel_type == ChannelType::SMS && has_media(&message.content) {
            ChannelType::MMS
        } else {
            self.channel_type.clone()
        };
        let provider = self.provider_name();
        let mut source = EventSource::new(self.channel_id.clone(), channel_type);
        source.participant_id = ParticipantId::new(message.sender_id.clone()).ok();
        source.external_id.clone_from(&message.external_id);
        source.provider.clone_from(&provider);
        source.raw_payload = message.raw_payload;
        source.provider_message_id = message.provider_message_id;
        let mut event = RoomEvent::new(context.room.id.clone(), source, message.content);
        event.r#type = message.event_type;
        event.channel_data = channel_data(provider, message.external_id, message.thread_id);
        event.idempotency_key = message.idempotency_key;
        event.metadata = message.metadata;
        if let Some(span) = span {
            span.end(Metadata::new());
        }
        ready(Ok(event))
    }

    /// No provider in 0.x: the event is delivered nowhere, as Python's
    /// transport delivers it without one.
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        tracing::debug!(
            target: "roomkit::channels",
            channel_id = %self.channel_id,
            "no provider configured: the delivery is skipped"
        );
        ready(Ok(ChannelOutput::empty()))
    }
}

impl TransportChannel {
    /// The span of a message's parse, Python's `channel.handle_inbound`,
    /// under the kit whose call this is.
    fn handle_inbound_span(&self, context: &RoomContext) -> Option<OpenSpan> {
        let telemetry = KitTelemetry {
            provider: current_telemetry()?,
            sensitive: current_records_sensitive_data(),
        };
        let channel_id = self.channel_id.to_string();
        let mut attributes = Metadata::new();
        attributes.insert(
            Attr::CHANNEL_ID.to_owned(),
            Value::from(channel_id.as_str()),
        );
        let channel_type = serde_json::to_value(&self.channel_type).unwrap_or_default();
        attributes.insert("channel_type".to_owned(), channel_type);
        Some(OpenSpan::start(
            &telemetry,
            SpanKind::InboundPipeline,
            "channel.handle_inbound",
            StartSpanOptions {
                room_id: Some(context.room.id.to_string()),
                channel_id: Some(channel_id),
                attributes,
                ..StartSpanOptions::default()
            },
        ))
    }
}

/// Whether `content` holds a media, itself or as a part.
fn has_media(content: &EventContent) -> bool {
    if let EventContent::Composite(composite) = content {
        return composite
            .parts()
            .iter()
            .any(|part| matches!(part, EventContent::Media(_)));
    }
    matches!(content, EventContent::Media(_))
}

/// The channel data of an event, Python's `ChannelData(provider,
/// external_id, thread_id)`, each field set when it is.
fn channel_data(
    provider: Option<String>,
    external_id: Option<String>,
    thread_id: Option<String>,
) -> Metadata {
    [
        ("provider", provider),
        ("external_id", external_id),
        ("thread_id", thread_id),
    ]
    .into_iter()
    .filter_map(|(key, value)| Some((key.to_owned(), Value::String(value?))))
    .collect()
}
