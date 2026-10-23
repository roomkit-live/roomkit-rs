//! A message arriving from outside the framework (RFC §5.12).

use serde::{Deserialize, Serialize};

use super::{ChannelId, EventContent, EventId, EventType, Metadata, Visibility};

/// The normalized form of a message arriving from outside (RFC §5.12).
///
/// The room, when the caller knows it, is passed beside the message rather
/// than in it. The RFC's `session` field, which connects a voice session, is
/// not modeled until the voice step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InboundMessage {
    /// The registered channel the message arrived on.
    pub channel_id: ChannelId,
    /// The sender's address: a phone number, an email address, a user id.
    /// The pipeline writes it in its channel's form before anything reads it
    /// (RFC §5.7, §10.1 step 1).
    pub sender_id: String,
    /// The parsed content.
    pub content: EventContent,
    /// The event type: `message`, `system` for a session start, or
    /// `instruction` (RFC §10.1.1).
    #[serde(default)]
    pub event_type: EventType,
    /// The provider's reference.
    #[serde(default)]
    pub external_id: Option<String>,
    /// The provider's native thread reference, passed through to it.
    #[serde(default)]
    pub thread_id: Option<String>,
    /// The in-app thread: the event this message replies to.
    #[serde(default)]
    pub parent_event_id: Option<EventId>,
    /// The key that prevents a duplicate (RFC §13.4).
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// The provider's original payload.
    #[serde(default)]
    pub raw_payload: Metadata,
    /// The provider's message id.
    #[serde(default)]
    pub provider_message_id: Option<String>,
    /// Extra data.
    #[serde(default)]
    pub metadata: Metadata,
    /// Who may see the event (RFC §7.3).
    #[serde(default)]
    pub visibility: Visibility,
    /// The intelligence channels asked to act; `None` for any eligible one
    /// (RFC §19.3).
    #[serde(default)]
    pub addressed_to: Option<Vec<ChannelId>>,
    /// Where the answer may go; `None` for anywhere.
    #[serde(default)]
    pub response_visibility: Option<Visibility>,
    /// The chain the message continues (RFC §8.3).
    #[serde(default)]
    pub chain_depth: u32,
    /// For an instruction only: the turn reads nothing of the room (RFC
    /// §10.1.1).
    #[serde(default)]
    pub standalone: bool,
    /// The caller's name for the message's delivery while it is in flight;
    /// never stored (RFC §10.1).
    #[serde(default)]
    pub delivery_key: Option<String>,
}

impl InboundMessage {
    /// A message with the RFC defaults: a `message` event visible to all.
    pub fn new(channel_id: ChannelId, sender_id: impl Into<String>, content: EventContent) -> Self {
        Self {
            channel_id,
            sender_id: sender_id.into(),
            content,
            event_type: EventType::Message,
            external_id: None,
            thread_id: None,
            parent_event_id: None,
            idempotency_key: None,
            raw_payload: Metadata::new(),
            provider_message_id: None,
            metadata: Metadata::new(),
            visibility: Visibility::All,
            addressed_to: None,
            response_visibility: None,
            chain_depth: 0,
            standalone: false,
            delivery_key: None,
        }
    }
}
