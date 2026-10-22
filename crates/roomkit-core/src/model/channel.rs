//! What a channel is and what it supports (RFC §4.2, §5.8, §6.2, §13.3).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::Metadata;

/// The kind of a channel (RFC §6.2).
///
/// The RFC expects this list to grow; adding a value is a minor-version
/// change before 1.0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelType {
    /// Short Message Service.
    Sms,
    /// Multimedia Message Service.
    Mms,
    /// Rich Communication Services.
    Rcs,
    /// Electronic mail.
    Email,
    /// WhatsApp Business Cloud API.
    Whatsapp,
    /// WhatsApp Web multidevice protocol.
    WhatsappPersonal,
    /// WebSocket real-time connection.
    Websocket,
    /// Facebook Messenger.
    Messenger,
    /// Microsoft Teams.
    Teams,
    /// Generic HTTP webhook.
    Webhook,
    /// Voice channel (STT/TTS pipeline).
    Voice,
    /// Speech-to-speech API.
    RealtimeVoice,
    /// Combined voice and video channel (STT/TTS pipeline and video).
    AudioVideo,
    /// Combined speech-to-speech and video channel.
    RealtimeAudioVideo,
    /// Video-only channel.
    Video,
    /// Multi-party SFU conference.
    Conference,
    /// Telegram Bot API.
    Telegram,
    /// Command-line interface, for development and testing.
    Cli,
    /// Push notification channel.
    Push,
    /// AI or LLM agent.
    Ai,
    /// Framework-generated events.
    System,
}

impl ChannelType {
    /// The category the RFC table gives this type, `None` for [`System`]
    /// which has none (RFC §6.2).
    ///
    /// [`System`]: ChannelType::System
    pub fn category(self) -> Option<ChannelCategory> {
        match self {
            Self::Sms
            | Self::Mms
            | Self::Rcs
            | Self::Email
            | Self::Whatsapp
            | Self::WhatsappPersonal
            | Self::Websocket
            | Self::Messenger
            | Self::Teams
            | Self::Webhook
            | Self::Voice
            | Self::RealtimeVoice
            | Self::AudioVideo
            | Self::RealtimeAudioVideo
            | Self::Video
            | Self::Conference
            | Self::Telegram
            | Self::Cli
            | Self::Push => Some(ChannelCategory::Transport),
            Self::Ai => Some(ChannelCategory::Intelligence),
            Self::System => None,
        }
    }
}

/// What a channel does (RFC §4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelCategory {
    /// Carries messages to and from external systems or humans.
    Transport,
    /// Processes events and produces responses or insights.
    Intelligence,
}

/// What a channel can do physically (RFC §4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelDirection {
    /// Receives from outside only.
    Inbound,
    /// Sends to outside only.
    Outbound,
    /// Receives and sends.
    Bidirectional,
}

impl ChannelDirection {
    /// Whether the channel receives from outside.
    pub fn receives(self) -> bool {
        match self {
            Self::Inbound | Self::Bidirectional => true,
            Self::Outbound => false,
        }
    }

    /// Whether the channel sends to outside.
    pub fn sends(self) -> bool {
        match self {
            Self::Outbound | Self::Bidirectional => true,
            Self::Inbound => false,
        }
    }
}

/// A category of content a channel carries (RFC §5.8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelMediaType {
    /// Plain text messages.
    Text,
    /// Formatted text with buttons, cards, quick replies.
    Rich,
    /// Images, documents, files.
    Media,
    /// Audio messages, voice notes.
    Audio,
    /// Video messages.
    Video,
    /// Geographic coordinates.
    Location,
    /// Pre-approved message templates.
    Template,
}

/// What a channel supports (RFC §5.8).
///
/// The RFC gives no default; [`Default`] is a text-only channel with no
/// limit and no optional feature.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors the RFC §5.8 record field for field"
)]
pub struct ChannelCapabilities {
    /// The content categories the channel carries.
    pub media_types: BTreeSet<ChannelMediaType>,
    /// The maximum text length, `None` for unlimited (counted as gap G-008
    /// decides).
    pub max_length: Option<u64>,
    /// Formatted text.
    pub supports_rich_text: bool,
    /// Interactive buttons.
    pub supports_buttons: bool,
    /// The maximum number of buttons, `None` for unlimited.
    pub max_buttons: Option<u32>,
    /// Structured cards.
    pub supports_cards: bool,
    /// Suggested quick replies.
    pub supports_quick_replies: bool,
    /// Pre-approved templates.
    pub supports_templates: bool,
    /// Media attachments.
    pub supports_media: bool,
    /// The MIME types of the media the channel accepts.
    pub supported_media_types: Vec<String>,
    /// The maximum media size in bytes, `None` for unlimited.
    pub max_media_size_bytes: Option<u64>,
    /// Audio.
    pub supports_audio: bool,
    /// Video.
    pub supports_video: bool,
    /// Threads.
    pub supports_threading: bool,
    /// Typing indicators.
    pub supports_typing: bool,
    /// Read receipts.
    pub supports_read_receipts: bool,
    /// Reactions.
    pub supports_reactions: bool,
    /// Native edits (RFC §10.3).
    pub supports_edit: bool,
    /// Native deletes (RFC §10.3).
    pub supports_delete: bool,
    /// The channel's own rate limit.
    pub rate_limit: Option<RateLimit>,
    /// Extensible capabilities.
    pub custom: Metadata,
}

impl ChannelCapabilities {
    /// Whether the channel carries `media_type`.
    pub fn carries(&self, media_type: ChannelMediaType) -> bool {
        self.media_types.contains(&media_type)
    }
}

impl Default for ChannelCapabilities {
    fn default() -> Self {
        Self {
            media_types: BTreeSet::from([ChannelMediaType::Text]),
            max_length: None,
            supports_rich_text: false,
            supports_buttons: false,
            max_buttons: None,
            supports_cards: false,
            supports_quick_replies: false,
            supports_templates: false,
            supports_media: false,
            supported_media_types: Vec::new(),
            max_media_size_bytes: None,
            supports_audio: false,
            supports_video: false,
            supports_threading: false,
            supports_typing: false,
            supports_read_receipts: false,
            supports_reactions: false,
            supports_edit: false,
            supports_delete: false,
            rate_limit: None,
            custom: Metadata::new(),
        }
    }
}

/// A rate limit (RFC §13.3).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimit {
    /// The maximum events per second.
    pub max_per_second: Option<f64>,
    /// The maximum events per minute.
    pub max_per_minute: Option<f64>,
    /// The burst allowance.
    pub burst: Option<u32>,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_channel_type_serializes_in_snake_case() {
        assert_eq!(
            serde_json::to_value(ChannelType::WhatsappPersonal).unwrap(),
            json!("whatsapp_personal")
        );
    }

    #[test]
    fn the_ai_type_is_an_intelligence_channel() {
        assert_eq!(
            ChannelType::Ai.category(),
            Some(ChannelCategory::Intelligence)
        );
    }

    #[test]
    fn the_system_type_has_no_category() {
        assert_eq!(ChannelType::System.category(), None);
    }

    #[test]
    fn an_outbound_channel_does_not_receive() {
        assert!(!ChannelDirection::Outbound.receives());
    }

    #[test]
    fn a_bidirectional_channel_sends() {
        assert!(ChannelDirection::Bidirectional.sends());
    }

    #[test]
    fn default_capabilities_carry_text_only() {
        let capabilities = ChannelCapabilities::default();
        assert_eq!(
            capabilities.media_types,
            BTreeSet::from([ChannelMediaType::Text])
        );
    }

    #[test]
    fn missing_capability_fields_take_their_defaults() {
        let parsed: ChannelCapabilities =
            serde_json::from_value(json!({ "max_length": 1600 })).unwrap();
        let expected = ChannelCapabilities {
            max_length: Some(1600),
            ..ChannelCapabilities::default()
        };
        assert_eq!(parsed, expected);
    }
}
