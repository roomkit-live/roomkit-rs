//! What a channel is and what it supports (RFC §4.2, §5.8, §6.2, §13.3).

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize, Serializer};

use super::{Metadata, ModelError};

/// The kind of a channel (RFC §6.2): an open name of lowercase ASCII
/// letters, digits, `_` and `-`, written as a plain string (`sms`, `teams`).
///
/// The RFC lists the types it knows, and the Python implementation writes
/// more (`discord`, `buzz`); any other name is a type too, so a record that
/// names one is read and written back as it is. What a type means for its
/// channels, how their senders are addressed and what they do, is the
/// channel's declaration (`ops::ChannelDeclaration`), which takes the
/// well-known names' defaults. The well-known names are the associated
/// constants. Two are reserved: [`HOST`](Self::HOST), which only a host
/// channel's declaration may claim, and [`SYSTEM`](Self::SYSTEM), the
/// framework's own records', which no channel may claim.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct ChannelType(Cow<'static, str>);

impl ChannelType {
    /// Short Message Service.
    pub const SMS: Self = Self(Cow::Borrowed("sms"));
    /// Multimedia Message Service.
    pub const MMS: Self = Self(Cow::Borrowed("mms"));
    /// Rich Communication Services.
    pub const RCS: Self = Self(Cow::Borrowed("rcs"));
    /// Electronic mail.
    pub const EMAIL: Self = Self(Cow::Borrowed("email"));
    /// WhatsApp Business Cloud API.
    pub const WHATSAPP: Self = Self(Cow::Borrowed("whatsapp"));
    /// WhatsApp Web multidevice protocol.
    pub const WHATSAPP_PERSONAL: Self = Self(Cow::Borrowed("whatsapp_personal"));
    /// WebSocket real-time connection.
    pub const WEBSOCKET: Self = Self(Cow::Borrowed("websocket"));
    /// Facebook Messenger.
    pub const MESSENGER: Self = Self(Cow::Borrowed("messenger"));
    /// Microsoft Teams.
    pub const TEAMS: Self = Self(Cow::Borrowed("teams"));
    /// Discord bot, a name the Python implementation writes and the RFC
    /// does not list.
    pub const DISCORD: Self = Self(Cow::Borrowed("discord"));
    /// Buzz (Nostr relay), a name the Python implementation writes and
    /// the RFC does not list.
    pub const BUZZ: Self = Self(Cow::Borrowed("buzz"));
    /// Generic HTTP webhook.
    pub const WEBHOOK: Self = Self(Cow::Borrowed("webhook"));
    /// Voice channel (STT/TTS pipeline).
    pub const VOICE: Self = Self(Cow::Borrowed("voice"));
    /// Speech-to-speech API.
    pub const REALTIME_VOICE: Self = Self(Cow::Borrowed("realtime_voice"));
    /// Combined voice and video channel (STT/TTS pipeline and video).
    pub const AUDIO_VIDEO: Self = Self(Cow::Borrowed("audio_video"));
    /// Combined speech-to-speech and video channel.
    pub const REALTIME_AUDIO_VIDEO: Self = Self(Cow::Borrowed("realtime_audio_video"));
    /// Video-only channel.
    pub const VIDEO: Self = Self(Cow::Borrowed("video"));
    /// Multi-party SFU conference.
    pub const CONFERENCE: Self = Self(Cow::Borrowed("conference"));
    /// Telegram Bot API.
    pub const TELEGRAM: Self = Self(Cow::Borrowed("telegram"));
    /// Command-line interface, for development and testing.
    pub const CLI: Self = Self(Cow::Borrowed("cli"));
    /// Push notification channel.
    pub const PUSH: Self = Self(Cow::Borrowed("push"));
    /// The host application's own voice in a room: a transport, `INBOUND`,
    /// never delivered to (RFC §6.2, §10.5, Appendix A.14.1). Reserved: only
    /// a host channel's declaration may claim it.
    pub const HOST: Self = Self(Cow::Borrowed("host"));
    /// AI or LLM agent.
    pub const AI: Self = Self(Cow::Borrowed("ai"));
    /// The framework's own records. Reserved: no channel may claim it.
    pub const SYSTEM: Self = Self(Cow::Borrowed("system"));

    /// The type named `name`: lowercase ASCII letters, digits, `_` and `-`,
    /// as every type of the Python implementation's enumeration, so that a
    /// name is written one way only (`SMS` is not `sms`). Python takes any
    /// key in an identity's addresses; a store reads a key that is no name
    /// as a member the model does not hold.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::EmptyIdentifier`] for an empty name, and
    /// [`ModelError::InvalidChannelType`] for one holding another
    /// character.
    pub fn new(name: impl Into<String>) -> Result<Self, ModelError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ModelError::EmptyIdentifier {
                kind: "channel type",
            });
        }
        let allowed = |character: char| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'
                || character == '-'
        };
        if !name.chars().all(allowed) {
            return Err(ModelError::InvalidChannelType { value: name });
        }
        Ok(Self(Cow::Owned(name)))
    }

    /// The name, as written.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for ChannelType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl TryFrom<String> for ChannelType {
    type Error = ModelError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

impl FromStr for ChannelType {
    type Err = ModelError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::new(name)
    }
}

impl fmt::Display for ChannelType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.pad(&self.0)
    }
}

impl AsRef<str> for ChannelType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// What a channel does (RFC §4.2).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChannelCategory {
    /// Carries messages to and from external systems or humans, the
    /// default.
    #[default]
    Transport,
    /// Processes events and produces responses or insights.
    Intelligence,
}

/// What a channel can do physically (RFC §4.2).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChannelDirection {
    /// Receives from outside only.
    Inbound,
    /// Sends to outside only.
    Outbound,
    /// Receives and sends, the default.
    #[default]
    Bidirectional,
}

impl ChannelDirection {
    /// Whether the channel receives from outside. Hidden: the Python
    /// implementation reads the directions inline.
    #[doc(hidden)]
    pub fn receives(self) -> bool {
        match self {
            Self::Inbound | Self::Bidirectional => true,
            Self::Outbound => false,
        }
    }

    /// Whether the channel sends to outside. Hidden, as `receives` is.
    #[doc(hidden)]
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
#[non_exhaustive]
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

impl ChannelMediaType {
    /// Every media type, as a channel that carries any declares them (a
    /// host channel, RFC §10.5).
    #[doc(hidden)]
    pub fn all() -> BTreeSet<Self> {
        // The match lists every type: a type added to the enum fails to
        // build until it is listed here too.
        [
            Self::Text,
            Self::Rich,
            Self::Media,
            Self::Audio,
            Self::Video,
            Self::Location,
            Self::Template,
        ]
        .into_iter()
        .filter(|media_type| match media_type {
            Self::Text
            | Self::Rich
            | Self::Media
            | Self::Audio
            | Self::Video
            | Self::Location
            | Self::Template => true,
        })
        .collect()
    }
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
    /// The longest audio the channel takes, in seconds, `None` for any.
    pub max_audio_duration_seconds: Option<u64>,
    /// The audio formats the channel takes; empty takes every format.
    pub supported_audio_formats: Vec<String>,
    /// Video.
    pub supports_video: bool,
    /// The longest video the channel takes, in seconds, `None` for any.
    pub max_video_duration_seconds: Option<u64>,
    /// The video formats the channel takes; empty takes every format.
    pub supported_video_formats: Vec<String>,
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
    /// Extensible capabilities.
    pub custom: Metadata,
}

impl ChannelCapabilities {
    /// Whether the channel carries `media_type`. Hidden: the Python
    /// implementation reads `media_types` inline.
    #[doc(hidden)]
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
            max_audio_duration_seconds: None,
            supported_audio_formats: Vec::new(),
            supports_video: false,
            max_video_duration_seconds: None,
            supported_video_formats: Vec::new(),
            supports_threading: false,
            supports_typing: false,
            supports_read_receipts: false,
            supports_reactions: false,
            supports_edit: false,
            supports_delete: false,
            custom: Metadata::new(),
        }
    }
}

/// A rate limit (RFC §13.3), the Python implementation's members.
///
/// A limit is greater than 0, as Python's `gt=0.0`: deserialization refuses
/// another, and so does a kit's `attach_channel`, since a struct literal
/// runs no check.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RateLimitWire")]
pub struct RateLimit {
    /// The maximum events per second.
    pub max_per_second: Option<f64>,
    /// The maximum events per minute.
    pub max_per_minute: Option<f64>,
    /// The maximum events per hour.
    pub max_per_hour: Option<f64>,
}

impl RateLimit {
    /// Refuses a limit that is not greater than 0, `NaN` included, naming
    /// the first such field. Outside the public API: the kit's
    /// `attach_channel` runs it on the limit it is given.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::NotPositive`] for a limit `<= 0`.
    #[doc(hidden)]
    pub fn check(&self) -> Result<(), ModelError> {
        [
            ("max_per_second", self.max_per_second),
            ("max_per_minute", self.max_per_minute),
            ("max_per_hour", self.max_per_hour),
        ]
        .into_iter()
        .find_map(|(name, limit)| {
            limit
                .filter(|value| value.is_nan() || *value <= 0.0)
                .map(|value| ModelError::NotPositive { name, value })
        })
        .map_or(Ok(()), Err)
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
#[expect(
    clippy::struct_field_names,
    reason = "the Python implementation's names"
)]
struct RateLimitWire {
    max_per_second: Option<f64>,
    max_per_minute: Option<f64>,
    max_per_hour: Option<f64>,
}

impl TryFrom<RateLimitWire> for RateLimit {
    type Error = ModelError;

    fn try_from(wire: RateLimitWire) -> Result<Self, Self::Error> {
        let limit = Self {
            max_per_second: wire.max_per_second,
            max_per_minute: wire.max_per_minute,
            max_per_hour: wire.max_per_hour,
        };
        limit.check()?;
        Ok(limit)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// Python's `gt=0.0`: a limit read must be greater than 0.
    #[test]
    fn a_rate_limit_not_above_zero_is_refused_when_read() {
        let read = [
            json!({ "max_per_second": 0.0 }),
            json!({ "max_per_hour": -1.0 }),
        ]
        .map(|json| serde_json::from_value::<RateLimit>(json).map_err(|e| e.to_string()));
        let kept: RateLimit = serde_json::from_value(json!({ "max_per_hour": 60.0 })).unwrap();
        assert_eq!(
            (read, kept),
            (
                [
                    Err("max_per_second 0 is not greater than 0".to_owned()),
                    Err("max_per_hour -1 is not greater than 0".to_owned()),
                ],
                RateLimit {
                    max_per_hour: Some(60.0),
                    ..RateLimit::default()
                }
            )
        );
    }

    /// A type is an open name (docs/rfc-gaps.md, "Found by step 4's shared
    /// SQLite file"): a well-known one, one the Python implementation writes
    /// that the RFC does not list, and one no implementation knows are read
    /// and written back as they are.
    #[test]
    fn any_name_reads_and_writes_back_as_it_is() {
        let names = ["whatsapp_personal", "discord", "buzz", "slack"];
        let written: Vec<serde_json::Value> = names
            .iter()
            .map(|name| {
                let read: ChannelType = serde_json::from_value(json!(name)).unwrap();
                serde_json::to_value(read).unwrap()
            })
            .collect();
        assert_eq!(written, names.map(|name| json!(name)));
    }

    #[test]
    fn a_well_known_name_is_its_constant() {
        assert_eq!(
            "whatsapp_personal".parse::<ChannelType>(),
            Ok(ChannelType::WHATSAPP_PERSONAL)
        );
    }

    /// A name is lowercase ASCII letters, digits, `_` and `-`, as every type
    /// of the Python implementation's enumeration: `SMS` would miss `sms`'s
    /// identities, hook filters and addressing, and `HOST` would pass for
    /// an ordinary name.
    #[test]
    fn a_name_other_than_lowercase_letters_digits_underscore_or_hyphen_is_refused() {
        let refused = |names: &[&str]| -> Vec<bool> {
            names
                .iter()
                .map(|name| serde_json::from_value::<ChannelType>(json!(name)).is_err())
                .collect()
        };
        assert_eq!(
            (
                refused(&["", " ", "SMS", "Sms", "x\ny", "HOST", "System", "sms "]),
                refused(&["slack", "whatsapp_personal", "my-type", "x2"])
            ),
            (vec![true; 8], vec![false; 4])
        );
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
