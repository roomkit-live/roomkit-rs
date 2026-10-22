//! The attachment of a channel to a room (RFC §5.7, §7.1, §13.2).

use serde::{Deserialize, Serialize};

use super::{
    ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId, ChannelType, Metadata,
    ParticipantId, RateLimit, RoomId, Timestamp, Visibility,
};

/// Whether a bound channel reads, writes, both or neither (RFC §7.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// Full participation.
    #[default]
    ReadWrite,
    /// Observes only, through `on_event()`.
    ReadOnly,
    /// A blind sender.
    WriteOnly,
    /// Bound but inactive.
    None,
}

impl Access {
    /// Whether the channel receives the room's events (RFC §7.5 rule 1).
    pub fn reads(self) -> bool {
        match self {
            Self::ReadWrite | Self::ReadOnly => true,
            Self::WriteOnly | Self::None => false,
        }
    }

    /// Whether the channel's events may be broadcast (RFC §7.5 rule 2).
    pub fn writes(self) -> bool {
        match self {
            Self::ReadWrite | Self::WriteOnly => true,
            Self::ReadOnly | Self::None => false,
        }
    }
}

/// How a failed delivery is retried (RFC §13.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// The maximum number of retries.
    pub max_retries: u32,
    /// The delay before the first retry, in seconds.
    pub base_delay_seconds: f64,
    /// The multiplier applied per attempt.
    #[serde(default = "default_exponential_base")]
    pub exponential_base: f64,
    /// The cap on any delay, in seconds.
    pub max_delay_seconds: f64,
    /// The error codes eligible for a retry, `None` for every retryable error.
    #[serde(default)]
    pub retryable_errors: Option<Vec<String>>,
}

fn default_exponential_base() -> f64 {
    2.0
}

/// A channel attached to a room, with its permissions (RFC §5.7).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelBinding {
    /// The bound channel.
    pub channel_id: ChannelId,
    /// The room it is bound to.
    pub room_id: RoomId,
    /// The channel's type.
    pub channel_type: ChannelType,
    /// How the channel takes part: its own category unless the attachment
    /// named one.
    pub category: ChannelCategory,
    /// What the channel can do physically.
    pub direction: ChannelDirection,
    /// Whether it reads, writes, both or neither.
    #[serde(default)]
    pub access: Access,
    /// Temporarily silenced: its response events are not broadcast (RFC §7.2).
    #[serde(default)]
    pub muted: bool,
    /// Speech output muted: events still arrive, nothing is spoken.
    #[serde(default)]
    pub output_muted: bool,
    /// Who sees what this channel writes (RFC §7.3).
    #[serde(default)]
    pub visibility: Visibility,
    /// The binding's correspondent (RFC §10.4).
    #[serde(default)]
    pub participant_id: Option<ParticipantId>,
    /// Several senders share this binding's conversation (RFC §10.4).
    #[serde(default)]
    pub group: bool,
    /// The read horizon, for unread tracking.
    #[serde(default)]
    pub last_read_index: Option<u64>,
    /// When the channel was attached.
    pub attached_at: Timestamp,
    /// What the channel supports.
    #[serde(default)]
    pub capabilities: ChannelCapabilities,
    /// The binding's rate limit.
    #[serde(default)]
    pub rate_limit: Option<RateLimit>,
    /// The binding's retry policy.
    #[serde(default)]
    pub retry_policy: Option<RetryPolicy>,
    /// Binding-specific data (recipient, persona, ...).
    #[serde(default)]
    pub metadata: Metadata,
}

impl ChannelBinding {
    /// A binding with the RFC defaults: read-write, unmuted, visible to all,
    /// no correspondent, text-only capabilities.
    pub fn new(
        channel_id: ChannelId,
        room_id: RoomId,
        channel_type: ChannelType,
        category: ChannelCategory,
        direction: ChannelDirection,
        attached_at: Timestamp,
    ) -> Self {
        Self {
            channel_id,
            room_id,
            channel_type,
            category,
            direction,
            access: Access::default(),
            muted: false,
            output_muted: false,
            visibility: Visibility::default(),
            participant_id: None,
            group: false,
            last_read_index: None,
            attached_at,
            capabilities: ChannelCapabilities::default(),
            rate_limit: None,
            retry_policy: None,
            metadata: Metadata::new(),
        }
    }

    /// Whether the channel's events may be broadcast: it can write and is not
    /// muted (RFC §7.5 rule 2).
    pub fn can_write(&self) -> bool {
        self.access.writes() && !self.muted
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn binding() -> ChannelBinding {
        ChannelBinding::new(
            ChannelId::new("sms").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::Sms,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    #[test]
    fn read_only_access_reads() {
        assert!(Access::ReadOnly.reads());
    }

    #[test]
    fn read_only_access_does_not_write() {
        assert!(!Access::ReadOnly.writes());
    }

    #[test]
    fn write_only_access_does_not_read() {
        assert!(!Access::WriteOnly.reads());
    }

    #[test]
    fn a_muted_binding_cannot_write() {
        let muted = ChannelBinding {
            muted: true,
            ..binding()
        };
        assert!(!muted.can_write());
    }

    #[test]
    fn a_new_binding_can_write() {
        assert!(binding().can_write());
    }

    #[test]
    fn access_serializes_in_snake_case() {
        assert_eq!(
            serde_json::to_value(Access::ReadWrite).unwrap(),
            json!("read_write")
        );
    }

    #[test]
    fn a_retry_policy_defaults_its_exponential_base_to_two() {
        let policy: RetryPolicy = serde_json::from_value(json!({
            "max_retries": 3,
            "base_delay_seconds": 0.5,
            "max_delay_seconds": 10.0,
        }))
        .unwrap();
        assert!((policy.exponential_base - 2.0).abs() < f64::EPSILON);
    }
}
