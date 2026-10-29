//! The attachment of a channel to a room (RFC §5.7, §7.1, §13.2).

use serde::{Deserialize, Serialize};

use super::positive::{self, deserializers};
use super::{
    ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId, ChannelType, Metadata,
    ModelError, ParticipantId, RateLimit, RoomId, Timestamp, Visibility,
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
    /// Hidden: the Python implementation reads the members inline.
    #[doc(hidden)]
    pub fn reads(self) -> bool {
        match self {
            Self::ReadWrite | Self::ReadOnly => true,
            Self::WriteOnly | Self::None => false,
        }
    }

    /// Whether the channel's events may be broadcast (RFC §7.5 rule 2).
    /// Hidden, as `reads` is.
    #[doc(hidden)]
    pub fn writes(self) -> bool {
        match self {
            Self::ReadWrite | Self::WriteOnly => true,
            Self::ReadOnly | Self::None => false,
        }
    }
}

/// How a failed delivery is retried (RFC §13.2), with the Python
/// implementation's defaults: 3 retries, the first after 1 s, each delay
/// doubled up to 60 s, for every retryable error.
///
/// Its delays and multiplier are greater than 0, as Python's `gt=0`:
/// deserialization refuses another, and so do the kit's `attach_channel`
/// and the stores, since a struct literal runs no check.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryPolicy {
    /// The maximum number of retries.
    pub max_retries: u32,
    /// The delay before the first retry, in seconds.
    #[serde(deserialize_with = "bounded::base_delay_seconds")]
    pub base_delay_seconds: f64,
    /// The multiplier applied per attempt.
    #[serde(deserialize_with = "bounded::exponential_base")]
    pub exponential_base: f64,
    /// The cap on any delay, in seconds.
    #[serde(deserialize_with = "bounded::max_delay_seconds")]
    pub max_delay_seconds: f64,
    /// The names of the error types that may be retried, as the Python
    /// implementation reads them; `None` retries any error that does not
    /// refuse it itself.
    pub retryable_errors: Option<Vec<String>>,
}

impl RetryPolicy {
    /// Refuses a delay or a multiplier that is not greater than 0, `NaN`
    /// included, naming the first such field. Outside the public API: the
    /// kit's `attach_channel` runs it on the policy it is given.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::NotPositive`] for a value `<= 0`.
    #[doc(hidden)]
    pub fn check(&self) -> Result<(), ModelError> {
        positive::number("base_delay_seconds", self.base_delay_seconds)?;
        positive::number("exponential_base", self.exponential_base)?;
        positive::number("max_delay_seconds", self.max_delay_seconds)
    }
}

/// The deserializers of [`RetryPolicy`]'s bounded fields.
mod bounded {
    super::deserializers!(super::positive::number =>
        base_delay_seconds: f64,
        exponential_base: f64,
        max_delay_seconds: f64,
    );
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_seconds: 1.0,
            exponential_base: 2.0,
            max_delay_seconds: 60.0,
            retryable_errors: None,
        }
    }
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
    /// named one; `transport` by default.
    #[serde(default)]
    pub category: ChannelCategory,
    /// What the channel can do physically, `bidirectional` by default.
    #[serde(default)]
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
    /// A binding with the Python implementation's defaults, attached now:
    /// a bidirectional transport, read-write, unmuted, visible to all, no
    /// correspondent, text-only capabilities. A caller sets the other fields
    /// by struct update.
    #[cfg(feature = "system-environment")]
    pub fn new(channel_id: ChannelId, room_id: RoomId, channel_type: ChannelType) -> Self {
        use crate::ops::{Environment, SystemEnvironment};
        Self::at(
            channel_id,
            room_id,
            channel_type,
            ChannelCategory::default(),
            ChannelDirection::default(),
            SystemEnvironment.now(),
        )
    }

    /// [`ChannelBinding::new`] with its category, direction and instant
    /// given: what the core's operations build. Outside the public API.
    #[doc(hidden)]
    pub fn at(
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

    /// Refuses a binding its deserialization would have refused, a limit,
    /// a delay or a multiplier not above 0 ([`ChannelCapabilities::check`],
    /// [`RateLimit::check`], [`RetryPolicy::check`]): a struct literal runs
    /// no check, so every door that writes a binding runs this one, the
    /// kit's `attach_channel` and the stores. Outside the public API.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::NotPositive`].
    #[doc(hidden)]
    pub fn check(&self) -> Result<(), ModelError> {
        self.capabilities.check()?;
        self.rate_limit.as_ref().map_or(Ok(()), RateLimit::check)?;
        self.retry_policy
            .as_ref()
            .map_or(Ok(()), RetryPolicy::check)
    }

    /// Whether the binding's direction delivers to the outside (RFC §4.2),
    /// Python's property `sends`: a transport is delivered to only when it
    /// sends.
    pub fn sends(&self) -> bool {
        self.direction.sends()
    }

    /// Whether the binding may write into the room (RFC §7.5), Python's
    /// property `can_write`: its access writes and it is not muted.
    pub fn can_write(&self) -> bool {
        self.access.writes() && !self.muted
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// Python's properties `sends` and `can_write`.
    #[test]
    fn a_binding_sends_by_its_direction_and_writes_unless_muted() {
        let binding = ChannelBinding::at(
            ChannelId::new("sms").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::SMS,
            ChannelCategory::Transport,
            ChannelDirection::Inbound,
            Timestamp::UNIX_EPOCH,
        );
        let muted = ChannelBinding {
            muted: true,
            direction: ChannelDirection::Bidirectional,
            ..binding.clone()
        };
        assert_eq!(
            [
                (binding.sends(), binding.can_write()),
                (muted.sends(), muted.can_write())
            ],
            [(false, true), (true, false)]
        );
    }

    /// Python's `gt=0` on a retry policy's delays and multiplier.
    #[test]
    fn a_retry_policy_not_above_zero_is_refused_when_read() {
        let bounded = [
            ("base_delay_seconds", json!(0)),
            ("exponential_base", json!(-1.5)),
            ("max_delay_seconds", json!(0.0)),
        ];
        let read = bounded.clone().map(|(name, value)| {
            serde_json::from_value::<RetryPolicy>(json!({ name: value }))
                .map_err(|error| error.to_string())
        });
        assert_eq!(
            read,
            [
                Err(
                    "base_delay_seconds: Input should be greater than 0 [input_value=0]".to_owned()
                ),
                Err(
                    "exponential_base: Input should be greater than 0 [input_value=-1.5]"
                        .to_owned()
                ),
                Err("max_delay_seconds: Input should be greater than 0 [input_value=0]".to_owned()),
            ]
        );
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
    fn access_serializes_in_snake_case() {
        assert_eq!(
            serde_json::to_value(Access::ReadWrite).unwrap(),
            json!("read_write")
        );
    }

    /// Python's `RetryPolicy()`: 3, 1.0, 60.0, 2.0, every error; a member
    /// left out reads its default.
    #[test]
    fn a_retry_policy_takes_python_s_defaults() {
        let read: RetryPolicy = serde_json::from_value(json!({ "max_retries": 5 })).unwrap();
        assert_eq!(
            (serde_json::to_value(RetryPolicy::default()).unwrap(), read),
            (
                json!({
                    "max_retries": 3,
                    "base_delay_seconds": 1.0,
                    "exponential_base": 2.0,
                    "max_delay_seconds": 60.0,
                    "retryable_errors": null,
                }),
                RetryPolicy {
                    max_retries: 5,
                    ..RetryPolicy::default()
                }
            )
        );
    }

    /// Python's model defaults: a binding without `category` and
    /// `direction` is a bidirectional transport.
    #[test]
    fn a_binding_without_category_and_direction_reads_a_bidirectional_transport() {
        let read: ChannelBinding = serde_json::from_value(json!({
            "channel_id": "sms",
            "room_id": "room_1",
            "channel_type": "sms",
            "attached_at": "1970-01-01T00:00:00Z",
        }))
        .unwrap();
        assert_eq!(
            (read.category, read.direction),
            (ChannelCategory::Transport, ChannelDirection::Bidirectional)
        );
    }
}
