//! What a new room and a new binding are made with beside what the
//! framework gives them (RFC §5.1, §5.7): the options of a room's creation
//! and of a channel's attach.

use super::{
    Access, AgentResponsePolicy, ChannelBinding, ChannelCapabilities, ChannelCategory, Metadata,
    ParticipantId, RateLimit, RetryPolicy, Room, RoomId, RoomTimers, Timestamp, Visibility,
};

/// What a room is created with beside its id (RFC §5.1): its timers, its
/// agent response policy and its metadata. Everything else is a new room's:
/// ACTIVE, created now, its counters at zero, its inactivity counted from its
/// creation. The organization it belongs to is its creator's, never an
/// option (RFC §17.2).
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct RoomOptions {
    /// Seconds without an event before the active room pauses; `None`
    /// never pauses it.
    pub inactive_after_seconds: Option<u64>,
    /// Seconds paused before the room closes; `None` never closes it.
    pub closed_after_seconds: Option<u64>,
    /// What an agent's own output solicits (RFC §19.3.1).
    pub agent_response_policy: AgentResponsePolicy,
    /// Integrator-defined data. A key under `roomkit:` belongs to the
    /// implementation, and refuses the creation (RFC §10.6.5).
    pub metadata: Metadata,
}

impl RoomOptions {
    /// The room `id`, created at `now` with these options, belonging to
    /// `organization_id`.
    pub fn room(self, id: RoomId, organization_id: Option<String>, now: Timestamp) -> Room {
        Room {
            organization_id,
            timers: RoomTimers {
                inactive_after_seconds: self.inactive_after_seconds,
                closed_after_seconds: self.closed_after_seconds,
                last_activity_at: None,
            },
            agent_response_policy: self.agent_response_policy,
            metadata: self.metadata,
            ..Room::new(id, now)
        }
    }
}

/// What a channel is attached to a room with (RFC §5.7, §7): how it takes
/// part and what it delivers to. The channel, the room, the channel's type
/// and direction, the attach instant and the read horizon are the
/// framework's. By default a binding is the RFC's: read-write, unmuted,
/// visible to all, naming no correspondent, with the channel's own category
/// and capabilities.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct BindingOptions {
    /// Whether the channel reads, writes, both or neither (RFC §7.1).
    pub access: Access,
    /// Temporarily silenced: its response events are not broadcast (RFC
    /// §7.2).
    pub muted: bool,
    /// Speech output muted: events still arrive, nothing is spoken.
    pub output_muted: bool,
    /// Who sees what the channel writes (RFC §7.3). `internal` is the
    /// framework's only, and refuses the attach (gap G-041).
    pub visibility: Visibility,
    /// The binding's correspondent (RFC §10.4).
    pub participant_id: Option<ParticipantId>,
    /// Several senders share the binding's conversation (RFC §10.4).
    pub group: bool,
    /// How the channel takes part; `None`, the channel's own category.
    pub category: Option<ChannelCategory>,
    /// What the channel supports; `None`, what the channel declares.
    pub capabilities: Option<ChannelCapabilities>,
    /// The binding's rate limit.
    pub rate_limit: Option<RateLimit>,
    /// The binding's retry policy.
    pub retry_policy: Option<RetryPolicy>,
    /// Binding-specific data: a transport's recipient, under the key the
    /// channel's declaration names (`phone_number`, `email_address`, ...),
    /// a persona.
    pub metadata: Metadata,
}

impl BindingOptions {
    /// `binding`, the channel's default binding to the room, with these
    /// options: its channel, room, type, direction, attach instant and read
    /// horizon stay `binding`'s.
    pub fn apply(self, binding: ChannelBinding) -> ChannelBinding {
        ChannelBinding {
            category: self.category.unwrap_or(binding.category),
            capabilities: self.capabilities.unwrap_or(binding.capabilities),
            access: self.access,
            muted: self.muted,
            output_muted: self.output_muted,
            visibility: self.visibility,
            participant_id: self.participant_id,
            group: self.group,
            rate_limit: self.rate_limit,
            retry_policy: self.retry_policy,
            metadata: self.metadata,
            ..binding
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{ChannelDirection, ChannelId, ChannelType};

    fn declared() -> ChannelBinding {
        let mut binding = ChannelBinding::new(
            ChannelId::new("ai").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        binding.capabilities.supports_rich_text = true;
        binding
    }

    #[test]
    fn a_binding_keeps_its_channel_s_category_and_capabilities_unless_named() {
        let options = BindingOptions {
            muted: true,
            ..BindingOptions::default()
        };
        assert_eq!(
            options.apply(declared()),
            ChannelBinding {
                muted: true,
                ..declared()
            }
        );
    }
}
