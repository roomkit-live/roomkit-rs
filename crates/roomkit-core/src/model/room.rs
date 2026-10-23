//! Rooms and their lifecycle (RFC §4.1, §5.1, §19.3.1).

use serde::{Deserialize, Serialize};

use super::{Metadata, RoomId, Timestamp};

/// Where a room is in its lifecycle (RFC §5.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomStatus {
    /// Channels send and receive.
    #[default]
    Active,
    /// Paused, for example by its inactivity timer; can be resumed.
    Paused,
    /// Closed; the integrator may reopen it.
    Closed,
    /// Archived; a terminal, read-only state.
    Archived,
}

impl RoomStatus {
    /// Whether the room accepts a new event. A closed or archived room MUST
    /// refuse one at every point where its timeline can grow (RFC §5.1).
    pub fn accepts_events(self) -> bool {
        match self {
            Self::Active | Self::Paused => true,
            Self::Closed | Self::Archived => false,
        }
    }
}

/// What an agent's own output solicits (RFC §19.3.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentResponsePolicy {
    /// Every eligible intelligence channel, the default.
    #[default]
    AgentChain,
    /// Only the channels the event addresses.
    AddressedOnly,
}

/// A room's automatic transitions (RFC §5.1); `rules::timers` says when
/// they fall due.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RoomTimers {
    /// Seconds without an event before an active room pauses.
    pub inactive_after_seconds: Option<u64>,
    /// Seconds paused before the room closes.
    pub closed_after_seconds: Option<u64>,
    /// When the room's last delivered event was committed, or when it was
    /// last resumed or reopened, which restarts the inactivity timer (gap
    /// G-045).
    pub last_activity_at: Option<Timestamp>,
}

/// A conversation space where channels connect and events flow (RFC §5.1).
///
/// A room holds no message content: its events are in its timeline.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Room {
    /// The room's id.
    pub id: RoomId,
    /// The organization the room belongs to, for multi-tenant isolation.
    #[serde(default)]
    pub organization_id: Option<String>,
    /// Where the room is in its lifecycle.
    #[serde(default)]
    pub status: RoomStatus,
    /// When the room was created.
    pub created_at: Timestamp,
    /// When the room was last modified.
    pub updated_at: Timestamp,
    /// When the room was closed.
    #[serde(default)]
    pub closed_at: Option<Timestamp>,
    /// When the room was paused, which its close timer counts from (gap
    /// G-045).
    #[serde(default)]
    pub paused_at: Option<Timestamp>,
    /// The room's automatic transitions.
    #[serde(default)]
    pub timers: RoomTimers,
    /// What an agent's own output solicits.
    #[serde(default)]
    pub agent_response_policy: AgentResponsePolicy,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
    /// How many events the timeline holds, refused ones included (RFC §14.1).
    #[serde(default)]
    pub event_count: u64,
    /// The highest index of the timeline, `None` while it is empty (gap
    /// G-012).
    #[serde(default)]
    pub latest_index: Option<u64>,
}

impl Room {
    /// An active, empty room created at `now`.
    pub fn new(id: RoomId, now: Timestamp) -> Self {
        Self {
            id,
            organization_id: None,
            status: RoomStatus::Active,
            created_at: now,
            updated_at: now,
            closed_at: None,
            paused_at: None,
            timers: RoomTimers::default(),
            agent_response_policy: AgentResponsePolicy::default(),
            metadata: Metadata::new(),
            event_count: 0,
            latest_index: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_paused_room_accepts_events() {
        assert!(RoomStatus::Paused.accepts_events());
    }

    #[test]
    fn a_closed_room_refuses_events() {
        assert!(!RoomStatus::Closed.accepts_events());
    }

    #[test]
    fn an_archived_room_refuses_events() {
        assert!(!RoomStatus::Archived.accepts_events());
    }

    #[test]
    fn a_new_room_is_active_and_empty() {
        let room = Room::new(RoomId::new("room_1").unwrap(), Timestamp::UNIX_EPOCH);
        assert_eq!(
            (room.status, room.event_count, room.latest_index),
            (RoomStatus::Active, 0, None)
        );
    }

    #[test]
    fn the_agent_response_policy_defaults_to_agent_chain() {
        let room: Room = serde_json::from_value(json!({
            "id": "room_1",
            "created_at": "1970-01-01T00:00:00Z",
            "updated_at": "1970-01-01T00:00:00Z",
        }))
        .unwrap();
        assert_eq!(room.agent_response_policy, AgentResponsePolicy::AgentChain);
    }
}
