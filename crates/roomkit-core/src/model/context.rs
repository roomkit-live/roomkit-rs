//! What a channel or a hook is told about its room (RFC §6.1, §10.1 step 3).

use serde::{Deserialize, Serialize};

use super::{ChannelBinding, Participant, Room, RoomEvent};

/// A room's state as one reader receives it (RFC §10.1 step 3).
///
/// Hooks receive the room's recent events whole, refused ones included; a
/// channel receives only the events it may see (RFC §7.5 rule 8), so each
/// channel of a broadcast gets a context of its own.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoomContext {
    /// The room.
    pub room: Room,
    /// The room's channel bindings.
    #[serde(default)]
    pub bindings: Vec<ChannelBinding>,
    /// The room's participants.
    #[serde(default)]
    pub participants: Vec<Participant>,
    /// The room's latest events, in timeline order.
    #[serde(default)]
    pub recent_events: Vec<RoomEvent>,
}

impl RoomContext {
    /// The context of `room` with no binding, participant or event.
    pub fn new(room: Room) -> Self {
        Self {
            room,
            bindings: Vec::new(),
            participants: Vec::new(),
            recent_events: Vec::new(),
        }
    }
}
