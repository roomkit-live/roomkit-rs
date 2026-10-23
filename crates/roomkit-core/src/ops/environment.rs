//! What an operation reads from its host besides effects: the clock and new
//! identifiers.

use std::time::Duration;

use crate::model::{EventId, ObservationId, RoomId, Timestamp};

/// The clock and the identifiers an operation uses (docs/architecture.md,
/// "Environment").
///
/// Reading the clock and creating an id are not effects: they never fail and
/// never wait. A driver gives the system clock and random ids; a test gives a
/// fixed clock and sequential ids, so a trace is reproducible.
pub trait Environment {
    /// The current instant.
    fn now(&mut self) -> Timestamp;

    /// A new, unique event id.
    fn new_event_id(&mut self) -> EventId;

    /// A new, unique observation id.
    fn new_observation_id(&mut self) -> ObservationId;

    /// A new, unique room id, for the room the framework creates for a
    /// message no room was found for (RFC §10.1 step 2, §10.4).
    fn new_room_id(&mut self) -> RoomId;
}

/// A deterministic environment, for tests and examples: a clock that moves
/// only when told to, and ids numbered in order (`evt_1`, `evt_2`, `obs_1`,
/// `new_room_1`).
#[derive(Clone, Debug)]
pub struct SequentialEnvironment {
    now: Timestamp,
    events: u64,
    observations: u64,
    rooms: u64,
}

impl SequentialEnvironment {
    /// An environment whose clock reads `now`.
    pub fn new(now: Timestamp) -> Self {
        Self {
            now,
            events: 0,
            observations: 0,
            rooms: 0,
        }
    }

    /// Moves the clock `duration` later; past the supported range it stays.
    pub fn advance(&mut self, duration: Duration) {
        if let Some(later) = self.now.checked_add(duration) {
            self.now = later;
        }
    }
}

impl Default for SequentialEnvironment {
    /// A clock at the Unix epoch.
    fn default() -> Self {
        Self::new(Timestamp::UNIX_EPOCH)
    }
}

impl Environment for SequentialEnvironment {
    fn now(&mut self) -> Timestamp {
        self.now
    }

    fn new_event_id(&mut self) -> EventId {
        self.events = self.events.saturating_add(1);
        EventId::generated(format!("evt_{}", self.events))
    }

    fn new_observation_id(&mut self) -> ObservationId {
        self.observations = self.observations.saturating_add(1);
        ObservationId::generated(format!("obs_{}", self.observations))
    }

    fn new_room_id(&mut self) -> RoomId {
        self.rooms = self.rooms.saturating_add(1);
        RoomId::generated(format!("new_room_{}", self.rooms))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn event_ids_are_numbered_in_order() {
        let mut environment = SequentialEnvironment::default();
        let ids = [environment.new_event_id(), environment.new_event_id()];
        assert_eq!(ids.map(|id| id.to_string()), ["evt_1", "evt_2"]);
    }

    #[test]
    fn room_ids_are_numbered_in_order() {
        let mut environment = SequentialEnvironment::default();
        let ids = [environment.new_room_id(), environment.new_room_id()];
        assert_eq!(ids.map(|id| id.to_string()), ["new_room_1", "new_room_2"]);
    }

    #[test]
    fn the_clock_moves_only_when_advanced() {
        let mut environment = SequentialEnvironment::default();
        environment.advance(Duration::from_secs(2));
        assert_eq!(environment.now().as_unix_millis(), 2000);
    }
}
