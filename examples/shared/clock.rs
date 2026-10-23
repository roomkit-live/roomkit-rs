//! A kit whose clock and ids are the example's: the same run prints the
//! same, and an example moves time on when it needs to.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit::RoomKit;
use roomkit::model::{EventId, ObservationId, RoomId, Timestamp};
use roomkit::ops::{Environment, SequentialEnvironment};

/// The instant every example starts at, 2026-10-22 14:03 UTC.
const START_MILLIS: i64 = 1_792_677_780_000;

/// A kit over an in-memory store, on a [`Clock`] of its own.
pub fn kit() -> RoomKit {
    Clock::new().kit()
}

/// A clock that moves only when told, and ids numbered in order (`evt_1`,
/// `new_room_1`, ...): a [`SequentialEnvironment`] the example keeps a
/// handle on while the kit reads it. Cloning it shares it.
#[derive(Clone, Debug)]
pub struct Clock(Arc<Mutex<SequentialEnvironment>>);

impl Clock {
    /// A clock at the examples' start.
    pub fn new() -> Self {
        let start = Timestamp::from_unix_millis(START_MILLIS).unwrap_or(Timestamp::UNIX_EPOCH);
        Self(Arc::new(Mutex::new(SequentialEnvironment::new(start))))
    }

    /// A kit over an in-memory store that reads this clock.
    pub fn kit(&self) -> RoomKit {
        let kit = RoomKit::new();
        kit.set_environment(self.clone());
        kit
    }

    /// The instant the clock reads.
    pub fn now(&self) -> Timestamp {
        self.environment().now()
    }

    /// Moves the clock `duration` later, for the kit and the example alike.
    pub fn advance(&self, duration: Duration) {
        self.environment().advance(duration);
    }

    fn environment(&self) -> MutexGuard<'_, SequentialEnvironment> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Environment for Clock {
    fn now(&mut self) -> Timestamp {
        self.environment().now()
    }

    fn new_event_id(&mut self) -> EventId {
        self.environment().new_event_id()
    }

    fn new_observation_id(&mut self) -> ObservationId {
        self.environment().new_observation_id()
    }

    fn new_room_id(&mut self) -> RoomId {
        self.environment().new_room_id()
    }
}
