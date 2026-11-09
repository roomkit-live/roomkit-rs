//! A kit clock that advances with its Tokio timers, including paused time.

#![expect(clippy::unwrap_used, reason = "test instants stay in range")]

use roomkit::model::{EventId, ObservationId, RoomId, Timestamp};
use roomkit::testing::Environment;
use roomkit_core::ops::SystemEnvironment;

/// The kits' clock on tokio's, which a paused test advances: the lease
/// expires as its time passes. Ids are the system's, unique across the
/// processes.
#[derive(Clone)]
pub(crate) struct TokioClock {
    start: tokio::time::Instant,
    base: Timestamp,
}

impl TokioClock {
    pub(crate) fn new() -> Self {
        Self {
            start: tokio::time::Instant::now(),
            base: SystemEnvironment.now(),
        }
    }
}

impl Environment for TokioClock {
    fn now(&self) -> Timestamp {
        self.base.checked_add(self.start.elapsed()).unwrap()
    }

    fn new_event_id(&self) -> EventId {
        SystemEnvironment.new_event_id()
    }

    fn new_observation_id(&self) -> ObservationId {
        SystemEnvironment.new_observation_id()
    }

    fn new_room_id(&self) -> RoomId {
        SystemEnvironment.new_room_id()
    }
}
