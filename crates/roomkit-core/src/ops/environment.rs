//! What an operation reads from its host besides effects: the clock and new
//! identifiers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::model::{EventId, ObservationId, RoomId, Timestamp};

/// The clock and the identifiers an operation uses.
///
/// Reading the clock and creating an id are not effects: they never fail and
/// never wait. A driver gives the system clock and random ids; a test gives a
/// fixed clock and sequential ids, so a trace is reproducible. The methods
/// take `&self`: one environment serves the runs of many operations at
/// once, each run keeping the one it started with.
pub trait Environment {
    /// The current instant.
    fn now(&self) -> Timestamp;

    /// A new, unique event id.
    fn new_event_id(&self) -> EventId;

    /// A new, unique observation id.
    fn new_observation_id(&self) -> ObservationId;

    /// A new, unique room id, for the room the framework creates for a
    /// message no room was found for (RFC §10.1 step 2, §10.4).
    fn new_room_id(&self) -> RoomId;
}

/// The system clock, at microsecond precision, and random version 4 UUIDs as
/// ids: the environment of a kit, and of the model's filling constructors
/// (`RoomEvent::new`, `Room::new`, ...), which fill what the Python
/// implementation's model defaults fill. Compiled under the
/// `system-environment` feature only: the core's operations read their
/// clock from the environment their run started with, never this one.
#[cfg(feature = "system-environment")]
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemEnvironment;

#[cfg(feature = "system-environment")]
impl Environment for SystemEnvironment {
    #[expect(
        clippy::disallowed_methods,
        reason = "the system environment is where the kit reads the wall clock"
    )]
    fn now(&self) -> Timestamp {
        // At the microsecond, as Python's `datetime.now(UTC)`: two instants
        // a millisecond apart at most, a message and the request opened
        // after it, read in their order (RFC §11.7.6's window).
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| {
                let millis = i64::try_from(elapsed.as_millis()).ok()?;
                let within = Duration::from_micros(u64::from(elapsed.subsec_micros() % 1000));
                Timestamp::from_unix_millis(millis)
                    .ok()?
                    .checked_add(within)
            });
        micros.unwrap_or_else(|| {
            tracing::error!(
                target: "roomkit::environment",
                "the system clock is outside the supported range; using the Unix epoch"
            );
            Timestamp::UNIX_EPOCH
        })
    }

    fn new_event_id(&self) -> EventId {
        EventId::from_u128(uuid::Uuid::new_v4().as_u128())
    }

    fn new_observation_id(&self) -> ObservationId {
        ObservationId::from_u128(uuid::Uuid::new_v4().as_u128())
    }

    fn new_room_id(&self) -> RoomId {
        RoomId::from_u128(uuid::Uuid::new_v4().as_u128())
    }
}

/// A shared environment: a host keeps a handle on the environment its kit
/// reads, to move a test's clock while the kit runs.
impl<E: Environment + ?Sized> Environment for Arc<E> {
    fn now(&self) -> Timestamp {
        E::now(self)
    }

    fn new_event_id(&self) -> EventId {
        E::new_event_id(self)
    }

    fn new_observation_id(&self) -> ObservationId {
        E::new_observation_id(self)
    }

    fn new_room_id(&self) -> RoomId {
        E::new_room_id(self)
    }
}

/// A deterministic environment, for tests and examples: a clock that moves
/// only when told to, and ids numbered in order (`evt_1`, `evt_2`, `obs_1`,
/// `new_room_1`). Shared, it serves many runs at once: its clock and its
/// counters are atomic, never locked.
#[derive(Debug)]
pub struct SequentialEnvironment {
    start: Timestamp,
    /// How far the clock moved since `start`, in nanoseconds.
    elapsed: AtomicU64,
    events: AtomicU64,
    observations: AtomicU64,
    rooms: AtomicU64,
}

impl SequentialEnvironment {
    /// An environment whose clock reads `now`.
    pub fn new(now: Timestamp) -> Self {
        Self {
            start: now,
            elapsed: AtomicU64::new(0),
            events: AtomicU64::new(0),
            observations: AtomicU64::new(0),
            rooms: AtomicU64::new(0),
        }
    }

    /// Moves the clock `duration` later; past the supported range, or some
    /// 580 years past its start, it stays.
    pub fn advance(&self, duration: Duration) {
        let Ok(nanos) = u64::try_from(duration.as_nanos()) else {
            return;
        };
        let mut elapsed = self.elapsed.load(Ordering::Relaxed);
        loop {
            let in_range = |later: &u64| {
                self.start
                    .checked_add(Duration::from_nanos(*later))
                    .is_some()
            };
            let Some(later) = elapsed.checked_add(nanos).filter(in_range) else {
                return;
            };
            match self.elapsed.compare_exchange_weak(
                elapsed,
                later,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => elapsed = current,
            }
        }
    }
}

impl Clone for SequentialEnvironment {
    /// An environment reading what this one reads now, its clock and its
    /// counters moving on their own from then on.
    fn clone(&self) -> Self {
        let read = |counter: &AtomicU64| AtomicU64::new(counter.load(Ordering::Relaxed));
        Self {
            start: self.start,
            elapsed: read(&self.elapsed),
            events: read(&self.events),
            observations: read(&self.observations),
            rooms: read(&self.rooms),
        }
    }
}

impl Default for SequentialEnvironment {
    /// A clock at the Unix epoch.
    fn default() -> Self {
        Self::new(Timestamp::UNIX_EPOCH)
    }
}

/// The next number of `counter`, from 1.
fn next(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, Ordering::Relaxed).saturating_add(1)
}

impl Environment for SequentialEnvironment {
    fn now(&self) -> Timestamp {
        let elapsed = Duration::from_nanos(self.elapsed.load(Ordering::Relaxed));
        self.start.checked_add(elapsed).unwrap_or(self.start)
    }

    fn new_event_id(&self) -> EventId {
        EventId::generated(format!("evt_{}", next(&self.events)))
    }

    fn new_observation_id(&self) -> ObservationId {
        ObservationId::generated(format!("obs_{}", next(&self.observations)))
    }

    fn new_room_id(&self) -> RoomId {
        RoomId::generated(format!("new_room_{}", next(&self.rooms)))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn event_ids_are_numbered_in_order() {
        let environment = SequentialEnvironment::default();
        let ids = [environment.new_event_id(), environment.new_event_id()];
        assert_eq!(ids.map(|id| id.to_string()), ["evt_1", "evt_2"]);
    }

    #[test]
    fn room_ids_are_numbered_in_order() {
        let environment = SequentialEnvironment::default();
        let ids = [environment.new_room_id(), environment.new_room_id()];
        assert_eq!(ids.map(|id| id.to_string()), ["new_room_1", "new_room_2"]);
    }

    #[test]
    fn the_clock_moves_only_when_advanced() {
        let environment = SequentialEnvironment::default();
        environment.advance(Duration::from_secs(2));
        assert_eq!(environment.now().as_unix_millis(), 2000);
    }
}
