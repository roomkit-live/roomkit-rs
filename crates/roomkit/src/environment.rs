//! The environment a kit gives its operations: the system clock and random
//! identifiers.

use std::time::{SystemTime, UNIX_EPOCH};

use roomkit_core::model::{EventId, ObservationId, RoomId, Timestamp};
use roomkit_core::ops::Environment;

/// The system clock, at millisecond precision, and random version 4 UUIDs as
/// ids.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemEnvironment;

impl Environment for SystemEnvironment {
    fn now(&mut self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .and_then(|millis| Timestamp::from_unix_millis(millis).ok());
        millis.unwrap_or_else(|| {
            tracing::error!(
                target: "roomkit::environment",
                "the system clock is outside the supported range; using the Unix epoch"
            );
            Timestamp::UNIX_EPOCH
        })
    }

    fn new_event_id(&mut self) -> EventId {
        EventId::from_u128(uuid::Uuid::new_v4().as_u128())
    }

    fn new_observation_id(&mut self) -> ObservationId {
        ObservationId::from_u128(uuid::Uuid::new_v4().as_u128())
    }

    fn new_room_id(&mut self) -> RoomId {
        RoomId::from_u128(uuid::Uuid::new_v4().as_u128())
    }
}
