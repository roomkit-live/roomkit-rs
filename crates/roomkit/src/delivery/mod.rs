//! Proactive delivery and what providers report of it (RFC §10.4, §22),
//! Python's `roomkit.delivery`: a delivery executed with its hooks
//! ([`execute_delivery`]), its strategies, its publication into the room
//! and its outcome; the persistent queue behind the kit, its in-memory
//! backend and its worker ([`run_worker_loop`]), a strategy's serialized
//! form ([`serialize_strategy`], [`deserialize_strategy`]); and the room a
//! delivery status belongs to.

pub(crate) mod backend;
pub(crate) mod execute;
pub(crate) mod item;
pub(crate) mod memory;
pub(crate) mod outcome;
pub(crate) mod queued;
mod serialization;
pub(crate) mod status;
pub(crate) mod strategy;
pub(crate) mod targets;
mod worker;

use roomkit_core::rules::publications::{self, PUBLICATION_KEY_PREFIX};

// Python's `roomkit.delivery`; the rest of the area is the crate root's,
// as Python's `roomkit`.
pub use self::execute::{ExecuteDeliveryOptions, execute_delivery};
pub use self::serialization::{deserialize_strategy, serialize_strategy};
pub use self::worker::{RunWorkerLoopOptions, run_worker_loop};
use crate::error::RoomKitError;

/// Refuses `key` in the framework's publication namespace (RFC §22.1): a
/// door that stores a caller's event as given (`send_event`, `post`,
/// `commit_event`) never writes one, which would stand in for a
/// publication. Python's `refuse_reserved`, its `ValueError`.
pub(crate) fn refuse_reserved(key: Option<&str>) -> Result<(), RoomKitError> {
    match key {
        Some(key) if publications::is_reserved(key) => Err(RoomKitError::Value(format!(
            "The idempotency_key namespace {PUBLICATION_KEY_PREFIX} is the framework's: it \
             keys deliver()'s publications (RFC §22.1)"
        ))),
        Some(_) | None => Ok(()),
    }
}
