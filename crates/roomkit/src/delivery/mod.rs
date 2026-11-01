//! Proactive delivery and what providers report of it (RFC §10.4, §22):
//! a delivery executed with its hooks, its publication into the room and
//! its outcome, and the room a delivery status belongs to.

pub(crate) mod execute;
mod outcome;
pub(crate) mod status;
pub(crate) mod targets;

use roomkit_core::rules::publications::{self, PUBLICATION_KEY_PREFIX};

pub use self::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
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
