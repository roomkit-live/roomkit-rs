//! The kit's lock manager and delivery gap a scenario's `setup.kit` names
//! (RFC §13.5): README, "Setup", `delivery_gap_timeout_ms`.

use std::time::Duration;

use roomkit::RoomKitOptions;
use serde_json::Value;

/// The framework event of a delivery set's gap skip (RFC §8.2, §13.5).
pub(crate) const LOCK_EVENTS: [&str; 1] = ["delivery_skipped"];

/// `options` with the kit's `delivery_gap_timeout` a scenario's
/// `setup.kit` names.
pub(crate) fn kit_options<S>(mut options: RoomKitOptions<S>, kit: &Value) -> RoomKitOptions<S> {
    if let Some(timeout) = kit["delivery_gap_timeout_ms"].as_u64() {
        options.delivery_gap_timeout = Duration::from_millis(timeout);
    }
    options
}
