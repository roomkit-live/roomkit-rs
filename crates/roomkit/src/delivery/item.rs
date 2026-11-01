//! The unit of work of a delivery queue (RFC §22.2): Python's
//! `DeliveryItem`, `DeliveryItemStatus` and `retry_delay`.

use std::time::Duration;

use roomkit_core::model::{ChannelId, Metadata, RoomId, Timestamp};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::outcome::DeliveryOutcome;
use crate::error::RoomKitError;

/// Where an item of a delivery queue stands: Python's `DeliveryItemStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryItemStatus {
    /// Waiting in the queue, or out a retry delay.
    #[default]
    Pending,
    /// Claimed by a worker.
    InProgress,
    /// Executed and sent.
    Delivered,
    /// Failed.
    Failed,
    /// Moved to the dead-letter queue: retries exhausted, or a refusal no
    /// retry overcomes.
    DeadLetter,
    /// Executed and refused.
    Blocked,
    /// Executed, its outcome unknown.
    Unknown,
}

/// A serializable delivery request, the unit of work of a delivery queue
/// (RFC §22.2): Python's `DeliveryItem(*, id=<factory>, room_id, content,
/// channel_id=None, addressed_to=None, idempotency_key=None,
/// session_id=None, chain_depth=0, instruction=False, strategy=<factory>,
/// metadata=<factory>, retry_count=0, max_retries=3, created_at=<factory>,
/// status=PENDING, worker_id=None, error=None, outcome=None)`. It keeps the
/// request's address, key, session and intent across retries, and its
/// strategy in its serialized form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeliveryItem {
    /// Its id, a fresh hex one by default.
    pub id: String,
    /// The room it delivers into.
    pub room_id: RoomId,
    /// Its text.
    pub content: String,
    /// The transport it names.
    #[serde(default)]
    pub channel_id: Option<ChannelId>,
    /// The intelligence channels asked to act (RFC §19.3).
    #[serde(default)]
    pub addressed_to: Option<Vec<ChannelId>>,
    /// The key naming its publication in the room (RFC §22.1).
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// The realtime session it names.
    #[serde(default)]
    pub session_id: Option<String>,
    /// The chain it continues (RFC §8.3).
    #[serde(default)]
    pub chain_depth: u32,
    /// Whether it is the application's direction to an agent (RFC §10.1.1).
    #[serde(default)]
    pub instruction: bool,
    /// Its strategy in its serialized form, `{"type": ..., "params": {...}}`
    /// (`serialize_strategy`).
    #[serde(default = "immediate")]
    pub strategy: Metadata,
    /// The metadata of the event it publishes.
    #[serde(default)]
    pub metadata: Metadata,
    /// The retries it took after its first attempt.
    #[serde(default)]
    pub retry_count: u32,
    /// The retries it may take after its first attempt, as a binding's
    /// `RetryPolicy` counts them (RFC §13.2): 3 means four attempts at most.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// When it was made.
    pub created_at: Timestamp,
    /// Where it stands.
    #[serde(default)]
    pub status: DeliveryItemStatus,
    /// The worker that claimed it.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// The last error it met.
    #[serde(default)]
    pub error: Option<String>,
    /// What its last execution came to.
    #[serde(default)]
    pub outcome: Option<DeliveryOutcome>,
}

/// Python's `max_retries=3`.
const MAX_RETRIES: u32 = 3;

fn default_max_retries() -> u32 {
    MAX_RETRIES
}

/// The immediate strategy's serialized form, an item's by default.
pub(crate) fn immediate() -> Metadata {
    let mut strategy = Metadata::new();
    strategy.insert("type".to_owned(), Value::from("immediate"));
    strategy.insert("params".to_owned(), Value::Object(serde_json::Map::new()));
    strategy
}

impl DeliveryItem {
    /// The request to deliver `content` into `room_id`, under a fresh id,
    /// made now, with Python's defaults for the rest.
    pub fn new(room_id: RoomId, content: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().simple().to_string(),
            room_id,
            content: content.into(),
            channel_id: None,
            addressed_to: None,
            idempotency_key: None,
            session_id: None,
            chain_depth: 0,
            instruction: false,
            strategy: immediate(),
            metadata: Metadata::new(),
            retry_count: 0,
            max_retries: MAX_RETRIES,
            created_at: SystemEnvironment.now(),
            status: DeliveryItemStatus::Pending,
            worker_id: None,
            error: None,
            outcome: None,
        }
    }

    /// The name of its strategy, `immediate` for one that names none.
    pub(crate) fn strategy_name(&self) -> String {
        self.strategy
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("immediate")
            .to_owned()
    }
}

/// The wait before retry number `retry_count` (from 1), doubling from
/// `base` up to `maximum`: the formula of a binding's `RetryPolicy` (RFC
/// §13.2) with its exponential base of 2, Python's `retry_delay`. The
/// exponent stops at 64, past any cap, so no retry count overflows it.
pub(crate) fn retry_delay(retry_count: u32, base: Duration, maximum: Duration) -> Duration {
    let exponent = retry_count.saturating_sub(1).min(64);
    let factor = 2.0_f64.powi(i32::try_from(exponent).unwrap_or(64));
    let delay = base.as_secs_f64() * factor;
    Duration::try_from_secs_f64(delay)
        .unwrap_or(maximum)
        .min(maximum)
}

/// Refuses retry delays out of order: Python's `check_retry_delays`, its
/// `ValueError`. A `Duration` is never negative nor infinite.
pub(crate) fn check_retry_delays(base: Duration, maximum: Duration) -> Result<(), RoomKitError> {
    if maximum < base {
        return Err(RoomKitError::Value(
            "retry delays must be finite, with 0 <= retry_delay <= max_retry_delay".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn room() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    // Python: tests/delivery/test_base.py::TestDeliveryItem::test_defaults
    #[test]
    fn test_defaults() {
        let item = DeliveryItem::new(room(), "hello");
        assert_eq!(item.room_id, room());
        assert_eq!(item.content, "hello");
        assert_eq!(item.channel_id, None);
        assert_eq!(
            Value::Object(item.strategy.clone().into_iter().collect()),
            json!({"type": "immediate", "params": {}})
        );
        assert!(item.metadata.is_empty());
        assert_eq!((item.retry_count, item.max_retries), (0, 3));
        assert_eq!(item.status, DeliveryItemStatus::Pending);
        assert_eq!((item.worker_id, item.error), (None, None));
        assert_eq!(item.id.len(), 32);
    }

    // Python: tests/delivery/test_base.py::TestDeliveryItem::test_unique_ids
    #[test]
    fn test_unique_ids() {
        assert_ne!(
            DeliveryItem::new(room(), "a").id,
            DeliveryItem::new(room(), "b").id
        );
    }

    // Python: tests/delivery/test_base.py::TestDeliveryItem::test_serialization_roundtrip
    // and test_json_roundtrip.
    #[test]
    fn test_serialization_roundtrip() {
        let item = DeliveryItem {
            channel_id: Some(ChannelId::new("ch1").unwrap()),
            strategy: serde_json::from_value(
                json!({"type": "wait_for_idle", "params": {"buffer": 2.0}}),
            )
            .unwrap(),
            metadata: serde_json::from_value(json!({"key": "value"})).unwrap(),
            ..DeliveryItem::new(room(), "hello")
        };
        let written = serde_json::to_string(&item).unwrap();
        let restored: DeliveryItem = serde_json::from_str(&written).unwrap();
        assert_eq!(restored, item);
        let minimal: DeliveryItem = serde_json::from_value(json!({
            "id": "abc", "room_id": "r1", "content": "test", "created_at": "2026-11-01T00:00:00Z",
        }))
        .unwrap();
        assert_eq!(minimal.strategy, immediate());
        assert_eq!(minimal.max_retries, 3);
    }

    // Python: tests/delivery/test_memory.py::TestRetryDelay::test_each_retry_waits_twice_as_long_up_to_the_cap
    #[test]
    fn test_each_retry_waits_twice_as_long_up_to_the_cap() {
        let seconds = |n| Duration::from_secs(n);
        let delays: Vec<_> = (1..=4)
            .map(|n| retry_delay(n, seconds(1), seconds(5)))
            .collect();
        assert_eq!(delays, [seconds(1), seconds(2), seconds(4), seconds(5)]);
        assert_eq!(retry_delay(5000, seconds(1), seconds(60)), seconds(60));
    }

    // Python: tests/delivery/test_memory.py::TestRetryDelay::test_bad_delays_are_refused,
    // its delay longer than its cap: a Duration is never negative nor
    // infinite, the two other cases.
    #[test]
    fn test_bad_delays_are_refused() {
        let refused = check_retry_delays(Duration::from_secs(2), Duration::from_secs(1));
        assert!(
            matches!(refused, Err(RoomKitError::Value(message)) if message.contains("retry delays"))
        );
        assert!(check_retry_delays(Duration::ZERO, Duration::ZERO).is_ok());
    }
}
