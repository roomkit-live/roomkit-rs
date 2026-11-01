//! A strategy in the serialized form a delivery queue keeps it in (RFC
//! §22.2), and back: Python's `serialize_strategy` and
//! `deserialize_strategy`.

use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::Metadata;
use serde_json::Value;

use super::item::immediate;
use super::queued::{Queued, QueuedOptions};
use super::strategy::{DeliveryStrategy, Immediate, WaitForIdle, WaitForIdleOptions};
use crate::store::ConversationStore;

/// `strategy` as a delivery queue keeps it, `{"type": ..., "params":
/// {...}}`: Python's `serialize_strategy(strategy)`. `None`, and a strategy
/// of no serialized form, a custom one, are `immediate`; the latter is
/// logged.
pub fn serialize_strategy<S: 'static>(strategy: Option<&dyn DeliveryStrategy<S>>) -> Metadata {
    let Some(strategy) = strategy else {
        return immediate();
    };
    match strategy.serialized() {
        Some(Value::Object(serialized)) => serialized.into_iter().collect(),
        Some(_) | None => {
            tracing::warn!(target: "roomkit::delivery", "a strategy with no serialized form is kept as immediate");
            immediate()
        }
    }
}

/// The strategy `data`, a serialized one, names: Python's
/// `deserialize_strategy(data)`. A type it does not know, or none, gives
/// `Immediate`, the former logged; a parameter it does not name takes its
/// default.
pub fn deserialize_strategy<S: ConversationStore>(data: &Metadata) -> Arc<dyn DeliveryStrategy<S>> {
    let params = data.get("params").and_then(Value::as_object);
    let seconds = |name: &str| {
        params
            .and_then(|params| params.get(name))
            .and_then(Value::as_f64)
            .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
    };
    match data
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("immediate")
    {
        "immediate" => Arc::new(Immediate),
        "wait_for_idle" => {
            let defaults = WaitForIdleOptions::default();
            Arc::new(WaitForIdle::new(WaitForIdleOptions {
                buffer: seconds("buffer").unwrap_or(defaults.buffer),
                playback_timeout: seconds("playback_timeout").unwrap_or(defaults.playback_timeout),
            }))
        }
        "queued" => {
            let defaults = QueuedOptions::default();
            let separator = params
                .and_then(|params| params.get("separator"))
                .and_then(Value::as_str)
                .map_or(defaults.separator, str::to_owned);
            Arc::new(Queued::new(QueuedOptions {
                buffer: seconds("buffer").unwrap_or(defaults.buffer),
                playback_timeout: seconds("playback_timeout").unwrap_or(defaults.playback_timeout),
                separator,
            }))
        }
        unknown => {
            tracing::warn!(target: "roomkit::delivery", strategy = %unknown, "an unknown strategy type runs as Immediate");
            Arc::new(Immediate)
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::store::InMemoryStore;

    fn written<S: 'static>(strategy: Option<&dyn DeliveryStrategy<S>>) -> Value {
        Value::Object(serialize_strategy(strategy).into_iter().collect())
    }

    fn read(data: Value) -> Arc<dyn DeliveryStrategy<InMemoryStore>> {
        deserialize_strategy(&serde_json::from_value(data).unwrap())
    }

    fn secs(seconds: f64) -> Duration {
        Duration::from_secs_f64(seconds)
    }

    // Python: tests/delivery/test_serialization.py::TestSerializeStrategy
    // (test_none_returns_immediate, test_immediate, test_wait_for_idle_defaults,
    // test_wait_for_idle_custom, test_queued_defaults, test_queued_custom)
    #[test]
    fn a_strategy_serializes_to_its_type_and_params() {
        let none: Option<&dyn DeliveryStrategy<InMemoryStore>> = None;
        assert_eq!(written(none), json!({"type": "immediate", "params": {}}));
        let immediate: &dyn DeliveryStrategy<InMemoryStore> = &Immediate;
        assert_eq!(
            written(Some(immediate)),
            json!({"type": "immediate", "params": {}})
        );
        let idle: &dyn DeliveryStrategy<InMemoryStore> = &WaitForIdle::default();
        assert_eq!(
            written(Some(idle)),
            json!({"type": "wait_for_idle", "params": {"buffer": 1.0, "playback_timeout": 15.0}})
        );
        let custom = WaitForIdle::new(WaitForIdleOptions {
            buffer: secs(3.0),
            playback_timeout: secs(30.0),
        });
        let custom: &dyn DeliveryStrategy<InMemoryStore> = &custom;
        assert_eq!(
            written(Some(custom))["params"],
            json!({"buffer": 3.0, "playback_timeout": 30.0})
        );
        let queued: &dyn DeliveryStrategy<InMemoryStore> = &Queued::<InMemoryStore>::default();
        assert_eq!(
            written(Some(queued)),
            json!({"type": "queued", "params": {"buffer": 1.0, "playback_timeout": 15.0, "separator": "\n\n"}})
        );
        let queued = Queued::<InMemoryStore>::new(QueuedOptions {
            buffer: secs(2.0),
            separator: " | ".to_owned(),
            ..QueuedOptions::default()
        });
        let queued: &dyn DeliveryStrategy<InMemoryStore> = &queued;
        let params = written(Some(queued))["params"].clone();
        assert_eq!(
            (params["buffer"].clone(), params["separator"].clone()),
            (json!(2.0), json!(" | "))
        );
    }

    // Python: tests/delivery/test_serialization.py::TestDeserializeStrategy and
    // TestRoundtrip: each type read back with its params; an unknown type and
    // none give Immediate.
    #[test]
    fn a_serialized_strategy_reads_back() {
        let idle = read(
            json!({"type": "wait_for_idle", "params": {"buffer": 2.5, "playback_timeout": 20.0}}),
        );
        assert_eq!(
            written(Some(&*idle))["params"],
            json!({"buffer": 2.5, "playback_timeout": 20.0})
        );
        let queued = read(
            json!({"type": "queued", "params": {"buffer": 1.0, "playback_timeout": 15.0, "separator": "---"}}),
        );
        assert_eq!(written(Some(&*queued))["params"]["separator"], json!("---"));
        for data in [
            json!({"type": "unknown_strategy", "params": {}}),
            json!({}),
            json!({"type": "immediate", "params": {}}),
        ] {
            assert_eq!(
                written(Some(&*read(data))),
                json!({"type": "immediate", "params": {}})
            );
        }
        let original = Queued::<InMemoryStore>::new(QueuedOptions {
            buffer: secs(2.0),
            separator: " // ".to_owned(),
            ..QueuedOptions::default()
        });
        let original: &dyn DeliveryStrategy<InMemoryStore> = &original;
        let restored = deserialize_strategy::<InMemoryStore>(&serialize_strategy(Some(original)));
        assert_eq!(
            written(Some(&*restored))["params"]["separator"],
            json!(" // ")
        );
        assert_eq!(written(Some(&*restored))["params"]["buffer"], json!(2.0));
    }
}
