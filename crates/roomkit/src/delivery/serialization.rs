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
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// `strategy` as a delivery queue keeps it, `{"type": ..., "params":
/// {...}}`: Python's `serialize_strategy(strategy)`. `None`, and a strategy
/// of no serialized form, a custom one, are `immediate`; the latter is
/// logged.
pub fn serialize_strategy<S: 'static>(strategy: Option<&dyn DeliveryStrategy<S>>) -> Metadata {
    let Some(strategy) = strategy else {
        return immediate();
    };
    if let Some(Value::Object(serialized)) = strategy.serialized() {
        return serialized.into_iter().collect();
    }
    tracing::warn!(target: "roomkit::delivery", "a strategy with no serialized form is kept as immediate");
    immediate()
}

/// The strategy `data`, a serialized one, names: Python's
/// `deserialize_strategy(data)`, which builds `cls(**params)`. A type it
/// does not know, or none, gives `Immediate`, the former logged and its
/// params unread; a parameter it does not name takes its default.
///
/// A duration is a number of seconds, one below zero waiting none, as the
/// Python implementation's strategies wait none for it.
///
/// # Errors
///
/// [`RoomKitError::Type`], Python's `TypeError`, for `params` that is no
/// object, a parameter the type takes no such argument for (`immediate`
/// takes none), or one whose value is of the wrong kind: a duration that
/// is no number, a separator that is no string. The Python implementation
/// keeps such a value and fails once it uses it; a strategy here holds a
/// duration and a string only, so it fails when read.
pub fn deserialize_strategy<S: ConversationStore>(
    data: &Metadata,
) -> Result<Arc<dyn DeliveryStrategy<S>>, RoomKitError> {
    let kind = match data.get("type") {
        None => "immediate",
        Some(Value::String(kind)) => kind.as_str(),
        Some(other) => {
            tracing::warn!(target: "roomkit::delivery", strategy = %other, "an unknown strategy type runs as Immediate");
            return Ok(Arc::new(Immediate));
        }
    };
    let (class, names): (&str, &[&str]) = match kind {
        "immediate" => ("Immediate", &[]),
        "wait_for_idle" => ("WaitForIdle", &["buffer", "playback_timeout"]),
        "queued" => ("Queued", &["buffer", "playback_timeout", "separator"]),
        unknown => {
            tracing::warn!(target: "roomkit::delivery", strategy = %unknown, "an unknown strategy type runs as Immediate");
            return Ok(Arc::new(Immediate));
        }
    };
    let params = Params::read(data, class, names)?;
    Ok(match kind {
        "wait_for_idle" => {
            let defaults = WaitForIdleOptions::default();
            Arc::new(WaitForIdle::new(WaitForIdleOptions {
                buffer: params.seconds("buffer")?.unwrap_or(defaults.buffer),
                playback_timeout: params
                    .seconds("playback_timeout")?
                    .unwrap_or(defaults.playback_timeout),
            }))
        }
        "queued" => {
            let defaults = QueuedOptions::default();
            Arc::new(Queued::new(QueuedOptions {
                buffer: params.seconds("buffer")?.unwrap_or(defaults.buffer),
                playback_timeout: params
                    .seconds("playback_timeout")?
                    .unwrap_or(defaults.playback_timeout),
                separator: params.text("separator")?.unwrap_or(defaults.separator),
            }))
        }
        _ => Arc::new(Immediate),
    })
}

/// A serialized strategy's `params`, checked against the arguments its
/// class takes, as Python's `cls(**params)` checks them.
struct Params<'a> {
    class: &'static str,
    params: Option<&'a serde_json::Map<String, Value>>,
}

impl<'a> Params<'a> {
    fn read(data: &'a Metadata, class: &'static str, names: &[&str]) -> Result<Self, RoomKitError> {
        let params = match data.get("params") {
            None => None,
            Some(Value::Object(params)) => Some(params),
            Some(other) => {
                return Err(RoomKitError::Type(format!(
                    "{class}() argument after ** must be a mapping, not {}",
                    python_type(other)
                )));
            }
        };
        let unknown = params
            .into_iter()
            .flat_map(serde_json::Map::keys)
            .find(|name| !names.contains(&name.as_str()));
        match unknown {
            Some(_) if names.is_empty() => {
                Err(RoomKitError::Type(format!("{class}() takes no arguments")))
            }
            Some(name) => Err(RoomKitError::Type(format!(
                "{class}.__init__() got an unexpected keyword argument '{name}'"
            ))),
            None => Ok(Self { class, params }),
        }
    }

    fn value(&self, name: &str) -> Option<&'a Value> {
        self.params.and_then(|params| params.get(name))
    }

    /// The duration `name` gives, in seconds: none below zero, the longest
    /// one past the range.
    fn seconds(&self, name: &str) -> Result<Option<Duration>, RoomKitError> {
        let Some(value) = self.value(name) else {
            return Ok(None);
        };
        let Some(seconds) = value.as_f64() else {
            return Err(self.wrong_kind(name, "a number of seconds", value));
        };
        Ok(Some(
            Duration::try_from_secs_f64(seconds.max(0.0)).unwrap_or(Duration::MAX),
        ))
    }

    fn text(&self, name: &str) -> Result<Option<String>, RoomKitError> {
        match self.value(name) {
            None => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(value) => Err(self.wrong_kind(name, "a str", value)),
        }
    }

    fn wrong_kind(&self, name: &str, wanted: &str, value: &Value) -> RoomKitError {
        RoomKitError::Type(format!(
            "{}() argument '{name}' must be {wanted}, not {}",
            self.class,
            python_type(value)
        ))
    }
}

/// The name of the Python type a JSON value reads as.
fn python_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_f64() => "float",
        Value::Number(_) => "int",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
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
        deserialize_strategy(&serde_json::from_value(data).unwrap()).unwrap()
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
        let restored =
            deserialize_strategy::<InMemoryStore>(&serialize_strategy(Some(original))).unwrap();
        assert_eq!(
            written(Some(&*restored))["params"]["separator"],
            json!(" // ")
        );
        assert_eq!(written(Some(&*restored))["params"]["buffer"], json!(2.0));
    }

    fn refusal(data: Value) -> String {
        match deserialize_strategy::<InMemoryStore>(&serde_json::from_value(data).unwrap()) {
            Err(RoomKitError::Type(message)) => message,
            Err(other) => panic!("not a TypeError: {other:?}"),
            Ok(_) => panic!("read"),
        }
    }

    // Python: delivery/serialization.py's deserialize_strategy, `cls(**params)`:
    // an argument the class does not take raises TypeError, and params that
    // is no mapping; an unknown type reads none of them. A value of the
    // wrong kind is refused when read (Python raises once it uses it); a
    // duration below zero waits none.
    #[test]
    fn params_the_strategy_takes_no_such_argument_for_are_refused() {
        assert_eq!(
            refusal(json!({"type": "wait_for_idle", "params": {"bufer": 2.0}})),
            "WaitForIdle.__init__() got an unexpected keyword argument 'bufer'"
        );
        assert_eq!(
            refusal(json!({"type": "immediate", "params": {"buffer": 1.0}})),
            "Immediate() takes no arguments"
        );
        assert_eq!(
            refusal(json!({"type": "queued", "params": null})),
            "Queued() argument after ** must be a mapping, not NoneType"
        );
        assert_eq!(
            refusal(json!({"type": "queued", "params": {"buffer": "x"}})),
            "Queued() argument 'buffer' must be a number of seconds, not str"
        );
        assert_eq!(
            refusal(json!({"type": "queued", "params": {"separator": 5}})),
            "Queued() argument 'separator' must be a str, not int"
        );
        let unknown = read(json!({"type": "later", "params": {"anything": 1}}));
        assert_eq!(written(Some(&*unknown))["type"], json!("immediate"));
        let negative = read(json!({"type": "wait_for_idle", "params": {"buffer": -1}}));
        assert_eq!(written(Some(&*negative))["params"]["buffer"], json!(0.0));
    }
}
