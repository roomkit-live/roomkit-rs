//! A callback's isolated changes, merged under the record's existing lock.

use super::{Metadata, ResponseMetadata, Value};

impl ResponseMetadata {
    /// Internal owner-call delta against the JSON snapshot the call carried
    /// (RFC §10.6.5). The returned data owns every value independently.
    #[doc(hidden)]
    pub fn call_changes(&self, carried: &Metadata) -> (Metadata, Vec<String>) {
        let inner = self.lock();
        let written = inner
            .data
            .iter()
            .filter(|(key, value)| carried.get(*key).is_none_or(|old| !same(old, value)))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let removed = carried
            .keys()
            .filter(|key| !inner.data.contains_key(*key))
            .cloned()
            .collect();
        (written, removed)
    }

    /// Apply an owner callback's completed reply before its caller resumes.
    /// Mapping entries are merged against what that call carried, never
    /// against another callback's snapshot (RFC §10.6.5).
    #[doc(hidden)]
    pub fn apply_call_changes(&self, carried: &Metadata, written: &Metadata, removed: &[String]) {
        let mut inner = self.lock();
        for key in removed {
            if inner.data.shift_remove(key).is_some() {
                inner.writes += 1;
                inner.wrote(|changed| changed == key);
            }
        }
        for (key, value) in written {
            let value = merged(inner.data.get(key), carried.get(key), value);
            inner.data.insert(key.clone(), value);
            inner.writes += 1;
            inner.wrote(|changed| changed == key);
        }
    }
}

fn merged(current: Option<&Value>, carried: Option<&Value>, value: &Value) -> Value {
    let (Some(Value::Object(current)), Value::Object(value)) = (current, value) else {
        return value.clone();
    };
    let base = match carried {
        Some(Value::Object(base)) => Some(base),
        None | Some(Value::Null) => None,
        Some(_) => return Value::Object(value.clone()),
    };
    let mut merged = current.clone();
    for (entry, item) in value {
        if base
            .and_then(|base| base.get(entry))
            .is_none_or(|old| !same(old, item))
        {
            merged.insert(entry.clone(), item.clone());
        }
    }
    if let Some(base) = base {
        for entry in base.keys().filter(|entry| !value.contains_key(*entry)) {
            merged.shift_remove(entry);
        }
    }
    Value::Object(merged)
}

/// Python compares carried JSON values with its ordinary equality: bools
/// compare as 0/1 and integral floats may equal integers. Compare integers
/// exactly so values beyond f64's consecutive-integer range stay distinct.
fn same(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, value)| right.get(key).is_some_and(|item| same(value, item)))
        }
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len() && left.iter().zip(right).all(|(a, b)| same(a, b))
        }
        _ => match (number(left), number(right)) {
            (Some(left), Some(right)) => equal_number(left, right),
            _ => left == right,
        },
    }
}

enum Number {
    Integer(i128),
    Float(f64),
}

fn number(value: &Value) -> Option<Number> {
    match value {
        Value::Bool(value) => Some(Number::Integer(i128::from(*value))),
        Value::Number(number) => number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(i128::from))
            .map(Number::Integer)
            .or_else(|| number.as_f64().map(Number::Float)),
        Value::Null | Value::String(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

#[expect(
    clippy::float_cmp,
    reason = "the source uses exact value equality, not a tolerance"
)]
fn equal_number(left: Number, right: Number) -> bool {
    match (left, right) {
        (Number::Integer(left), Number::Integer(right)) => left == right,
        (Number::Float(left), Number::Float(right)) => left == right,
        (Number::Float(float), Number::Integer(integer))
        | (Number::Integer(integer), Number::Float(float)) => float_is_integer(float, integer),
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "finite, integral JSON integer range checked first"
)]
fn float_is_integer(float: f64, integer: i128) -> bool {
    // JSON's Rust integer representations cover i64::MIN through u64::MAX.
    // The exclusive upper bound is exactly representable as f64 (2**64).
    (-9_223_372_036_854_775_808.0..18_446_744_073_709_551_616.0).contains(&float)
        && float.fract() == 0.0
        && float as i128 == integer
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use serde_json::json;

    use super::*;

    fn keys(value: Value) -> Metadata {
        value.as_object().expect("object").clone()
    }

    #[test]
    fn an_unchanged_json_value_leaves_no_delta() {
        let base = keys(json!({"zero": 0, "one": 1, "nested": [true, {"count": 2}]}));
        let worked = ResponseMetadata::from(keys(json!({
            "zero": false, "one": 1.0, "nested": [1, {"count": 2.0}]
        })));
        assert_eq!(worked.call_changes(&base), (Metadata::new(), Vec::new()));
        assert!(!same(
            &json!(9_007_199_254_740_993_u64),
            &json!(9_007_199_254_740_992.0)
        ));
        assert!(same(&json!(u64::MAX), &json!(u64::MAX)));
        assert!(!same(
            &json!(u64::MAX),
            &json!(18_446_744_073_709_551_616.0)
        ));
        assert!(same(&json!(i64::MIN), &json!(-9_223_372_036_854_775_808.0)));
    }

    #[test]
    fn replies_keep_changes_absent_from_their_own_snapshots() {
        let base = keys(json!({"names": {"a": 1, "stale": 0}, "old": true, "choice": "old"}));
        let record = ResponseMetadata::from(base.clone());
        let first =
            keys(json!({"names": {"a": 1, "stale": 0, "b": 2}, "new": 1, "choice": "first"}));
        let last = keys(json!({"names": {"c": 3}, "choice": "last"}));
        record.apply_call_changes(&base, &first, &["old".to_owned()]);
        record.apply_call_changes(&base, &last, &[]);
        assert_eq!(
            record.to_metadata(),
            keys(json!({"names": {"b": 2, "c": 3}, "new": 1, "choice": "last"}))
        );
        // One removal and each written top-level key, matching Python's
        // record[key] assignments; entry edits do not count as extra writes.
        assert_eq!(record.writes(), 6);
    }

    #[test]
    fn two_concurrent_replies_keep_both_contributions_to_a_new_mapping() {
        let record = ResponseMetadata::new();
        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            for name in ["a", "b"] {
                let record = &record;
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    let changes = keys(json!({"names": {name: 1}}));
                    barrier.wait();
                    record.apply_call_changes(&Metadata::new(), &changes, &[]);
                });
            }
        });
        assert_eq!(
            record.to_metadata(),
            keys(json!({"names": {"a": 1, "b": 1}}))
        );
        assert_eq!(record.writes(), 2);
    }

    #[test]
    fn mapping_changes_replace_scalar_bases_and_do_not_merge_recursively() {
        let base = keys(json!({"map": 7, "nested": {"item": {"a": 1}}}));
        let record = ResponseMetadata::from(keys(json!({
            "map": {"old": true}, "nested": {"item": {"a": 1, "other": 2}, "kept": true}
        })));
        let written = keys(json!({"map": {"new": true}, "nested": {"item": {"b": 3}}}));
        record.apply_call_changes(&base, &written, &[]);
        assert_eq!(
            record.to_metadata(),
            keys(json!({
                "map": {"new": true}, "nested": {"item": {"b": 3}, "kept": true}
            }))
        );
    }
}
