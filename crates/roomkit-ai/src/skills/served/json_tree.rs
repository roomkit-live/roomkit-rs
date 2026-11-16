//! Operations on owned frontmatter without recursion on its containers.

use std::fmt;

use roomkit::model::Metadata;
use serde_json::Value;

use crate::memory::repr::repr;
use crate::skills::texts::quoted_name;

/// Clone only leaves; the work stack borrows slots whose parent never moves again.
pub(super) fn copy_map(source: &Metadata) -> Metadata {
    let mut copied = empty_map(source);
    let mut pending: Vec<_> = source.values().zip(copied.values_mut()).collect();
    while let Some((source, target)) = pending.pop() {
        match source {
            Value::Array(items) => {
                *target = Value::Array(vec![Value::Null; items.len()]);
                if let Value::Array(copies) = target {
                    pending.extend(items.iter().zip(copies.iter_mut()));
                }
            }
            Value::Object(fields) => {
                *target = Value::Object(empty_map(fields));
                if let Value::Object(copies) = target {
                    pending.extend(fields.values().zip(copies.values_mut()));
                }
            }
            leaf @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
                *target = leaf.clone();
            }
        }
    }
    copied
}

fn empty_map(source: &Metadata) -> Metadata {
    source
        .keys()
        .map(|key| (key.clone(), Value::Null))
        .collect()
}

/// Move descendants out before their ordinary container destructors run.
pub(super) fn drop_map(map: Metadata) {
    let mut pending: Vec<_> = map.into_values().collect();
    while let Some(value) = pending.pop() {
        match value {
            Value::Array(mut items) => pending.append(&mut items),
            Value::Object(fields) => pending.extend(fields.into_values()),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
}

pub(super) fn maps_equal(left: &Metadata, right: &Metadata) -> bool {
    let mut pending = Vec::new();
    if !pair_fields(left, right, &mut pending) {
        return false;
    }
    while let Some((left, right)) = pending.pop() {
        match (left, right) {
            (Value::Array(left), Value::Array(right)) if left.len() == right.len() => {
                pending.extend(left.iter().zip(right));
            }
            (Value::Object(left), Value::Object(right)) => {
                if !pair_fields(left, right, &mut pending) {
                    return false;
                }
            }
            (Value::Bool(_) | Value::Number(_), Value::Bool(_) | Value::Number(_)) => {
                if !numbers_equal(number(left), number(right)) {
                    return false;
                }
            }
            (Value::String(left), Value::String(right)) if left == right => {}
            (Value::Null, Value::Null) => {}
            _ => return false,
        }
    }
    true
}

fn pair_fields<'a>(
    left: &'a Metadata,
    right: &'a Metadata,
    pending: &mut Vec<(&'a Value, &'a Value)>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for (key, value) in left {
        let Some(other) = right.get(key) else {
            return false;
        };
        pending.push((value, other));
    }
    true
}

enum Number {
    Integer(i128),
    Float(f64),
}

fn number(value: &Value) -> Option<Number> {
    match value {
        Value::Bool(value) => Some(Number::Integer(i128::from(*value))),
        Value::Number(value) => value
            .as_i64()
            .map(|n| Number::Integer(i128::from(n)))
            .or_else(|| value.as_u64().map(|n| Number::Integer(i128::from(n))))
            .or_else(|| value.as_f64().map(Number::Float)),
        Value::Null | Value::String(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

#[expect(
    clippy::float_cmp,
    reason = "Python numeric equality is exact, including signed zero"
)]
fn numbers_equal(left: Option<Number>, right: Option<Number>) -> bool {
    match (left, right) {
        (Some(Number::Integer(left)), Some(Number::Integer(right))) => left == right,
        (Some(Number::Float(left)), Some(Number::Float(right))) => left == right,
        (Some(Number::Integer(integer)), Some(Number::Float(float)))
        | (Some(Number::Float(float)), Some(Number::Integer(integer))) => {
            whole(float) == Some(integer)
        }
        _ => false,
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "The integral float is checked within the JSON integer range before conversion"
)]
fn whole(value: f64) -> Option<i128> {
    if value.fract() != 0.0
        || !(-9_223_372_036_854_775_808.0..18_446_744_073_709_551_616.0).contains(&value)
    {
        return None;
    }
    Some(value as i128)
}

enum Piece<'a> {
    Value(&'a Value),
    Literal(&'static str),
    Key(&'a str),
}

/// The string accessors use str at the root, repr for nested strings.
pub(super) fn text_of(value: &Value) -> String {
    if let Value::String(text) = value {
        return text.clone();
    }
    render(vec![Piece::Value(value)], String::new())
}

fn render(mut pending: Vec<Piece<'_>>, mut text: String) -> String {
    while let Some(piece) = pending.pop() {
        match piece {
            Piece::Literal(literal) => text.push_str(literal),
            Piece::Key(key) => text.push_str(&quoted_name(key)),
            Piece::Value(Value::Array(items)) => {
                text.push('[');
                pending.push(Piece::Literal("]"));
                for (index, item) in items.iter().enumerate().rev() {
                    pending.push(Piece::Value(item));
                    if index > 0 {
                        pending.push(Piece::Literal(", "));
                    }
                }
            }
            Piece::Value(Value::Object(fields)) => {
                text.push('{');
                push_fields(fields, &mut pending);
            }
            Piece::Value(leaf) => text.push_str(&repr(leaf)),
        }
    }
    text
}

fn push_fields<'a>(fields: &'a Metadata, pending: &mut Vec<Piece<'a>>) {
    pending.push(Piece::Literal("}"));
    for (index, (key, value)) in fields.iter().enumerate().rev() {
        pending.push(Piece::Value(value));
        pending.push(Piece::Literal(": "));
        pending.push(Piece::Key(key));
        if index > 0 {
            pending.push(Piece::Literal(", "));
        }
    }
}

pub(super) struct MapDebug<'a>(pub &'a Metadata);

impl fmt::Debug for MapDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut pending = Vec::new();
        push_fields(self.0, &mut pending);
        f.write_str(&render(pending, "{".to_owned()))
    }
}
