//! Reading the provider's JSON keeps shape errors distinct from invalid JSON.

use roomkit::ChannelError;
use serde_json::Value;

use super::Thought;
use crate::ProviderError;
use crate::classifiers::error::invalid;

pub(super) fn thought(text: &str) -> Result<Thought, ChannelError> {
    let answer: Value = serde_json::from_str(text).map_err(|error| {
        ProviderError::from(format!("the thought is not JSON: {error}")).with_source(error)
    })?;
    let object = answer
        .as_object()
        .ok_or_else(|| attribute(&answer, "get"))?;
    let mut wanted = Vec::new();
    if let Some(items) = object.get("want_to_say") {
        for item in iterable(items)? {
            let text = stripped(&item)?;
            if !text.is_empty() {
                wanted.push(text);
            }
        }
    }
    wanted.truncate(3);
    let text = object
        .get("text")
        .map(stripped)
        .transpose()?
        .unwrap_or_default();
    Ok(Thought {
        text,
        want_to_say: wanted,
        urgent: object.get("urgent").is_some_and(truthy),
    })
}

fn stripped(value: &Value) -> Result<String, ChannelError> {
    Ok(value
        .as_str()
        .ok_or_else(|| attribute(value, "strip"))?
        .trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
        .to_owned())
}

fn iterable(value: &Value) -> Result<Vec<Value>, ChannelError> {
    match value {
        Value::Array(items) => Ok(items.clone()),
        Value::Object(items) => Ok(items.keys().cloned().map(Value::String).collect()),
        Value::String(text) => Ok(text.chars().map(|c| Value::String(c.to_string())).collect()),
        Value::Null | Value::Bool(_) | Value::Number(_) => Err(invalid(
            "TypeError",
            format!("'{}' object is not iterable", kind(value)),
        )),
    }
}

fn attribute(value: &Value, name: &str) -> ChannelError {
    invalid(
        "AttributeError",
        format!("'{}' object has no attribute '{name}'", kind(value)),
    )
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
        Value::Number(number) => {
            if number.is_f64() {
                "float"
            } else {
                "int"
            }
        }
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}
