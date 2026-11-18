//! The two result forms of a tool call on the turn link.
use roomkit_core::model::Metadata;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;

/// A call's result as a tool-call end carries it: a JSON value, or the
/// content parts the Python implementation's tools may answer with.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum WireResult {
    /// The tool returned an ordered list of content parts.
    Parts {
        /// Structured content parts returned by the tool.
        parts: Vec<Value>,
    },
    /// The tool returned a JSON value.
    Value {
        /// Value returned by the operation.
        value: Value,
    },
}

impl From<Value> for WireResult {
    fn from(value: Value) -> Self {
        Self::Value { value }
    }
}

impl<'de> Deserialize<'de> for WireResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::read(Metadata::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl WireResult {
    /// Strict interpretation of a hook's replacement. Unknown fields make a mapping
    /// ordinary data. The wire decoder is more permissive when a sender already named parts.
    pub(crate) fn strict_parts(items: &[Value]) -> Option<Vec<Value>> {
        if items.is_empty() {
            return None;
        }
        items
            .iter()
            .map(|item| {
                let fields = item.as_object()?;
                fields.get("type")?;
                let part = content_part(item.clone()).ok()?;
                fields
                    .keys()
                    .all(|key| part.get(key).is_some())
                    .then_some(part)
            })
            .collect()
    }

    /// Python's `decode_result` reads parts first, including when a value is also present.
    pub(in crate::turns) fn read(mut record: Metadata) -> Result<Self, ResultError> {
        if let Some(parts) = record.shift_remove("parts") {
            return Ok(Self::Parts {
                parts: content_parts(parts)?,
            });
        }
        record
            .shift_remove("value")
            .map(Self::from)
            .ok_or(ResultError::MissingValue)
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::turns) enum ResultError {
    #[error("missing tool result key 'value'")]
    MissingValue,
    #[error("tool result parts are not iterable")]
    NotIterable,
    #[error("a tool result content part must be an object")]
    NotAnObject,
    #[error("invalid tool result content part: {0}")]
    InvalidPart(serde_json::Error),
}

impl ResultError {
    pub(in crate::turns) fn kind(&self) -> &'static str {
        match self {
            Self::MissingValue => "KeyError",
            Self::NotIterable => "TypeError",
            Self::NotAnObject => "AttributeError",
            Self::InvalidPart(_) => "ValidationError",
        }
    }
}

fn content_parts(parts: Value) -> Result<Vec<Value>, ResultError> {
    // Python iterates the supplied collection. Empty strings and objects have
    // no elements; nonempty ones yield strings rather than content records.
    match parts {
        Value::Array(parts) => parts.into_iter().map(content_part).collect(),
        Value::String(text) if text.is_empty() => Ok(Vec::new()),
        Value::Object(fields) if fields.is_empty() => Ok(Vec::new()),
        Value::String(_) | Value::Object(_) => Err(ResultError::NotAnObject),
        Value::Null | Value::Bool(_) | Value::Number(_) => Err(ResultError::NotIterable),
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentPart {
    Text {
        text: String,
    },
    Image {
        url: String,
        mime_type: Option<String>,
    },
}

fn content_part(part: Value) -> Result<Value, ResultError> {
    let Value::Object(mut fields) = part else {
        return Err(ResultError::NotAnObject);
    };
    // The Python decoder selects the image model for a part without a type.
    fields.entry("type").or_insert_with(|| Value::from("image"));
    let normalized: ContentPart =
        serde_json::from_value(Value::Object(fields)).map_err(ResultError::InvalidPart)?;
    serde_json::to_value(normalized).map_err(ResultError::InvalidPart)
}
