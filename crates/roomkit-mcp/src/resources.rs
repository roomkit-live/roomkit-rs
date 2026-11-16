//! Validate the known Python resource model without discarding extension JSON.
use roomkit::model::Metadata;
use serde::Deserialize;
use serde_json::Value;

use crate::McpError;

#[derive(Deserialize)]
struct Resource {
    contents: Vec<Content>,
    _meta: Option<Metadata>,
}

#[derive(Deserialize)]
struct Content {
    uri: String,
    #[serde(rename = "mimeType")]
    _mime_type: Option<String>,
    _meta: Option<Metadata>,
    #[serde(flatten)]
    other: Metadata,
}

pub(crate) fn invalid() -> McpError {
    McpError::Validation {
        reason: "expected valid URI and resource contents",
    }
}

pub(crate) fn uri(value: &str) -> Result<String, McpError> {
    url::Url::parse(value)
        .map(Into::into)
        .map_err(|_| invalid())
}

pub(crate) fn result(mut result: Metadata) -> Result<Metadata, McpError> {
    let parsed: Resource =
        serde_json::from_value(Value::Object(result.clone())).map_err(|_| invalid())?;
    let contents = result
        .get_mut("contents")
        .and_then(Value::as_array_mut)
        .ok_or_else(invalid)?;
    for (value, content) in contents.iter_mut().zip(parsed.contents) {
        // Python's union accepts either text or blob; the other member, when
        // present too, is an ordinary extension of the selected model.
        if !["text", "blob"]
            .iter()
            .any(|key| content.other.get(*key).is_some_and(Value::is_string))
        {
            return Err(invalid());
        }
        value
            .as_object_mut()
            .ok_or_else(invalid)?
            .insert("uri".into(), Value::String(uri(&content.uri)?));
    }
    Ok(result)
}
