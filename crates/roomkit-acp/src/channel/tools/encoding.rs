//! ACP result text: Python's JSON spacing, without escaping Unicode.

use serde_json::Value;

pub(super) fn result_text(value: &Value) -> String {
    if let Value::String(text) = value {
        return text.clone();
    }
    json_text(value)
}

fn json_text(value: &Value) -> String {
    match value {
        Value::Array(items) => format!(
            "[{}]",
            items.iter().map(json_text).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|(name, value)| {
                    format!("{}: {}", Value::String(name.clone()), json_text(value))
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
            other.to_string()
        }
    }
}

/// Both SDKs dump the same fields. Python's tagged subclasses append the
/// discriminator after their base fields; serde writes it first.
pub(super) fn display_content(mut value: Value) -> Value {
    if let Value::Array(items) = &mut value {
        for item in items {
            tag_last(item);
            if let Some(content) = item.get_mut("content") {
                tag_last(content);
            }
        }
    }
    value
}

fn tag_last(value: &mut Value) {
    if let Value::Object(fields) = value
        && let Some(tag) = fields.shift_remove("type")
    {
        fields.insert("type".to_owned(), tag);
    }
}
