//! Tool declarations a provider sends (RFC §6.7).

use serde_json::{Value, json};

use crate::tools::AiTool;

/// The schema to declare for a tool. The returned object owns all its fields.
///
/// A missing or empty schema takes no arguments. An object without a type
/// gets the object type. An explicitly typed schema keeps its type.
pub fn declared_parameters(parameters: Option<&Value>) -> Value {
    let Some(parameters) = parameters.filter(|value| !value.is_null()) else {
        return json!({"type":"object","properties":{}});
    };
    let Some(fields) = parameters.as_object() else {
        return parameters.clone();
    };
    if fields.is_empty() {
        return json!({"type":"object","properties":{}});
    }
    let mut fields = fields.clone();
    if fields.get("type").is_none_or(Value::is_null) {
        fields.insert("type".into(), "object".into());
    }
    Value::Object(fields)
}

/// Chat Completions function declarations in the order the caller supplied.
pub fn chat_tool_declarations(tools: &[AiTool]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({"type":"function","function":{
                "name":tool.name, "description":tool.description,
                "parameters":declared_parameters(Some(&tool.parameters))
            }})
        })
        .collect()
}

/// Whether at least one provider accepts the characters in this tool name.
pub fn some_vendor_accepts_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b':' | b'-'))
}
