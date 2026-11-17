//! The projection Python's `tools.compose._definition` makes of a schema.

use roomkit::model::Metadata;
use serde_json::Value;

use super::AiTool;
use crate::AiError;
use crate::turn_settings::lax_bool;

pub(super) fn schema(schema: &Metadata) -> Result<AiTool, AiError> {
    // Python indexes name before constructing the tags list, and constructs
    // that list before AITool validates the name and the other fields.
    let name = schema
        .get("name")
        .ok_or_else(|| refused("name", "required field is missing"))?;
    let tags = tags(schema.get("tags"))?;
    let name = text(name, "name")?;
    AiTool::checked(name)?;
    let description = schema
        .get("description")
        .map(|value| text(value, "description"))
        .transpose()?
        .unwrap_or("");
    let parameters = schema
        .get("parameters")
        .cloned()
        .unwrap_or_else(|| Value::Object(Metadata::new()));
    check_parameters(&parameters)?;
    let changes_state = schema
        .get("changes_state")
        .map(|value| lax_bool(value).map_err(|error| refused("changes_state", error.to_string())))
        .transpose()?
        .unwrap_or(false);
    Ok(AiTool {
        name: name.to_owned(),
        description: description.to_owned(),
        parameters,
        tags,
        changes_state,
        defer_loading: false,
    })
}

pub(super) fn tool(mut definition: AiTool) -> Result<AiTool, AiError> {
    AiTool::checked(&definition.name)?;
    check_parameters(&definition.parameters)?;
    definition.defer_loading = false;
    Ok(definition)
}

fn text<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, AiError> {
    value
        .as_str()
        .ok_or_else(|| refused(field, "expected a string"))
}

fn check_parameters(value: &Value) -> Result<(), AiError> {
    if value.is_object() {
        Ok(())
    } else {
        Err(refused("parameters", "expected an object"))
    }
}

/// Python applies `list(tags or [])`: null and false values become empty,
/// strings contribute characters, and mappings contribute keys in input order.
fn tags(value: Option<&Value>) -> Result<Vec<String>, AiError> {
    match value {
        None | Some(Value::Null | Value::Bool(false)) => Ok(Vec::new()),
        Some(Value::Number(number)) if number.as_f64() == Some(0.0) => Ok(Vec::new()),
        Some(Value::String(text)) => Ok(text.chars().map(|c| c.to_string()).collect()),
        Some(Value::Object(fields)) => Ok(fields.keys().cloned().collect()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| text(item, "tags").map(str::to_owned))
            .collect(),
        Some(Value::Bool(true) | Value::Number(_)) => {
            Err(refused("tags", "expected an iterable of strings"))
        }
    }
}

fn refused(field: &'static str, reason: impl Into<String>) -> AiError {
    AiError::ToolDefinition {
        field,
        reason: reason.into(),
    }
}
