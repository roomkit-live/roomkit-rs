//! A call's arguments checked against its tool's parameters (RFC §21.1,
//! step 4 decision 7), as the reference implementation's
//! `validate_tool_arguments` checks them: the names `required` lists, the
//! type of each argument whose top-level property declares a primitive
//! one, and, on a schema that closes itself (`additionalProperties:
//! false`), no argument it does not declare. Nothing deeper is checked.

use roomkit::model::Metadata;
use serde_json::Value;

/// Why `arguments` do not fit `parameters`, a JSON Schema object, if they
/// do not: the first required argument missing, then the first argument
/// a closed schema does not declare, named with the ones it does, so that
/// the model can correct its call, or of another type than its property
/// declares.
pub(crate) fn mismatch(parameters: &Value, arguments: &Metadata) -> Option<String> {
    let required = parameters
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    for name in required {
        if !arguments.contains_key(name) {
            return Some(format!("missing required argument '{name}'"));
        }
    }
    let properties = parameters.get("properties").and_then(Value::as_object)?;
    let closed = parameters.get("additionalProperties") == Some(&Value::Bool(false));
    arguments.iter().find_map(|(name, value)| {
        let Some(property) = properties.get(name).filter(|property| property.is_object()) else {
            return closed.then(|| undeclared(name, properties));
        };
        let declared = property.get("type")?.as_str()?;
        (!has_type(value, declared))
            .then(|| format!("argument '{name}' must be of type {declared}"))
    })
}

/// What the model reads of the argument `name` its closed schema, of
/// `properties`, does not declare: the names it accepts, in order.
fn undeclared(name: &str, properties: &serde_json::Map<String, Value>) -> String {
    let mut accepted: Vec<&str> = properties.keys().map(String::as_str).collect();
    accepted.sort_unstable();
    let accepted = if accepted.is_empty() {
        "none".to_owned()
    } else {
        accepted.join(", ")
    };
    format!("unknown argument '{name}' (this tool accepts: {accepted})")
}

/// Whether `value` is of the JSON Schema primitive type `declared`; a type
/// this check does not know is not checked.
fn has_type(value: &Value, declared: &str) -> bool {
    match declared {
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "integer" => value.is_i64() || value.is_u64(),
        "number" => value.is_number(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn arguments(value: &Value) -> Metadata {
        value.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn a_missing_required_argument_is_named() {
        let parameters = json!({ "type": "object", "required": ["city"] });
        assert_eq!(
            mismatch(&parameters, &arguments(&json!({ "town": "Paris" }))),
            Some("missing required argument 'city'".to_owned())
        );
    }

    #[test]
    fn an_argument_of_another_type_is_named() {
        let parameters = json!({ "properties": { "count": { "type": "integer" } } });
        assert_eq!(
            [
                json!({ "count": 2 }),
                json!({ "count": 2.5 }),
                json!({ "count": "2" })
            ]
            .map(|given| mismatch(&parameters, &arguments(&given))),
            [
                None,
                Some("argument 'count' must be of type integer".to_owned()),
                Some("argument 'count' must be of type integer".to_owned())
            ]
        );
    }

    /// Python's `validate_tool_arguments`: an argument the schema does not
    /// declare passes an open schema, and a closed one with no properties;
    /// a closed one refuses it, naming the ones it accepts
    /// (`tasks/probes/RMK-1042/py_closed_schema.out`).
    #[test]
    fn an_argument_the_schema_does_not_declare_passes_only_an_open_schema() {
        let properties = json!({ "command": { "type": "string" }, "cwd": { "type": "string" } });
        let schema = |additional: Value| json!({ "properties": properties, "required": ["command"], "additionalProperties": additional });
        let given = arguments(&json!({ "command": "ls", "extra": 1 }));
        assert_eq!(
            [
                mismatch(&schema(json!(true)), &given),
                mismatch(&json!({ "properties": properties }), &given),
                mismatch(&json!({ "additionalProperties": false }), &given),
                mismatch(&schema(json!(false)), &given),
                mismatch(
                    &json!({ "properties": {}, "additionalProperties": false }),
                    &given
                ),
            ],
            [
                None,
                None,
                None,
                Some("unknown argument 'extra' (this tool accepts: command, cwd)".to_owned()),
                Some("unknown argument 'command' (this tool accepts: none)".to_owned()),
            ]
        );
    }
}
