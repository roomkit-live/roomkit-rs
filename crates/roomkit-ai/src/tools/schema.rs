//! A call's arguments checked against its tool's parameters (RFC §21.1,
//! step 4 decision 7): the names `required` lists, and the type of each
//! argument whose top-level property declares a primitive one. Nothing
//! deeper is checked.

use roomkit::model::Metadata;
use serde_json::Value;

/// Why `arguments` do not fit `parameters`, a JSON Schema object, if they
/// do not: the first required argument missing, or the first argument of
/// another type than its property declares.
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
    arguments.iter().find_map(|(name, value)| {
        let declared = properties.get(name)?.get("type")?.as_str()?;
        (!has_type(value, declared))
            .then(|| format!("argument '{name}' must be of type {declared}"))
    })
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

    #[test]
    fn an_argument_the_schema_does_not_declare_passes() {
        let parameters = json!({ "properties": { "city": { "type": "string" } } });
        assert_eq!(mismatch(&parameters, &arguments(&json!({ "x": 1 }))), None);
    }
}
