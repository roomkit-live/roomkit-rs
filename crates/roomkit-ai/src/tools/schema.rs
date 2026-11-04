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
        "integer" => value.is_i64() || value.is_u64() || is_wider_integer(value),
        "number" => value.is_number(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => true,
    }
}

/// Whether `value` is an integer past what a JSON value holds as one, which it
/// reads as a float (the smallest past `u64`, or below `i64`): whole, as every
/// float that wide is. It reads so whether the model wrote its digits or `1e24`,
/// which Python's check refuses: only the digits are an `int` there.
fn is_wider_integer(value: &Value) -> bool {
    const PAST_U64: f64 = 18_446_744_073_709_551_616.0;
    const BELOW_I64: f64 = -9_223_372_036_854_775_808.0;
    value
        .as_f64()
        .is_some_and(|number| !(BELOW_I64..PAST_U64).contains(&number))
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

    // Python: tools/validation.py _TYPE_CHECKS reads any int; the first float
    // past each bound is one here, the last within it stays no integer.
    #[test]
    fn an_integer_past_64_bits_is_an_integer() {
        let parameters = json!({ "properties": { "count": { "type": "integer" } } });
        let refused = Some("argument 'count' must be of type integer".to_owned());
        for (count, read) in [
            (json!(u64::MAX), None),
            (json!(i64::MIN), None),
            (json!(18_446_744_073_709_551_616.0_f64), None),
            (json!(-1e24_f64), None),
            (json!(-9_223_372_036_854_775_808.0_f64), refused.clone()),
            (json!(9_007_199_254_740_992.0_f64), refused.clone()),
        ] {
            let given = arguments(&json!({ "count": count }));
            assert_eq!(mismatch(&parameters, &given), read, "{count}");
        }
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
