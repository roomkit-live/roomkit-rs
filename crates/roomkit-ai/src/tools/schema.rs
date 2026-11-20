//! A call's arguments checked against its tool's parameters (RFC §21.1,
//! step 4 decision 7), as the reference implementation's
//! `validate_tool_arguments` checks them: the names `required` lists, the
//! type of each argument whose top-level property declares a primitive
//! one, and, on a schema that closes itself (`additionalProperties:
//! false`), no argument it does not declare. Nothing deeper is checked.

use roomkit::model::Metadata;
use serde_json::Value;

/// Why `arguments` do not fit `parameters`, a JSON Schema object, if they
/// do not: every reason at once, joined by `; `, so that a model which
/// renamed an argument reads the name it lacks beside its own. The required
/// arguments missing come first, in the schema's order; then the arguments
/// a closed schema does not declare, named with the ones it does; then each
/// argument of another type than its property declares, in the call's
/// order. Several missing arguments share one reason, as several undeclared.
pub(crate) fn mismatch(parameters: &Value, arguments: &Metadata) -> Option<String> {
    let missing: Vec<&str> = parameters
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|name| !arguments.contains_key(*name))
        .collect();
    let mut reasons = Vec::new();
    if !missing.is_empty() {
        reasons.push(named("missing required argument", &missing));
    }
    if let Some(properties) = parameters.get("properties").and_then(Value::as_object) {
        let closed = parameters.get("additionalProperties") == Some(&Value::Bool(false));
        reasons.extend(unfit(properties, closed, arguments));
    }
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

/// The reasons the `arguments` given do not fit `properties`: the ones a
/// `closed` schema does not declare, as one reason, then one per argument of
/// another type than its property declares.
fn unfit(
    properties: &serde_json::Map<String, Value>,
    closed: bool,
    arguments: &Metadata,
) -> Vec<String> {
    let mut undeclared = Vec::new();
    let mut mistyped = Vec::new();
    for (name, value) in arguments {
        let Some(property) = properties.get(name).filter(|property| property.is_object()) else {
            if closed {
                undeclared.push(name.as_str());
            }
            continue;
        };
        let declared = property.get("type").and_then(Value::as_str);
        if let Some(declared) = declared.filter(|declared| !has_type(value, declared)) {
            mistyped.push(format!("argument '{name}' must be of type {declared}"));
        }
    }
    let mut reasons = Vec::new();
    if !undeclared.is_empty() {
        let unknown = named("unknown argument", &undeclared);
        reasons.push(format!(
            "{unknown} (this tool accepts: {})",
            accepted(properties)
        ));
    }
    reasons.extend(mistyped);
    reasons
}

/// `what` said of the quoted `names`: `what 'a'`, or `whats 'a', 'b'`.
fn named(what: &str, names: &[&str]) -> String {
    let plural = if names.len() > 1 { "s" } else { "" };
    let quoted: Vec<String> = names.iter().map(|name| format!("'{name}'")).collect();
    format!("{what}{plural} {}", quoted.join(", "))
}

/// The names a closed schema of `properties` accepts, as the model reads
/// them: in order, or `none`.
fn accepted(properties: &serde_json::Map<String, Value>) -> String {
    let mut accepted: Vec<&str> = properties.keys().map(String::as_str).collect();
    accepted.sort_unstable();
    if accepted.is_empty() {
        "none".to_owned()
    } else {
        accepted.join(", ")
    }
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
/// reads as a float: whole, as every float that wide is. An integer just below
/// `i64` rounds to its bound, which `i64` itself never reads as. It reads so
/// whether the model wrote its digits or `1e24`, which Python's check refuses:
/// only the digits are an `int` there.
fn is_wider_integer(value: &Value) -> bool {
    const PAST_U64: f64 = 18_446_744_073_709_551_616.0;
    const I64_BOUND: f64 = -9_223_372_036_854_775_808.0;
    value
        .as_f64()
        .is_some_and(|number| number >= PAST_U64 || number <= I64_BOUND)
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
            (json!(-9_223_372_036_854_775_808.0_f64), None),
            (json!(-9_223_372_036_854_774_784.0_f64), refused.clone()),
            (json!(18_446_744_073_709_549_568.0_f64), refused.clone()),
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
                Some("unknown arguments 'command', 'extra' (this tool accepts: none)".to_owned()),
            ]
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_every_violation_is_named_in_one_answer
    #[test]
    fn every_reason_is_named_in_one_answer() {
        let parameters = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "title": { "type": "string" },
                "steps": { "type": "array" },
                "count": { "type": "integer" },
                "draft": { "type": "boolean" }
            },
            "required": ["title", "steps"]
        });
        let call = json!({ "name": "Ship", "draft": "maybe", "goal": "Ship it", "count": "many" });
        assert_eq!(
            mismatch(&parameters, &arguments(&call)).as_deref(),
            Some(
                "missing required arguments 'title', 'steps'; \
                 unknown arguments 'name', 'goal' (this tool accepts: count, draft, steps, title); \
                 argument 'draft' must be of type boolean; \
                 argument 'count' must be of type integer"
            )
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_closed_schema_reports_missing_required_before_unknown
    // and ::test_an_open_schema_names_what_it_checks_and_passes_the_rest
    #[test]
    fn a_missing_argument_comes_before_the_ones_given() {
        let properties = json!({ "query": { "type": "string" }, "days": { "type": "integer" } });
        let closed = json!({ "properties": properties, "required": ["query"], "additionalProperties": false });
        let open = json!({ "properties": properties, "required": ["query"] });
        let call = arguments(&json!({ "days": "three", "depth": "2" }));
        assert_eq!(
            [mismatch(&closed, &call), mismatch(&open, &call)],
            [
                Some(
                    "missing required argument 'query'; \
                     unknown argument 'depth' (this tool accepts: days, query); \
                     argument 'days' must be of type integer"
                        .to_owned()
                ),
                Some(
                    "missing required argument 'query'; argument 'days' must be of type integer"
                        .to_owned()
                ),
            ]
        );
    }
}
