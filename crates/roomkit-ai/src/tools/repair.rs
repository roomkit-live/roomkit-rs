//! The repairs a model's call gets before its arguments are checked against
//! its tool's schema (RFC §21.1), as the reference implementation's
//! `repair_tool_arguments` makes them: a hub tool's hoisted arguments folded
//! back into its `params`, then a primitive the model's server quoted read
//! as the type its property declares. Each repair is narrow and certain;
//! anything else is left to the schema check, which names what is wrong.
//! Arguments a `BEFORE_TOOL_USE` hook rewrote are never repaired: a hook is
//! the integrator's code, and repairing it would hide its bug.

use roomkit::model::Metadata;
use serde_json::{Map, Value};

/// The container property a hub tool declares for its own arguments: one
/// tool per domain behind `{action, params}`, which a model trained on flat
/// schemas flattens. The name is fixed rather than inferred, so that a real
/// typo is never moved into an unrelated object argument.
const PARAMS: &str = "params";

/// The longest text read as a literal: past it, a string is no literal a
/// tool takes, and reading it costs what the model chose to send.
const MAX_LITERAL_CHARS: usize = 64;

/// A model's call arguments after the repairs, and what each touched.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Repaired {
    /// The arguments the call runs with when nothing refuses it.
    pub(crate) arguments: Metadata,
    /// The root keys folded into a hub tool's `params`.
    pub(crate) folded: Vec<String>,
    /// The arguments read off their quotes as their declared primitive.
    pub(crate) unquoted: Vec<String>,
    /// Why the call is refused unrepaired: both hub forms at once.
    pub(crate) error: Option<String>,
}

/// `arguments` repaired for `parameters`, a JSON Schema object: folded,
/// then their quoted primitives read; unrepaired with the reason when both
/// hub forms are given at once (Python's `repair_tool_arguments`).
pub(crate) fn repair(parameters: &Value, arguments: &Metadata) -> Repaired {
    let (folded, hoisted) = match fold_hoisted(parameters, arguments) {
        Fold::Ambiguous(error) => {
            return Repaired {
                arguments: arguments.clone(),
                folded: Vec::new(),
                unquoted: Vec::new(),
                error: Some(error),
            };
        }
        Fold::Untouched => (arguments.clone(), Vec::new()),
        Fold::Folded { arguments, hoisted } => (arguments, hoisted),
    };
    let (arguments, unquoted) = unquote_primitives(parameters, folded);
    Repaired {
        arguments,
        folded: hoisted,
        unquoted,
        error: None,
    }
}

/// What folding a call's root keys into its hub tool's `params` comes to.
#[derive(Clone, Debug, PartialEq)]
enum Fold {
    /// The call is none of the fold's business.
    Untouched,
    /// The call, its `hoisted` root keys moved into `params`.
    Folded {
        arguments: Metadata,
        hoisted: Vec<String>,
    },
    /// Both forms at once: why the call is refused, naming which to keep.
    Ambiguous(String),
}

/// `arguments` with their root keys `parameters` does not declare folded
/// into its `params` (Python's `fold_hoisted_arguments`), when every
/// condition holds: the schema closes itself, it declares `params` an
/// object of no shape of its own (a hub container cannot have one, its
/// shape varies with `action`), a root key is undeclared, and `params` is
/// absent or empty. A non-object `params` is left to the schema check,
/// which names its type.
fn fold_hoisted(parameters: &Value, arguments: &Metadata) -> Fold {
    if parameters.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Fold::Untouched;
    }
    let Some(properties) = parameters.get("properties").and_then(Value::as_object) else {
        return Fold::Untouched;
    };
    let Some(container) = properties.get(PARAMS).and_then(Value::as_object) else {
        return Fold::Untouched;
    };
    if container.get("type").and_then(Value::as_str) != Some("object")
        || container.get("properties").is_some_and(truthy)
    {
        return Fold::Untouched;
    }
    let hoisted: Vec<String> = arguments
        .keys()
        .filter(|key| !properties.contains_key(*key))
        .cloned()
        .collect();
    if hoisted.is_empty() {
        return Fold::Untouched;
    }
    match arguments.get(PARAMS) {
        None | Some(Value::Null) => {}
        Some(Value::Object(given)) if given.is_empty() => {}
        Some(Value::Object(_)) => return Fold::Ambiguous(both_forms(&hoisted)),
        Some(_) => return Fold::Untouched,
    }
    let mut folded = Metadata::new();
    let mut container = Map::new();
    for (key, value) in arguments {
        if hoisted.contains(key) {
            container.insert(key.clone(), value.clone());
        } else {
            folded.insert(key.clone(), value.clone());
        }
    }
    folded.insert(PARAMS.to_owned(), Value::Object(container));
    Fold::Folded {
        arguments: folded,
        hoisted,
    }
}

/// Why a call giving `params` and the root keys `hoisted` is refused.
fn both_forms(hoisted: &[String]) -> String {
    let names: Vec<String> = hoisted.iter().map(|key| format!("'{key}'")).collect();
    format!(
        "{} passed at the root while '{PARAMS}' is already set \u{2014} pass every tool argument \
         inside '{PARAMS}'",
        names.join(", ")
    )
}

/// Whether `value` reads as true, as Python's truth test reads it.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// `arguments` with each string whose property declares a primitive type
/// read as that type's literal, when it spells one exactly: `"0"` for an
/// `integer` is `0`, `"0.5"` for a `number` is `0.5`, `"true"` for a
/// `boolean` is `true` (Python's `unquote_primitive_arguments`); and the
/// names it read. Some servers type a model's call by the schema the
/// request declared, so a tool missing from the request has every value
/// arrive as a string, which no error can teach the model to avoid.
fn unquote_primitives(parameters: &Value, mut arguments: Metadata) -> (Metadata, Vec<String>) {
    let Some(properties) = parameters.get("properties").and_then(Value::as_object) else {
        return (arguments, Vec::new());
    };
    let mut read = Vec::new();
    for (key, value) in &mut arguments {
        let declared = properties
            .get(key)
            .and_then(|property| property.get("type"))
            .and_then(Value::as_str);
        let (Value::String(text), Some(declared)) = (&*value, declared) else {
            continue;
        };
        if let Some(literal) = literal(text, declared) {
            *value = literal;
            read.push(key.clone());
        }
    }
    (arguments, read)
}

/// What `text`, its surrounding whitespace left out, is as a literal of the
/// JSON type `declared`, if it spells one exactly.
///
/// The digits read are ASCII's, and an integer is read only when it fits
/// 64 bits: a JSON value holds no wider one. Python reads every Unicode
/// decimal digit, and any integer up to the length bound.
fn literal(text: &str, declared: &str) -> Option<Value> {
    let text = text.trim();
    if text.chars().count() > MAX_LITERAL_CHARS {
        return None;
    }
    match declared {
        "integer" if spells_integer(text) => text
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| text.parse::<u64>().map(Value::from))
            .ok(),
        "number" if spells_number(text) => text
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .map(Value::from),
        "boolean" => match text.to_lowercase().as_str() {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            _ => None,
        },
        _ => None,
    }
}

/// Whether `text` is `-?\d+`.
fn spells_integer(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// Whether `text` is `-?\d+(\.\d+)?([eE][+-]?\d+)?`, JSON's spelling of a
/// number.
fn spells_number(text: &str) -> bool {
    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (text, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (mantissa, None),
    };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let exponent_digits = |part: &str| digits(part.strip_prefix(['+', '-']).unwrap_or(part));
    spells_integer(whole) && fraction.is_none_or(digits) && exponent.is_none_or(exponent_digits)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::tools::schema::mismatch;

    fn arguments(value: Value) -> Metadata {
        match value {
            Value::Object(members) => members,
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => Metadata::new(),
        }
    }

    fn schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "city": { "type": "string" },
                "days": { "type": "integer" },
                "metric": { "type": "boolean" },
                "ratio": { "type": "number" },
            },
            "required": ["city", "days"],
        })
    }

    fn hub() -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "action": { "type": "string" }, "params": { "type": "object" } },
            "required": ["action"],
        })
    }

    fn folded(parameters: &Value, given: Value) -> Fold {
        fold_hoisted(parameters, &arguments(given))
    }

    fn into(value: Value) -> Fold {
        Fold::Folded {
            hoisted: Vec::new(),
            arguments: arguments(value),
        }
    }

    /// The fold's arguments, the keys it hoisted left out of the
    /// comparison.
    fn arguments_of(fold: Fold) -> Fold {
        match fold {
            Fold::Folded { arguments, .. } => Fold::Folded {
                hoisted: Vec::new(),
                arguments,
            },
            other @ (Fold::Untouched | Fold::Ambiguous(_)) => other,
        }
    }

    // Python: tests/test_tool_arg_validation.py::test_hoisted_arguments_are_folded_into_params
    #[test]
    fn hoisted_arguments_are_folded_into_params() {
        let fold = folded(
            &hub(),
            json!({ "action": "list_columns", "board_id": "board-1" }),
        );
        let expected = json!({ "action": "list_columns", "params": { "board_id": "board-1" } });
        assert_eq!(arguments_of(fold), into(expected.clone()));
        assert_eq!(mismatch(&hub(), &arguments(expected)), None);
    }

    // Python: tests/test_tool_arg_validation.py::test_fold_keeps_declared_root_arguments_at_the_root
    #[test]
    fn fold_keeps_declared_root_arguments_at_the_root() {
        assert_eq!(
            arguments_of(folded(&hub(), json!({ "action": "x", "a": 1, "b": 2 }))),
            into(json!({ "action": "x", "params": { "a": 1, "b": 2 } }))
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_fold_into_an_empty_params_container
    #[test]
    fn fold_into_an_empty_params_container() {
        assert_eq!(
            arguments_of(folded(
                &hub(),
                json!({ "action": "x", "params": {}, "a": 1 })
            )),
            into(json!({ "action": "x", "params": { "a": 1 } }))
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_both_forms_at_once_is_refused_and_names_the_survivor
    #[test]
    fn both_forms_at_once_is_refused_and_names_the_survivor() {
        assert_eq!(
            folded(
                &hub(),
                json!({ "action": "x", "params": { "a": 1 }, "b": 2 })
            ),
            Fold::Ambiguous(
                "'b' passed at the root while 'params' is already set \u{2014} pass every tool \
                 argument inside 'params'"
                    .to_owned()
            )
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_nothing_to_fold_when_the_call_is_already_well_formed
    #[test]
    fn nothing_to_fold_when_the_call_is_already_well_formed() {
        assert_eq!(
            [
                folded(&hub(), json!({ "action": "x", "params": { "a": 1 } })),
                folded(&hub(), json!({ "action": "x" })),
            ],
            [Fold::Untouched, Fold::Untouched]
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_open_schema_is_left_alone
    #[test]
    fn open_schema_is_left_alone() {
        let mut open = hub();
        open.as_object_mut()
            .unwrap()
            .shift_remove("additionalProperties");
        assert_eq!(
            folded(&open, json!({ "action": "x", "board_id": "1" })),
            Fold::Untouched
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_tool_without_a_params_property_is_left_to_the_validator
    #[test]
    fn tool_without_a_params_property_is_left_to_the_validator() {
        let closed = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "query": { "type": "string" } },
            "required": ["query"],
        });
        let call = json!({ "query": "x", "web_search_depth": "2" });
        assert_eq!(folded(&closed, call.clone()), Fold::Untouched);
        assert!(mismatch(&closed, &arguments(call)).is_some());
    }

    // Python: tests/test_tool_arg_validation.py::test_params_declared_as_a_non_object_is_not_a_container
    #[test]
    fn params_declared_as_a_non_object_is_not_a_container() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "action": { "type": "string" }, "params": { "type": "string" } },
        });
        assert_eq!(
            folded(&schema, json!({ "action": "x", "a": 1 })),
            Fold::Untouched
        );
    }

    fn options() -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "title": { "type": "string" },
                "params": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": { "width": { "type": "integer" }, "height": { "type": "integer" } },
                },
            },
        })
    }

    // Python: tests/test_tool_arg_validation.py::test_params_declaring_its_own_shape_is_not_a_hub_container
    #[test]
    fn params_declaring_its_own_shape_is_not_a_hub_container() {
        assert_eq!(
            folded(&options(), json!({ "title": "Q3", "width": 800 })),
            Fold::Untouched
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_a_misspelt_root_property_is_named_rather_than_buried
    #[test]
    fn a_misspelt_root_property_is_named_rather_than_buried() {
        let call = json!({ "titel": "Q3" });
        assert_eq!(folded(&options(), call.clone()), Fold::Untouched);
        assert_eq!(
            mismatch(&options(), &arguments(call)),
            Some("unknown argument 'titel' (this tool accepts: params, title)".to_owned())
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_an_empty_properties_declaration_is_still_free_form
    #[test]
    fn an_empty_properties_declaration_is_still_free_form() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "action": { "type": "string" },
                "params": { "type": "object", "properties": {} },
            },
        });
        assert_eq!(
            arguments_of(folded(&schema, json!({ "action": "x", "a": 1 }))),
            into(json!({ "action": "x", "params": { "a": 1 } }))
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_params_supplied_as_a_non_object_is_left_to_the_type_check
    #[test]
    fn params_supplied_as_a_non_object_is_left_to_the_type_check() {
        let call = json!({ "action": "x", "params": "a=1", "b": 2 });
        assert_eq!(folded(&hub(), call), Fold::Untouched);
        // The check reads the arguments in name order, a map's here: the
        // root key after `params` leaves its type the first violation.
        let call = json!({ "action": "x", "params": "a=1", "z": 2 });
        assert_eq!(
            mismatch(&hub(), &arguments(call)),
            Some("argument 'params' must be of type object".to_owned())
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_a_quoted_primitive_is_read_as_its_declared_type
    #[test]
    fn a_quoted_primitive_is_read_as_its_declared_type() {
        let given =
            arguments(json!({ "city": "Laval", "days": "0", "ratio": " 0.5 ", "metric": "True" }));
        let (unquoted, mut names) = unquote_primitives(&schema(), given);
        names.sort();
        assert_eq!(
            (Value::Object(unquoted.clone()), names),
            (
                json!({ "city": "Laval", "days": 0, "ratio": 0.5, "metric": true }),
                vec!["days".to_owned(), "metric".to_owned(), "ratio".to_owned()]
            )
        );
        assert_eq!(mismatch(&schema(), &unquoted), None);
    }

    // Python: tests/test_tool_arg_validation.py::test_what_does_not_spell_the_literal_is_left_for_the_validator
    #[test]
    fn what_does_not_spell_the_literal_is_left_for_the_validator() {
        for (key, sent) in [
            ("days", "top"),
            ("days", "1.5"),
            ("days", "1_000"),
            ("ratio", "nan"),
            ("ratio", "1e999"),
            ("metric", "yes"),
            ("metric", "1"),
        ] {
            let mut given = arguments(json!({ "city": "Laval", "days": 1 }));
            given.insert(key.to_owned(), json!(sent));
            let (unquoted, names) = unquote_primitives(&schema(), given.clone());
            assert_eq!((&unquoted, names), (&given, Vec::new()), "{key}");
            assert!(mismatch(&schema(), &unquoted).is_some(), "{key}");
        }
    }

    // Python: tests/test_tool_arg_validation.py::test_an_oversized_literal_is_refused_never_raised
    #[test]
    fn an_oversized_literal_is_refused_never_raised() {
        for key in ["days", "ratio"] {
            let mut given = arguments(json!({ "city": "Laval", "days": 1 }));
            given.insert(key.to_owned(), Value::String("9".repeat(5000)));
            assert_eq!(
                unquote_primitives(&schema(), given.clone()),
                (given.clone(), Vec::new())
            );
            assert!(mismatch(&schema(), &given).is_some());
        }
    }

    // Python: tests/test_tool_arg_validation.py::test_a_string_property_keeps_its_digits
    #[test]
    fn a_string_property_keeps_its_digits() {
        let given = arguments(json!({ "city": "3", "days": 1 }));
        assert_eq!(
            unquote_primitives(&schema(), given.clone()),
            (given, Vec::new())
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_the_repair_folds_a_hub_call_then_reads_its_root_primitives
    #[test]
    fn the_repair_folds_a_hub_call_then_reads_its_root_primitives() {
        let mut schema = hub();
        schema["properties"]["limit"] = json!({ "type": "integer" });
        let repaired = repair(
            &schema,
            &arguments(json!({ "action": "list_columns", "board_id": "board-1", "limit": "5" })),
        );
        assert_eq!(
            repaired,
            Repaired {
                arguments: arguments(json!({
                    "action": "list_columns",
                    "limit": 5,
                    "params": { "board_id": "board-1" },
                })),
                folded: vec!["board_id".to_owned()],
                unquoted: vec!["limit".to_owned()],
                error: None,
            }
        );
    }

    // Python: tests/test_tool_arg_validation.py::test_an_ambiguous_hub_call_is_refused_unrepaired
    #[test]
    fn an_ambiguous_hub_call_is_refused_unrepaired() {
        let given = arguments(json!({ "action": "x", "board_id": "b", "params": { "a": 1 } }));
        let repaired = repair(&hub(), &given);
        assert!(repaired.error.is_some());
        assert_eq!(repaired.arguments, given);
    }

    /// A number literal reads as JSON spells one, an integer's digits
    /// included, and a JSON `null` for `params` is a container left empty.
    #[test]
    fn a_number_reads_as_json_spells_it_and_a_null_container_takes_the_fold() {
        assert_eq!(
            ["5", "-0.25", "1e3", "2E-2", "1.", ".5", "+1", "1e"]
                .map(|text| literal(text, "number")),
            [
                Some(json!(5.0)),
                Some(json!(-0.25)),
                Some(json!(1000.0)),
                Some(json!(0.02)),
                None,
                None,
                None,
                None
            ]
        );
        assert_eq!(
            arguments_of(folded(
                &hub(),
                json!({ "action": "x", "params": null, "a": 1 })
            )),
            into(json!({ "action": "x", "params": { "a": 1 } }))
        );
    }
}
