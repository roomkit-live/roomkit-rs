use pretty_assertions::assert_eq;
use serde_json::{Value, json};

use super::*;

/// Python's `VERDICT`: every portable keyword.
fn verdict() -> Value {
    json!({
        "title": "Verdict",
        "type": "object",
        "properties": {
            "label": { "type": "string", "enum": ["yes", "no"], "description": "The answer." },
            "confidence": { "type": "number" },
            "reasons": { "type": "array", "items": { "type": "string" } },
            "detail": {
                "type": "object",
                "properties": { "count": { "type": "integer" }, "flag": { "type": "boolean" } },
                "required": ["count", "flag"],
                "additionalProperties": false,
            },
        },
        "required": ["label", "confidence", "reasons", "detail"],
        "additionalProperties": false,
    })
}

/// A copy of `verdict()` with `changes` made to the subschema at `path`, a
/// `null` change removing the keyword: Python's `_with`.
fn with(path: &[&str], changes: Value) -> Value {
    let mut schema = verdict();
    let mut node = &mut schema;
    for key in path {
        node = &mut node[*key];
    }
    let (Value::Object(node), Value::Object(changes)) = (node, changes) else {
        panic!("a change applies to an object");
    };
    for (key, value) in changes {
        if value.is_null() {
            node.shift_remove(&key);
        } else {
            node.insert(key, value);
        }
    }
    schema
}

/// The words of `schema`'s refusal.
fn refused(schema: &Value) -> String {
    match check_portable_schema(schema) {
        Err(AiError::ResponseSchema { reason }) => reason,
        other => panic!("not refused: {other:?}"),
    }
}

// Python: tests/test_response_schema.py::TestPortableSubset::test_accepts_every_portable_keyword
#[test]
fn accepts_every_portable_keyword() {
    assert_eq!(check_portable_schema(&verdict()), Ok(()));
}

// Python: tests/test_response_schema.py::TestPortableSubset::test_accepts_an_object_without_properties
#[test]
fn accepts_an_object_without_properties() {
    let schema = json!({
        "type": "object", "properties": {}, "required": [], "additionalProperties": false,
    });
    assert_eq!(check_portable_schema(&schema), Ok(()));
}

// Python: tests/test_response_schema.py::TestPortableSubset::test_refuses_what_one_provider_or_another_breaks_on
#[test]
fn refuses_what_one_provider_or_another_breaks_on() {
    let cases = [
        (
            json!({ "type": "array", "items": { "type": "string" } }),
            "a response schema must be a JSON Schema whose root type is 'object'",
        ),
        (
            with(
                &["properties", "label"],
                json!({ "type": ["string", "null"] }),
            ),
            "$.label: 'type' must be one of object, array, string, number, integer, boolean, as a single string",
        ),
        (
            with(&["properties", "confidence"], json!({ "minimum": 0 })),
            "$.confidence: ['minimum'] not portable on a 'number' schema",
        ),
        (
            with(&[], json!({ "additionalProperties": null })),
            "$: an object must set 'additionalProperties' to false",
        ),
        (
            with(&[], json!({ "additionalProperties": true })),
            "$: an object must set 'additionalProperties' to false",
        ),
        (
            with(
                &[],
                json!({ "required": ["label", "confidence", "reasons"] }),
            ),
            "$: 'required' must list every property, each once",
        ),
        (
            with(
                &[],
                json!({ "required": ["label", "label", "confidence", "reasons"] }),
            ),
            "$: 'required' must list every property, each once",
        ),
        (
            with(&["properties", "reasons"], json!({ "items": null })),
            "$.reasons: an array must declare 'items'",
        ),
        (
            with(&["properties", "confidence"], json!({ "enum": ["1"] })),
            "$.confidence: ['enum'] not portable on a 'number' schema",
        ),
        (
            with(&["properties", "label"], json!({ "enum": [] })),
            "$.label: 'enum' must be a non-empty list of distinct strings",
        ),
        (
            with(&["properties", "label"], json!({ "enum": ["yes", "yes"] })),
            "$.label: 'enum' must be a non-empty list of distinct strings",
        ),
        (
            with(
                &["properties", "label"],
                json!({ "type": null, "anyOf": [{ "type": "string" }] }),
            ),
            "$.label: 'type' must be one of object, array, string, number, integer, boolean, as a single string",
        ),
        (
            with(
                &["properties", "detail"],
                json!({ "type": null, "$ref": "#/$defs/x" }),
            ),
            "$.detail: 'type' must be one of object, array, string, number, integer, boolean, as a single string",
        ),
        (
            with(&["properties", "label"], json!({ "description": 3 })),
            "$.label: 'description' must be a string",
        ),
        (
            with(
                &["properties", "detail", "properties", "count"],
                json!({ "type": "null" }),
            ),
            "$.detail.count: 'type' must be one of object, array, string, number, integer, boolean, as a single string",
        ),
    ];
    for (schema, words) in cases {
        assert_eq!(refused(&schema), words);
    }
}

// Python: tests/test_response_schema.py::TestPortableSubset::test_refuses_property_names_that_are_not_strings
// JSON's names are texts: a `properties` that is no object is what is left
// to refuse there.
#[test]
fn refuses_properties_that_are_not_keyed_by_name() {
    let schema = with(
        &[],
        json!({ "properties": [{ "type": "string" }], "required": [] }),
    );
    assert_eq!(
        refused(&schema),
        "$: an object must declare 'properties', keyed by name"
    );
}

/// Python's `ANSWER`, with `changes` made to it: `_answer`.
fn answer(changes: Value) -> Value {
    let mut document = json!({
        "label": "yes", "confidence": 0.9, "reasons": [], "detail": { "count": 1, "flag": true },
    });
    if let (Value::Object(document), Value::Object(changes)) = (&mut document, changes) {
        document.extend(changes);
    }
    document
}

fn mismatch_of(document: &Value) -> Option<String> {
    schema_mismatch(&verdict(), document, SchemaMismatchOptions::default())
}

// Python: tests/test_response_schema.py::TestSchemaMismatch::test_a_fitting_document_has_none
#[test]
fn a_fitting_document_has_none() {
    assert_eq!(mismatch_of(&answer(json!({}))), None);
}

// Python: tests/test_response_schema.py::TestSchemaMismatch::test_a_whole_float_counts_as_an_integer
#[test]
fn a_whole_float_counts_as_an_integer() {
    let document = answer(json!({ "detail": { "count": 2.0, "flag": false } }));
    assert_eq!(mismatch_of(&document), None);
}

// Python: tests/test_response_schema.py::TestSchemaMismatch::test_names_where_a_document_departs
#[test]
fn names_where_a_document_departs() {
    let cases = [
        (json!([]), "$: expected an object"),
        (
            json!({ "label": "yes" }),
            "$: missing ['confidence', 'reasons', 'detail'], unexpected []",
        ),
        (
            answer(json!({ "extra": 1 })),
            "$: missing [], unexpected ['extra']",
        ),
        (
            answer(json!({ "label": "maybe" })),
            "$.label: 'maybe' is not one of ['yes', 'no']",
        ),
        (
            answer(json!({ "label": 1 })),
            "$.label: expected string, got int",
        ),
        (
            answer(json!({ "confidence": true })),
            "$.confidence: expected number, got bool",
        ),
        (
            answer(json!({ "reasons": ["ok", 3] })),
            "$.reasons[1]: expected string, got int",
        ),
        (
            answer(json!({ "reasons": "ok" })),
            "$.reasons: expected an array",
        ),
        (
            answer(json!({ "detail": { "count": 1.5, "flag": true } })),
            "$.detail.count: expected integer, got float",
        ),
        (
            answer(json!({ "detail": { "count": 1, "flag": "yes" } })),
            "$.detail.flag: expected boolean, got str",
        ),
    ];
    for (document, words) in cases {
        assert_eq!(mismatch_of(&document).as_deref(), Some(words));
    }
}
