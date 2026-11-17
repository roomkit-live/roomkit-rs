use std::sync::Arc;

use roomkit_ai::{AiError, AiTool, ToolEntry, extract_tools};
use serde_json::{Value, json};

use crate::{FakeTool, schema};

// Python: tests/tools/test_extract_tools.py::test_a_minimal_schema_supplies_the_extraction_defaults
#[test]
fn a_minimal_schema_supplies_the_extraction_defaults() {
    let (definitions, handler) = extract_tools(vec![schema(json!({"name":"read"}))]).unwrap();
    assert_eq!(definitions, [AiTool::new("read", "").unwrap()]);
    assert!(handler.is_none());
}

// Python: tests/tools/test_extract_tools.py::test_typed_definitions_keep_fields_that_raw_schemas_do_not_extract
// Python: tests/test_tool_outcome_residuals.py::TestTagsSurvive::test_extracting_a_tool_object_keeps_its_tags
// Python: tests/test_tool_outcome_residuals.py::TestTagsSurvive::test_extracting_a_schema_keeps_its_tags
#[test]
fn typed_definitions_keep_fields_that_raw_schemas_do_not_extract() {
    let typed = AiTool {
        defer_loading: true,
        tags: vec!["typed".into()],
        ..AiTool::new("typed", "typed").unwrap()
    };
    let raw = json!({"name":"raw", "parameters":{"nested":{"value":1}}, "tags":["météo","forecast"], "changes_state":true, "defer_loading":true, "ignored":"metadata"});
    let local = AiTool {
        parameters: raw["parameters"].clone(),
        tags: vec!["météo".into(), "forecast".into()],
        changes_state: true,
        defer_loading: true,
        ..AiTool::new("local", "").unwrap()
    };
    let (definitions, handler) = extract_tools(vec![
        typed.clone().into(),
        schema(raw.clone()),
        ToolEntry::Tool(Arc::new(FakeTool {
            definition: local.clone(),
            result: "ok",
        })),
    ])
    .unwrap();
    assert_eq!(
        definitions,
        [
            typed,
            AiTool {
                name: "raw".into(),
                defer_loading: false,
                ..local.clone()
            },
            AiTool {
                defer_loading: false,
                ..local
            }
        ]
    );
    assert_eq!(raw["defer_loading"], true);
    assert!(handler.is_some());
}

// Python: tests/tools/test_extract_tools.py::test_only_typed_definitions_skip_revalidation
#[test]
fn only_typed_definitions_skip_revalidation() {
    let typed = AiTool {
        name: "invalid name".into(),
        ..AiTool::new("typed", "typed").unwrap()
    };
    let (definitions, handler) = extract_tools(vec![typed.clone().into()]).unwrap();
    assert_eq!(definitions.as_slice(), std::slice::from_ref(&typed));
    assert!(handler.is_none());
    assert!(extract_tools(vec![schema(json!({"name":"invalid name"}))]).is_err());
    assert!(
        extract_tools(vec![ToolEntry::Tool(Arc::new(FakeTool {
            definition: typed,
            result: "ok"
        }))])
        .is_err()
    );
}

// Python: tests/tools/test_extract_tools.py::test_raw_tags_follow_the_extractor_iterable_contract
#[test]
fn raw_tags_follow_the_extractor_iterable_contract() {
    for (tags, expected) in [
        (Value::Null, vec![]),
        (json!(false), vec![]),
        (json!(0), vec![]),
        (json!(""), vec![]),
        (json!([]), vec![]),
        (json!("ab"), vec!["a", "b"]),
        (json!({"z":1,"a":2}), vec!["z", "a"]),
    ] {
        let (definitions, handler) =
            extract_tools(vec![schema(json!({"name":"read","tags":tags}))]).unwrap();
        assert_eq!(definitions[0].tags, expected);
        assert!(handler.is_none());
    }
}

// Python: tests/tools/test_extract_tools.py::test_a_raw_schema_requires_a_name
// Python: tests/tools/test_extract_tools.py::test_truthy_non_iterable_tags_are_rejected
#[test]
fn missing_names_and_non_iterable_tags_are_rejected() {
    for raw in [
        json!({}),
        json!({"name":"read","tags":1}),
        json!({"name":"read","tags":true}),
    ] {
        assert!(extract_tools(vec![schema(raw)]).is_err());
    }
}

// Python: tools/compose.py::_definition indexes name, then constructs tags,
// before AITool validates name; retained Python boundary witnesses assert both errors.
#[test]
fn schema_extraction_preserves_the_order_of_field_reads() {
    for (raw, expected) in [
        (json!({"tags": 1}), "name"),
        (json!({"name": "invalid name", "tags": 1}), "tags"),
    ] {
        assert!(matches!(
            extract_tools(vec![schema(raw)]),
            Err(AiError::ToolDefinition { field, .. }) if field == expected
        ));
    }
}

// Python reference: tools/compose.py::_definition -> AITool validation.
// The retained Python coercion/validation witnesses run separately from the library suite.
#[test]
fn raw_schema_validation_matches_the_reference() {
    for (expected, values) in [
        (
            true,
            vec![
                json!(true),
                json!(1),
                json!(1.0),
                json!("true"),
                json!("yes"),
                json!("ON"),
                json!("t"),
                json!("1"),
            ],
        ),
        (
            false,
            vec![
                json!(false),
                json!(0),
                json!(0.0),
                json!("false"),
                json!("no"),
                json!("OFF"),
                json!("f"),
                json!("0"),
            ],
        ),
    ] {
        for value in values {
            let (definitions, _) =
                extract_tools(vec![schema(json!({"name":"read","changes_state":value}))]).unwrap();
            assert_eq!(definitions[0].changes_state, expected);
        }
    }
    for raw in [
        json!({"name":""}),
        json!({"name":"with space"}),
        json!({"name":null}),
        json!({"name":1}),
        json!({"name":"read","description":null}),
        json!({"name":"read","description":1}),
        json!({"name":"read","parameters":null}),
        json!({"name":"read","parameters":[]}),
        json!({"name":"read","tags":[1]}),
        json!({"name":"read","changes_state":null}),
        json!({"name":"read","changes_state":2}),
        json!({"name":"read","changes_state":"maybe"}),
    ] {
        assert!(extract_tools(vec![schema(raw.clone())]).is_err(), "{raw}");
    }
}
