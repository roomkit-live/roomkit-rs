//! The schemas and model-facing declarations returned to an application (RFC §6.7).
use roomkit_ai::{
    AiTool, chat_tool_declarations, declared_parameters, some_vendor_accepts_tool_name,
};
use serde_json::json;

#[test]
fn absent_empty_and_untyped_schemas_get_an_object_root() {
    let empty = json!({"type":"object","properties":{}});
    assert_eq!(json!(declared_parameters(None)), empty);
    assert_eq!(json!(declared_parameters(Some(&json!({})))), empty);
    let null = json!({"type":null,"required":["q"]});
    assert_eq!(
        json!(declared_parameters(Some(&null))),
        json!({"type":"object","required":["q"]})
    );
    let typed = json!({"type":"array","items":{"type":"string"}});
    assert_eq!(json!(declared_parameters(Some(&typed))), typed);
}

#[test]
fn each_projected_schema_owns_its_nested_fields() {
    let mut tool = AiTool::new("lookup", "Find a value").unwrap();
    tool.parameters = json!({"properties":{"q":["original"]}});
    let mut projection = declared_parameters(Some(&tool.parameters));
    let mut declarations = chat_tool_declarations(std::slice::from_ref(&tool));
    let [row] = declarations.as_mut_slice() else {
        panic!("expected one tool")
    };
    assert_eq!(
        row,
        &json!({"type":"function","function":{
            "name":"lookup","description":"Find a value","parameters":{"type":"object","properties":{"q":["original"]}}
        }})
    );
    projection
        .pointer_mut("/properties/q/0")
        .unwrap()
        .clone_from(&json!("projection"));
    row.pointer_mut("/function/parameters/properties/q/0")
        .unwrap()
        .clone_from(&json!("declaration"));
    assert_eq!(tool.parameters, json!({"properties":{"q":["original"]}}));
    assert_eq!(
        projection.pointer("/properties/q/0"),
        Some(&json!("projection"))
    );
}

#[test]
fn the_union_of_vendor_alphabets_is_available_to_tool_authors() {
    for name in ["lookup", "files.read", "ns:tool", "1abc"] {
        assert!(some_vendor_accepts_tool_name(name));
        assert!(AiTool::new(name, "test").is_ok());
    }
    for name in ["", "not allowed", "bad/path", "é"] {
        assert!(!some_vendor_accepts_tool_name(name));
        assert!(AiTool::new(name, "test").is_err());
    }
}
