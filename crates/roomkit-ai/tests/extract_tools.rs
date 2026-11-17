//! Python's public tool extraction helper (RFC §21.1, §21.4).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;

use roomkit::model::Metadata;
use roomkit_ai::{AiTool, Tool, ToolEntry, ToolFuture, ToolResult, extract_tools};
use serde_json::json;

mod extract_tools {
    mod channel;
    mod schemas;
}

struct FakeTool {
    definition: AiTool,
    result: &'static str,
}

impl Tool for FakeTool {
    fn definition(&self) -> AiTool {
        self.definition.clone()
    }

    fn handler(&self, name: String, _arguments: Metadata) -> ToolFuture {
        let result = if name == self.definition.name {
            self.result.to_owned()
        } else {
            format!("{{\"error\": \"Unknown tool: {name}\"}}")
        };
        Box::pin(async move { Ok(ToolResult::Text(result)) })
    }
}

fn tool(name: &str, result: &'static str) -> ToolEntry {
    ToolEntry::Tool(Arc::new(FakeTool {
        definition: AiTool::new(name, format!("Fake {name}")).unwrap(),
        result,
    }))
}

fn schema(value: serde_json::Value) -> ToolEntry {
    ToolEntry::Schema(serde_json::from_value(value).unwrap())
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_empty_list
#[test]
fn empty_list() {
    let (definitions, handler) = extract_tools(Vec::new()).unwrap();
    assert_eq!(definitions, Vec::<AiTool>::new());
    assert!(handler.is_none());
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_extract_tool_objects
#[test]
fn extract_tool_objects() {
    let (definitions, handler) =
        extract_tools(vec![tool("alpha", "a"), tool("beta", "b")]).unwrap();
    assert_eq!(
        definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert!(handler.is_some());
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_composed_handler_dispatches
#[tokio::test]
async fn composed_handler_dispatches() {
    let (_, handler) =
        extract_tools(vec![tool("alpha", "result-a"), tool("beta", "result-b")]).unwrap();
    let handler = handler.unwrap();
    for (name, result) in [("alpha", "result-a"), ("beta", "result-b")] {
        assert_eq!(
            handler.call(name.into(), Metadata::new()).await.unwrap(),
            ToolResult::Text(result.into())
        );
    }
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_extract_aitool_objects
#[test]
fn extract_aitool_objects() {
    let given = AiTool {
        defer_loading: true,
        ..AiTool::new("foo", "a foo tool").unwrap()
    };
    let (definitions, handler) = extract_tools(vec![given.clone().into()]).unwrap();
    assert_eq!(definitions, [given]);
    assert!(handler.is_none());
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_extract_dicts
#[test]
fn extract_dicts() {
    let (definitions, handler) = extract_tools(vec![schema(
        json!({"name":"bar", "description":"a bar", "parameters":{}}),
    )])
    .unwrap();
    assert_eq!(definitions, [AiTool::new("bar", "a bar").unwrap()]);
    assert!(handler.is_none());
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_a_schema_dict_declares_that_its_tool_changes_state
#[test]
fn a_schema_dict_declares_that_its_tool_changes_state() {
    let writer = FakeTool {
        definition: AiTool {
            changes_state: true,
            ..AiTool::new("write", "w").unwrap()
        },
        result: "ok",
    };
    let (definitions, _) = extract_tools(vec![
        schema(json!({"name":"edit", "changes_state":true})),
        ToolEntry::Tool(Arc::new(writer)),
        schema(json!({"name":"read"})),
    ])
    .unwrap();
    assert_eq!(
        definitions
            .iter()
            .map(|d| d.changes_state)
            .collect::<Vec<_>>(),
        [true, true, false]
    );
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_mixed_tools_and_aitool
#[test]
fn mixed_tools_and_aitool() {
    let (definitions, handler) = extract_tools(vec![
        tool("local", "ok"),
        AiTool::new("search", "web search").unwrap().into(),
    ])
    .unwrap();
    assert_eq!(
        definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect::<Vec<_>>(),
        ["local", "search"]
    );
    assert!(handler.is_some());
}

// Python: tests/test_tool_protocol.py::TestExtractTools::test_single_tool_handler_not_composed
// Python: tests/tools/test_extract_tools.py::test_one_handler_is_returned_directly_with_its_decline_unchanged
#[tokio::test]
async fn one_handler_is_returned_directly_with_its_decline_unchanged() {
    let (_, handler) = extract_tools(vec![tool("only", "result")]).unwrap();
    let handler = handler.unwrap();
    assert_eq!(
        handler.call("only".into(), Metadata::new()).await.unwrap(),
        ToolResult::Text("result".into())
    );
    assert_eq!(
        handler
            .call("unknown".into(), Metadata::new())
            .await
            .unwrap(),
        ToolResult::Text("{\"error\": \"Unknown tool: unknown\"}".into())
    );
}

// Python: tests/tools/test_extract_tools.py::test_duplicate_definitions_keep_order_and_the_first_handler_wins
#[tokio::test]
async fn duplicate_definitions_keep_order_and_the_first_handler_wins() {
    let (definitions, handler) =
        extract_tools(vec![tool("same", "first"), tool("same", "second")]).unwrap();
    assert_eq!(
        definitions
            .iter()
            .map(|d| d.name.as_str())
            .collect::<Vec<_>>(),
        ["same", "same"]
    );
    assert_eq!(
        handler
            .unwrap()
            .call("same".into(), Metadata::new())
            .await
            .unwrap(),
        ToolResult::Text("first".into())
    );
}
