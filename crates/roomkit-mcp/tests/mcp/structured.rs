//! A server's structured copy reaches the current call through both public wrappers.

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{Metadata, ResponseMetadata, ToolCallContent, ToolCallOutcome};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolError, ToolHandler, ToolResult, current_tool_call, dumps};
use roomkit_mcp::{AsToolHandlerOptions, CallToolOptions, McpToolProvider};
use serde_json::{Value, json};

use super::support::{Room, connected, serving};

/// The server's wrapper inside a real handler, retaining what it published
/// before the loop applies its outcome rules.
async fn run(
    server: &McpToolProvider,
    direct: bool,
    copy: Value,
    error: bool,
    initial: Option<Value>,
) -> (Option<Value>, ToolCallContent) {
    let arguments = json!({"copy": copy, "text": "model text", "error": error}).to_string();
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "structured", arguments.as_str())]),
        ScriptedRound::answering("Done."),
    ]);
    let seen = Arc::new(Mutex::new(None));
    let target = Arc::clone(&seen);
    let provider = server.clone();
    let wrapped = server
        .as_tool_handler(AsToolHandlerOptions::default())
        .unwrap();
    let handler = ToolHandler::new(move |name, arguments| {
        let (provider, wrapped, target, initial) = (
            provider.clone(),
            wrapped.clone(),
            Arc::clone(&target),
            initial.clone(),
        );
        async move {
            let call = current_tool_call().unwrap();
            call.structured_content.replace(
                initial
                    .and_then(|value| value.as_object().cloned())
                    .map(ResponseMetadata::from),
            );
            let answer = if direct {
                provider
                    .call_tool(&name, arguments, CallToolOptions::default())
                    .await
                    .map(ToolResult::Text)
                    .map_err(|error| ToolError::raised(&error))
            } else {
                wrapped.call(name, arguments).await
            };
            *target.lock().unwrap() = call
                .structured_content
                .get()
                .map(|map| Value::Object(map.to_metadata()));
            answer
        }
    });
    let room = Room::with(serving(&model, server, |mut options| {
        options.tool_handler = Some(handler);
        options
    }))
    .await;
    room.says("Read the page.").await;
    let row = room.ends().await.remove(0);
    room.kit.close().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert!(current_tool_call().is_none());
    (seen, row)
}

// Python: tests/test_mcp_structured_content.py::test_structured_content_published_to_tool_call_context
#[tokio::test]
async fn structured_content_published_to_tool_call_context() {
    let server = connected("normal").await;
    for direct in [false, true] {
        for copy in [
            json!({}),
            json!({"menu": {"items": [{"name": "Espresso"}]}}),
        ] {
            let (seen, row) = run(&server, direct, copy.clone(), false, None).await;
            assert_eq!(seen, Some(copy.clone()));
            assert_eq!(
                row.structured_content.clone().map(Value::Object),
                Some(copy)
            );
            assert_eq!(row.result, json!("model text"));
        }
    }
    server.close().await;
}

// Python: tests/test_mcp_structured_content.py::test_no_context_and_no_structured_content_are_harmless,
// test_non_dict_structured_content_is_ignored, test_error_results_never_publish_structured_content
#[tokio::test]
async fn absent_invalid_or_failed_copies_are_not_published() {
    let server = connected("normal").await;
    for direct in [false, true] {
        for (copy, error) in [
            (Value::Null, false),
            (json!([1, 2]), false),
            (json!({"a": 1}), true),
        ] {
            let (seen, row) = run(&server, direct, copy, error, None).await;
            assert_eq!(seen, None);
            assert_eq!(row.structured_content.clone().map(Value::Object), None);
        }
    }
    assert!(
        server
            .call_tool("structured", Metadata::new(), CallToolOptions::default())
            .await
            .is_ok()
    );
    server.close().await;
}

// Python: tests/test_mcp_structured_content.py::test_structured_content_obeys_the_serialized_size_bound
#[tokio::test]
async fn structured_content_obeys_the_serialized_size_bound() {
    let server = connected("normal").await;
    let limit = 512 * 1024;
    for character in ["x", "😀"] {
        let width = dumps(&json!(character)).len() - 2;
        let available = limit - dumps(&json!({"blob": ""})).len();
        for extra in [0, 1] {
            let blob = character.repeat(available / width) + &"x".repeat(available % width + extra);
            let copy = json!({"blob": blob});
            assert_eq!(dumps(&copy).len(), limit + extra);
            for direct in [false, true] {
                let (seen, row) = run(&server, direct, copy.clone(), false, None).await;
                let expected = (extra == 0).then_some(copy.clone());
                assert_eq!(seen, expected);
                assert_eq!(row.structured_content.clone().map(Value::Object), expected);
            }
        }
    }
    server.close().await;
}

// Python: tests/test_mcp_structured_content.py::test_a_non_publication_keeps_the_existing_call_copy
#[tokio::test]
async fn a_non_publication_keeps_the_existing_call_copy() {
    let server = connected("normal").await;
    for direct in [false, true] {
        for (copy, error) in [
            (Value::Null, false),
            (json!([1]), false),
            (json!({"blob": "x".repeat(600 * 1024)}), false),
            (json!({"a": 1}), true),
        ] {
            let initial = json!({"page": 7});
            let (seen, row) = run(&server, direct, copy, error, Some(initial.clone())).await;
            assert_eq!(seen, Some(initial.clone()));
            // The handler raises on isError; the direct wrapper returns text.
            assert_eq!(
                row.structured_content.clone().map(Value::Object),
                (direct || !error).then_some(initial)
            );
            assert_eq!(
                row.outcome,
                Some(if !direct && error {
                    ToolCallOutcome::Failed
                } else {
                    ToolCallOutcome::Served
                })
            );
        }
    }
    server.close().await;
}
