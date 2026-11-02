//! `compose_tool_handlers` (RFC §21.4): the handlers chained, the first that
//! serves a call winning, as the Python implementation's
//! `tests/test_compose_tool_handlers.py` runs it (step 5's
//! `compose_tool_handlers` proof, RMK-1103).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit_ai::{AiError, ToolError, ToolHandler, ToolResult, compose_tool_handlers};
use serde_json::{Value, json};

/// `{"error": "Unknown tool: <name>"}`, the answer of a handler a call is
/// not for.
fn unknown(name: &str) -> ToolResult {
    ToolResult::Text(json!({ "error": format!("Unknown tool: {name}") }).to_string())
}

/// A handler serving `get_weather` alone.
fn weather() -> ToolHandler {
    ToolHandler::new(|name, arguments: Metadata| async move {
        if name != "get_weather" {
            return Ok(unknown(&name));
        }
        let city = arguments.get("city").cloned().unwrap_or(json!("unknown"));
        Ok(ToolResult::Text(
            json!({ "temp": 20, "city": city }).to_string(),
        ))
    })
}

/// A handler serving `add` alone.
fn math() -> ToolHandler {
    ToolHandler::new(|name, arguments: Metadata| async move {
        if name != "add" {
            return Ok(unknown(&name));
        }
        let term = |key: &str| arguments.get(key).and_then(Value::as_i64).unwrap_or(0);
        Ok(ToolResult::Text(
            json!({ "result": term("a") + term("b") }).to_string(),
        ))
    })
}

/// A handler answering every call with `answer`.
fn answering(answer: ToolResult) -> ToolHandler {
    ToolHandler::new(move |_name, _arguments| {
        let answer = answer.clone();
        async move { Ok(answer) }
    })
}

/// A handler declining every call as not its own.
fn declining() -> ToolHandler {
    ToolHandler::new(|name, _arguments| async move {
        Err(ToolError::unserved(format!("{name} is not this handler's")))
    })
}

/// What `composed` answers a call to `name` with `arguments`, read as JSON
/// when it is.
async fn call(composed: &ToolHandler, name: &str, arguments: Value) -> Value {
    let arguments = arguments.as_object().cloned().unwrap_or_default();
    match composed.call(name.to_owned(), arguments).await.unwrap() {
        ToolResult::Text(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ToolResult::Json(value) => value,
        // No handler here answers parts.
        ToolResult::Parts(_) => Value::Null,
    }
}

// Python: tests/test_compose_tool_handlers.py::test_first_handler_match
#[tokio::test]
async fn first_handler_match() {
    let composed = compose_tool_handlers(vec![weather(), math()]).unwrap();
    assert_eq!(
        call(&composed, "get_weather", json!({ "city": "Montreal" })).await,
        json!({ "temp": 20, "city": "Montreal" })
    );
}

// Python: tests/test_compose_tool_handlers.py::test_second_handler_match
#[tokio::test]
async fn second_handler_match() {
    let composed = compose_tool_handlers(vec![weather(), math()]).unwrap();
    assert_eq!(
        call(&composed, "add", json!({ "a": 3, "b": 4 })).await,
        json!({ "result": 7 })
    );
}

// Python: tests/test_compose_tool_handlers.py::test_three_handlers
#[tokio::test]
async fn three_handlers() {
    let greet = ToolHandler::new(|name, arguments: Metadata| async move {
        if name != "greet" {
            return Ok(unknown(&name));
        }
        let who = arguments
            .get("who")
            .and_then(Value::as_str)
            .unwrap_or("world");
        Ok(ToolResult::Text(format!("Hello, {who}!")))
    });
    let composed = compose_tool_handlers(vec![greet, weather(), math()]).unwrap();
    assert_eq!(
        [
            call(&composed, "greet", json!({ "who": "Alice" })).await,
            call(&composed, "get_weather", json!({ "city": "Paris" })).await["city"].clone(),
            call(&composed, "add", json!({ "a": 1, "b": 2 })).await["result"].clone(),
        ],
        [json!("Hello, Alice!"), json!("Paris"), json!(3)]
    );
}

// Python: tests/test_compose_tool_handlers.py::test_no_match_passthrough
#[tokio::test]
async fn no_match_passthrough() {
    let composed =
        compose_tool_handlers(vec![weather(), answering(unknown("unknown_tool"))]).unwrap();
    assert_eq!(
        call(&composed, "unknown_tool", json!({})).await,
        json!({ "error": "Unknown tool: unknown_tool" })
    );
}

// Python: tests/test_compose_tool_handlers.py::test_non_json_result_not_treated_as_unknown
#[tokio::test]
async fn non_json_result_not_treated_as_unknown() {
    let plain = answering(ToolResult::Text("plain text result".to_owned()));
    let composed = compose_tool_handlers(vec![plain, math()]).unwrap();
    assert_eq!(
        call(&composed, "anything", json!({})).await,
        json!("plain text result")
    );
}

// Python: tests/test_compose_tool_handlers.py::test_error_without_unknown_tool_prefix_not_skipped
#[tokio::test]
async fn error_without_unknown_tool_prefix_not_skipped() {
    let failing = answering(ToolResult::Text(
        r#"{"error": "Something went wrong"}"#.to_owned(),
    ));
    let composed = compose_tool_handlers(vec![failing, math()]).unwrap();
    assert_eq!(
        call(&composed, "add", json!({ "a": 1, "b": 2 })).await,
        json!({ "error": "Something went wrong" })
    );
}

// Python: tests/test_compose_tool_handlers.py::test_requires_at_least_two_handlers
#[test]
fn requires_at_least_two_handlers() {
    let refused = compose_tool_handlers(vec![weather()]).unwrap_err();
    assert!(matches!(refused, AiError::TooFewHandlers { given: 1 }));
    assert!(refused.to_string().contains("at least 2"));
}

// Python: tests/test_tool_outcome_typed.py::TestDeclinedAnswers::test_a_composition_hands_a_declined_call_to_the_next_handler
#[tokio::test]
async fn a_composition_hands_a_declined_call_to_the_next_handler() {
    let serves = answering(ToolResult::Text(r#"{"served_by": "second"}"#.to_owned()));
    for first in [
        declining(),
        answering(ToolResult::Json(json!({ "error": "Unknown tool: lookup" }))),
    ] {
        let composed = compose_tool_handlers(vec![first, serves.clone()]).unwrap();
        assert_eq!(
            call(&composed, "lookup", json!({})).await,
            json!({ "served_by": "second" })
        );
    }
}

// Python: tests/test_tool_outcome_typed.py::TestDeclinedAnswers::test_the_last_handlers_decline_is_the_compositions
#[tokio::test]
async fn the_last_handlers_decline_is_the_compositions() {
    let envelope = answering(ToolResult::Json(json!({ "error": "Unknown tool: lookup" })));
    let composed = compose_tool_handlers(vec![envelope, declining()]).unwrap();
    let answer = composed.call("lookup".to_owned(), Metadata::new()).await;
    assert!(matches!(answer, Err(ToolError::UnservedToolCall(_))));
}
