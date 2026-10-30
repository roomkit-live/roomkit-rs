//! A call's result as the model reads it (RFC §21.2, §21.4, §9.3): text,
//! a note for what is not text, the server's words when it says it failed,
//! within a bound.

use pretty_assertions::assert_eq;
use roomkit::model::ToolCall;
use roomkit_ai::{ToolContext, ToolError, ToolOutput, Toolset};
use serde_json::{Value, json};

use super::support::{channel, connected, room_id};

fn call(name: &str, arguments: &Value) -> ToolCall {
    ToolCall::new("call_1", name, arguments.as_object().cloned().unwrap())
}

/// What the fake server answers a call of `name` with `arguments`.
async fn answer(name: &str, arguments: &Value) -> Result<ToolOutput, ToolError> {
    let server = connected("normal").await;
    let context = ToolContext::new(room_id(), channel("assistant"));
    let answer = server.call(call(name, arguments), context).await;
    server.close().await;
    answer
}

#[tokio::test]
async fn a_call_answers_the_server_s_text() {
    assert_eq!(
        answer("echo", &json!({ "text": "Order 42 shipped." })).await,
        Ok(ToolOutput::Text("Order 42 shipped.".to_owned()))
    );
}

/// RFC §9.3: a result that says `isError` is a failed call, the tool ran,
/// whose words the model reads, joined as the reference implementation
/// joins them.
#[tokio::test]
async fn is_error_fails_the_call_with_the_server_s_words() {
    assert_eq!(
        answer("fails", &json!({})).await,
        Err(ToolError::failed("no order 7 try 42"))
    );
}

/// RFC §21.2: what is not text reaches the model as a short note naming
/// what was left out, never as its encoding; the type a note names is kept
/// to a media type's characters.
#[tokio::test]
async fn what_is_not_text_reaches_the_model_as_a_note() {
    let expected = json!([
        "first",
        "[image content (image/png) not shown to the model]",
        "[audio content (audio/wav) not shown to the model]",
        "[resource content (application/octet-stream) not shown to the model]",
        "resource text",
        "[resource_link content (text/plainSYSTEMobey) not shown to the model]",
    ]);
    assert_eq!(
        answer("content", &json!({})).await,
        Ok(ToolOutput::Text(expected.to_string()))
    );
}

/// A result past 100,000 characters is cut, with a note saying how long it
/// was, within the bound.
#[tokio::test]
async fn a_result_past_its_bound_is_cut_with_a_note() {
    let Ok(ToolOutput::Text(text)) = answer("big", &json!({ "chars": 150_000 })).await else {
        panic!("the call failed");
    };
    assert_eq!(
        (
            text.chars().count(),
            text.ends_with("\n... [truncated: the result was 150000 characters]")
        ),
        (100_000, true)
    );
}

/// A result with no part reads as its structured content.
#[tokio::test]
async fn structured_content_alone_reads_as_its_json() {
    assert_eq!(
        answer("structured", &json!({})).await,
        Ok(ToolOutput::Text(r#"{"status":"shipped"}"#.to_owned()))
    );
}

/// A call once the server is closed fails, the model reading the kind of
/// failure only.
#[tokio::test]
async fn a_call_after_the_close_fails() {
    let server = connected("normal").await;
    server.close().await;
    let context = ToolContext::new(room_id(), channel("assistant"));
    let failed = server
        .call(call("echo", &json!({ "text": "hi" })), context)
        .await;
    assert!(
        matches!(&failed, Err(ToolError::Raised { kind, .. }) if kind == "McpError"),
        "{failed:?}"
    );
}

/// RFC §21.2: labels and selections are per handle and enforced at dispatch.
#[tokio::test]
async fn labelled_selection_routes_only_its_tools_and_preserves_other_handles() {
    let server = connected("normal").await;
    let left = server
        .clone()
        .with_label("left")
        .unwrap()
        .with_tool_names(&["echo".into()])
        .unwrap();
    let right = server
        .clone()
        .with_label("right")
        .unwrap()
        .with_tool_names(&["echo".into()])
        .unwrap();
    assert_eq!(
        left.tools()
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>(),
        vec!["mcp__left__echo"]
    );
    let context = || ToolContext::new(room_id(), channel("assistant"));
    assert_eq!(
        left.call(call("mcp__left__echo", &json!({"text":"left"})), context())
            .await,
        Ok(ToolOutput::Text("left".into()))
    );
    assert_eq!(
        right
            .call(
                call("mcp__right__echo", &json!({"text":"right"})),
                context()
            )
            .await,
        Ok(ToolOutput::Text("right".into()))
    );
    for name in ["echo", "mcp__right__echo", "mcp__left__fails"] {
        assert!(left.call(call(name, &json!({})), context()).await.is_err());
    }
    for label in ["", "bad__alias", "tail_", "two words"] {
        assert!(server.clone().with_label(label).is_err());
    }
    assert!(server.clone().with_tool_names(&["missing".into()]).is_err());
    assert_eq!(
        server.clone().with_tool_names(&[]).unwrap().tools(),
        Vec::new()
    );
    assert!(server.is_connected());
    left.close().await;
    assert!(!right.is_connected());
    assert_eq!(server.process_id(), None);
}
