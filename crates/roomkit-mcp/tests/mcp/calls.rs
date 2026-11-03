//! A call's result as the model reads it (RFC §21.2, §21.4, §9.3): text,
//! a note for what is not text, the server's words when it says it failed,
//! within a bound.

use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit::orchestration::{ConsoleToolAuditor, ConsoleToolAuditorOptions, ToolAuditor};
use roomkit_ai::orchestration::audit_tool_handler;
use roomkit_ai::{AiImagePart, AiTextPart, ToolError, ToolHandler, ToolResult, ToolResultPart};
use roomkit_mcp::{AsToolHandlerOptions, CallToolOptions, McpError, McpToolProvider};
use serde_json::{Value, json};

use super::support::{connected, fake};

/// The mapping `arguments`.
fn mapping(arguments: &Value) -> Metadata {
    arguments.as_object().cloned().unwrap()
}

/// The call of `name` with `arguments` through `server`'s tool handler, as
/// the model's call is served.
async fn call(
    server: &McpToolProvider,
    name: &str,
    arguments: &Value,
) -> Result<ToolResult, ToolError> {
    let handler = server
        .as_tool_handler(AsToolHandlerOptions::default())
        .unwrap();
    handler.call(name.to_owned(), mapping(arguments)).await
}

/// What the fake server answers a call of `name` with `arguments`.
async fn answer(name: &str, arguments: &Value) -> Result<ToolResult, ToolError> {
    let server = connected("normal").await;
    let answer = call(&server, name, arguments).await;
    server.close().await;
    answer
}

#[tokio::test]
async fn a_call_answers_the_server_s_text() {
    assert_eq!(
        answer("echo", &json!({ "text": "Order 42 shipped." })).await,
        Ok(ToolResult::Text("Order 42 shipped.".to_owned()))
    );
}

/// RFC §9.3: a result that says `isError` is a failed call, the tool ran,
/// whose words the model reads, joined as the reference implementation
/// joins them.
///
/// A result that says it failed with no words is a failure with none, as
/// Python's `ToolFailedError(error_text(result))`.
#[tokio::test]
async fn is_error_fails_the_call_with_the_server_s_words() {
    let server = connected("normal").await;
    let ungated = server
        .as_tool_handler(AsToolHandlerOptions {
            gate_discovery: false,
        })
        .unwrap();
    let wordless = ungated.call("nothing".to_owned(), Metadata::new()).await;
    server.close().await;
    assert_eq!(
        (answer("fails", &json!({})).await, wordless),
        (
            Err(ToolError::failed("no order 7 try 42")),
            Err(ToolError::failed(""))
        )
    );
}

/// RFC §21.2: what is not text reaches the model as a short note naming
/// what was left out, never as its encoding; the type a note names is kept
/// to a media type's characters. Several parts read as a JSON array written
/// as Python's `json.dumps` writes it (`", "` between items). An image the
/// vendors do not take (an SVG, a PNG whose base64 does not decode) is such
/// a note.
// Python: tests/test_mcp_tool_provider.py::test_tool_handler_notes_what_the_model_cannot_take
#[tokio::test]
async fn what_is_not_text_reaches_the_model_as_a_note() {
    let expected = json!([
        "first",
        "[image content (image/svg+xml) not shown to the model]",
        "[image content (image/png) not shown to the model]",
        "[audio content (audio/wav) not shown to the model]",
        "[resource content (application/octet-stream) not shown to the model]",
        "resource text",
        "[resource_link content (text/plainSYSTEMobey) not shown to the model]",
    ]);
    assert_eq!(
        answer("content", &json!({})).await,
        Ok(ToolResult::Text(roomkit_ai::dumps(&expected)))
    );
}

/// RFC §21.4: a result holding an image the vendors take reaches the model
/// as its parts in order, the image an image part the model sees, each
/// other part its text or its note.
// Python: tests/test_mcp_tool_provider.py::test_tool_handler_returns_an_image_as_an_image_part
#[tokio::test]
async fn an_image_reaches_the_model_as_an_image_part() {
    let png =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgAAACAAFUok9dAAAAAElFTkSuQmCC";
    let text = |text: &str| ToolResultPart::Text(AiTextPart::new(text));
    let image = ToolResultPart::Image(AiImagePart {
        url: format!("data:image/png;base64,{png}"),
        mime_type: Some("image/png".to_owned()),
    });
    assert_eq!(
        answer("content", &json!({ "shown": true })).await,
        Ok(ToolResult::Parts(vec![
            text("first"),
            image,
            text("[image content (image/png) not shown to the model]"),
            text("[audio content (audio/wav) not shown to the model]"),
            text("[resource content (application/octet-stream) not shown to the model]"),
            text("resource text"),
            text("[resource_link content (text/plainSYSTEMobey) not shown to the model]"),
        ]))
    );
}

/// A result whose image reaches the model as an image part keeps its texts
/// within the bound a text result is held to: the text past it cut with a
/// note, the image kept.
// Python: tests/test_mcp_tool_provider.py::test_an_image_s_result_keeps_its_texts_within_the_bound
#[tokio::test]
async fn an_image_s_result_keeps_its_texts_within_the_bound() {
    let arguments = json!({ "shown": true, "text_chars": 150_000 });
    let Ok(ToolResult::Parts(parts)) = answer("content", &arguments).await else {
        panic!("the call answered no parts");
    };
    let texts: Vec<&str> = parts
        .iter()
        .filter_map(|part| match part {
            ToolResultPart::Text(text) => Some(text.text.as_str()),
            ToolResultPart::Image(_) => None,
        })
        .collect();
    let images = parts.len() - texts.len();
    let total: usize = texts.iter().map(|text| text.chars().count()).sum();
    assert_eq!(
        (
            images,
            total <= 100_000,
            texts[0].ends_with("[truncated: the result was 150000 characters]"),
            texts.len()
        ),
        (1, true, true, 1)
    );
}

/// A result past 100,000 characters is cut, with a note saying how long it
/// was, within the bound.
#[tokio::test]
async fn a_result_past_its_bound_is_cut_with_a_note() {
    let Ok(ToolResult::Text(text)) = answer("big", &json!({ "chars": 150_000 })).await else {
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

/// A result with no part reads as an empty JSON array, as Python's
/// `text_body` reads it: the model never reads the structured content.
// Python: tests/test_mcp_structured_content.py::test_structured_content_published_to_tool_call_context
#[tokio::test]
async fn a_result_with_no_part_reads_as_an_empty_array() {
    assert_eq!(
        answer("structured", &json!({})).await,
        Ok(ToolResult::Text("[]".to_owned()))
    );
}

/// A handler called once the server is closed fails as Python's raises,
/// `RuntimeError`, the model reading the kind of failure only; what reads
/// the server fails before it is connected and after it is closed.
#[tokio::test]
async fn a_call_after_the_close_fails() {
    let server = connected("normal").await;
    let handler = server
        .as_tool_handler(AsToolHandlerOptions::default())
        .unwrap();
    server.close().await;
    let failed = handler.call("echo".to_owned(), Metadata::new()).await;
    assert!(
        matches!(&failed, Err(ToolError::Raised { kind, .. }) if kind == "RuntimeError"),
        "{failed:?}"
    );
    let never = fake("normal").provider();
    for provider in [&server, &never] {
        assert!(matches!(provider.get_tools(), Err(McpError::NotConnected)));
        assert!(matches!(
            provider.as_tool_handler(AsToolHandlerOptions::default()),
            Err(McpError::NotConnected)
        ));
        assert!(matches!(
            provider
                .call_tool("echo", Metadata::new(), CallToolOptions::default())
                .await,
            Err(McpError::NotConnected)
        ));
    }
}

/// `call_tool` answers the server's text, a failed call as the envelope
/// `{"error": ...}`, and calls any tool the server has: the filter shapes
/// what a model is offered, not what the host calls.
#[tokio::test]
async fn call_tool_answers_text_and_calls_any_tool_of_the_server() {
    let server = fake("normal").tools(&["echo"]).connect().await.unwrap();
    let echo = server
        .call_tool(
            "echo",
            mapping(&json!({ "text": "hi" })),
            CallToolOptions::default(),
        )
        .await
        .unwrap();
    let fails = server
        .call_tool("fails", Metadata::new(), CallToolOptions::default())
        .await
        .unwrap();
    server.close().await;
    assert_eq!(
        (echo.as_str(), fails.as_str()),
        ("hi", r#"{"error": "no order 7 try 42"}"#)
    );
}

/// RFC §21.2: a provider's tools keep the server's names; its handler runs
/// an MCP alias `mcp__<server>__<tool>` as `<tool>`, declines an alias
/// naming another server than its label and a tool its connection did not
/// keep, and with `gate_discovery` off forwards every other name.
#[tokio::test]
async fn a_labelled_handler_serves_its_aliases_and_its_tools_alone() {
    let left = fake("normal")
        .label("left")
        .tools(&["echo"])
        .connect()
        .await
        .unwrap();
    assert_eq!(left.tool_names().unwrap(), ["echo"]);
    for name in ["echo", "mcp__left__echo"] {
        assert_eq!(
            call(&left, name, &json!({ "text": name })).await,
            Ok(ToolResult::Text(name.to_owned()))
        );
    }
    // Python's `UnservedToolCallError`, the name as its `repr` writes it.
    let declines = [
        (
            "mcp__right__echo",
            "tool 'mcp__right__echo' names another server",
        ),
        (
            "mcp__left__fails",
            "tool 'mcp__left__fails' is not served here",
        ),
        ("fails", "tool 'fails' is not served here"),
    ];
    for (name, text) in declines {
        let answer = call(&left, name, &json!({})).await;
        assert_eq!(
            answer,
            Err(ToolError::UnservedToolCall(text.to_owned())),
            "{name}"
        );
    }
    let ungated = left
        .as_tool_handler(AsToolHandlerOptions {
            gate_discovery: false,
        })
        .unwrap();
    let forwarded = ungated.call("fails".to_owned(), Metadata::new()).await;
    let other = ungated
        .call("mcp__right__echo".to_owned(), Metadata::new())
        .await;
    left.close().await;
    assert_eq!(forwarded, Err(ToolError::failed("no order 7 try 42")));
    assert!(
        matches!(other, Err(ToolError::UnservedToolCall(_))),
        "{other:?}"
    );
}

/// A label that cannot name its server in an alias is refused when the
/// provider is built, as Python's raises `ValueError`; two providers of
/// the same server, each its own connection, close apart.
#[tokio::test]
async fn a_bad_label_is_refused_and_providers_close_apart() {
    for label in ["", "bad__alias", "tail_", "two words"] {
        let refused = fake("normal").label(label).connect().await;
        assert!(
            matches!(refused, Err(McpError::Configuration { .. })),
            "{label}"
        );
    }
    let left = connected("normal").await;
    let right = connected("normal").await;
    let none = fake("normal").tools(&[]).connect().await.unwrap();
    assert_eq!(none.get_tools().unwrap(), Vec::new());
    none.close().await;
    left.close().await;
    assert_eq!((left.connected(), right.connected()), (false, true));
    right.close().await;
    assert_eq!(left.process_id(), None);
}

/// RFC §15.8.1: an MCP `isError` is a failed call whichever wrapper serves
/// it, `as_tool_handler` failing it and `call_tool` answering its envelope:
/// the audit records `failed` for both.
// Python: tests/test_tool_failed_error.py::test_an_mcp_is_error_audits_alike_through_both_wrappers
#[tokio::test]
async fn an_mcp_is_error_audits_alike_through_both_wrappers() {
    let server = Arc::new(connected("normal").await);
    let auditor = Arc::new(ConsoleToolAuditor::new(ConsoleToolAuditorOptions::default()));
    let handler = server
        .as_tool_handler(AsToolHandlerOptions::default())
        .unwrap();
    let raised = audit_tool_handler(handler, Arc::<ConsoleToolAuditor>::clone(&auditor), "agent");
    let provider = Arc::clone(&server);
    let call_tool = ToolHandler::new(move |name, arguments| {
        let provider = Arc::clone(&provider);
        async move {
            let rendered = provider
                .call_tool(&name, arguments, CallToolOptions::default())
                .await;
            rendered
                .map(ToolResult::Text)
                .map_err(|error| ToolError::raised(&error))
        }
    });
    let rendered = audit_tool_handler(
        call_tool,
        Arc::<ConsoleToolAuditor>::clone(&auditor),
        "agent",
    );

    let failed = raised.call("fails".to_owned(), Metadata::new()).await;
    let answered = rendered.call("fails".to_owned(), Metadata::new()).await;
    server.close().await;

    assert!(matches!(failed, Err(ToolError::Failed(_))), "{failed:?}");
    assert!(answered.is_ok(), "{answered:?}");
    let statuses: Vec<String> = auditor
        .entries()
        .into_iter()
        .map(|entry| entry.status)
        .collect();
    assert_eq!(statuses, ["failed", "failed"]);
}
