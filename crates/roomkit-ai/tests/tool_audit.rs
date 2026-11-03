//! The audit of a tool handler's calls (RFC §15.8.1): Python's
//! `tests/orchestration/test_tool_audit.py`, `audit_tool_handler`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::time::Duration;

use roomkit::model::Metadata;
use roomkit::orchestration::{
    ConsoleToolAuditor, ConsoleToolAuditorOptions, ToolAuditEntry, ToolAuditor,
};
use roomkit_ai::orchestration::audit_tool_handler;
use roomkit_ai::{AiImagePart, AiTextPart, ToolError, ToolHandler, ToolResult, ToolResultPart};
use serde_json::json;
use tokio::sync::Notify;

/// An auditor keeping its entries, and the handler answering `answer`,
/// wrapped to record to it under `agent_id`.
fn audited(
    answer: Result<ToolResult, ToolError>,
    agent_id: &str,
) -> (Arc<ConsoleToolAuditor>, ToolHandler) {
    let auditor = Arc::new(ConsoleToolAuditor::new(ConsoleToolAuditorOptions::default()));
    let handler = ToolHandler::new(move |_name, _arguments| {
        let answer = answer.clone();
        async move { answer }
    });
    let wrapped = audit_tool_handler(
        handler,
        Arc::<ConsoleToolAuditor>::clone(&auditor),
        agent_id,
    );
    (auditor, wrapped)
}

fn only(auditor: &ConsoleToolAuditor) -> ToolAuditEntry {
    let mut entries = auditor.entries();
    assert_eq!(entries.len(), 1, "{entries:?}");
    entries.remove(0)
}

fn arguments(value: &serde_json::Value) -> Metadata {
    value.as_object().cloned().unwrap()
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_tool_handler_records
#[tokio::test]
async fn audit_tool_handler_records() {
    let body = r#"{"status": "ok", "data": "hello"}"#;
    let (auditor, wrapped) = audited(Ok(ToolResult::Text(body.to_owned())), "test-agent");

    let answer = wrapped
        .call("my_tool".to_owned(), arguments(&json!({ "x": 1 })))
        .await;

    assert_eq!(answer, Ok(ToolResult::Text(body.to_owned())));
    let entry = only(&auditor);
    assert_eq!(
        (
            entry.agent_id.as_str(),
            entry.tool_name.as_str(),
            entry.status.as_str(),
            entry.result.as_str()
        ),
        ("test-agent", "my_tool", "ok", body)
    );
    assert_eq!(entry.arguments, arguments(&json!({ "x": 1 })));
    assert!(entry.ts.ends_with("+00:00"), "{}", entry.ts);
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_tool_handler_detects_failed
// (every failure envelope of RFC §15.8.1, and bodies outside them)
#[tokio::test]
async fn audit_tool_handler_detects_failed() {
    let cases = [
        (r#"{"status": "failed", "error": "not found"}"#, "failed"),
        (r#"{"status": "ERROR"}"#, "failed"),
        (r#"{"success": false}"#, "failed"),
        (r#"{"error": "quota exceeded"}"#, "failed"),
        (r#"{"error": "partial", "success": true}"#, "ok"),
        (r#"{"error": ""}"#, "ok"),
        (r#"["error"]"#, "ok"),
        ("not json", "ok"),
    ];
    for (body, status) in cases {
        let (auditor, wrapped) = audited(Ok(ToolResult::Text(body.to_owned())), "a");
        let _answer = wrapped.call("tool".to_owned(), Metadata::new()).await;
        assert_eq!(only(&auditor).status, status, "{body}");
    }
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_tool_handler_error
#[tokio::test]
async fn audit_tool_handler_error() {
    let raised = ToolError::raised_as("ValueError", "boom");
    let (auditor, wrapped) = audited(Err(raised.clone()), "a");

    assert_eq!(
        wrapped.call("tool".to_owned(), Metadata::new()).await,
        Err(raised)
    );
    let entry = only(&auditor);
    assert_eq!(
        (entry.status.as_str(), entry.result.as_str()),
        ("error", "boom")
    );
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_tool_handler_truncates
#[tokio::test]
async fn audit_tool_handler_truncates() {
    let long = "é".repeat(1000);
    let (auditor, wrapped) = audited(Ok(ToolResult::Text(long.clone())), "a");

    let answer = wrapped.call("tool".to_owned(), Metadata::new()).await;

    assert_eq!(answer, Ok(ToolResult::Text(long)));
    assert_eq!(only(&auditor).result.chars().count(), 500);
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_returns_the_answer_unchanged
#[tokio::test]
async fn audit_returns_the_answer_unchanged() {
    let parts = ToolResult::Parts(vec![
        ToolResultPart::Text(AiTextPart::new("a chart")),
        ToolResultPart::Image(AiImagePart::new("https://x/c.png")),
    ]);
    let cases = [
        (parts, "a chart\n[image]", "ok"),
        (
            ToolResult::Json(json!({ "found": 3 })),
            r#"{"found": 3}"#,
            "ok",
        ),
        (
            ToolResult::Json(json!({ "error": "quota exceeded" })),
            r#"{"error": "quota exceeded"}"#,
            "failed",
        ),
    ];
    for (answer, recorded, status) in cases {
        let (auditor, wrapped) = audited(Ok(answer.clone()), "a");
        assert_eq!(
            wrapped.call("tool".to_owned(), Metadata::new()).await,
            Ok(answer)
        );
        let entry = only(&auditor);
        assert_eq!(
            (entry.result.as_str(), entry.status.as_str()),
            (recorded, status)
        );
    }
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_records_a_declined_call_as_failed_and_lets_it_through
#[tokio::test]
async fn audit_records_a_declined_call_as_failed_and_lets_it_through() {
    let cases = [
        (ToolError::refused("not allowed here"), "not allowed here"),
        (ToolError::unserved("not mine"), "not mine"),
        (ToolError::failed("the disk is full"), "the disk is full"),
    ];
    for (declined, words) in cases {
        let (auditor, wrapped) = audited(Err(declined.clone()), "a");
        assert_eq!(
            wrapped.call("tool".to_owned(), Metadata::new()).await,
            Err(declined)
        );
        let entry = only(&auditor);
        assert_eq!(
            (entry.status.as_str(), entry.result.as_str()),
            ("failed", words)
        );
    }
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_records_a_cancelled_call
#[tokio::test]
async fn audit_records_a_cancelled_call() {
    let auditor = Arc::new(ConsoleToolAuditor::new(ConsoleToolAuditorOptions::default()));
    let started = Arc::new(Notify::new());
    let running = Arc::clone(&started);
    let handler = ToolHandler::new(move |_name, _arguments| {
        let running = Arc::clone(&running);
        async move {
            running.notify_one();
            std::future::pending::<Result<ToolResult, ToolError>>().await
        }
    });
    let wrapped = audit_tool_handler(handler, Arc::<ConsoleToolAuditor>::clone(&auditor), "a");
    let call = tokio::spawn(wrapped.call("tool".to_owned(), Metadata::new()));
    started.notified().await;
    call.abort();

    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(only(&auditor).status, "cancelled");
}

// Python: tests/orchestration/test_tool_audit.py::test_audit_records_a_call_the_channel_bound_cut_as_cancelled
// (the bound drops the call from outside, as the channel's per-call bound
// does, RFC §21.6; scenario orchestration/an-audited-call-is-recorded-by-how-it-ended
// cuts it through the channel)
#[tokio::test(start_paused = true)]
async fn audit_records_a_call_the_channel_bound_cut_as_cancelled() {
    let auditor = Arc::new(ConsoleToolAuditor::new(ConsoleToolAuditorOptions::default()));
    let handler = ToolHandler::new(|_name, _arguments| std::future::pending());
    let wrapped = audit_tool_handler(handler, Arc::<ConsoleToolAuditor>::clone(&auditor), "a");

    let bounded = tokio::time::timeout(
        Duration::from_millis(10),
        wrapped.call("tool".to_owned(), Metadata::new()),
    )
    .await;

    assert!(bounded.is_err());
    let entry = only(&auditor);
    assert_eq!(
        (entry.status.as_str(), entry.result.as_str()),
        ("cancelled", "")
    );
    assert!(entry.duration_ms >= 10.0, "{}", entry.duration_ms);
}

/// A call dropped before it ran is recorded as nothing ran: Python's
/// coroutine never awaited runs no `finally`.
#[tokio::test]
async fn a_call_dropped_before_it_ran_records_nothing() {
    let (auditor, wrapped) = audited(Ok(ToolResult::Text("done".to_owned())), "a");
    drop(wrapped.call("tool".to_owned(), Metadata::new()));
    assert_eq!(auditor.entries(), []);
}
