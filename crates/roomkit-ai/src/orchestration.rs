//! The audit of a tool handler's calls (RFC §15.8.1): Python's
//! `audit_tool_handler`, of `roomkit.orchestration`. The auditors and their
//! entries are `roomkit::orchestration`'s; the wrapper is here, beside the
//! [`ToolHandler`] it wraps.

use std::sync::Arc;
use tokio::time::Instant;

use roomkit::model::Metadata;
use roomkit::orchestration::{ToolAuditEntry, ToolAuditor};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::Value;

use crate::tools::{ToolError, ToolHandler, ToolResult};

/// The longest result an entry records, in characters.
const MAX_RESULT_LEN: usize = 500;

/// `handler`, each of its calls recorded to `auditor` under `agent_id`:
/// Python's `audit_tool_handler(handler, auditor, agent_id)`.
///
/// The wrapper answers what `handler` answers, its error included, so the
/// channel reads the call as it would without the audit. The record reads
/// the answer as text, cut at 500 characters: `failed` when its body is a
/// failure envelope (`{"status": "failed"}`, `{"error": ...}`,
/// `{"success": false}`), `ok` otherwise; a refusal, a failure in the
/// handler's words or a decline records `failed` with its words, an error
/// the handler raised `error` with its message, and a call dropped before
/// its handler answered, its turn ended or the channel's per-call bound
/// cut (RFC §21.6), `cancelled`.
///
/// ```
/// use std::sync::Arc;
/// use roomkit::orchestration::{ConsoleToolAuditor, ToolAuditor};
/// use roomkit_ai::orchestration::audit_tool_handler;
/// use roomkit_ai::{ToolHandler, ToolResult};
///
/// let auditor = Arc::new(ConsoleToolAuditor::new(Default::default()));
/// let handler = ToolHandler::new(|_name, _arguments| async { Ok(ToolResult::Text("done".into())) });
/// let audited = audit_tool_handler(handler, auditor.clone(), "assistant");
/// # let _ = audited;
/// ```
pub fn audit_tool_handler(
    handler: ToolHandler,
    auditor: Arc<dyn ToolAuditor>,
    agent_id: impl Into<String>,
) -> ToolHandler {
    let agent_id: Arc<str> = agent_id.into().into();
    ToolHandler::new(move |name, arguments| {
        let (handler, auditor, agent_id) =
            (handler.clone(), Arc::clone(&auditor), Arc::clone(&agent_id));
        async move {
            // Started once the call runs, as Python's coroutine reads its
            // clock: a call dropped before then never ran.
            let mut record = Record {
                auditor,
                agent_id,
                tool_name: name.clone(),
                arguments: arguments.clone(),
                started: Instant::now(),
                outcome: None,
            };
            let answer = handler.call(name, arguments).await;
            record.outcome = Some(outcome(&answer));
            answer
        }
    })
}

/// A call under way, recorded when it ends or is dropped: Python's
/// `finally`, which a cancellation runs too.
struct Record {
    auditor: Arc<dyn ToolAuditor>,
    agent_id: Arc<str>,
    tool_name: String,
    arguments: Metadata,
    started: Instant,
    /// The status and the text recorded, once the handler answered.
    outcome: Option<(&'static str, String)>,
}

impl Drop for Record {
    fn drop(&mut self) {
        let (status, recorded) = self.outcome.take().unwrap_or_else(|| {
            // A panic unwinding the call is the handler's error; a call
            // dropped otherwise was cancelled.
            if std::thread::panicking() {
                ("error", "the tool handler panicked".to_owned())
            } else {
                ("cancelled", String::new())
            }
        });
        let duration_ms = self.started.elapsed().as_secs_f64() * 1000.0;
        self.auditor.record(ToolAuditEntry::new(
            SystemEnvironment.now().to_iso_offset(),
            self.agent_id.as_ref(),
            std::mem::take(&mut self.tool_name),
            std::mem::take(&mut self.arguments),
            cut(&recorded),
            status,
            duration_ms,
        ));
    }
}

/// The status and the text a call's `answer` records.
fn outcome(answer: &Result<ToolResult, ToolError>) -> (&'static str, String) {
    match answer {
        Ok(result) => {
            let text = result.as_text();
            (detect_status(&text), text)
        }
        // Refused in the handler's words, failed in them, or not its tool:
        // the call did not do it, which is neither ok nor an error.
        Err(
            ToolError::Refused(words)
            | ToolError::Failed(words)
            | ToolError::UnservedToolCall(words),
        ) => ("failed", words.clone()),
        Err(ToolError::Raised { message, .. }) => ("error", message.clone()),
    }
}

/// `text`'s first 500 characters.
fn cut(text: &str) -> String {
    text.chars().take(MAX_RESULT_LEN).collect()
}

/// The status a result's body says: `failed` for a failure envelope,
/// `ok` for any other body, Python's `_detect_status`. A body outside
/// every envelope reads `ok`: what a producer that said nothing degrades
/// to, not a contract (RFC §15.8.1).
fn detect_status(result: &str) -> &'static str {
    let Ok(Value::Object(body)) = serde_json::from_str::<Value>(result) else {
        return "ok";
    };
    let stated = body.get("status").and_then(Value::as_str);
    if stated.is_some_and(|status| matches!(status.to_lowercase().as_str(), "failed" | "error")) {
        return "failed";
    }
    let success = body.get("success");
    let errs = body.get("error").is_some_and(truthy) && success != Some(&Value::Bool(true));
    if success == Some(&Value::Bool(false)) || errs {
        return "failed";
    }
    "ok"
}

/// Whether `value` reads as true, as Python's `if value`.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}
