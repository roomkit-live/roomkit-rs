//! Ordinary tool diagnostics, separate from payload hooks and telemetry spans.

use std::time::Duration;

use roomkit::content_logging_enabled;
use roomkit::model::ToolCall;
use roomkit_core::rules::python_repr;
use serde_json::Value as Json;

use crate::skills::dumps_unescaped;
use crate::tools::{ToolError, ToolResult, ToolResultPart};

pub(super) fn calling(call: &ToolCall) {
    let mut keys: Vec<_> = call.arguments.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let keys = if keys.is_empty() {
        "no arguments".to_owned()
    } else {
        keys.join(", ")
    };
    tracing::info!(target: "roomkit::ai", tool = %call.name, call = %call.id,
        "Executing tool {} (call {}) with {keys}", call.name, call.id);
    tracing::debug!(target: "roomkit::ai", tool = %call.name,
        "Tool {} arguments: {}", call.name, redact(&dumps_unescaped(&Json::Object(call.arguments.clone()))));
}

pub(super) fn answered(name: &str, output: &ToolResult, elapsed: Duration) {
    let text = if matches!(output, ToolResult::Parts(_)) {
        String::new()
    } else {
        output.as_text()
    };
    let size = if matches!(output, ToolResult::Parts(_)) {
        "-1".to_owned()
    } else {
        text.chars().count().to_string()
    };
    let milliseconds = elapsed.as_secs_f64() * 1_000.0;
    tracing::info!(target: "roomkit::ai", tool = %name, chars = %size, duration_ms = milliseconds,
        "Tool {name} returned {size} chars in {milliseconds:.0} ms");
    tracing::debug!(target: "roomkit::ai", tool = %name,
        "Tool {name} result: {}", redact(&rendered(output, text)));
}

pub(super) fn declined(name: &str, elapsed: Duration) {
    let milliseconds = elapsed.as_secs_f64() * 1_000.0;
    tracing::info!(target: "roomkit::ai", tool = %name, duration_ms = milliseconds,
        "Tool {name}: nothing served it ({milliseconds:.0} ms)");
}

pub(super) fn failed(name: &str, error: &ToolError) {
    match error {
        ToolError::Refused(message) => refused(name, message),
        ToolError::Failed(message) => {
            tracing::warn!(target: "roomkit::ai", tool = %name, "Tool {name} failed: {message}");
        }
        ToolError::Raised { kind, message } => {
            tracing::warn!(target: "roomkit::ai", tool = %name, "Tool {name} raised {kind}: {message}");
        }
        // An absent answer is logged by `declined`, without the private cause.
        ToolError::UnservedToolCall(_) => {}
    }
}

pub(super) fn refused(name: &str, message: &str) {
    tracing::info!(target: "roomkit::ai", tool = %name, "Tool {name} refused: {message}");
}

pub(super) fn timed_out(name: &str, detail: &str) {
    tracing::warn!(target: "roomkit::ai", tool = %name, "Tool {name} raised {detail}");
}

fn redact(text: &str) -> String {
    let text = text.replace('\n', " ");
    let count = text.chars().count();
    let preview = if count > 500 {
        format!(
            "{}… ({count} chars)",
            text.chars().take(500).collect::<String>()
        )
    } else {
        text
    };
    if content_logging_enabled() {
        preview
    } else {
        format!("<redacted:{} chars>", preview.chars().count())
    }
}

fn rendered(output: &ToolResult, text: String) -> String {
    let ToolResult::Parts(parts) = output else {
        return text;
    };
    let parts = parts
        .iter()
        .map(|part| {
            // Python's bounded DEBUG preview uses `json.dumps(default=str)` on
            // existing result parts. This formats data, without running media.
            Json::String(match part {
                ToolResultPart::Text(part) => {
                    format!("type='text' text={}", python_repr(&part.text))
                }
                ToolResultPart::Image(part) => format!(
                    "type='image' url={} mime_type={}",
                    python_repr(&part.url),
                    part.mime_type
                        .as_deref()
                        .map_or_else(|| "None".to_owned(), python_repr)
                ),
            })
        })
        .collect();
    dumps_unescaped(&Json::Array(parts))
}

pub(super) fn partial(call: &ToolCall, unreadable: bool) {
    let reason = if unreadable {
        "sent unreadable arguments for"
    } else {
        "cut"
    };
    tracing::warn!(target: "roomkit::ai", tool = %call.name, call = %call.id,
        "Provider {reason} tool call {} ({}): it does not run", call.name, call.id);
}

pub(super) fn withdrawn(name: &str) {
    tracing::warn!(target: "roomkit::ai", tool = %name, "Provider called {name}, withdrawn for this turn");
}

pub(super) fn undeclared(name: &str) {
    tracing::warn!(target: "roomkit::ai", tool = %name, "Provider requested undeclared tool {name}");
}

pub(super) fn policy(name: &str) {
    tracing::warn!(target: "roomkit::ai", tool = %name, "Tool {name} blocked by policy");
}

pub(super) fn skill(name: &str) {
    tracing::warn!(target: "roomkit::ai", tool = %name, "Tool {name} blocked by skill gating");
}

pub(super) fn before_refusal(name: &str) {
    tracing::info!(target: "roomkit::ai", tool = %name, "Tool {name} denied by BEFORE_TOOL_USE hook");
}

pub(super) fn rewritten(name: &str, why: &str) {
    tracing::warn!(target: "roomkit::ai", tool = %name, "Tool {name}: Invalid rewritten arguments for '{name}': {why}");
}
