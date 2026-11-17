//! The default worker result tool for a structured delegation (RFC §23.3).

use std::sync::{Arc, LazyLock};

use roomkit_core::model::{Metadata, truthy_python_str};
use serde_json::{Value, json};

use super::ResultTool;

/// The default worker result tool, shared with [`crate::SUBMIT_RESULT`].
/// Pass its clone in [`crate::DelegateOptions::result_tool`] to select it explicitly.
pub static SUBMIT_RESULT: LazyLock<ResultTool> = LazyLock::new(|| {
    ResultTool::new(
        submit_result_definition(),
        Arc::new(normalize),
        Arc::new(missing_payload),
        "You did not submit a result. You MUST now call the `submit_result` tool \
with your final structured result. Do NOT reply with plain text or a question — call submit_result.",
    )
});

/// A result call's arguments in the payload's shape, in Python's order:
/// Python's `normalize_result`.
fn normalize(arguments: &Metadata) -> Metadata {
    let truthy = |key: &str| arguments.get(key).filter(|value| is_truthy(value));
    let text = |key: &str| {
        Value::String(
            arguments
                .get(key)
                .and_then(truthy_python_str)
                .unwrap_or_default(),
        )
    };
    let shaped = |key: &str, keep: fn(&Value) -> bool, empty: Value| {
        arguments
            .get(key)
            .filter(|value| keep(value))
            .cloned()
            .unwrap_or(empty)
    };
    Metadata::from_iter(
        [
            (
                "status",
                truthy("status")
                    .cloned()
                    .unwrap_or_else(|| json!("completed")),
            ),
            ("summary", text("summary")),
            ("data", shaped("data", Value::is_object, json!({}))),
            (
                "deliverables",
                shaped("deliverables", Value::is_array, json!([])),
            ),
            ("reason", text("reason")),
        ]
        .map(|(key, value)| (key.to_owned(), value)),
    )
}

/// What a delegation returns for a worker that never called the result
/// tool after `attempts` tries: Python's `orchestration_fail`, a failure of
/// the mechanism, not the worker's own.
fn missing_payload(role: &str, last_output: &str, attempts: u32) -> Metadata {
    Metadata::from_iter(
        [
            ("status", json!("failed")),
            ("by", json!("orchestration")),
            (
                "reason",
                json!(format!("no_structured_result_after_{attempts}_attempts")),
            ),
            ("role", json!(role)),
            ("last_output", json!(last_output)),
        ]
        .map(|(key, value)| (key.to_owned(), value)),
    )
}

/// Whether Python reads `value` as true.
fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// The result tool's definition: Python's `SUBMIT_RESULT_TOOL`.
fn submit_result_definition() -> Value {
    json!({
        "name": "submit_result",
        "description": "Submit your FINAL result for this task. You MUST call this exactly once, \
    when your work is done — it is the ONLY way to hand your work to the next step. Do NOT end your \
    turn with a plain message or a question back to the user; call submit_result with your \
    structured result.",
        "parameters": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["completed", "failed"],
                    "description": "completed if you did the task; failed only if you genuinely could not.",
                },
                "summary": {
                    "type": "string",
                    "description": "One or two sentences summarizing what you produced.",
                },
                "data": {
                    "type": "object",
                    "description": "Your structured result, for the next step to build on.",
                },
                "deliverables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "url": {"type": "string"},
                        },
                    },
                    "description": "Concrete artifacts you produced (e.g. a published report URL).",
                },
                "reason": {
                    "type": "string",
                    "description": "If status is failed, explain why.",
                },
            },
            "required": ["status", "summary"],
        },
    })
}
