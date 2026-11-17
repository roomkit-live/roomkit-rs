//! Validation and presentation of a plan. The public argument guard remains
//! shallow; errors here are served JSON results, not refused tool calls.

use roomkit::model::Metadata;
use roomkit_core::rules::transcript::quoted;
use serde_json::{Value, json};

use super::{MAX_TASKS, MAX_TITLE_LENGTH, PLAN_TOOL};
use crate::AiTool;

pub(super) const STATUSES: [&str; 4] = ["completed", "in_progress", "blocked", "pending"];

/// Host edits keep Python's `one_of` and `quoted(Any)` rendering. Only model
/// arguments pass through validation; an observer's open object does not.
pub(super) fn line(task: &Metadata) -> String {
    let status = task
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| STATUSES.contains(status))
        .unwrap_or("pending");
    let title = task.get("title").map_or_else(
        || "Untitled".to_owned(),
        |value| match value {
            Value::String(text) => text.clone(),
            value @ (Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::Array(_)
            | Value::Object(_)) => crate::memory::repr::repr(value),
        },
    );
    let icon = match status {
        "completed" => "[x]",
        "in_progress" => "[-]",
        "blocked" => "[!]",
        _ => "[ ]",
    };
    format!("- {icon} {} ({status})", quoted(&title, MAX_TITLE_LENGTH))
}

pub(super) fn validated(given: Option<&Value>) -> Result<Vec<Metadata>, String> {
    let tasks = match given {
        None => return Ok(Vec::new()),
        Some(Value::Array(tasks)) => tasks,
        Some(_) => return Err("Invalid plan: 'tasks' must be an array".to_owned()),
    };
    if tasks.len() > MAX_TASKS {
        return Err(format!(
            "Invalid plan: at most {MAX_TASKS} tasks are allowed"
        ));
    }
    tasks
        .iter()
        .enumerate()
        .map(|(index, task)| validated_task(index, task))
        .collect()
}

fn validated_task(index: usize, task: &Value) -> Result<Metadata, String> {
    let prefix = format!("Invalid plan: task {index}");
    let Some(task) = task.as_object() else {
        return Err(format!("{prefix} must be an object"));
    };
    let Some(title) = task.get("title").and_then(Value::as_str) else {
        return Err(format!("{prefix} title must be a string"));
    };
    if title.chars().count() > MAX_TITLE_LENGTH {
        return Err(format!(
            "{prefix} title must be at most {MAX_TITLE_LENGTH} characters"
        ));
    }
    let Some(status) = task
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| STATUSES.contains(status))
    else {
        return Err(format!(
            "{prefix} status must be one of {}",
            STATUSES.join(", ")
        ));
    };
    Ok(Metadata::from_iter([
        ("title".to_owned(), json!(title)),
        ("status".to_owned(), json!(status)),
    ]))
}

pub(super) fn summary(tasks: &[Metadata]) -> String {
    let count = |status: &str| {
        tasks
            .iter()
            .filter(|task| task.get("status").and_then(Value::as_str) == Some(status))
            .count()
    };
    format!(
        "{{\"status\": \"ok\", \"task_count\": {}, \"pending\": {}, \"in_progress\": {}, \"completed\": {}, \"blocked\": {}}}",
        tasks.len(),
        count("pending"),
        count("in_progress"),
        count("completed"),
        count("blocked"),
    )
}

pub(super) fn definition() -> AiTool {
    AiTool {
        name: PLAN_TOOL.to_owned(),
        description: "Create or update a structured task plan. Use this to break down complex work into steps and track progress.".to_owned(),
        parameters: json!({
            "type": "object", "properties": {"tasks": {
                "type": "array", "maxItems": MAX_TASKS, "items": {
                    "type": "object", "properties": {
                        "title": {"type": "string", "maxLength": MAX_TITLE_LENGTH},
                        "status": {"type": "string", "enum": ["pending", "in_progress", "completed", "blocked"]}
                    }, "required": ["title", "status"], "additionalProperties": false
                }
            }}, "required": ["tasks"], "additionalProperties": false
        }),
        tags: Vec::new(), changes_state: false, defer_loading: false,
    }
}
