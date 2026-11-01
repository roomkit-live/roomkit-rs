//! A room's tasks as its status bus lists them (RFC §23.4): Python's
//! `tasks.status.room_tasks` and `room_task_lines`, what `task_status` and
//! a turn's host's `room_tasks` read.

use roomkit_core::model::Metadata;
use serde_json::{Value, json};

use crate::orchestration::{RecentOptions, StatusBus, StatusEntry, StatusLevel};

/// The action a task's entries carry.
const TASK_ACTION: &str = "task";

/// The metadata key of a strategy run's own entries.
const RUN_ENTRY: &str = "task_run";

/// How many of the bus's latest entries a room's tasks are read from.
pub(crate) const ROOM_TASKS_WINDOW: usize = 500;

/// The tasks `bus` lists for `room_id`, read from its latest `window`
/// entries: a task's own, and a strategy run's terminal entry naming its
/// task under the run's own action.
pub(crate) async fn room_task_lines(
    bus: &StatusBus,
    room_id: &str,
    window: usize,
) -> Vec<Metadata> {
    let options = RecentOptions {
        n: window,
        ..RecentOptions::default()
    };
    let entries = bus.recent(options).await;
    room_tasks(
        entries
            .iter()
            .filter(|entry| entry.room_id() == Some(room_id) && of_a_task(entry)),
    )
}

/// Whether a task's line reads `entry`: one posted under the task action,
/// or the end of a strategy's run that names its task.
fn of_a_task(entry: &StatusEntry) -> bool {
    if entry.action == TASK_ACTION {
        return true;
    }
    let names_its_task = entry.metadata.get(RUN_ENTRY).is_some_and(truthy)
        && entry.metadata.get("task_id").is_some_and(truthy);
    names_its_task && entry.status != StatusLevel::Pending
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::String(text) => !text.is_empty(),
        Value::Number(number) => number.as_f64() != Some(0.0),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// One line per task, from its entries in posting order: the task from
/// its `pending` entry, its state and result from its latest terminal
/// one, its progress from its latest `info` one, which never ends it.
pub(crate) fn room_tasks<'a>(entries: impl Iterator<Item = &'a StatusEntry>) -> Vec<Metadata> {
    let mut order: Vec<String> = Vec::new();
    let mut lines: std::collections::HashMap<String, Metadata> = std::collections::HashMap::new();
    for entry in entries {
        let task_id = entry
            .metadata
            .get("task_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if entry.action != TASK_ACTION {
            if let Some(line) = lines.get_mut(&task_id) {
                end_run_line(line, entry);
            }
            continue;
        }
        let by_agent = format!("agent:{}", entry.agent_id);
        if !task_id.is_empty() && !lines.contains_key(&task_id) && lines.contains_key(&by_agent) {
            if let Some(line) = lines.remove(&by_agent) {
                lines.insert(task_id.clone(), line);
            }
            for key in &mut order {
                if *key == by_agent {
                    key.clone_from(&task_id);
                }
            }
        }
        let key = if task_id.is_empty() {
            by_agent
        } else {
            task_id.clone()
        };
        if !lines.contains_key(&key) {
            order.push(key.clone());
        }
        let line = lines.entry(key).or_insert_with(|| {
            let mut line = Metadata::new();
            line.insert("task_id".to_owned(), json!(task_id));
            line.insert("agent".to_owned(), json!(entry.agent_id));
            line.insert("task".to_owned(), json!(""));
            line
        });
        if !task_id.is_empty() {
            line.insert("task_id".to_owned(), json!(task_id));
        }
        read_entry(line, entry);
    }
    order
        .into_iter()
        .filter_map(|key| lines.remove(&key))
        .collect()
}

/// What a task's own entry says of its line.
fn read_entry(line: &mut Metadata, entry: &StatusEntry) {
    match entry.status {
        StatusLevel::Pending => {
            line.insert("task".to_owned(), json!(entry.detail));
            line.insert("status".to_owned(), json!("running"));
            line.insert("since".to_owned(), json!(entry.ts));
            line.remove("result");
            line.remove("ended");
        }
        StatusLevel::Info => {
            line.entry("status").or_insert_with(|| json!("running"));
            line.insert("progress".to_owned(), json!(entry.detail));
            line.insert("progress_at".to_owned(), json!(entry.ts));
        }
        StatusLevel::Ok | StatusLevel::Completed | StatusLevel::Failed => {
            let status = entry
                .metadata
                .get("task_status")
                .filter(|status| truthy(status))
                .and_then(Value::as_str)
                .map_or_else(|| level_name(entry.status), str::to_owned);
            line.insert("ended".to_owned(), json!(entry.ts));
            if status == "completed" {
                let result = entry
                    .metadata
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| json!(entry.detail));
                line.insert("result".to_owned(), result);
            }
            line.insert("status".to_owned(), json!(status));
        }
    }
}

/// Ends `line` with `entry`, the terminal entry its strategy's run posted
/// under its own action: failed when the run says so, else completed.
fn end_run_line(line: &mut Metadata, entry: &StatusEntry) {
    let status = if entry.status == StatusLevel::Failed {
        "failed"
    } else {
        "completed"
    };
    line.insert("status".to_owned(), json!(status));
    line.insert("ended".to_owned(), json!(entry.ts));
    if status == "completed" {
        let result = entry
            .metadata
            .get("result")
            .cloned()
            .unwrap_or_else(|| json!(entry.detail));
        line.insert("result".to_owned(), result);
    }
}

/// A level's name, as Python's `str(StatusLevel)` writes it.
fn level_name(level: StatusLevel) -> String {
    serde_json::to_value(level)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}
