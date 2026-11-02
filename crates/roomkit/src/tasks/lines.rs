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

/// One task's line, its members in the order Python's dict holds them: a
/// member written again keeps its place, a new one goes last.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Line {
    members: Vec<(String, Value)>,
}

impl Line {
    fn insert(&mut self, key: &str, value: Value) {
        match self.members.iter_mut().find(|(name, _)| name == key) {
            Some((_, held)) => *held = value,
            None => self.members.push((key.to_owned(), value)),
        }
    }

    fn remove(&mut self, key: &str) {
        self.members.retain(|(name, _)| name != key);
    }

    fn contains(&self, key: &str) -> bool {
        self.members.iter().any(|(name, _)| name == key)
    }

    /// The member `key`, if the line holds it.
    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        self.members
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    /// The members, in their order.
    pub(crate) fn members(&self) -> &[(String, Value)] {
        &self.members
    }

    /// The line as a record.
    pub(crate) fn to_metadata(&self) -> Metadata {
        self.members.iter().cloned().collect()
    }
}

/// The tasks `bus` lists for `room_id`, read from its latest `window`
/// entries: a task's own, and a strategy run's terminal entry naming its
/// task under the run's own action.
pub(crate) async fn room_task_lines(
    bus: &StatusBus,
    room_id: &str,
    window: usize,
) -> Vec<Metadata> {
    room_lines(bus, room_id, window)
        .await
        .iter()
        .map(Line::to_metadata)
        .collect()
}

/// The lines of [`room_task_lines`], in Python's member order.
pub(crate) async fn room_lines(bus: &StatusBus, room_id: &str, window: usize) -> Vec<Line> {
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
pub(crate) fn room_tasks<'a>(entries: impl Iterator<Item = &'a StatusEntry>) -> Vec<Line> {
    let mut order: Vec<String> = Vec::new();
    let mut lines: std::collections::HashMap<String, Line> = std::collections::HashMap::new();
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
            // Keyed again by its task, the run's line goes last, as
            // Python's `tasks[task_id] = tasks.pop(by_agent)` moves it.
            if let Some(line) = lines.remove(&by_agent) {
                lines.insert(task_id.clone(), line);
            }
            order.retain(|key| *key != by_agent);
            order.push(task_id.clone());
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
            let mut line = Line::default();
            line.insert("task_id", json!(task_id));
            line.insert("agent", json!(entry.agent_id));
            line.insert("task", json!(""));
            line
        });
        if !task_id.is_empty() {
            line.insert("task_id", json!(task_id));
        }
        read_entry(line, entry);
    }
    order
        .into_iter()
        .filter_map(|key| lines.remove(&key))
        .collect()
}

/// What a task's own entry says of its line.
fn read_entry(line: &mut Line, entry: &StatusEntry) {
    match entry.status {
        StatusLevel::Pending => {
            line.insert("task", json!(entry.detail));
            line.insert("status", json!("running"));
            line.insert("since", json!(entry.ts));
            line.remove("result");
            line.remove("ended");
        }
        StatusLevel::Info => {
            if !line.contains("status") {
                line.insert("status", json!("running"));
            }
            line.insert("progress", json!(entry.detail));
            line.insert("progress_at", json!(entry.ts));
        }
        StatusLevel::Ok | StatusLevel::Completed | StatusLevel::Failed => {
            let status = entry
                .metadata
                .get("task_status")
                .filter(|status| truthy(status))
                .and_then(Value::as_str)
                .map_or_else(|| level_name(entry.status), str::to_owned);
            line.insert("status", json!(status));
            line.insert("ended", json!(entry.ts));
            if status == "completed" {
                let result = entry
                    .metadata
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| json!(entry.detail));
                line.insert("result", result);
            }
        }
    }
}

/// Ends `line` with `entry`, the terminal entry its strategy's run posted
/// under its own action: failed when the run says so, else completed.
fn end_run_line(line: &mut Line, entry: &StatusEntry) {
    let status = if entry.status == StatusLevel::Failed {
        "failed"
    } else {
        "completed"
    };
    line.insert("status", json!(status));
    line.insert("ended", json!(entry.ts));
    if status == "completed" {
        let result = entry
            .metadata
            .get("result")
            .cloned()
            .unwrap_or_else(|| json!(entry.detail));
        line.insert("result", result);
    }
}

/// A level's name, as Python's `str(StatusLevel)` writes it.
fn level_name(level: StatusLevel) -> String {
    serde_json::to_value(level)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn entry(agent: &str, status: StatusLevel, detail: &str, task_id: Option<&str>) -> StatusEntry {
        let mut entry = StatusEntry::new("2026-10-05T18:00:00+00:00", agent, TASK_ACTION, status);
        entry.detail = detail.to_owned();
        entry.metadata.insert("room_id".to_owned(), json!("r"));
        if let Some(task_id) = task_id {
            entry.metadata.insert("task_id".to_owned(), json!(task_id));
        }
        entry
    }

    fn read(lines: &[Line]) -> Vec<(Value, Value, Value)> {
        lines
            .iter()
            .map(|line| {
                let member = |key| line.get(key).cloned().unwrap_or(Value::Null);
                (member("task_id"), member("agent"), member("status"))
            })
            .collect()
    }

    // Python: tests/test_task_status.py::test_a_running_task_reads_running_and_an_orchestration_run_reads_as_one_task
    #[test]
    fn a_running_task_reads_running_and_an_orchestration_run_reads_as_one_task() {
        let entries = [
            entry("worker", StatusLevel::Pending, "The weather?", Some("t1")),
            // A worker run, no id yet.
            entry("writer", StatusLevel::Pending, "Write it.", None),
            entry("writer", StatusLevel::Completed, "The text.", Some("t2")),
        ];
        let lines = room_tasks(entries.iter());

        assert_eq!(
            read(&lines),
            vec![
                (json!("t1"), json!("worker"), json!("running")),
                (json!("t2"), json!("writer"), json!("completed")),
            ]
        );
        assert_eq!(lines[1].get("task"), Some(&json!("Write it.")));
    }

    /// The run's line, keyed again by its task once its terminal entry names
    /// it, goes last, as Python's `tasks[task_id] = tasks.pop(by_agent)`
    /// moves it in the dict's order.
    #[test]
    fn a_run_s_line_keyed_by_its_task_goes_last() {
        let entries = [
            entry("writer", StatusLevel::Pending, "Write it.", None),
            entry("worker", StatusLevel::Pending, "The weather?", Some("t1")),
            entry("writer", StatusLevel::Completed, "The text.", Some("t2")),
        ];

        assert_eq!(
            read(&room_tasks(entries.iter())),
            vec![
                (json!("t1"), json!("worker"), json!("running")),
                (json!("t2"), json!("writer"), json!("completed")),
            ]
        );
    }
}
