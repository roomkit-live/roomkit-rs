//! The task projection is available to a caller with its own ordered status entries.

use roomkit::orchestration::{StatusEntry, StatusLevel};
use roomkit::tasks::room_tasks;
use serde_json::{Value, json};

fn entry(agent: &str, action: &str, status: StatusLevel, detail: &str, id: Value) -> StatusEntry {
    let mut entry = StatusEntry::new(detail, agent, action, status);
    detail.clone_into(&mut entry.detail);
    if !id.is_null() {
        entry.metadata.insert("task_id".to_owned(), id);
    }
    entry
}

#[test]
fn rekeying_and_progress_preserve_each_row_and_its_field_order() {
    use StatusLevel::{Completed, Info, Pending};

    let mut entries = vec![
        entry("writer", "task", Pending, "draft", Value::Null),
        entry("reader", "task", Info, "looking", json!("t1")),
        entry("writer", "task", Completed, "done", json!("t2")),
        entry("writer", "task", Info, "late", json!("t2")),
        entry("ignored", "review", Completed, "unused", json!("unseen")),
    ];
    entries[0].metadata.insert("room_id".into(), json!("r1"));
    entries[1].metadata.insert("room_id".into(), json!("r2"));
    entries[2]
        .metadata
        .insert("result".into(), json!({"last": 1, "first": 2}));
    let mut result = room_tasks(&entries);
    assert_eq!(
        serde_json::to_string(&result).unwrap(),
        concat!(
            r#"[{"task_id":"t1","agent":"reader","task":"","status":"running","progress":"looking","progress_at":"looking"},"#,
            r#"{"task_id":"t2","agent":"writer","task":"draft","status":"completed","since":"draft","ended":"done","result":{"last":1,"first":2},"progress":"late","progress_at":"late"}]"#
        )
    );
    result[1]["result"]["first"] = json!("altered");
    assert_eq!(entries[2].metadata["result"]["first"], 2);
    assert_eq!(room_tasks(entries.into_iter())[1]["result"]["first"], 2);
}

#[test]
fn a_strategy_closes_an_existing_line_and_a_new_pending_clears_its_result() {
    use StatusLevel::{Completed, Info, Pending};

    let mut run = entry("strategy", "review", Completed, "reviewed", json!("t1"));
    run.metadata.insert("result".into(), Value::Null);
    let mut entries = vec![entry("worker", "task", Info, "working", json!("t1")), run];
    let complete = room_tasks(&entries);
    assert_eq!(complete[0].get("result"), Some(&Value::Null));
    assert_eq!(complete[0]["ended"], "reviewed");
    entries.push(entry("worker", "task", Pending, "again", json!("t1")));
    let running = room_tasks(entries);
    assert_eq!(running[0]["task"], "again");
    assert_eq!(running[0]["since"], "again");
    assert_eq!(running[0]["status"], "running");
    assert_eq!(running[0]["progress"], "working");
    assert!(!running[0].contains_key("result"));
    assert!(!running[0].contains_key("ended"));
    assert_eq!(
        room_tasks(Vec::<StatusEntry>::new()),
        Vec::<serde_json::Map<String, Value>>::new()
    );
}

#[test]
fn task_ids_and_status_overrides_read_json_as_python_does() {
    use StatusLevel::Completed;

    let mut numbered = entry("a", "task", Completed, "ignored", json!(42));
    numbered.metadata.insert("task_status".into(), json!(true));
    let mut nested = entry("b", "task", Completed, "ignored", json!(["id", false]));
    nested
        .metadata
        .insert("task_status".into(), json!({"mode": null}));
    let fallback = entry("c", "task", Completed, "fallback", json!(false));
    let tasks = room_tasks([numbered, nested, fallback]);
    assert_eq!(tasks[0]["task_id"], "42");
    assert_eq!(tasks[0]["status"], "True");
    assert!(!tasks[0].contains_key("result"));
    assert_eq!(tasks[1]["task_id"], "['id', False]");
    assert_eq!(tasks[1]["status"], "{'mode': None}");
    assert_eq!(tasks[2]["task_id"], "");
    assert_eq!(tasks[2]["result"], "fallback");
}
