//! Calls kept by one channel's tool memory when it reopens a room.
use super::open_calls::OpenCalls;
use crate::model::{EventContent, EventType, Metadata, RoomEvent};
use crate::rules::{json_truthy, withheld_history};
use serde_json::{Value, json};

/// Read a channel's stored tool rows in timeline order. The model's arguments
/// come from its start, including when the handler received a rewritten copy.
/// Only skill activation also records the arguments with which it actually ran.
#[must_use]
pub fn remembered_calls(events: &[RoomEvent]) -> Vec<Metadata> {
    let rows = withheld_history(events);
    let mut pending = OpenCalls::new();
    let mut calls = Vec::new();
    for row in &rows {
        let turn = row.correlation_id.as_deref();
        let EventContent::ToolCall(content) = &row.content else {
            pending.round_over(turn);
            continue;
        };
        let key = (turn, content.tool_id.as_str());
        if row.r#type == EventType::ToolCallStart {
            pending.hold(key, &content.arguments);
            continue;
        }
        pending.round_over(turn);
        if row.r#type != EventType::ToolCallEnd || content.tool_name.is_empty() {
            continue;
        }
        let arguments = pending.take(key);
        if content.kept_in_tool_memory() {
            let mut details = Metadata::from_iter([
                ("name".into(), json!(content.tool_name)),
                (
                    "arguments".into(),
                    Value::Object(arguments.cloned().unwrap_or_default()),
                ),
                (
                    "result".into(),
                    if json_truthy(&content.result) {
                        content.result.clone()
                    } else {
                        Value::String(String::new())
                    },
                ),
                ("outcome".into(), json!(content.outcome)),
            ]);
            if content.tool_name == "activate_skill" {
                details.insert("ran_with".into(), json!(content.arguments));
            }
            calls.push(details);
        }
    }
    calls
}
