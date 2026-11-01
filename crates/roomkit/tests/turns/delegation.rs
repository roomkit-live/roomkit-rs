//! A delegated turn on the trace path (RFC §23.3 steps 4 to 6), through a
//! kit and an AI agent that streams: the child room keeps the agent's
//! trace, text segments and calls in order, each row answering the task's
//! description, and the task ends with the agent's last segment.

use pretty_assertions::assert_eq;
use roomkit::model::{EventContent, EventType, TaskStatus, Visibility};
use roomkit::{DelegateOptions, GetTimelineOptions};
use roomkit_ai::ToolResult;

use crate::support::{channel, kit, room};

/// What a row of the child room says: its text, or its call's tool.
fn said(content: &EventContent) -> String {
    if let EventContent::Text(text) = content {
        return text.body.clone();
    }
    if let EventContent::ToolCall(call) = content {
        return call.tool_name.clone();
    }
    format!("{content:?}")
}

// Python: tests/tasks/test_child_room_trace.py::TestPersistChildStream::test_persists_tool_calls_and_text_segments_in_order
#[tokio::test(start_paused = true)]
async fn a_streamed_worker_s_trace_keeps_its_calls_and_segments_in_order() {
    let kit = kit(|_call, _context| async { Ok(ToolResult::Text("42".to_owned())) }).await;

    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let task = kit
        .delegate(&room(), &channel("ai"), "What is it?", options)
        .await
        .unwrap();

    let result = task.result().unwrap();
    let rows: Vec<_> = kit
        .get_timeline(&task.child_room_id, GetTimelineOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.visibility != Visibility::Internal)
        .collect();
    let description = rows.first().map(|row| row.id.clone());
    let trace: Vec<(EventType, String, bool)> = rows
        .iter()
        .skip(1)
        .map(|row| {
            let answers = row.responds_to == description;
            (row.r#type, said(&row.content), answers)
        })
        .collect();
    assert_eq!(
        (result.status, result.output.as_deref(), trace),
        (
            TaskStatus::Completed,
            Some("It is 42."),
            vec![
                (EventType::Message, "Let me check.".to_owned(), true),
                (EventType::ToolCallStart, "lookup".to_owned(), true),
                (EventType::ToolCallEnd, "lookup".to_owned(), true),
                (EventType::Message, "It is 42.".to_owned(), true),
            ]
        )
    );
}
