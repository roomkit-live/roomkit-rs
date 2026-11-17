//! Tool text is opt-in for subscribers, independently of stored rows.

use super::super::channel_with_options;
use super::{Agent, PATIENCE, close, room, send};
use crate::AcpChannelOptions;
use roomkit::model::{EventContent, EventType, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::{ConversationStore, SubscribeRoomOptions, TurnRecord};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

async fn update(agent: &mut Agent, update: Value) {
    let line = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1", "update":update}});
    agent
        .writer
        .write_all(format!("{line}\n").as_bytes())
        .await
        .unwrap();
}

async fn events(forced: bool, publish_tool_call_text: bool) -> Vec<EphemeralEvent> {
    let (channel, mut opened, _) = channel_with_options(AcpChannelOptions {
        publish_tool_call_text,
        ..Default::default()
    });
    let kit = room(channel).await;
    let (sent, mut heard) = mpsc::unbounded_channel();
    kit.subscribe_room(
        &RoomId::new("r1").unwrap(),
        move |event| {
            let sent = sent.clone();
            async move {
                sent.send(event).unwrap();
            }
        },
        SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    update(
        &mut agent,
        json!({"sessionUpdate":"tool_call","toolCallId":"read",
        "title":"Read","status":"in_progress","rawInput":{"secret":"private-input"}}),
    )
    .await;
    let start = loop {
        let event = heard.recv().await.unwrap();
        if event.r#type == EphemeralEventType::ToolCallStart {
            break event;
        }
    };
    assert!(
        !running.is_finished(),
        "the start must precede prompt completion"
    );
    let progress = if forced {
        None
    } else {
        update(&mut agent, rich_progress()).await;
        Some(next_progress(&mut heard).await)
    };
    if !forced {
        update(
            &mut agent,
            json!({"sessionUpdate":"tool_call_update","toolCallId":"read",
            "status":"completed","rawOutput":"é🦀".repeat(300)}),
        )
        .await;
    }
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(running.await.unwrap().error.is_none());
    let end = loop {
        let event = heard.recv().await.unwrap();
        if event.r#type == EphemeralEventType::ToolCallEnd {
            break event;
        }
    };
    assert_rows(&kit, forced).await;
    close(&kit, &mut agent).await;
    assert!(heard.try_recv().is_err(), "one start and end only");
    assert_shape(&start, &end, forced);
    let mut events = vec![start, end];
    events.extend(progress);
    events
}

fn rich_progress() -> Value {
    json!({"sessionUpdate":"tool_call_update","toolCallId":"read","status":"in_progress",
        "rawInput":{"path":"step.example"},"rawOutput":"intermediate output",
        "content":[{"type":"content","content":{"type":"text","text":"progress text"}}],
        "locations":[{"path":"step.example","line":3}]})
}

async fn next_progress(heard: &mut mpsc::UnboundedReceiver<EphemeralEvent>) -> EphemeralEvent {
    let event = heard.recv().await.unwrap();
    assert_eq!(event.r#type, EphemeralEventType::Custom);
    assert_eq!(event.data["type"], "acp_tool_progress");
    event
}

async fn assert_rows(kit: &roomkit::RoomKit, forced: bool) {
    let rows = kit
        .store()
        .list_events(
            &RoomId::new("r1").unwrap(),
            roomkit::store::ListEventsOptions::default(),
        )
        .await
        .unwrap();
    let start_row = rows
        .iter()
        .find(|row| row.r#type == EventType::ToolCallStart)
        .unwrap();
    let EventContent::ToolCall(call) = &start_row.content else {
        panic!("tool start");
    };
    assert_eq!(
        call.arguments,
        serde_json::from_value(json!({"secret":"private-input"})).unwrap()
    );
    if !forced {
        let end_row = rows
            .iter()
            .find(|row| row.r#type == EventType::ToolCallEnd)
            .unwrap();
        let EventContent::ToolCall(call) = &end_row.content else {
            panic!("tool end");
        };
        assert_eq!(call.result, json!("é🦀".repeat(300)));
    }
}

fn assert_shape(start: &EphemeralEvent, end: &EphemeralEvent, forced: bool) {
    assert_eq!(start.data["round"], 0);
    assert_eq!(end.data["round"], 0);
    assert_eq!(
        end.data["tool_calls"][0]["status"],
        if forced { "failed" } else { "completed" }
    );
    assert!(end.data["duration_ms"].is_number());
    for event in [start, end] {
        assert_eq!(event.room_id.as_str(), "r1");
        assert_eq!(event.data["channel_id"], "claude");
        assert_eq!(event.data["tool_calls"][0]["id"], "read");
        assert_eq!(event.data["tool_calls"][0]["name"], "Read");
    }
}

// Shared source: acp/tool-publication-default; also checks the stored rows and live prompt.
#[tokio::test]
async fn default_tool_start_omits_arguments_but_keeps_the_stored_call() {
    let events = tokio::time::timeout(PATIENCE, events(false, false))
        .await
        .unwrap();
    assert!(events[0].data["tool_calls"][0].get("arguments").is_none());
}

#[tokio::test]
async fn default_tool_end_omits_result_but_keeps_the_stored_call() {
    let events = tokio::time::timeout(PATIENCE, events(false, false))
        .await
        .unwrap();
    assert!(events[1].data["tool_calls"][0].get("result").is_none());
}

#[tokio::test]
async fn default_forced_tool_end_omits_its_result_text() {
    let events = tokio::time::timeout(PATIENCE, events(true, false))
        .await
        .unwrap();
    assert!(events[1].data["tool_calls"][0].get("result").is_none());
}

// The same rich update as acp/tool-publication-default; the stored result is separate.
#[tokio::test]
async fn default_tool_progress_omits_rich_call_contents() {
    let events = tokio::time::timeout(PATIENCE, events(false, false))
        .await
        .unwrap();
    let progress = &events[2];
    assert_eq!(progress.data["session_id"], "session-r1");
    let call = &progress.data["tool_call"];
    assert_eq!(call["sessionUpdate"], "tool_call_update");
    assert_eq!(call["toolCallId"], "read");
    assert_eq!(call["status"], "in_progress");
    assert_eq!(
        call,
        &json!({"sessionUpdate":"tool_call_update","toolCallId":"read","status":"in_progress"})
    );
}

#[tokio::test]
async fn opted_in_tool_events_keep_input_and_a_unicode_result_preview() {
    let events = tokio::time::timeout(PATIENCE, events(false, true))
        .await
        .unwrap();
    assert_eq!(
        events[0].data["tool_calls"][0]["arguments"],
        json!({"secret":"private-input"})
    );
    assert_eq!(events[1].data["tool_calls"][0]["result"], "é🦀".repeat(250));
    assert_eq!(events[2].data["tool_call"], rich_progress());
}

#[tokio::test]
async fn opted_in_forced_tool_end_keeps_its_preview() {
    let events = tokio::time::timeout(PATIENCE, events(true, true))
        .await
        .unwrap();
    assert_eq!(
        events[0].data["tool_calls"][0]["arguments"],
        json!({"secret":"private-input"})
    );
    assert_eq!(
        events[1].data["tool_calls"][0]["result"],
        "The ACP turn ended before the tool reported a result"
    );
}
