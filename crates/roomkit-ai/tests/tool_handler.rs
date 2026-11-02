//! An AI channel's host handler, which its setter replaces (RFC §21.1),
//! as the Python implementation's `AIChannel.tool_handler` setter, and the
//! call a handler runs within, which a task it spawns with
//! `roomkit::keep_context` reads (RFC §21.4).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource, Room,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, ToolHandler, ToolResult};

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A handler answering `answer`, recording each tool it served.
fn recording(answer: &'static str, ran: &Arc<Mutex<Vec<String>>>) -> ToolHandler {
    let ran = Arc::clone(ran);
    ToolHandler::new(move |name, _arguments| {
        ran.lock().unwrap().push(name);
        async move { Ok(ToolResult::Text(answer.to_owned())) }
    })
}

/// One turn of `agent` on a message.
async fn turn(agent: &AiChannel<ScriptedProvider>) {
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(ChannelId::new("sms1").unwrap(), ChannelType::SMS),
        EventContent::Text(TextContent::new("hello")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(ChannelId::new("ai1").unwrap(), room_id(), ChannelType::AI)
    };
    let context = RoomContext::new(Room::new(room_id()));
    agent.on_event(event, binding, context).await.unwrap();
}

/// The tool results the model read, in order.
fn results(model: &ScriptedProvider) -> Vec<String> {
    model
        .requests()
        .iter()
        .flat_map(|request| request.messages.clone())
        .filter_map(|message| message.tool_result().map(|result| result.result.clone()))
        .collect()
}

/// Replacing the host's handler serves the next calls with the new one,
/// and the reading gives it back; clones share it.
#[tokio::test]
async fn a_handler_set_later_serves_the_next_calls() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("first"),
        ScriptedRound::calling("", [("c2", "lookup", "{}")]),
        ScriptedRound::answering("second"),
    ]);
    let (old, new) = (Arc::default(), Arc::default());
    let options = AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Looks up").unwrap().into()],
        tool_handler: Some(recording("old answer", &old)),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model.clone(), options).unwrap();
    turn(&agent).await;
    let replacement = recording("new answer", &new);
    agent.clone().set_tool_handler(Some(replacement));
    turn(&agent).await;
    assert_eq!(
        results(&model).last().map(String::as_str),
        Some("new answer")
    );
    assert_eq!(
        (old.lock().unwrap().clone(), new.lock().unwrap().clone()),
        (vec!["lookup".to_owned()], vec!["lookup".to_owned()])
    );
    assert!(agent.tool_handler().is_some());
    agent.set_tool_handler(None);
    assert!(agent.tool_handler().is_none());
}

// Python: tests/test_hook_tool_restrictions.py::test_nothing_declared_nothing_callable
#[tokio::test]
async fn nothing_declared_nothing_callable() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c0", "delegate_to_researcher", "{}")]),
        ScriptedRound::answering("done"),
    ]);
    let ran = Arc::default();
    let agent = AiChannel::new(
        ChannelId::new("ai1").unwrap(),
        model.clone(),
        AiChannelOptions::default(),
    )
    .unwrap();
    agent.set_tool_handler(Some(recording("ran", &ran)));
    turn(&agent).await;
    assert_eq!(ran.lock().unwrap().clone(), Vec::<String>::new());
    assert!(results(&model)[0].contains("is not declared"));
}

/// A task the handler spawns with `roomkit::keep_context` reads the call,
/// as Python's `asyncio.create_task` copies its context; one spawned
/// plainly reads none (RMK-1076).
#[tokio::test]
async fn a_task_the_handler_spawns_with_keep_context_reads_the_call() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("done"),
    ]);
    let seen: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
    let record = Arc::clone(&seen);
    let handler = ToolHandler::new(move |_name, _arguments| {
        let record = Arc::clone(&record);
        async move {
            let kept = tokio::spawn(roomkit::keep_context(async {
                roomkit_ai::current_tool_call().map(|call| call.tool_call_id)
            }));
            let plain = tokio::spawn(async {
                roomkit_ai::current_tool_call().map(|call| call.tool_call_id)
            });
            let (kept, plain) = (kept.await.unwrap(), plain.await.unwrap());
            record.lock().unwrap().extend([kept, plain]);
            Ok(ToolResult::Text("ok".to_owned()))
        }
    });
    let options = AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Looks up").unwrap().into()],
        tool_handler: Some(handler),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model, options).unwrap();
    turn(&agent).await;
    assert_eq!(seen.lock().unwrap().clone(), [Some("c1".to_owned()), None]);
}
