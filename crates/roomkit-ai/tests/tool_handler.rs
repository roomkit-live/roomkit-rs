//! An AI channel's host handler, which its setter replaces (RFC §21.1),
//! as the Python implementation's `AIChannel.tool_handler` setter, and the
//! call a handler runs within, which a task it spawns with
//! `roomkit::keep_context` reads (RFC §21.4).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource, EventType,
    Room, RoomContext, RoomEvent, RoomId, TextContent,
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
async fn turn(agent: &AiChannel<ScriptedProvider>) -> roomkit::model::ChannelOutput {
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
    agent.on_event(event, binding, context).await.unwrap()
}

/// The tool results the model read, in order.
fn results(model: &ScriptedProvider) -> Vec<String> {
    model
        .requests()
        .iter()
        .flat_map(|request| request.messages.clone())
        .flat_map(|message| {
            message
                .tool_results()
                .into_iter()
                .map(roomkit_ai::AiToolResultPart::as_text)
                .collect::<Vec<_>>()
        })
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

// Python: tests/test_response_metadata.py::TestLiveRecord::test_accessor_is_none_outside_a_turn
#[test]
fn no_record_is_read_outside_a_turn() {
    assert!(roomkit_ai::current_response_metadata().is_none());
}

// Python: tests/test_response_metadata.py::TestResponseMetadataDirect::test_streaming_tool_loop_output_carries_response_metadata,
// TestLiveRecord::test_tool_handler_write_lands_on_the_answer
#[tokio::test]
async fn a_handler_s_write_lands_on_the_answer_and_a_task_it_spawns_with_keep_context_writes_too() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("Let me read.", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let handler = ToolHandler::new(move |_name, _arguments| async move {
        let record = roomkit_ai::current_response_metadata().expect("a turn's record");
        record.insert("cited", "doc-1".into());
        tokio::spawn(roomkit::keep_context(async {
            let record = roomkit_ai::current_response_metadata().expect("kept");
            record.insert("kept", true.into());
        }))
        .await
        .unwrap();
        let plain = tokio::spawn(async { roomkit_ai::current_response_metadata().is_none() });
        record.insert("plain_reads_none", plain.await.unwrap().into());
        Ok(ToolResult::Text("ok".to_owned()))
    });
    let options = AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Looks up").unwrap().into()],
        tool_handler: Some(handler),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model, options).unwrap();
    let output = turn(&agent).await;

    let record = &output.response_metadata;
    assert_eq!(record.get("cited"), Some("doc-1".into()));
    assert_eq!(record.get("kept"), Some(true.into()));
    assert_eq!(record.get("plain_reads_none"), Some(true.into()));
    let messages: Vec<_> = output
        .response_events
        .iter()
        .filter(|event| event.r#type == EventType::Message)
        .collect();
    // The segment written before the call carries what was known then; the
    // answer, written after, what the handler wrote.
    assert_eq!(messages[0].metadata.get("cited"), None);
    assert_eq!(messages[1].metadata.get("cited"), Some(&"doc-1".into()));
}

// RFC §6.4: a turn that wrote no message holds its record on its last row, run
// outside a kit too.
#[tokio::test]
async fn a_turn_with_no_message_records_on_its_last_row() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("\n"),
        ScriptedRound::answering("\n"),
    ]);
    let ran = Arc::new(Mutex::new(Vec::new()));
    let options = AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Look it up.").unwrap().into()],
        tool_handler: Some(recording("found", &ran)),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model, options).unwrap();
    let output = turn(&agent).await;
    let ends: Vec<_> = output
        .response_events
        .iter()
        .map(|row| {
            (
                row.r#type,
                row.metadata
                    .get("loop_end_reason")
                    .and_then(|end| end.as_str()),
            )
        })
        .collect();
    assert_eq!(
        ends,
        [
            (EventType::ToolCallStart, None),
            (EventType::ToolCallEnd, Some("empty_response")),
        ]
    );
}

#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

// Python: tests/test_human_input.py::test_warns_once_when_a_human_input_tool_is_never_offered,
// test_no_warning_when_the_handler_declares_the_tool
#[tokio::test]
async fn human_tools_missing_from_the_model_are_reported_once() {
    use roomkit_ai::{HumanInputToolHandler, HumanInputToolHandlerOptions};

    let (warnings, _capture) = warnings::Warnings::capture();
    for declared in [false, true] {
        let human = HumanInputToolHandler::new(
            ["AskUser"],
            HumanInputToolHandlerOptions {
                tool_definitions: if declared {
                    vec![AiTool::new("AskUser", "Ask").unwrap()]
                } else {
                    Vec::new()
                },
                ..HumanInputToolHandlerOptions::default()
            },
        )
        .unwrap();
        let model = ScriptedProvider::new([
            ScriptedRound::answering("ok"),
            ScriptedRound::answering("ok"),
        ]);
        let agent = AiChannel::new(
            ChannelId::new("ai1").unwrap(),
            model,
            AiChannelOptions {
                human_input_handler: Some(human),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        turn(&agent).await;
        turn(&agent.clone()).await;
        let lines = warnings.containing("never offers them");
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("AskUser"));
        assert!(lines[0].contains("ai1"));
    }
}

// Python: tests/test_human_input.py::test_tool_call_context_read_from_contextvar
#[tokio::test]
async fn a_plain_human_handler_reads_the_current_tool_call() {
    use roomkit_ai::{AskOptions, HumanInputToolHandler, HumanInputToolHandlerOptions};

    let human =
        HumanInputToolHandler::new(["AskUser"], HumanInputToolHandlerOptions::default()).unwrap();
    let served = human.clone();
    let handler = ToolHandler::new(move |name, arguments| {
        let human = served.clone();
        async move {
            human
                .ask(&name, arguments, AskOptions::default())
                .await
                .map(ToolResult::Text)
        }
    });
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("tc42", "AskUser", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let agent = AiChannel::new(
        ChannelId::new("ai1").unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: vec![AiTool::new("AskUser", "Ask").unwrap().into()],
            tool_handler: Some(handler),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let answer = async {
        loop {
            if let Some(pending) = human.handler().pending().values().next() {
                assert_eq!(pending.room_id, "r1");
                assert_eq!(pending.tool_call_id, "tc42");
                assert_eq!(pending.channel_id, "ai1");
                assert!(human.handler().resolve(&pending.pending_id, "yes"));
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(turn(&agent), answer)
    })
    .await
    .unwrap();
    assert_eq!(results(&model), ["yes"]);
}
