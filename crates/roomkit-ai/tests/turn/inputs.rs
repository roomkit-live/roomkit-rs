//! A turn's human-input requests (RFC §9.3, §10.6.5): what the shared
//! conformance suite cannot script, work a tool leaves for after its turn,
//! a handler the channel was not given, the channel's own bound per call.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::human_input::{CreateOptions, HumanInputHandler, PendingInputEvent, WaitOptions};
use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, Metadata, RoomContext, ToolCall};
use roomkit::{HookExecution, HookOptions, HookTrigger, RoomKitError, ToolCallEvent};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannelOptions, AiTool, HumanInputToolHandler, HumanInputToolHandlerOptions, ToolContext,
    ToolResult, Tools,
};
use serde_json::json;
use tokio::sync::{Notify, oneshot};

use super::support::Room;

/// The tool that asks the user, as the person's handler declares it.
fn ask_user() -> AiTool {
    AiTool {
        parameters: json!({ "type": "object", "properties": { "question": { "type": "string" } } }),
        ..AiTool::new("ask_user", "Ask the user.").unwrap()
    }
}

/// The person's tools, asking `ask_user`, under `timeout`.
fn human(timeout: Duration) -> HumanInputToolHandler {
    let options = HumanInputToolHandlerOptions {
        timeout,
        tool_definitions: vec![ask_user()],
        ..HumanInputToolHandlerOptions::default()
    };
    HumanInputToolHandler::new(["ask_user"], options).unwrap()
}

/// A model that calls `tool` once, then answers.
fn calling(tool: &str) -> ScriptedProvider {
    ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", tool, r#"{"question": "Color?"}"#)]),
        ScriptedRound::answering("Done."),
    ])
}

/// A tool `name` doing `work` with each call.
fn tool<F, Fut>(name: &str, work: F) -> Tools
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = String> + Send + 'static,
{
    let definition = AiTool {
        parameters: json!({ "type": "object" }),
        ..AiTool::new(name, "Ask later.").unwrap()
    };
    let work = Arc::new(work);
    Tools::new().tool(definition, move |_call: ToolCall, _context: ToolContext| {
        let work = Arc::clone(&work);
        async move { Ok(ToolResult::Text(work().await)) }
    })
}

/// The request ids the room's `ON_USER_INPUT_REQUIRED` hooks were handed.
fn requests_seen(room: &Room) -> Arc<Mutex<Vec<String>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    room.kit
        .hook(
            HookTrigger::OnUserInputRequired,
            move |event: PendingInputEvent, _context: RoomContext| {
                recorded.lock().unwrap().push(event.pending_id);
                async { Ok(HookResult::allow(AllowOptions::default())) }
            },
            HookOptions::default(),
        )
        .unwrap();
    seen
}

/// What the room's `ON_TOOL_CALL` observers read of each call's result.
fn results_seen(room: &Room) -> Arc<Mutex<Vec<serde_json::Value>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    room.kit
        .hook(
            HookTrigger::OnToolCall,
            move |event: ToolCallEvent, _context: RoomContext| {
                recorded
                    .lock()
                    .unwrap()
                    .push(event.result.clone().unwrap_or_default());
                async { Ok(HookResult::allow(AllowOptions::default())) }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
    seen
}

// Python: tests/test_turns_inputs.py::test_a_request_still_open_when_its_turn_ends_is_withdrawn
#[tokio::test]
async fn a_request_still_open_when_its_turn_ends_is_withdrawn() {
    let human = human(Duration::from_secs(2));
    let handler = human.handler().clone();
    let (asked, asked_id) = oneshot::channel::<String>();
    let asked = Arc::new(Mutex::new(Some(asked)));
    let (waited, outcome) = oneshot::channel::<Result<String, RoomKitError>>();
    let waited = Arc::new(Mutex::new(Some(waited)));
    let later = tool("later_ask", move || {
        let (handler, asked, waited) = (handler.clone(), Arc::clone(&asked), Arc::clone(&waited));
        // Work the tool leaves running past its turn, in the turn's context.
        tokio::spawn(roomkit::keep_context(async move {
            let options = CreateOptions {
                room_id: "r1".to_owned(),
                ..CreateOptions::default()
            };
            let pending = handler
                .create("ask_user", Metadata::new(), options)
                .await
                .unwrap();
            drop(
                asked
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(pending.pending_id.clone()),
            );
            let options = WaitOptions {
                timeout: Duration::from_secs(2),
            };
            let read = handler.wait(&pending.pending_id, options).await;
            drop(waited.lock().unwrap().take().unwrap().send(read));
        }));
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            "asked".to_owned()
        }
    });
    let room = Room::equipped(calling("later_ask"), later, |options| AiChannelOptions {
        human_input_handler: Some(human),
        ..options
    })
    .await;

    room.says("Hi").await;
    let pending_id = asked_id.await.unwrap();

    // Withdrawn on both sides: the waiter reads it given up, the kit's door
    // answers it no more.
    let read = outcome.await.unwrap().unwrap_err();
    assert_eq!(read.to_string(), "Released before an answer arrived");
    assert!(
        !room
            .kit
            .answer_input(&pending_id, "Too late")
            .await
            .unwrap()
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_inputs.py::test_a_detached_request_a_turn_raised_outlives_the_turn
#[tokio::test]
async fn a_detached_request_a_turn_raised_outlives_the_turn() {
    let handler = HumanInputHandler::default();
    let raised = Arc::new(Mutex::new(Vec::new()));
    let (creator, kept) = (handler.clone(), Arc::clone(&raised));
    let ask_later = tool("ask_later", move || {
        let (creator, kept) = (creator.clone(), Arc::clone(&kept));
        async move {
            let options = CreateOptions {
                room_id: "r1".to_owned(),
                channel_id: "assistant".to_owned(),
                ..CreateOptions::default()
            };
            let pending = creator
                .create_detached("ask_user", Metadata::new(), options.into())
                .await
                .unwrap();
            kept.lock().unwrap().push(pending.pending_id);
            "asked; the answer comes back later".to_owned()
        }
    });
    let room = Room::new(calling("ask_later"), ask_later).await;

    room.says("Hi").await;
    let pending_id = raised.lock().unwrap()[0].clone();

    // The owner's record went with the turn; the creator's way answers it.
    assert!(
        !room
            .kit
            .answer_input(&pending_id, "From the kit")
            .await
            .unwrap()
    );
    assert!(handler.resolve(&pending_id, "yes"));
    let options = WaitOptions {
        timeout: Duration::from_secs(1),
    };
    assert_eq!(handler.wait(&pending_id, options).await.unwrap(), "yes");
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_inputs.py::test_a_request_asked_after_its_turn_ended_is_the_handlers_alone
#[tokio::test]
async fn a_request_asked_after_its_turn_ended_is_the_handlers_alone() {
    let human = human(Duration::from_secs(2));
    let handler = human.handler().clone();
    let turn_ended = Arc::new(Notify::new());
    let (answered, answer) = oneshot::channel::<String>();
    let answered = Arc::new(Mutex::new(Some(answered)));
    let ended = Arc::clone(&turn_ended);
    let later = tool("later_ask", move || {
        let (handler, ended, answered) =
            (handler.clone(), Arc::clone(&ended), Arc::clone(&answered));
        tokio::spawn(roomkit::keep_context(async move {
            ended.notified().await; // asked once its turn is over
            let options = CreateOptions {
                room_id: "r1".to_owned(),
                ..CreateOptions::default()
            };
            let pending = handler
                .create("ask_user", Metadata::new(), options)
                .await
                .unwrap();
            assert!(handler.resolve(&pending.pending_id, "Yes"));
            let read = handler
                .wait(&pending.pending_id, WaitOptions::default())
                .await;
            drop(answered.lock().unwrap().take().unwrap().send(read.unwrap()));
        }));
        async { "asked later".to_owned() }
    });
    let room = Room::equipped(calling("later_ask"), later, |options| AiChannelOptions {
        human_input_handler: Some(human),
        ..options
    })
    .await;

    room.says("Hi").await;
    turn_ended.notify_one();

    assert_eq!(answer.await.unwrap(), "Yes");
    room.kit.close().await.unwrap();
}

// Python: tests/test_turns_inputs.py::test_the_channels_handler_answers_where_the_tool_waits_first_come_first_served
#[tokio::test]
async fn the_channel_s_handler_answers_where_the_tool_waits_first_come_first_served() {
    let human = human(Duration::from_secs(5));
    let handler = human.handler().clone();
    let room = Room::equipped(calling("ask_user"), Tools::new(), |options| {
        AiChannelOptions {
            human_input_handler: Some(human),
            ..options
        }
    })
    .await;
    let asked = requests_seen(&room);
    let results = results_seen(&room);

    let kit = room.kit.clone();
    let turn = tokio::spawn(async move { Room::says_on(&kit, "Hi").await });
    let pending_id = loop {
        if let Some(id) = asked.lock().unwrap().first().cloned() {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };

    assert!(handler.resolve(&pending_id, "Green"));
    assert!(!handler.resolve(&pending_id, "Red"));
    // The kit's door, right after: the tool already has its answer.
    assert!(!room.kit.answer_input(&pending_id, "Blue").await.unwrap());
    turn.await.unwrap();
    room.kit.close().await.unwrap();

    assert_eq!(*results.lock().unwrap(), [json!("Green")]);
}

// Python: tests/test_human_input_doors.py::test_the_handler_s_timeout_bounds_the_call_not_the_channel_s
#[tokio::test]
async fn the_handler_s_timeout_bounds_the_call_not_the_channel_s() {
    let human = human(Duration::from_millis(100));
    // The channel's bound would end the call first, were it the one applied.
    let room = Room::equipped(calling("ask_user"), Tools::new(), |options| {
        AiChannelOptions {
            human_input_handler: Some(human),
            tool_timeout: Some(Duration::from_millis(20)),
            ..options
        }
    })
    .await;
    let results = results_seen(&room);

    room.says("Hi").await;
    room.kit.close().await.unwrap();

    assert_eq!(
        *results.lock().unwrap(),
        [json!(
            "{\"error\": \"Human input timed out after 0.1s for tool 'ask_user'\"}"
        )]
    );
}
