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

/// What a kit saw of the requests of its agent: the ids its
/// `ON_USER_INPUT_REQUIRED` hooks were handed, and its
/// `user_input_required` events.
struct Seen {
    requests: Arc<Mutex<Vec<String>>>,
    events: tokio::sync::mpsc::UnboundedReceiver<roomkit::FrameworkEvent>,
}

/// A kit whose agent `agent` asks a person through `shared`, its room
/// `room` created, and what it sees of the agent's requests.
async fn kit_asking_through(
    shared: &HumanInputHandler,
    agent: &str,
    room: &str,
) -> (roomkit::RoomKit, Seen) {
    use roomkit::model::{ChannelId, RoomId};
    use roomkit::{CreateRoomOptions, RoomKit, RoomKitOptions};
    use roomkit_ai::AiChannel;

    let options = HumanInputToolHandlerOptions {
        handler: Some(shared.clone()),
        ..HumanInputToolHandlerOptions::default()
    };
    let human = HumanInputToolHandler::new(["AskUser"], options).unwrap();
    let options = AiChannelOptions {
        human_input_handler: Some(human),
        ..AiChannelOptions::default()
    };
    let model = ScriptedProvider::new([ScriptedRound::answering("Done.")]);
    let agent = AiChannel::new(ChannelId::new(agent).unwrap(), model, options).unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(RoomId::new(room).unwrap()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    kit.hook(
        HookTrigger::OnUserInputRequired,
        move |event: PendingInputEvent, _context: RoomContext| {
            recorded.lock().unwrap().push(event.pending_id);
            async { Ok(HookResult::allow(AllowOptions::default())) }
        },
        HookOptions::default(),
    )
    .unwrap();
    let (emitted, events) = tokio::sync::mpsc::unbounded_channel();
    kit.on("user_input_required", move |event| {
        drop(emitted.send(event));
        async {}
    })
    .unwrap();
    (kit, Seen { requests, events })
}

// Python: tests/test_human_input.py::test_shared_handler_routes_framework_callbacks_by_channel
#[tokio::test]
async fn a_shared_handler_s_requests_fire_the_hooks_of_their_channel_s_kit() {
    use roomkit::human_input::CreateDetachedOptions;

    let shared = HumanInputHandler::default();
    let (first_kit, mut first) = kit_asking_through(&shared, "ai-first", "room-first").await;
    let (second_kit, mut second) = kit_asking_through(&shared, "ai-second", "room-second").await;

    // Asked outside every turn, as from a host's webhook or job.
    let asked = |room: &str, agent: &str| CreateDetachedOptions {
        room_id: room.to_owned(),
        channel_id: agent.to_owned(),
        ..CreateDetachedOptions::default()
    };
    let first_request = shared
        .create_detached("AskUser", Metadata::new(), asked("room-first", "ai-first"))
        .await
        .unwrap();
    let second_request = shared
        .create_detached(
            "AskUser",
            Metadata::new(),
            asked("room-second", "ai-second"),
        )
        .await
        .unwrap();

    let within = Duration::from_secs(1);
    let first_event = tokio::time::timeout(within, first.events.recv())
        .await
        .unwrap()
        .unwrap();
    let second_event = tokio::time::timeout(within, second.events.recv())
        .await
        .unwrap()
        .unwrap();

    let named = |event: &roomkit::FrameworkEvent| {
        (
            event.channel_id.as_ref().map(ToString::to_string),
            event.data["pending_id"].clone(),
        )
    };
    assert_eq!(
        named(&first_event),
        (Some("ai-first".to_owned()), json!(first_request.pending_id))
    );
    assert_eq!(
        named(&second_event),
        (
            Some("ai-second".to_owned()),
            json!(second_request.pending_id)
        )
    );
    assert!(first.events.try_recv().is_err());
    assert!(second.events.try_recv().is_err());
    assert_eq!(
        first.requests.lock().unwrap().as_slice(),
        std::slice::from_ref(&first_request.pending_id)
    );
    assert_eq!(
        second.requests.lock().unwrap().as_slice(),
        std::slice::from_ref(&second_request.pending_id)
    );

    shared.release(&first_request.pending_id);
    shared.release(&second_request.pending_id);
    first_kit.close().await.unwrap();
    second_kit.close().await.unwrap();
}

/// An agent `ai` asking a person through `shared`.
fn agent_asking_through(shared: &HumanInputHandler) -> roomkit_ai::AiChannel<ScriptedProvider> {
    let options = HumanInputToolHandlerOptions {
        handler: Some(shared.clone()),
        ..HumanInputToolHandlerOptions::default()
    };
    let human = HumanInputToolHandler::new(["AskUser"], options).unwrap();
    let options = AiChannelOptions {
        human_input_handler: Some(human),
        ..AiChannelOptions::default()
    };
    let model = ScriptedProvider::new([ScriptedRound::answering("Done.")]);
    let id = roomkit::model::ChannelId::new("ai").unwrap();
    roomkit_ai::AiChannel::new(id, model, options).unwrap()
}

// Python: turns/_wiring.py _wire_ai_channel (RMK-1201): an AI channel
// registers on its handler when a kit registers it, the channel a kit
// registered last owning its id's requests, whichever was built first: the
// close of its kit settles them.
#[tokio::test]
async fn the_channel_a_kit_registered_last_owns_its_id_s_requests() {
    use roomkit::human_input::CreateDetachedOptions;
    use roomkit::{RoomKit, RoomKitOptions};

    let shared = HumanInputHandler::default();
    let built_first = agent_asking_through(&shared);
    let built_second = agent_asking_through(&shared);
    let other = RoomKit::new(RoomKitOptions::default());
    other.register_channel(built_second).unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(built_first).unwrap();
    let options = CreateDetachedOptions {
        room_id: "room-1".to_owned(),
        channel_id: "ai".to_owned(),
        ..CreateDetachedOptions::default()
    };
    let request = shared
        .create_detached("AskUser", Metadata::new(), options)
        .await
        .unwrap();

    kit.close().await.unwrap();

    let settled = !shared.pending().contains_key(&request.pending_id);
    other.close().await.unwrap();
    assert!(settled);
}

// Python: core/framework.py register_channel raises before
// _wire_ai_channel (RMK-1201): a registration the kit refuses, or an
// agent a strategy is given and never installs, takes nothing of the id's
// requests from the channel the kit holds, whose kit's close settles them.
#[tokio::test]
async fn a_registration_that_never_lands_owns_no_request() {
    use roomkit::human_input::CreateDetachedOptions;
    use roomkit::{RoomKit, RoomKitError, RoomKitOptions};

    let shared = HumanInputHandler::default();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent_asking_through(&shared)).unwrap();
    let refused = kit.register_channel(agent_asking_through(&shared));
    let _never_installed =
        roomkit::orchestration::StrategyAgent::channel(agent_asking_through(&shared));
    let options = CreateDetachedOptions {
        room_id: "room-1".to_owned(),
        channel_id: "ai".to_owned(),
        ..CreateDetachedOptions::default()
    };
    let request = shared
        .create_detached("AskUser", Metadata::new(), options)
        .await
        .unwrap();

    kit.close().await.unwrap();

    let settled = !shared.pending().contains_key(&request.pending_id);
    assert!(matches!(
        refused,
        Err(RoomKitError::ChannelAlreadyRegistered(_))
    ));
    assert!(settled);
}

// Python: core/mixins/channel_ops.py register_channel, through
// turns/_wiring.py and HumanInputHandler._set_on_input_required (RMK-1255
// item 6): an agent whose person's handler closed whole is refused at its
// registration with the handler's RuntimeError.
#[tokio::test]
async fn an_agent_whose_handler_closed_whole_is_refused_at_its_registration() {
    use roomkit::human_input::CloseOptions;
    use roomkit::{RoomKit, RoomKitOptions};

    let shared = HumanInputHandler::default();
    shared.close(CloseOptions::default()).await;
    let kit = RoomKit::new(RoomKitOptions::default());
    let refused = kit.register_channel(agent_asking_through(&shared));
    assert!(
        matches!(&refused, Err(RoomKitError::Runtime(text)) if text == "Human input handler is closed"),
        "{refused:?}"
    );
    // The kit holds the channel all the same, as Python's holds it before
    // its wiring raises: a second registration is refused as a duplicate.
    let again = kit.register_channel(agent_asking_through(&shared));
    assert!(
        matches!(&again, Err(RoomKitError::ChannelAlreadyRegistered(id)) if id.as_str() == "ai"),
        "{again:?}"
    );
    kit.close().await.unwrap();
}
