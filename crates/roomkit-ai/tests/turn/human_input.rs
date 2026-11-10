//! Public channel wiring for human input that shared scenarios cannot replace.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::human_input::{
    CreateDetachedOptions, HumanInputHandler, PendingInputEvent, WaitOptions,
};
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{ChannelType, HookResult, InboundMessage, Metadata, RoomContext, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiTool, HumanInputToolHandler, HumanInputToolHandlerOptions,
    ToolHandler, ToolResult,
};
use tokio::sync::mpsc;

use super::support::{Room, channel, room_id};

fn agent(shared: &HumanInputHandler, model: ScriptedProvider) -> AiChannel<ScriptedProvider> {
    let human = HumanInputToolHandler::new(
        ["ask"],
        HumanInputToolHandlerOptions {
            handler: Some(shared.clone()),
            tool_definitions: vec![AiTool::new("ask", "Ask the person").unwrap()],
            ..HumanInputToolHandlerOptions::default()
        },
    )
    .unwrap();
    AiChannel::new(
        channel("assistant"),
        model,
        AiChannelOptions {
            human_input_handler: Some(human),
            ..AiChannelOptions::default()
        },
    )
    .unwrap()
}

fn request() -> CreateDetachedOptions {
    CreateDetachedOptions {
        room_id: room_id().to_string(),
        channel_id: "assistant".to_owned(),
        ..CreateDetachedOptions::default()
    }
}

// Python: tests/test_human_input.py::test_rebuilt_channel_survives_its_predecessors_teardown
#[tokio::test]
async fn a_rebuilt_channel_survives_its_predecessors_teardown() {
    let shared = HumanInputHandler::default();
    let build = || agent(&shared, ScriptedProvider::new([]));
    let displaced = build();
    let room = Room::with_agent(displaced.clone()).await;
    room.kit.unregister_channel(&channel("assistant")).unwrap();
    room.kit.register_channel(build()).unwrap();
    displaced.close().await;

    let (seen, mut events) = mpsc::unbounded_channel();
    room.kit
        .hook(
            HookTrigger::OnUserInputRequired,
            move |event: PendingInputEvent, _: RoomContext| {
                seen.send(event.pending_id).unwrap();
                async { Ok(HookResult::allow(AllowOptions::default())) }
            },
            HookOptions::default(),
        )
        .unwrap();
    let pending = shared
        .create_detached("ask", Metadata::new(), request())
        .await
        .unwrap();
    assert!(shared.pending().contains_key(&pending.pending_id));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap(),
        pending.pending_id
    );
    assert!(shared.release(&pending.pending_id));
    room.kit.unregister_channel(&channel("assistant")).unwrap();
    room.kit.register_channel(build()).unwrap();
    room.kit.close().await.unwrap();
}

// Python: tests/test_human_input.py::test_ai_channel_close_stops_its_human_input_notifications
#[tokio::test]
async fn closing_the_channel_finishes_its_request_notification() {
    struct Cancelled(Arc<AtomicBool>);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let shared = HumanInputHandler::default();
    let agent = agent(&shared, ScriptedProvider::new([]));
    let room = Room::with_agent(agent.clone()).await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let dropped = Arc::clone(&cancelled);
    let (started, mut starts) = mpsc::unbounded_channel();
    room.kit
        .hook(
            HookTrigger::OnUserInputRequired,
            move |_: PendingInputEvent, _: RoomContext| {
                let guard = Cancelled(Arc::clone(&dropped));
                let started = started.clone();
                async move {
                    let _guard = guard;
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                }
            },
            HookOptions::default(),
        )
        .unwrap();
    let pending = shared
        .create_detached("ask", Metadata::new(), request())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), starts.recv())
        .await
        .unwrap()
        .unwrap();
    agent.close().await;
    assert!(cancelled.load(Ordering::Acquire));
    assert_eq!(
        shared
            .wait(&pending.pending_id, WaitOptions::default())
            .await
            .unwrap_err()
            .to_string(),
        "Human input handler closed"
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_human_input_doors.py::test_replacing_the_host_s_handler_keeps_the_person_s_tools
#[tokio::test]
async fn replacing_the_host_handler_keeps_the_persons_tools() {
    let shared = HumanInputHandler::default();
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "ask", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let agent = agent(&shared, model.clone());
    let room = Room::with_agent(agent.clone()).await;
    let host_ran = Arc::new(AtomicBool::new(false));
    let ran = Arc::clone(&host_ran);
    agent.set_tool_handler(Some(ToolHandler::new(move |_, _| {
        ran.store(true, Ordering::Release);
        async { Ok(ToolResult::Text("host answer".to_owned())) }
    })));
    let (asked, mut requests) = mpsc::unbounded_channel();
    room.kit
        .hook(
            HookTrigger::OnUserInputRequired,
            move |event: PendingInputEvent, _: RoomContext| {
                asked.send(event.pending_id).unwrap();
                async { Ok(HookResult::block("no", BlockOptions::default())) }
            },
            HookOptions::default(),
        )
        .unwrap();
    room.says("Hi").await;
    assert!(requests.try_recv().is_ok());
    assert!(requests.try_recv().is_err());
    assert!(!host_ran.load(Ordering::Acquire));
    assert_eq!(
        super::tool_results(&model)[1],
        ["{\"error\": \"Human input rejected: Denied by ON_USER_INPUT_REQUIRED hook\"}"]
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_human_input.py::test_request_names_the_speaker_through_the_framework
#[tokio::test]
async fn requests_name_each_speaker_on_one_group_binding() {
    let shared = HumanInputHandler::default();
    let model = ScriptedProvider::new((0..2).flat_map(|_| {
        [
            ScriptedRound::calling("", [("c1", "ask", "{}")]),
            ScriptedRound::answering("Thanks."),
        ]
    }));
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent(&shared, model.clone())).unwrap();
    let ws = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(ws).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("assistant"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("ws"),
        AttachChannelOptions {
            group: true,
            ..AttachChannelOptions::default()
        },
    )
    .await
    .unwrap();
    let actors = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&actors);
    kit.hook(
        HookTrigger::OnUserInputRequired,
        move |event: PendingInputEvent, _: RoomContext| {
            seen.lock().unwrap().push(event.actor_id);
            assert!(shared.resolve(&event.pending_id, "yes"));
            async { Ok(HookResult::allow(AllowOptions::default())) }
        },
        HookOptions::default(),
    )
    .unwrap();
    for who in ["alice", "bob"] {
        kit.process_inbound(
            InboundMessage::new(channel("ws"), who, TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..ProcessInboundOptions::default()
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(
        actors.lock().unwrap().clone(),
        [Some("alice".to_owned()), Some("bob".to_owned())]
    );
    assert_eq!(super::tool_results(&model).len(), 4);
    kit.close().await.unwrap();
}
