//! Public room observations of calls cut by channel shutdown.
use super::*;
use roomkit::model::{EventContent, EventType, HookResult, InboundMessage, ToolCallOutcome};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions, ToolCallEvent,
};
use roomkit_ai::{AiResponseEvent, ToolRoundEvent};

type Reports = Arc<Mutex<Vec<ToolCallEvent>>>;

pub(super) async fn room(agent: AiChannel<Model>) -> (RoomKit, Reports) {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        "web".parse().unwrap(),
        ChannelType::WEBHOOK,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some("r".parse().unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["web", "ai"] {
        kit.attach_channel(
            &"r".parse().unwrap(),
            &id.parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let reports: Reports = Arc::default();
    let observed = reports.clone();
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            observed.lock().unwrap().push(event);
            async {}
        },
        HookOptions {
            name: "audit".into(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    (kit, reports)
}

pub(super) async fn says(kit: RoomKit) {
    kit.process_inbound(
        InboundMessage::new("web".parse().unwrap(), "alice", TextContent::new("go")),
        ProcessInboundOptions {
            room_id: Some("r".parse().unwrap()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

pub(super) async fn rows(kit: &RoomKit) -> Vec<ToolCallOutcome> {
    kit.store()
        .list_events(&"r".parse().unwrap(), Default::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.r#type == EventType::ToolCallEnd)
        .filter_map(|row| match row.content {
            EventContent::ToolCall(call) => call.outcome,
            _ => None,
        })
        .collect()
}

// Python: tests/test_channel_close_turns.py::test_a_close_cancels_the_running_call_and_reports_it_once
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_cancels_the_running_call_and_reports_it_once() {
    for streaming in [false, true] {
        for through_kit in [false, true] {
            cut(streaming, through_kit).await;
        }
    }
}

async fn cut(streaming: bool, through_kit: bool) {
    let trace: Trace = Arc::default();
    let started = Arc::new(Notify::new());
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "held", "{}")]),
            ScriptedRound::answering("late"),
        ]),
        streaming,
        trace: trace.clone(),
    };
    let handler = {
        let (trace, started) = (trace.clone(), started.clone());
        ToolHandler::new(move |_, _| {
            let (trace, started) = (trace.clone(), started.clone());
            async move {
                let _dropped = Dropped(trace);
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(ToolResult::Text("late".into()))
            }
        })
    };
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: vec![AiTool::new("held", "Wait").unwrap().into()],
            tool_handler: Some(handler),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, reports) = room(agent.clone()).await;
    let task = tokio::spawn(says(kit.clone()));
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    if through_kit {
        kit.close().await.unwrap();
    } else {
        agent.close().await;
    }
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(trace.lock().unwrap().first(), Some(&"call ended"));
    let reports = reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].cancelled && reports[0].is_error);
    assert!(
        reports[0]
            .result
            .as_ref()
            .unwrap()
            .to_string()
            .contains("Tool call cancelled")
    );
    assert_eq!(rows(&kit).await, [ToolCallOutcome::Cancelled]);
    assert_eq!(model.inner.requests().len(), 1);
    kit.close().await.unwrap();
}

// Python: tests/test_channel_close_turns.py::test_a_close_from_a_hook_of_the_turn_does_not_wait_for_that_turn
#[tokio::test(start_paused = true)]
async fn a_close_from_a_hook_of_the_turn_does_not_wait_for_that_turn() {
    for after_round in [true, false] {
        let model = Model {
            inner: ScriptedProvider::new([
                ScriptedRound::calling("", [("c1", "lookup", "{}")]),
                ScriptedRound::answering("done"),
            ]),
            streaming: false,
            trace: Arc::default(),
        };
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                tools: vec![AiTool::new("lookup", "Lookup").unwrap().into()],
                tool_handler: Some(ToolHandler::new(|_, _| async {
                    Ok(ToolResult::Text("found".into()))
                })),
                ..Default::default()
            },
        )
        .unwrap();
        let (kit, reports) = room(agent.clone()).await;
        let took: Arc<Mutex<Vec<Duration>>> = Arc::default();
        let times = took.clone();
        let close = move || {
            let (agent, times) = (agent.clone(), times.clone());
            async move {
                let start = tokio::time::Instant::now();
                agent.close().await;
                times.lock().unwrap().push(start.elapsed());
            }
        };
        let opts = HookOptions {
            name: "close".into(),
            ..Default::default()
        };
        if after_round {
            kit.hook(
                HookTrigger::AfterToolRound,
                move |_: ToolRoundEvent, _| {
                    let closing = close();
                    async {
                        closing.await;
                        HookResult::allow(Default::default())
                    }
                },
                opts,
            )
            .unwrap();
        } else {
            kit.hook(
                HookTrigger::OnAiResponse,
                move |_: AiResponseEvent, _| {
                    let closing = close();
                    async {
                        closing.await;
                        HookResult::allow(Default::default())
                    }
                },
                opts,
            )
            .unwrap();
        }
        says(kit.clone()).await;
        roomkit::testing::idle(&kit).await;
        assert_eq!(*took.lock().unwrap(), [Duration::ZERO]);
        assert_eq!(reports.lock().unwrap().len(), 1);
        assert!(!reports.lock().unwrap()[0].cancelled);
        assert_eq!(rows(&kit).await, [ToolCallOutcome::Served]);
        assert_eq!(
            model.inner.requests().len(),
            if after_round { 1 } else { 2 }
        );
        kit.close().await.unwrap();
    }
}
