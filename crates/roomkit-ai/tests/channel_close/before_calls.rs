//! A consumer closes the channel before the announced tool starts.
use super::*;
use roomkit::model::{EventContent, EventType, HookResult, ToolCallOutcome};
use roomkit::{ConversationStore, HookOptions, HookTrigger};
use std::sync::atomic::{AtomicUsize, Ordering};

// Python: tests/test_channel_close_before_calls.py::test_a_close_while_the_start_row_is_read_never_starts_the_call
#[tokio::test(start_paused = true)]
async fn a_close_while_the_start_row_is_read_never_starts_the_call() {
    for streaming in [false, true] {
        closes_before_call(streaming).await;
    }
}

async fn closes_before_call(streaming: bool) {
    let ran = Arc::new(AtomicUsize::new(0));
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "lookup", "{}")]),
            ScriptedRound::answering("late"),
        ]),
        streaming,
        trace: Arc::default(),
    };
    let called = ran.clone();
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: vec![AiTool::new("lookup", "Lookup").unwrap().into()],
            tool_handler: Some(ToolHandler::new(move |_, _| {
                called.fetch_add(1, Ordering::SeqCst);
                async { Ok(ToolResult::Text("must not execute".into())) }
            })),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, reports) = kit::room(agent.clone()).await;
    let took: Arc<Mutex<Vec<Duration>>> = Arc::default();
    let times = took.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (agent, times) = (agent.clone(), times.clone());
            async move {
                if event.r#type == EventType::ToolCallStart {
                    let now = tokio::time::Instant::now();
                    agent.close().await;
                    times.lock().unwrap().push(now.elapsed());
                }
                HookResult::allow(Default::default())
            }
        },
        HookOptions {
            name: "close before call start".into(),
            ..Default::default()
        },
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(8), kit::says(kit.clone()))
        .await
        .unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(*took.lock().unwrap(), [Duration::from_secs(5)]);
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert_eq!(model.inner.requests().len(), 1);
    let reports = reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].cancelled && reports[0].is_error);
    assert!(
        reports[0]
            .result
            .as_ref()
            .unwrap()
            .to_string()
            .contains("The turn ended before its result.")
    );
    let ends: Vec<_> = kit
        .store()
        .list_events(&"r".parse().unwrap(), Default::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.r#type == EventType::ToolCallEnd)
        .collect();
    assert_eq!(ends.len(), 1);
    let EventContent::ToolCall(call) = &ends[0].content else {
        panic!("tool row")
    };
    assert_eq!(call.outcome, Some(ToolCallOutcome::Cancelled));
    assert_eq!(call.result, serde_json::Value::Null);
    assert_eq!(call.duration_ms, Some(0));
    assert_eq!(call.error.as_deref(), Some("cancelled"));
    kit.close().await.unwrap();
}
