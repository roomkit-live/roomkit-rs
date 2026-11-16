//! Shutdown reaches a call waiting in its hook, before the tool is invoked.
use super::*;
use roomkit::model::{HookResult, ToolCallOutcome};
use roomkit::{HookOptions, HookTrigger, ToolCallEvent};
use std::sync::atomic::{AtomicUsize, Ordering};

// Python: tests/test_channel_close_turns.py::test_a_close_cancels_the_running_call_and_reports_it_once
// Python: _ai_tools._serve_announced scopes the gate and tool together.
#[tokio::test]
async fn a_close_cuts_a_pending_gate_without_invoking_the_tool() {
    let started = Arc::new(Notify::new());
    let ran = Arc::new(AtomicUsize::new(0));
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "lookup", "{}")]),
            ScriptedRound::answering("late"),
        ]),
        streaming: true,
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
                async { Ok(ToolResult::Text("late".into())) }
            })),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, reports) = kit::room(agent.clone()).await;
    let entered = started.clone();
    kit.hook(
        HookTrigger::BeforeToolUse,
        move |_: ToolCallEvent, _| {
            entered.notify_one();
            async {
                std::future::pending::<()>().await;
                HookResult::allow(Default::default())
            }
        },
        HookOptions {
            name: "pending gate".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let task = tokio::spawn(kit::says(kit.clone()));
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    bounded(agent.close()).await;
    bounded(task).await.unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert_eq!(model.inner.requests().len(), 1);
    let reports = reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].cancelled && reports[0].is_error);
    assert_eq!(kit::rows(&kit).await, [ToolCallOutcome::Cancelled]);
    kit.close().await.unwrap();
}

// Python: tests/test_channel_close_turns.py::test_a_handler_closing_its_channel_spares_itself_not_its_round
// A call already reported served keeps that outcome when its pending sibling is cut.
#[tokio::test]
async fn a_close_keeps_an_already_decided_result() {
    let started = Arc::new(Notify::new());
    let served = Arc::new(Notify::new());
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "fast", "{}"), ("c2", "held", "{}")]),
            ScriptedRound::answering("late"),
        ]),
        streaming: true,
        trace: Arc::default(),
    };
    let entered = started.clone();
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: ["fast", "held"]
                .into_iter()
                .map(|name| AiTool::new(name, name).unwrap().into())
                .collect(),
            tool_handler: Some(ToolHandler::new(move |name, _| {
                let entered = entered.clone();
                async move {
                    if name == "held" {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(ToolResult::Text("found".into()))
                }
            })),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, reports) = kit::room(agent.clone()).await;
    let noted = served.clone();
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            if event.tool_call_id == "c1" {
                noted.notify_one();
            }
            async {}
        },
        HookOptions {
            name: "served".into(),
            execution: roomkit::HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    let task = tokio::spawn(kit::says(kit.clone()));
    bounded(started.notified()).await;
    bounded(served.notified()).await;
    bounded(agent.close()).await;
    bounded(task).await.unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(model.inner.requests().len(), 1);
    let outcomes: Vec<_> = reports
        .lock()
        .unwrap()
        .iter()
        .map(|r| (r.tool_call_id.clone(), r.cancelled))
        .collect();
    assert_eq!(outcomes, [("c1".into(), false), ("c2".into(), true)]);
    assert_eq!(
        kit::rows(&kit).await,
        [ToolCallOutcome::Served, ToolCallOutcome::Cancelled]
    );
    kit.close().await.unwrap();
}
