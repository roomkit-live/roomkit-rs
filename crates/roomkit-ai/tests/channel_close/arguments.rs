//! The cancellation report and stored end keep the arguments that ran.
use super::*;
use roomkit::model::{EventContent, EventType, HookResult, ToolCallOutcome};
use roomkit::{ConversationStore, HookOptions, HookTrigger, ToolCallEvent};
use serde_json::json;

// Python: tests/test_channel_close_arguments.py::test_a_closed_call_reports_the_arguments_its_handler_ran_with
#[tokio::test]
async fn a_closed_call_reports_the_arguments_its_handler_ran_with() {
    for streaming in [false, true] {
        rewritten(streaming).await;
    }
}

async fn rewritten(streaming: bool) {
    let started = Arc::new(Notify::new());
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "lookup", r#"{"q":"model"}"#)]),
            ScriptedRound::answering("late"),
        ]),
        streaming,
        trace: Arc::default(),
    };
    let entered = started.clone();
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: vec![AiTool::new("lookup", "Lookup").unwrap().into()],
            tool_handler: Some(ToolHandler::new(move |_, arguments| {
                assert_eq!(arguments.get("q"), Some(&json!("rewritten")));
                let entered = entered.clone();
                async move {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                    Ok(ToolResult::Text("late".into()))
                }
            })),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, reports) = kit::room(agent.clone()).await;
    kit.hook(
        HookTrigger::BeforeToolUse,
        |mut event: ToolCallEvent, _| async move {
            event.arguments.insert("q".into(), json!("rewritten"));
            HookResult::modify(event, Default::default())
        },
        HookOptions {
            name: "rewrite".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let task = tokio::spawn(kit::says(kit.clone()));
    started.notified().await;
    agent.close().await;
    task.await.unwrap();
    roomkit::testing::idle(&kit).await;
    let reports = reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].arguments.get("q"), Some(&json!("rewritten")));
    assert!(
        reports[0]
            .result
            .as_ref()
            .unwrap()
            .to_string()
            .contains("The channel closed before its result.")
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
    assert_eq!(call.arguments.get("q"), Some(&json!("rewritten")));
    assert_eq!(call.outcome, Some(ToolCallOutcome::Cancelled));
    assert!(
        call.result
            .to_string()
            .contains("The channel closed before its result.")
    );
    assert_eq!(model.inner.requests().len(), 1);
    kit.close().await.unwrap();
}
