//! Directly wired channels apply ordinary tool policy before registration.
use super::{Env, link, worker};
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{HookResult, RoomContext};
use roomkit::turns::HookOptions;
use roomkit::{Channel, HookExecution, HookTrigger, ToolCallEvent};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, ToolResult, Tools};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn a_wired_channel_checks_an_ordinary_tool_and_reports_its_refusal_once() {
    ordinary_tool(true).await;
}

#[tokio::test]
async fn a_wired_channel_reports_the_result_its_synchronous_hook_rewrote() {
    ordinary_tool(false).await;
}

async fn ordinary_tool(denied: bool) {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let env = Arc::new(Env::new(req.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    worker
        .hook(
            HookTrigger::BeforeToolUse,
            move |event: ToolCallEvent, ctx: RoomContext| async move {
                assert!(roomkit::current_turn_host().is_none());
                assert_eq!(ctx.room.id, event.room_id.unwrap());
                if denied {
                    HookResult::block("requires approval", BlockOptions::default())
                } else {
                    HookResult::allow(AllowOptions::default())
                }
            },
            HookOptions::default(),
        )
        .unwrap();
    worker
        .hook(
            HookTrigger::OnToolCall,
            |_: ToolCallEvent, _: RoomContext| async {
                let mut answer = HookResult::allow(AllowOptions::default());
                answer
                    .metadata
                    .insert("result".into(), json!("approved copy"));
                answer
            },
            HookOptions::default(),
        )
        .unwrap();
    let (reports, mut observed) = tokio::sync::mpsc::unbounded_channel();
    worker
        .hook(
            HookTrigger::OnToolCall,
            move |event: ToolCallEvent, _: RoomContext| {
                let reports = reports.clone();
                async move {
                    reports.send(event).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&runs);
    let tools = Tools::new().tool(
        AiTool {
            parameters: json!({"type":"object"}),
            ..AiTool::new("lookup", "Look up").unwrap()
        },
        move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(ToolResult::Text("found".into())) }
        },
    );
    let ai = AiChannel::new(
        req.channel_id.clone(),
        ScriptedProvider::new([
            ScriptedRound::calling("", [("look-1", "lookup", "{}")]),
            ScriptedRound::answering("done"),
        ]),
        AiChannelOptions {
            tools: tools.into(),
            ..Default::default()
        },
    )
    .unwrap();
    worker.wire(&ai).unwrap();
    ai.on_event(req.event.clone(), req.binding.clone(), req.context.clone())
        .await
        .unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), usize::from(!denied));
    let report = tokio::time::timeout(Duration::from_secs(2), observed.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.tool_call_id, "look-1");
    assert_eq!(report.refused, denied);
    if !denied {
        assert_eq!(report.result, Some(json!("approved copy")));
    }
    assert_eq!(
        env.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "tool_call")
            .count(),
        1
    );
    worker.close().await;
    assert!(observed.try_recv().is_err());
}
