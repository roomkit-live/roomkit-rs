//! The public controller of the tool chain, independent of its bound callbacks.
use super::{Env, link, worker};
use roomkit::model::{
    ChannelId, ChannelType, HookResult, Metadata, Observation, ObservationId, RoomContext, RoomId,
    Task, TaskId,
    hook::{AllowOptions, BlockOptions},
    tool_verdict::VerdictResult,
};
use roomkit::turns::HookOptions;
use roomkit::{HookExecution, HookTrigger, RoomKit, ToolCallEvent};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

fn event(id: &ChannelId) -> ToolCallEvent {
    let mut call = ToolCallEvent::new(id.clone(), ChannelType::AI, "id", "lookup", Metadata::new());
    call.result = Some(json!("provider"));
    call
}

async fn next(rx: &mut mpsc::UnboundedReceiver<ToolCallEvent>) -> ToolCallEvent {
    tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

fn register_rewrite(worker: &roomkit::TurnExecutor, runs: &Arc<AtomicUsize>) {
    let ran = Arc::clone(runs);
    worker
        .hook(
            HookTrigger::OnToolCall,
            move |_: ToolCallEvent, ctx: RoomContext| {
                ran.fetch_add(1, Ordering::Relaxed);
                async move {
                    let mut result = HookResult::allow(AllowOptions::default());
                    result.metadata.insert("result".into(), json!("rewrite"));
                    result.metadata.insert("extra".into(), json!(true));
                    result.tasks.push(Task::new(
                        TaskId::new("t").unwrap(),
                        ctx.room.id.clone(),
                        "check",
                    ));
                    result.observations.push(Observation::new(
                        ObservationId::new("o").unwrap(),
                        ctx.room.id,
                        None,
                        "noted",
                    ));
                    result
                }
            },
            HookOptions::default(),
        )
        .unwrap();
}

#[tokio::test]
async fn applications_run_the_chain_and_claim_their_own_reports() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let env = Arc::new(Env::new(req.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    let tools = worker.wiring().tools();
    let channel = ChannelId::new("bound").unwrap();
    let mut call = event(&req.channel_id);
    let (raw, ctx) = tools
        .run_tool_call_chain(call.clone(), &req.room_id, None)
        .await
        .unwrap();
    assert_eq!(raw.event.as_ref(), Some(&call));
    assert!(ctx.is_none());

    let runs = Arc::new(AtomicUsize::new(0));
    register_rewrite(&worker, &runs);
    let (tx, mut rx) = mpsc::unbounded_channel();
    worker
        .hook(
            HookTrigger::OnToolCall,
            move |e: ToolCallEvent, _: RoomContext| {
                let tx = tx.clone();
                async move {
                    tx.send(e).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    worker
        .hook(
            HookTrigger::BeforeToolUse,
            |_: ToolCallEvent, _: RoomContext| async {
                HookResult::block("approval", BlockOptions::default())
            },
            HookOptions::default(),
        )
        .unwrap();
    let roomless = tools
        .decide_before_tool_use(&call, &channel, None)
        .await
        .unwrap();
    assert!(roomless.0.allowed && roomless.1.is_none());
    let (raw, selected) = tools
        .run_tool_call_chain(call.clone(), &req.room_id, Some(&req.context))
        .await
        .unwrap();
    assert_eq!(selected.as_ref(), Some(&req.context));
    assert_eq!(raw.event.unwrap().result, Some(json!("rewrite")));
    assert_eq!(raw.metadata["extra"], true);
    assert_eq!(raw.tasks[0].title, "check");
    assert_eq!(raw.observations[0].content, "noted");
    assert!(raw.allowed && !raw.failed_closed && raw.reason.is_none() && raw.blocked_by.is_none());
    assert!(raw.injected_events.is_empty() && raw.hook_errors.is_empty());
    assert!(env.events.lock().unwrap().is_empty() && rx.try_recv().is_err());

    tools
        .tell_tool_call(&call, selected.as_ref(), &channel)
        .await
        .unwrap();
    assert!(next(&mut rx).await.room_id.is_none());
    assert!(env.fields.lock().unwrap()[0].room_id.is_none());
    call.room_id = Some(req.room_id.clone());
    let (gate, selected) = tools
        .decide_before_tool_use(&call, &channel, Some(&req.context))
        .await
        .unwrap();
    assert!(!gate.allowed && gate.reason.as_deref() == Some("approval"));
    assert_eq!(selected.as_ref(), Some(&req.context));
    let judged = tools
        .judge_tool_call(call.clone(), &channel, selected.as_ref(), Some(&|| false))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(judged.result(), Some(VerdictResult::Text(s)) if s == "rewrite"));
    tools
        .report_tool_call(&call, &channel, Some(&|| false))
        .await
        .unwrap();
    tools.report_tool_call(&call, &channel, None).await.unwrap();
    assert_eq!(next(&mut rx).await.result, Some(json!("provider")));
    call.refused = true;
    tools.report_tool_call(&call, &channel, None).await.unwrap();
    assert!(next(&mut rx).await.refused);
    tools
        .observe_failed_tool_call(&call, &channel, None)
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await.result, Some(json!("provider")));
    assert_eq!(runs.load(Ordering::Relaxed), 4);
    assert_eq!(env.events.lock().unwrap().len(), 5);
    worker.close().await;
}

#[tokio::test]
async fn the_raw_result_identifies_a_deliberate_block_and_a_closed_failure() {
    let (link, _, _) = link(false);
    let req = &link.request;
    for fail_closed in [false, true] {
        let worker = worker();
        worker.use_wiring(Arc::new(Env::new(req.context.clone())));
        worker
            .hook(
                HookTrigger::OnToolCall,
                |_: ToolCallEvent, _: RoomContext| async {
                    Err::<HookResult<ToolCallEvent>, roomkit::HookError>("failure".into())
                },
                HookOptions {
                    fail_closed,
                    name: "first".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        worker
            .hook(
                HookTrigger::OnToolCall,
                |_: ToolCallEvent, _: RoomContext| async {
                    HookResult::block("reason", BlockOptions::default())
                },
                HookOptions {
                    name: "second".into(),
                    priority: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        let tools = worker.wiring().tools();
        let (result, _) = tools
            .run_tool_call_chain(event(&req.channel_id), &req.room_id, None)
            .await
            .unwrap();
        assert!(!result.allowed);
        assert_eq!(result.failed_closed, fail_closed);
        assert_eq!(result.hook_errors.len(), 1);
        assert_eq!(result.hook_errors[0]["error"], "failure");
        assert_eq!(result.hook_errors[0]["hook"], "first");
        let expected = if fail_closed {
            ("first", "hook_error:first")
        } else {
            ("second", "reason")
        };
        assert_eq!(result.blocked_by.as_deref(), Some(expected.0));
        assert_eq!(result.reason.as_deref(), Some(expected.1));
        worker.close().await;
    }
}

#[tokio::test]
async fn kit_hooks_refresh_room_state_and_only_reuse_a_current_rooms_history() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let kit = RoomKit::new(roomkit::RoomKitOptions::default());
    let room = kit
        .create_room(roomkit::CreateRoomOptions {
            room_id: Some(req.room_id.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    kit.hook(
        HookTrigger::OnToolCall,
        |_: ToolCallEvent, _: RoomContext| async { HookResult::allow(AllowOptions::default()) },
        roomkit::HookOptions::default(),
    )
    .unwrap();
    let tools = kit.turn_executor().wiring().tools();
    let mut carried = RoomContext::new(room.clone());
    carried.recent_events.push(req.event.clone());
    carried.history_start_index = Some(3);
    carried.room.metadata.insert("stale".into(), json!(true));
    let run = |carrying: RoomContext| {
        let tools = tools.clone();
        let room = room.id.clone();
        async move {
            tools
                .run_tool_call_chain(event(&req.channel_id), &room, Some(&carrying))
                .await
                .unwrap()
                .1
                .unwrap()
        }
    };
    let fresh = run(carried.clone()).await;
    assert!(!fresh.room.metadata.contains_key("stale"));
    assert_eq!(fresh.recent_events, carried.recent_events);
    assert_eq!(fresh.history_start_index, Some(3));
    let mut stale = carried.clone();
    stale.room.event_count += 1;
    assert_eq!(
        run(stale).await.recent_events,
        Vec::<roomkit::model::RoomEvent>::new()
    );
    let mut previous = carried.clone();
    previous.room.created_at = roomkit::model::Timestamp::UNIX_EPOCH;
    assert_eq!(
        run(previous).await.recent_events,
        Vec::<roomkit::model::RoomEvent>::new()
    );
    let mut different_owner = carried.clone();
    different_owner.room.organization_id = Some("another-organization".into());
    assert_eq!(
        run(different_owner).await.recent_events,
        Vec::<roomkit::model::RoomEvent>::new()
    );
    carried.room.id = RoomId::new("another-room").unwrap();
    assert_eq!(
        run(carried).await.recent_events,
        Vec::<roomkit::model::RoomEvent>::new()
    );
    kit.close().await.unwrap();
}
