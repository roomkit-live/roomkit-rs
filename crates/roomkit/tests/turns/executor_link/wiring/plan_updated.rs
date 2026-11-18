//! A callback obtained directly from the wiring reaches the planner's typed observers.
use super::{Env, link, worker};
use roomkit::model::RoomContext;
use roomkit::turns::HookOptions;
use roomkit::{HookExecution, HookTrigger};
use roomkit_ai::{PlanTaskHandle, PlanUpdatedEvent};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn updated_plan_callbacks_share_tasks_with_the_public_planner_event() {
    let (link, _, _) = link(false);
    let request = &link.request;
    let worker = worker();
    worker.use_wiring(Arc::new(Env::new(request.context.clone())));
    let update = worker.wiring().build_plan_updated_hook(&request.channel_id);
    let (seen, mut observed) = tokio::sync::mpsc::unbounded_channel();
    worker
        .hook(
            HookTrigger::OnPlanUpdated,
            move |mut event: PlanUpdatedEvent, context: RoomContext| {
                let seen = seen.clone();
                async move {
                    let task = event.tasks.pop().unwrap();
                    task.with_mut(|fields| {
                        fields.insert("status".into(), json!("checked"));
                    });
                    seen.send((event.room_id, event.channel_id, context))
                        .unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let task = PlanTaskHandle::new(json!({"status":"new"}).as_object().unwrap().clone());
    update(request.room_id.clone(), vec![task.clone()]).await;
    let (room, channel, context) =
        tokio::time::timeout(std::time::Duration::from_secs(2), observed.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(room, request.room_id.as_str());
    assert_eq!(channel, request.channel_id.as_str());
    assert_eq!(context, request.context);
    assert_eq!(task.get()["status"], "checked");
    worker.close().await;
}

#[tokio::test(start_paused = true)]
async fn a_direct_plan_tool_reads_the_hook_context_before_requesting_the_next_round() {
    use roomkit::Channel;
    use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
    use roomkit_ai::{AiChannel, AiChannelOptions};
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let mut env = Env::new(req.context.clone());
    let gate = Arc::new(tokio::sync::Notify::new());
    env.context_gate = Some(Arc::clone(&gate));
    worker.use_wiring(Arc::new(env));
    worker
        .hook(
            HookTrigger::OnPlanUpdated,
            |_: PlanUpdatedEvent, _: RoomContext| async {},
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let provider = ScriptedProvider::new([
        ScriptedRound::calling("", [("p", "plan_tasks", r#"{"tasks":[]}"#)]),
        ScriptedRound::answering("done"),
    ]);
    let agent = AiChannel::new(
        req.channel_id.clone(),
        provider.clone(),
        AiChannelOptions {
            enable_planning: true,
            ..Default::default()
        },
    )
    .unwrap();
    worker.wire(&agent).unwrap();
    let delivery = agent.on_event(req.event.clone(), req.binding.clone(), req.context.clone());
    tokio::pin!(delivery);
    let waiting = tokio::time::timeout(std::time::Duration::from_millis(1), &mut delivery).await;
    assert!(
        waiting.is_err(),
        "the channel completed while its plan hook context was suspended"
    );
    assert_eq!(provider.requests().len(), 1);
    gate.notify_one();
    delivery.await.unwrap();
    assert_eq!(provider.requests().len(), 2);
    worker.close().await;
}
