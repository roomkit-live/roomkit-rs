//! Hook filters read the event after its callback has obtained the room.
use super::{Env, link, worker};
use roomkit::human_input::PendingInputEvent;
use roomkit::model::hook::BlockOptions;
use roomkit::model::{ChannelId, ChannelType, HookResult, Metadata, RoomContext};
use roomkit::turns::HookOptions;
use roomkit::{HookExecution, HookTrigger};
use roomkit_ai::{AiContext, AiGenerationEvent, AiResponseEvent, TurnWiringExt};
use std::sync::Arc;

#[tokio::test]
async fn a_registered_trigger_requires_its_room_before_reporting_a_filtered_callback() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let mut env = Env::new(req.context.clone());
    env.unreadable = true;
    let env = Arc::new(env);
    worker.use_wiring(Arc::<Env>::clone(&env));
    let other = Some([ChannelId::new("other").unwrap()].into_iter().collect());
    worker
        .hook(
            HookTrigger::BeforeAiGeneration,
            |_: AiGenerationEvent, _: RoomContext| async {
                HookResult::block("unrelated generation hook ran", BlockOptions::default())
            },
            HookOptions {
                channel_ids: other.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    worker
        .hook(
            HookTrigger::OnAiResponse,
            |_: AiResponseEvent, _: RoomContext| std::future::ready(()),
            HookOptions {
                channel_ids: other.clone(),
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    worker
        .hook(
            HookTrigger::OnUserInputRequired,
            |_: PendingInputEvent, _: RoomContext| {
                std::future::ready(HookResult::block(
                    "unrelated input hook ran",
                    BlockOptions::default(),
                ))
            },
            HookOptions {
                channel_ids: other,
                ..Default::default()
            },
        )
        .unwrap();
    let wiring = worker.wiring();
    let mut generation = AiGenerationEvent::new(AiContext::default(), req.channel_id.clone());
    generation.room_id = Some(req.room_id.clone());
    let result = wiring.build_before_generation_hook(&req.channel_id)(generation)
        .await
        .unwrap();
    assert!(result.allowed && result.event.is_none());
    let mut response = AiResponseEvent::new(req.channel_id.clone(), "done");
    response.room_id = Some(req.room_id.clone());
    wiring.build_after_response_hook(&req.channel_id)(response)
        .await
        .unwrap();
    let input = PendingInputEvent::new(
        "pending",
        "ask",
        Metadata::new(),
        req.room_id.to_string(),
        "call",
        req.channel_id.to_string(),
        ChannelType::AI,
    );
    assert!(
        wiring.build_on_user_input_required_hook(&req.channel_id)(input)
            .await
            .unwrap()
    );
    assert!(env.events.lock().unwrap().is_empty());
    worker.close().await;
}

#[tokio::test]
async fn an_unnamed_input_uses_the_bound_channel_only_for_its_notification() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let env = Arc::new(Env::new(req.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    worker
        .hook(
            HookTrigger::OnUserInputRequired,
            |_: PendingInputEvent, _: RoomContext| async {
                HookResult::block("denied", BlockOptions::default())
            },
            HookOptions {
                channel_ids: Some([req.channel_id.clone()].into_iter().collect()),
                ..Default::default()
            },
        )
        .unwrap();
    let callback = worker
        .wiring()
        .build_on_user_input_required_hook(&req.channel_id);
    let mut event = PendingInputEvent::new(
        "pending",
        "ask",
        Metadata::new(),
        "",
        "call",
        "",
        ChannelType::AI,
    );
    event.room_id = req.room_id.to_string();
    assert!(callback(event.clone()).await.unwrap());
    event.channel_id = req.channel_id.to_string();
    assert!(!callback(event).await.unwrap());
    let fields = env.fields.lock().unwrap().clone();
    assert_eq!(fields.len(), 2);
    assert!(
        fields
            .iter()
            .all(|f| f.channel_id.as_ref() == Some(&req.channel_id))
    );
    assert_eq!(fields[0].data.as_ref().unwrap()["allowed"], true);
    assert_eq!(fields[1].data.as_ref().unwrap()["allowed"], false);
    worker.close().await;
}
