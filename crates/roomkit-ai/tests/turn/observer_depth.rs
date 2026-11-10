//! An observer keeps its trigger's depth while its own body runs, including
//! after the turn that fired it has ended (RFC §8.3, §9.5.1).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{RoomEvent, TextContent};
use roomkit::{HookExecution, HookOptions, HookTrigger, RoomKit, RoomKitOptions, SendEventOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiGenerationEvent, AiResponseEvent};
use tokio::sync::{mpsc, oneshot};

use super::support::{Room, channel, room_id};

const BOUND: Duration = Duration::from_secs(5);

async fn room() -> (Room, ScriptedProvider) {
    let model = ScriptedProvider::new((0..20).map(|_| ScriptedRound::answering("ok")));
    let agent = AiChannel::new(
        channel("assistant"),
        model.clone(),
        AiChannelOptions::default(),
    )
    .unwrap();
    let room = Room::on_kit(
        RoomKit::new(RoomKitOptions {
            max_chain_depth: 5,
            ..Default::default()
        }),
        agent,
    )
    .await;
    (room, model)
}

fn observer() -> HookOptions {
    HookOptions {
        name: "remind".to_owned(),
        execution: HookExecution::Async,
        ..Default::default()
    }
}

async fn post(kit: &RoomKit, text: &str) -> RoomEvent {
    kit.send_event(
        &room_id(),
        &channel("sms"),
        TextContent::new(text),
        SendEventOptions::default(),
    )
    .await
    .unwrap()
}

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_hook_telling_the_agent_again_from_its_body_ends_at_the_depth_limit[once the turn is over]
#[tokio::test]
async fn a_generation_observer_posting_after_its_turn_ends_keeps_its_depth() {
    let (room, model) = room().await;
    let (gates, mut waiting) = mpsc::unbounded_channel();
    let (posted, mut posts) = mpsc::unbounded_channel();
    let weak = room.kit.downgrade();
    room.kit
        .hook(
            HookTrigger::BeforeAiGeneration,
            move |_event: AiGenerationEvent, _| {
                let (kit, gates, posted) = (weak.upgrade().unwrap(), gates.clone(), posted.clone());
                async move {
                    let (open, wait) = oneshot::channel();
                    gates.send(open).unwrap();
                    if wait.await.is_err() {
                        return;
                    }
                    let event = post(&kit, "Cite sources.").await;
                    posted.send(event).unwrap();
                }
            },
            observer(),
        )
        .unwrap();
    // A completed public call, not an End frame merely queued by its
    // executor, proves the seed's delivery context has actually ended.
    tokio::time::timeout(BOUND, post(&room.kit, "What is RoomKit?"))
        .await
        .unwrap();
    let mut depths = Vec::new();
    for _ in 0..4 {
        tokio::time::timeout(BOUND, waiting.recv())
            .await
            .unwrap()
            .unwrap()
            .send(())
            .unwrap();
        // Each observer's send returns after its child's delivery. Only
        // then may that child's observer post outside its original turn.
        let event = tokio::time::timeout(BOUND, posts.recv())
            .await
            .unwrap()
            .unwrap();
        depths.push(event.chain_depth);
    }
    assert_eq!(depths, [1, 2, 3, 4]);
    tokio::time::timeout(BOUND, roomkit::testing::idle(&room.kit))
        .await
        .unwrap();
    assert_eq!(model.requests().len(), 4);
    assert!(waiting.try_recv().is_err());
    room.kit.close().await.unwrap();
}

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_hook_telling_the_agent_again_from_its_body_ends_at_the_depth_limit[while the turn awaits it]
#[tokio::test]
async fn a_response_observer_posting_while_its_turn_awaits_it_keeps_its_depth() {
    let (room, model) = room().await;
    let posted = Arc::new(Mutex::new(Vec::new()));
    let (kept, weak) = (Arc::clone(&posted), room.kit.downgrade());
    room.kit
        .hook(
            HookTrigger::OnAiResponse,
            move |_event: AiResponseEvent, _| {
                let (kit, posted) = (weak.upgrade().unwrap(), Arc::clone(&kept));
                async move {
                    if posted.lock().unwrap().len() >= 20 {
                        return;
                    }
                    let event = post(&kit, "Cite sources.").await;
                    posted.lock().unwrap().push(event.chain_depth);
                }
            },
            observer(),
        )
        .unwrap();
    tokio::time::timeout(BOUND, post(&room.kit, "What is RoomKit?"))
        .await
        .unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&room.kit))
        .await
        .unwrap();
    let mut depths = posted.lock().unwrap().clone();
    depths.sort_unstable();
    assert_eq!(depths, [1, 2, 3, 4]);
    assert_eq!(model.requests().len(), 4);
    room.kit.close().await.unwrap();
}
