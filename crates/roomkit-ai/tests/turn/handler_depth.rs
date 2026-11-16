//! A handler of a framework event a turn emits keeps the turn's depth while
//! it runs, the turn over or not, as an ASYNC hook's body keeps its
//! trigger's; a task it starts has it no more once it has returned (RFC
//! §8.3).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{RoomEvent, TextContent};
use roomkit::{HookExecution, HookOptions, HookTrigger, RoomKit, RoomKitOptions, SendEventOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiResponseEvent};
use tokio::sync::{Notify, mpsc, oneshot};

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

async fn post(kit: &RoomKit, source: &str, text: &str) -> RoomEvent {
    kit.send_event(
        &room_id(),
        &channel(source),
        TextContent::new(text),
        SendEventOptions::default(),
    )
    .await
    .unwrap()
}

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_framework_event_handler_telling_the_agent_again_ends_at_the_depth_limit[before_ai_generation-once the turn is over]
#[tokio::test]
async fn a_generation_handler_posting_after_its_turn_ends_keeps_its_depth() {
    let (room, model) = room().await;
    let (gates, mut waiting) = mpsc::unbounded_channel();
    let (posted, mut posts) = mpsc::unbounded_channel();
    let weak = room.kit.downgrade();
    room.kit
        .on("before_ai_generation", move |_event| {
            let (kit, gates, posted) = (weak.upgrade().unwrap(), gates.clone(), posted.clone());
            async move {
                let (open, wait) = oneshot::channel();
                gates.send(open).unwrap();
                if wait.await.is_err() {
                    return;
                }
                let event = post(&kit, "sms", "Cite sources.").await;
                posted.send(event).unwrap();
            }
        })
        .unwrap();
    // The seed's call returned: its turn is over before the handler posts.
    tokio::time::timeout(BOUND, post(&room.kit, "sms", "What is RoomKit?"))
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

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_framework_event_handler_telling_the_agent_again_ends_at_the_depth_limit[ai_response-while the turn awaits it]
#[tokio::test]
async fn a_response_handler_posting_at_each_answer_ends_at_the_depth_limit() {
    let (room, model) = room().await;
    let posted = Arc::new(Mutex::new(Vec::new()));
    let (kept, weak) = (Arc::clone(&posted), room.kit.downgrade());
    room.kit
        .on("ai_response", move |_event| {
            let (kit, posted) = (weak.upgrade().unwrap(), Arc::clone(&kept));
            async move {
                if posted.lock().unwrap().len() >= 20 {
                    return;
                }
                let event = post(&kit, "sms", "Cite sources.").await;
                posted.lock().unwrap().push(event.chain_depth);
            }
        })
        .unwrap();
    tokio::time::timeout(BOUND, post(&room.kit, "sms", "What is RoomKit?"))
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

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_framework_event_handler_telling_the_agent_again_ends_at_the_depth_limit[ai_response-after a slower ON_AI_RESPONSE hook]
// ai_response follows the turn's ON_AI_RESPONSE observers: with the turn's
// floor lent only while the turn ran, the first post, made after the turn,
// was at depth 0, and the loop went on.
#[tokio::test]
async fn a_response_handler_posting_after_a_slower_response_hook_ends_at_the_depth_limit() {
    let (room, model) = room().await;
    let audit = Arc::new(Notify::new());
    let held = Arc::new(Mutex::new(true));
    let (waits, first) = (Arc::clone(&audit), Arc::clone(&held));
    room.kit
        .hook(
            HookTrigger::OnAiResponse,
            move |_event: AiResponseEvent, _| {
                let (waits, first) = (Arc::clone(&waits), Arc::clone(&first));
                async move {
                    // The first observer outlives its turn.
                    if std::mem::take(&mut *first.lock().unwrap()) {
                        waits.notified().await;
                    }
                }
            },
            HookOptions {
                name: "audit".to_owned(),
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let posted = Arc::new(Mutex::new(Vec::new()));
    let (kept, weak) = (Arc::clone(&posted), room.kit.downgrade());
    room.kit
        .on("ai_response", move |_event| {
            let (kit, posted) = (weak.upgrade().unwrap(), Arc::clone(&kept));
            async move {
                if posted.lock().unwrap().len() >= 20 {
                    return;
                }
                let event = post(&kit, "sms", "Cite sources.").await;
                posted.lock().unwrap().push(event.chain_depth);
            }
        })
        .unwrap();
    tokio::time::timeout(BOUND, post(&room.kit, "sms", "What is RoomKit?"))
        .await
        .unwrap();
    // The seed's call returned: its turn is over before ai_response.
    audit.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&room.kit))
        .await
        .unwrap();
    let mut depths = posted.lock().unwrap().clone();
    depths.sort_unstable();
    assert_eq!(depths, [1, 2, 3, 4]);
    assert_eq!(model.requests().len(), 4);
    room.kit.close().await.unwrap();
}

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_task_a_turn_s_framework_event_handler_starts_calls_as_work_of_its_own
// Lent to the event's whole dispatch instead, the floor made the task's
// post at depth 1.
#[tokio::test]
async fn a_task_a_response_handler_starts_calls_as_work_of_its_own() {
    let (room, _model) = room().await;
    let sent = Arc::new(Notify::new());
    let (depths, mut posted) = mpsc::unbounded_channel();
    let (told, weak) = (Arc::clone(&sent), room.kit.downgrade());
    room.kit
        .on("ai_response", move |_event| {
            let (kit, depths, sent) = (weak.upgrade().unwrap(), depths.clone(), Arc::clone(&told));
            async move {
                let returned = Arc::new(Notify::new());
                let waits = Arc::clone(&returned);
                drop(tokio::spawn(roomkit::keep_context(async move {
                    waits.notified().await;
                    let event = post(&kit, "assistant", "later").await;
                    depths.send(event.chain_depth).unwrap();
                    sent.notify_one();
                })));
                // It returns right after.
                returned.notify_one();
            }
        })
        .unwrap();
    let waits = Arc::clone(&sent);
    room.kit
        .on("ai_response", move |_event| {
            let waits = Arc::clone(&waits);
            // The event's next handler still runs while the task posts.
            async move { tokio::time::timeout(BOUND, waits.notified()).await.unwrap() }
        })
        .unwrap();
    tokio::time::timeout(BOUND, post(&room.kit, "sms", "What is RoomKit?"))
        .await
        .unwrap();
    let depth = tokio::time::timeout(BOUND, posted.recv()).await.unwrap();
    assert_eq!(depth, Some(0));
    room.kit.close().await.unwrap();
}

/// The handler's whole call keeps the floor, not only the future it
/// returns: a task its closure starts before returning that future, which
/// the future then awaits, posts at the turn's depth. Python has no such
/// part: its handler is one coroutine, awaited whole.
#[tokio::test]
async fn a_task_a_handler_s_closure_starts_keeps_the_floor_while_the_handler_runs() {
    let (room, _model) = room().await;
    let (depths, mut posted) = mpsc::unbounded_channel();
    let weak = room.kit.downgrade();
    room.kit
        .on("ai_response", move |_event| {
            let (kit, depths) = (weak.upgrade().unwrap(), depths.clone());
            // Started by the closure itself, before its future exists.
            let task = tokio::spawn(roomkit::keep_context(async move {
                let event = post(&kit, "assistant", "noted").await;
                depths.send(event.chain_depth).unwrap();
            }));
            async move { task.await.unwrap() }
        })
        .unwrap();
    tokio::time::timeout(BOUND, post(&room.kit, "sms", "What is RoomKit?"))
        .await
        .unwrap();
    let depth = tokio::time::timeout(BOUND, posted.recv()).await.unwrap();
    assert_eq!(depth, Some(1));
    room.kit.close().await.unwrap();
}
