//! Shutdown boundaries of observers, timers, mutations and injected passes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, InjectedEvent, RoomEvent, RoomStatus, RoomTimers};
use roomkit::{
    ConversationStore, HookExecution, HookOptions, HookTrigger, InMemoryStore, RoomKitError,
};
use roomkit_ai::{MockAiProvider, MockAiProviderOptions, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::held::{
    Hold, Holding, attach_agent, closed, into, kit_of, record, result_of, room, said, spawn_close,
    spawn_said, stored,
};
use crate::store::StuckStore;
use crate::support::channel;
use crate::warnings::Warnings;
use roomkit_core::rules::transcoding::text_of;

struct Dropped(Arc<Mutex<Vec<&'static str>>>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("cut");
    }
}

// Python: tests/test_kit_close.py::test_a_pass_the_close_cuts_starts_no_before_broadcast_observer
#[tokio::test(start_paused = true)]
async fn a_pass_the_close_cuts_starts_no_before_broadcast_observer() {
    let kit = kit_of(StuckStore::hanging("slow", false), &[], &["r"]).await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            seen.lock().unwrap().push(text_of(&event.content));
            async {}
        },
        HookOptions {
            name: "watch".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    kit.process_inbound(said("fast"), into("r")).await.unwrap();
    let slow = spawn_said(&kit, "slow", "r");
    kit.store().committing.notified().await;
    closed(spawn_close(&kit)).await;
    let result = result_of(slow).await;
    assert_eq!(
        (
            result.cancellation_reason.as_deref(),
            observed.lock().unwrap().clone(),
            stored(&kit, "r").await
        ),
        (
            Some("kit_closed"),
            vec!["fast".to_owned()],
            vec!["fast".to_owned()]
        )
    );
}

// Python: tests/test_kit_close.py::test_a_room_change_outlasting_the_grace_is_cut_and_never_lands
#[tokio::test(start_paused = true)]
async fn a_metadata_write_cut_by_the_close_never_lands() {
    let kit = kit_of(StuckStore::default(), &[], &["r"]).await;
    kit.store()
        .rooms_hang
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let writing = tokio::spawn({
        let kit = kit.clone();
        async move {
            kit.update_room_metadata(
                &room("r"),
                [("late".to_owned(), json!(true))].into_iter().collect(),
                Default::default(),
            )
            .await
        }
    });
    kit.store().committing.notified().await;
    closed(spawn_close(&kit)).await;
    assert!(matches!(
        writing.await.unwrap(),
        Err(RoomKitError::KitClosed)
    ));
    let room = kit.store().get_room(&room("r")).await.unwrap().unwrap();
    assert!(!room.metadata.contains_key("late"));
    assert!(kit.store().was_cut("room"));
}

// Python: tests/test_kit_close_doors.py::test_a_caller_waiting_for_its_injected_events_returns_when_the_close_cuts_them
#[tokio::test(start_paused = true)]
async fn a_caller_waiting_for_an_injected_pass_keeps_its_committed_event() {
    let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
    let entered = Arc::new(Notify::new());
    let entering = entered.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let entered = entering.clone();
            async move {
                let body = text_of(&event.content);
                if body == "injected" {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                }
                let injected = (body == "hello").then(|| {
                    vec![InjectedEvent {
                        target_channel_ids: Some(vec![channel("ws")]),
                        ..InjectedEvent::new(record("injected"))
                    }]
                });
                HookResult::allow(AllowOptions {
                    injected,
                    ..Default::default()
                })
            }
        },
        HookOptions {
            name: "inject".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let call = spawn_said(&kit, "hello", "r");
    entered.notified().await;
    closed(spawn_close(&kit)).await;
    let result = result_of(call).await;
    assert_eq!(
        result.event.map(|e| text_of(&e.content)),
        Some("hello".to_owned())
    );
    assert_eq!(stored(&kit, "r").await, vec!["hello".to_owned()]);
}

// Python: tests/test_kit_close_doors.py::test_a_timer_check_whose_hooks_outlast_the_grace_is_cut
#[tokio::test(start_paused = true)]
async fn a_timer_check_keeps_its_pause_when_the_close_cuts_its_hook() {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    kit.set_room_timers(
        &room("r"),
        RoomTimers {
            inactive_after_seconds: Some(0),
            ..Default::default()
        },
        Default::default(),
    )
    .await
    .unwrap();
    let entered = Arc::new(Notify::new());
    let steps = Arc::new(Mutex::new(Vec::new()));
    let (entering, noting) = (entered.clone(), steps.clone());
    kit.hook(
        HookTrigger::OnRoomPaused,
        move |_: RoomEvent, _| {
            let (entering, note) = (entering.clone(), Dropped(noting.clone()));
            async move {
                let _note = note;
                entering.notify_one();
                std::future::pending::<()>().await;
            }
        },
        HookOptions {
            name: "slow".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    let checked = kit
        .check_room_timers(&room("r"), Default::default())
        .await
        .unwrap();
    entered.notified().await;
    closed(spawn_close(&kit)).await;
    let stored = kit.get_room(&room("r"), Default::default()).await.unwrap();
    assert_eq!(
        (checked.status, stored.status, steps.lock().unwrap().clone()),
        (RoomStatus::Paused, RoomStatus::Paused, vec!["cut"])
    );
}

// Python: tests/test_kit_close.py::test_an_observer_closing_on_an_agent_s_answer_logs_nothing
#[tokio::test(start_paused = true)]
async fn an_observer_closing_on_a_completed_answer_keeps_its_end_without_warnings() {
    for streaming in [false, true] {
        let (warnings, logging) = Warnings::capture();
        let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
        attach_agent(
            &kit,
            MockAiProvider::new(MockAiProviderOptions {
                responses: vec!["Answer.".to_owned()],
                streaming,
                ..Default::default()
            }),
            Tools::new(),
        )
        .await;
        let done = Arc::new(Notify::new());
        let (closer, ended) = (kit.clone(), done.clone());
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _| {
                let (kit, ended) = (closer.clone(), ended.clone());
                async move {
                    if event.source.channel_id == channel("agent") {
                        kit.close().await.unwrap();
                        ended.notify_one();
                    }
                }
            },
            HookOptions {
                name: "bye".to_owned(),
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
        kit.process_inbound(said("Hi."), into("r")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), done.notified())
            .await
            .unwrap();
        let events = kit
            .store()
            .list_events(
                &room("r"),
                roomkit::store::ListEventsOptions {
                    limit: u64::MAX,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let answers: Vec<_> = events
            .into_iter()
            .filter(|e| e.source.channel_id == channel("agent"))
            .collect();
        assert_eq!(answers.len(), 1);
        assert_eq!(
            answers[0].metadata.get("loop_end_reason"),
            Some(&json!("completed"))
        );
        drop(logging);
        assert!(
            warnings.containing("").is_empty(),
            "streaming={streaming}: {:?}",
            warnings.containing("")
        );
    }
}

// Python: tests/test_kit_close_doors.py::test_a_close_from_a_hook_cuts_another_call_s_send_under_way
#[tokio::test(start_paused = true)]
async fn a_close_from_an_after_broadcast_hook_cuts_another_calls_send() {
    let held = Holding::new("ws", [("Two", Hold::ForEver)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    let go = Arc::new(Notify::new());
    let (closer, release) = (kit.clone(), go.clone());
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (kit, go) = (closer.clone(), release.clone());
            async move {
                if text_of(&event.content) == "One" {
                    go.notified().await;
                    kit.close().await.unwrap();
                }
            }
        },
        HookOptions {
            name: "quit".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    kit.process_inbound(said("One"), into("r")).await.unwrap();
    let call = spawn_said(&kit, "Two", "r");
    held.entered("Two").await;
    go.notify_one();
    let result = result_of(call).await;
    kit.close().await.unwrap();
    assert_eq!(held.steps(), vec!["send cut", "closed"]);
    assert_eq!(
        result
            .delivery_results
            .get(&channel("ws"))
            .and_then(|r| r.error.as_ref())
            .map(|e| e.code.as_str()),
        Some("kit_closed")
    );
}
