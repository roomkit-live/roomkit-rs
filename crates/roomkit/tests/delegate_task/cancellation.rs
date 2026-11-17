//! The inline child's terminal write belongs to its owner's cancellation.

use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{Metadata, TaskStatus};
use roomkit::store::{ConversationStore, FindRoomsOptions};
use roomkit::tasks::{DelegateHandlerOptions, DelegatedTask, WaitOptions};
use roomkit::{CancelTaskOptions, DelegateOptions, RoomKit, RoomKitOptions, TurnTimeouts};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, MockAiProvider, ToolResult, Tools};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::{Instant, timeout};

use super::completion_store::CompletionGateStore;
use super::{conversation, id, kit_delegating_with_store};

#[path = "../support/warnings.rs"]
mod warnings;

fn digger(started: Arc<Notify>) -> AiChannel<ScriptedProvider> {
    let tools = Tools::new().tool(AiTool::new("lookup", "Looks up").unwrap(), move |_, _| {
        let started = Arc::clone(&started);
        async move {
            started.notify_one();
            std::future::pending::<()>().await;
            Ok(ToolResult::Text("found".to_owned()))
        }
    });
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("l", "lookup", "{}")]),
        ScriptedRound::answering("Found it."),
    ]);
    AiChannel::new(
        id("digger"),
        model,
        AiChannelOptions {
            tools: tools.into(),
            ..Default::default()
        },
    )
    .unwrap()
}

async fn waiting_child() -> (RoomKit<CompletionGateStore>, MockAiProvider, DelegatedTask) {
    let started = Arc::new(Notify::new());
    let kit = RoomKit::new(RoomKitOptions::with_store(CompletionGateStore::default()));
    let (kit, researcher) = kit_delegating_with_store(
        kit,
        digger(Arc::clone(&started)),
        DelegateHandlerOptions {
            wait_in_task_rooms: true,
            ..Default::default()
        },
    );
    roomkit::testing::set_turn_timeouts(
        &kit,
        TurnTimeouts {
            cancel: Duration::from_secs(10),
            ..Default::default()
        },
    );
    let room = conversation(&kit).await;
    let task = kit
        .delegate(
            &room,
            &id("researcher"),
            "Look it up.",
            DelegateOptions::default(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    (kit, researcher, task)
}

async fn child_status(kit: &RoomKit<CompletionGateStore>) -> Value {
    let rooms = kit
        .store()
        .find_rooms(FindRoomsOptions {
            metadata_filter: Some(Metadata::from_iter([(
                "task_agent_id".to_owned(),
                json!("digger"),
            )])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rooms.len(), 1);
    rooms
        .first()
        .unwrap()
        .metadata
        .get("task_status")
        .unwrap()
        .clone()
}

fn cancelling(
    kit: &RoomKit<CompletionGateStore>,
    task: &DelegatedTask,
) -> tokio::task::JoinHandle<bool> {
    let kit = kit.clone();
    let id = task.id.clone();
    tokio::spawn(async move { kit.cancel_task(&id, CancelTaskOptions::default()).await })
}

// Python: the held-terminal-write variant in TestWaitInTaskRooms.
#[tokio::test]
async fn cancelling_the_worker_waits_for_the_child_s_terminal_write() {
    let (kit, researcher, task) = waiting_child().await;
    let mut cancel = cancelling(&kit, &task);
    timeout(Duration::from_secs(5), kit.store().reached.notified())
        .await
        .unwrap();
    let before = timeout(Duration::from_millis(50), &mut cancel).await;
    let returned_early = before.is_ok();
    let parent_before = task.status();
    let child_before = child_status(&kit).await;
    kit.store().release.notify_one();
    assert!(match before {
        Ok(answer) => answer.unwrap(),
        Err(_) => cancel.await.unwrap(),
    });
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        task.wait(WaitOptions::default()).await.unwrap().status,
        TaskStatus::Cancelled
    );
    assert_eq!(child_status(&kit).await, json!("cancelled"));
    assert_eq!(researcher.calls().len(), 1);
    kit.close().await.unwrap();
    assert!(
        !returned_early,
        "Before release: parent={parent_before:?}, child={child_before}"
    );
}

// Python: test_turn_owner_drain::test_end_received_with_owner_cleanup_held_keeps_its_original_cancel_bound.
#[tokio::test(start_paused = true)]
async fn cancelling_twice_does_not_renew_the_inline_completion_deadline() {
    let (kit, _, task) = waiting_child().await;
    let (warnings, _capture) = warnings::Warnings::capture();
    let started = Instant::now();
    let first = cancelling(&kit, &task);
    kit.store().reached.notified().await;
    assert!(!first.is_finished());
    tokio::time::advance(Duration::from_secs(6)).await;
    let second = cancelling(&kit, &task);
    tokio::task::yield_now().await;
    // Both await the same original end. The held write outlasts that end,
    // so the existing incomplete-drain path must still release the parent.
    assert!(first.await.unwrap());
    assert!(second.await.unwrap());
    assert!(started.elapsed() < Duration::from_secs(11));
    let incomplete = warnings.containing("drainage incomplete");
    assert_eq!(incomplete.len(), 1, "{incomplete:?}");
    assert_eq!(child_status(&kit).await, json!("pending"));
    kit.store().release.notify_one();
    roomkit::testing::idle(&kit).await;
    assert_eq!(child_status(&kit).await, json!("cancelled"));
    kit.close().await.unwrap();
}
