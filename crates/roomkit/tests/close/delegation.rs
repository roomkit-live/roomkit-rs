//! Local delegated turns and their terminal callbacks at shutdown.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{RoomEvent, TaskStatus};
use roomkit::tasks::{DelegatedTaskResult, OnCompleteCallback};
use roomkit::{
    CancelTaskOptions, DelegateOptions, HookExecution, HookOptions, HookTrigger, InMemoryStore,
    RoomKit,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiProvider, AiTool, ToolResult, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::held::{closed, kit_of, room, spawn_close};
use crate::stream_witness::WaitingModel;
use crate::support::channel;
use crate::warnings::Warnings;

pub(super) fn worker(kit: &RoomKit, model: impl AiProvider, tools: Tools) {
    kit.register_channel(
        AiChannel::new(
            channel("worker"),
            model,
            AiChannelOptions {
                tools: tools.into(),
                ..AiChannelOptions::default()
            },
        )
        .unwrap(),
    )
    .unwrap();
}

type Endings = Arc<Mutex<Vec<(String, String)>>>;

fn task_ends(kit: &RoomKit) -> Endings {
    let endings = Endings::default();
    let heard = Arc::clone(&endings);
    kit.hook(
        HookTrigger::OnTaskCompleted,
        move |event: RoomEvent, _| {
            let heard = Arc::clone(&heard);
            async move {
                let read = |key: &str| {
                    event
                        .metadata
                        .get(key)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_owned()
                };
                heard
                    .lock()
                    .unwrap()
                    .push((read("task_status"), read("error")));
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    endings
}

// Python: tests/test_kit_close_doors.py::test_a_delegation_ends_cancelled_with_its_hook
#[tokio::test(start_paused = true)]
async fn a_delegation_ends_cancelled_once_with_or_without_shared_channels() {
    for wait in [false, true] {
        for shared in [false, true] {
            let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
            let model = WaitingModel::new("Working ", false);
            worker(&kit, model.clone(), Tools::new());
            let endings = task_ends(&kit);
            let call = tokio::spawn({
                let kit = kit.clone();
                async move {
                    kit.delegate(
                        &room("r"),
                        &channel("worker"),
                        "Go.",
                        DelegateOptions {
                            wait,
                            share_channels: shared.then(|| vec![channel("sms")]),
                            ..DelegateOptions::default()
                        },
                    )
                    .await
                }
            });
            tokio::time::timeout(Duration::from_secs(1), model.started.notified())
                .await
                .unwrap();
            let (logs, _capture) = Warnings::capture();
            closed(spawn_close(&kit)).await;
            let task = tokio::time::timeout(Duration::from_secs(1), call)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let result = task.result().unwrap();
            assert_eq!(
                (result.status, result.error.as_deref(), result.output),
                (TaskStatus::Cancelled, Some("cancelled"), None)
            );
            assert_eq!(
                *endings.lock().unwrap(),
                [("cancelled".to_owned(), "cancelled".to_owned())]
            );
            assert!(model.dropped.load(Ordering::SeqCst));
            assert_eq!(logs.containing(""), Vec::<String>::new());
        }
    }
}

// Python: tests/test_kit_close_doors.py::test_the_close_waits_for_the_end_of_a_task_being_cancelled
#[tokio::test(start_paused = true)]
async fn close_waits_for_the_cancelled_callback_already_owned_by_cancel() {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    let model = WaitingModel::new("Working ", false);
    worker(&kit, model.clone(), Tools::new());
    let (entered, release, completed) = (
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Mutex::new(Vec::new())),
    );
    let callback: OnCompleteCallback = {
        let (entered, release, completed) = (
            Arc::clone(&entered),
            Arc::clone(&release),
            Arc::clone(&completed),
        );
        Arc::new(move |result: DelegatedTaskResult| {
            let (entered, release, completed) = (
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&completed),
            );
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                completed.lock().unwrap().push(result.status);
            })
        })
    };
    let task = kit
        .delegate(
            &room("r"),
            &channel("worker"),
            "Go.",
            DelegateOptions {
                on_complete: Some(callback),
                ..DelegateOptions::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), model.started.notified())
        .await
        .unwrap();
    let cancel = tokio::spawn({
        let kit = kit.clone();
        async move {
            kit.cancel_task(&task.id, CancelTaskOptions::default())
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let mut closing = spawn_close(&kit);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut closing)
            .await
            .is_err()
    );
    release.notify_one();
    closed(closing).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cancel)
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(*completed.lock().unwrap(), [TaskStatus::Cancelled]);
}

fn closing_tools(kit: &RoomKit, returned: &Arc<Notify>) -> Tools {
    let weak = kit.downgrade();
    let returned = Arc::clone(returned);
    Tools::new().tool(
        AiTool {
            parameters: json!({"type":"object"}),
            ..AiTool::new("shutdown", "Shut down.").unwrap()
        },
        move |_, _| {
            let (weak, returned) = (weak.clone(), Arc::clone(&returned));
            async move {
                let kit = weak.upgrade().unwrap();
                kit.close().await.unwrap();
                returned.notify_one();
                Ok(ToolResult::Text("closed".to_owned()))
            }
        },
    )
}

// Python: tests/test_kit_close_doors.py::test_a_close_from_an_inline_workers_tool_does_not_wait_for_it
// Python: tests/test_kit_close_doors.py::test_a_close_from_a_tool_returns_and_seals_the_kit
#[tokio::test(start_paused = true)]
async fn a_delegated_tools_own_close_returns_and_seals_the_kit() {
    for (wait, shared) in [(true, false), (true, true), (false, false)] {
        let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
        let returned = Arc::new(Notify::new());
        let model = ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "shutdown", "{}")]),
            ScriptedRound::answering("done"),
        ]);
        worker(&kit, model, closing_tools(&kit, &returned));
        let call = tokio::spawn({
            let kit = kit.clone();
            async move {
                kit.delegate(
                    &room("r"),
                    &channel("worker"),
                    "Go.",
                    DelegateOptions {
                        wait,
                        share_channels: shared.then(|| vec![channel("sms")]),
                        ..DelegateOptions::default()
                    },
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), returned.notified())
            .await
            .unwrap();
        // Python requires the caller to end, without prescribing its task result.
        let _result = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            kit.create_room(roomkit::CreateRoomOptions::default()).await,
            Err(roomkit::RoomKitError::KitClosed)
        ));
    }
}
