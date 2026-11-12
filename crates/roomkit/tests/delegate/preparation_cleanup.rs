//! The terminal write keeps the preparation's error and belongs to shutdown.

use super::*;
use pretty_assertions::assert_eq;

async fn held(boundary: Boundary) -> RoomKit<RoomReadsFail> {
    let kit = kit_with(RoomReadsFail {
        preparation: Some(Preparation::new(boundary)),
        ..Default::default()
    })
    .await;
    kit.store()
        .preparation
        .as_ref()
        .unwrap()
        .armed
        .store(true, Ordering::SeqCst);
    kit
}

async fn at_gate<F: Future>(call: &mut Pin<Box<F>>, gate: &Notify) {
    tokio::time::timeout(BOUND, async {
        tokio::select! {
            _ = call => panic!("call ended before its gate"),
            () = gate.notified() => (),
        }
    })
    .await
    .unwrap();
}

async fn child(kit: &RoomKit<RoomReadsFail>) -> Room {
    kit.store()
        .store
        .list_rooms(store::ListRoomsOptions::default())
        .await
        .unwrap()
        .into_iter()
        .find(|child| child.id != room())
        .unwrap()
}

// Python: tests/tasks/test_delegation_preparation.py::test_preparation_end_survives_repeated_cancellation
// Dropping a future is final in Rust; drop even the wait for an error's terminal write.
#[tokio::test]
async fn preparation_end_survives_its_caller_and_is_drained_by_close() {
    for failed in [false, true] {
        let kit = held(Boundary::BeforeAnnounce).await;
        let gate = kit.store().preparation.as_ref().unwrap();
        gate.write_held.store(true, Ordering::SeqCst);
        let (parent, worker) = (room(), channel("worker"));
        let mut call = Box::pin(kit.delegate(&parent, &worker, "go", DelegateOptions::default()));
        at_gate(&mut call, &gate.reached).await;
        if failed {
            gate.release.notify_one();
            at_gate(&mut call, &gate.write_entered).await;
            drop(call);
        } else {
            drop(call);
            tokio::time::timeout(BOUND, gate.write_entered.notified())
                .await
                .unwrap();
        }
        let mut close = Box::pin(kit.close());
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        gate.write_release.notify_one();
        tokio::time::timeout(BOUND, close).await.unwrap().unwrap();
        assert_eq!(
            child(&kit).await.metadata["task_status"],
            if failed { "failed" } else { "cancelled" }
        );
    }
}

// Python: tests/tasks/test_delegation_preparation.py::test_preparation_cleanup_failure_keeps_the_original_error
#[tokio::test]
async fn preparation_cleanup_failure_keeps_the_original_error() {
    for read in [true, false] {
        for cancelled in [true, false] {
            let logs = Logs::default();
            let _logging = tracing::subscriber::set_default(logs.clone());
            let kit = held(Boundary::BeforeAnnounce).await;
            let gate = kit.store().preparation.as_ref().unwrap();
            let (parent, worker) = (room(), channel("worker"));
            let mut call =
                Box::pin(kit.delegate(&parent, &worker, "go", DelegateOptions::default()));
            at_gate(&mut call, &gate.reached).await;
            if read {
                gate.read_fails.store(true, Ordering::SeqCst);
            } else {
                gate.write_fails.store(true, Ordering::SeqCst);
            }
            if cancelled {
                drop(call);
            } else {
                gate.release.notify_one();
                let error = tokio::time::timeout(BOUND, call)
                    .await
                    .unwrap()
                    .unwrap_err();
                assert!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .to_string()
                        .contains("preparation failed")
                );
            }
            tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
            gate.read_fails.store(false, Ordering::SeqCst);
            gate.write_fails.store(false, Ordering::SeqCst);
            assert_eq!(child(&kit).await.metadata["task_status"], "pending");
            let message = if read {
                "the unannounced child could not be read to store its terminal status"
            } else {
                "the child room's metadata could not be updated"
            };
            assert!(
                logs.0.lock().unwrap().iter().any(|line| line == message),
                "{:?}",
                logs.0.lock().unwrap()
            );
            kit.close().await.unwrap();
        }
    }
}

// Python: tests/tasks/test_delegation_preparation.py::test_failed_creation_never_marks_another_tasks_room
#[tokio::test]
async fn failed_creation_never_marks_another_tasks_room() {
    let kit = held(Boundary::AfterCreate).await;
    let gate = kit.store().preparation.as_ref().unwrap();
    gate.foreign.store(true, Ordering::SeqCst);
    let (parent, worker) = (room(), channel("worker"));
    let mut call = Box::pin(kit.delegate(&parent, &worker, "go", DelegateOptions::default()));
    at_gate(&mut call, &gate.reached).await;
    gate.release.notify_one();
    assert!(tokio::time::timeout(BOUND, call).await.unwrap().is_err());
    assert_eq!(
        serde_json::to_value(child(&kit).await.metadata).unwrap(),
        serde_json::json!({
            "task_id": "another-task", "task_status": "pending", "keep": true
        })
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_delegation_preparation.py::test_preparation_preserves_the_child_record
#[tokio::test]
async fn preparation_preserves_the_child_record() {
    for cancelled in [true, false] {
        let kit = held(Boundary::BeforeAnnounce).await;
        let gate = kit.store().preparation.as_ref().unwrap();
        let (parent, worker) = (room(), channel("worker"));
        let mut call = Box::pin(kit.delegate(&parent, &worker, "go", DelegateOptions::default()));
        at_gate(&mut call, &gate.reached).await;
        let child_id = child(&kit).await.id;
        kit.store()
            .patch_room_metadata(
                &child_id,
                serde_json::from_value(serde_json::json!({"audit": {"keep": true}})).unwrap(),
                store::PatchRoomMetadataOptions::default(),
            )
            .await
            .unwrap();
        let record = RoomEvent::new(
            child_id.clone(),
            roomkit::model::EventSource::new(worker.clone(), ChannelType::AI),
            roomkit::model::TextContent::new("Keep this preparation record."),
        );
        kit.commit_event(&child_id, record, roomkit::CommitEventOptions::default())
            .await
            .unwrap();
        let mut before = child(&kit).await;
        let bindings = kit.store().list_bindings(&child_id).await.unwrap();
        let events = kit
            .store()
            .list_events(&child_id, store::ListEventsOptions::default())
            .await
            .unwrap();
        if cancelled {
            drop(call);
        } else {
            gate.release.notify_one();
            assert!(tokio::time::timeout(BOUND, call).await.unwrap().is_err());
        }
        tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
        let after = child(&kit).await;
        assert!(after.updated_at >= before.updated_at);
        before.updated_at = after.updated_at;
        before.metadata.insert(
            "task_status".to_owned(),
            serde_json::json!(if cancelled { "cancelled" } else { "failed" }),
        );
        before
            .metadata
            .insert("task_result".to_owned(), serde_json::Value::Null);
        assert_eq!(after, before);
        assert_eq!(
            kit.store().list_bindings(&child_id).await.unwrap(),
            bindings
        );
        assert_eq!(
            kit.store()
                .list_events(&child_id, store::ListEventsOptions::default())
                .await
                .unwrap(),
            events
        );
        kit.close().await.unwrap();
    }
}

// A cancelled lifecycle hook is not prolonged for cleanup; it may itself close the kit.
#[tokio::test(start_paused = true)]
async fn cancelled_preparation_does_not_retain_its_creation_hook() {
    let kit = kit_with(InMemoryStore::default()).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let weak = kit.downgrade();
    let (seen, go) = (entered.clone(), release.clone());
    kit.hook(
        HookTrigger::OnRoomCreated,
        move |_: RoomEvent, _: RoomContext| {
            let kit = weak.upgrade().unwrap();
            let (seen, go) = (seen.clone(), go.clone());
            async move {
                seen.notify_one();
                go.notified().await;
                kit.close().await.unwrap();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    let (parent, worker) = (room(), channel("worker"));
    let mut call = Box::pin(kit.delegate(&parent, &worker, "go", DelegateOptions::default()));
    at_gate(&mut call, &entered).await;
    drop(call);
    release.notify_one();
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();
    let children = kit
        .store()
        .list_rooms(store::ListRoomsOptions::default())
        .await
        .unwrap();
    assert_eq!(
        children
            .iter()
            .find(|child| child.id != parent)
            .unwrap()
            .metadata["task_status"],
        "cancelled"
    );
}

// Python: tests/tasks/test_delegation_preparation.py::test_close_past_grace_preserves_original_preparation_error
#[tokio::test(start_paused = true)]
async fn close_past_grace_keeps_the_preparation_error_and_logs_its_cut_write() {
    let logs = Logs::default();
    let _logging = tracing::subscriber::set_default(logs.clone());
    let kit = held(Boundary::BeforeAnnounce).await;
    let gate = kit.store().preparation.as_ref().unwrap();
    gate.write_held.store(true, Ordering::SeqCst);
    let (parent, worker) = (room(), channel("worker"));
    let mut call = Box::pin(kit.delegate(&parent, &worker, "go", DelegateOptions::default()));
    at_gate(&mut call, &gate.reached).await;
    gate.release.notify_one();
    at_gate(&mut call, &gate.write_entered).await;
    tokio::time::timeout(Duration::from_secs(20), kit.close())
        .await
        .unwrap()
        .unwrap();
    let error = call.await.unwrap_err();
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .to_string()
            .contains("preparation failed")
    );
    assert_eq!(child(&kit).await.metadata["task_status"], "pending");
    assert!(
        logs.0
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "the unannounced child's terminal write did not finish")
    );
}
