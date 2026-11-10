//! The supervisor's bound ends the task and its delegation span together.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::ready;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit_core::model::{ChannelId, RoomEvent, RoomId, TaskStatus};
use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger};

use super::Endless;
use crate::channel::Channel;
use crate::orchestration::worker_run::{run_worker, task_output};
use crate::tasks::DelegatedTaskResult;
use crate::telemetry::{MockTelemetryProvider, SpanKind};
use crate::{CreateRoomOptions, DelegateOptions, HookOptions, RoomKit, RoomKitOptions};

async fn kit_with_ends(
    telemetry: &Arc<MockTelemetryProvider>,
    completed: &Arc<Mutex<Vec<(String, String)>>>,
    worker: &Endless,
    room: &RoomId,
) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::clone(telemetry).into()),
        ..RoomKitOptions::default()
    });
    kit.register_channel(worker.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    let heard = Arc::clone(completed);
    kit.hook(
        HookTrigger::OnTaskCompleted,
        move |event: RoomEvent, _| {
            let read = |key: &str| event.metadata[key].as_str().unwrap().to_owned();
            heard
                .lock()
                .unwrap()
                .push((read("task_status"), read("error")));
            ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    kit
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_supervisors_task_timeout_ends_the_task_cancelled
// The shared supervisor timeout scenario covers its public call and child
// room. This checks the completion hook and span on the same run_worker path.
#[tokio::test(start_paused = true)]
async fn a_supervisors_task_timeout_ends_the_task_cancelled() {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let completed = Arc::default();
    let worker = Endless::default();
    let room = RoomId::new("r").unwrap();
    let kit = kit_with_ends(&telemetry, &completed, &worker, &room).await;
    let (running, worker_id) = (kit.clone(), worker.channel_id());
    let run = tokio::spawn(async move {
        run_worker(
            &running,
            &room,
            &worker_id,
            "go",
            Some(Duration::from_secs(5)),
            None,
            DelegateOptions::default(),
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(1), worker.started.notified())
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    let outcome = run.await.unwrap();
    crate::testing::idle(&kit).await;

    assert!(!outcome.completed);
    assert_eq!(outcome.output, "The task timed out after 5s.");
    assert_eq!(
        *completed.lock().unwrap(),
        vec![("cancelled".to_owned(), "cancelled".to_owned())]
    );
    let spans = telemetry.get_spans(SpanKind::Delegation);
    let ended: Vec<_> = spans
        .iter()
        .map(|span| (span.name.as_str(), span.status.as_str()))
        .collect();
    assert_eq!(ended, vec![("delegation.inline", "cancelled")]);
    assert!(
        !telemetry
            .get_active_spans()
            .iter()
            .any(|span| span.kind == SpanKind::Delegation)
    );
    kit.close().await.unwrap();
    assert_eq!(telemetry.get_spans(SpanKind::Delegation).len(), 1);
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_supervisor_reads_a_cancelled_task_as_cancelled
#[test]
fn a_supervisor_reads_a_cancelled_task_as_cancelled() {
    let mut result = DelegatedTaskResult::new(
        "t",
        RoomId::new("c").unwrap(),
        RoomId::new("p").unwrap(),
        ChannelId::new("worker").unwrap(),
    );
    result.status = TaskStatus::Cancelled;
    result.error = Some("cancelled".to_owned());
    assert_eq!(task_output(Some(&result)), "The task was cancelled.");
}
