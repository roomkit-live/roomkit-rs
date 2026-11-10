//! RFC §12.10.4: sent, active and queued results at the same shutdown.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::task::Poll;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{DeliveryResultStatus, HookResult, RoomEvent, Task, TaskId};
use roomkit::store::ListTasksOptions;
use roomkit::{ConversationStore, HookOptions, HookTrigger, RoomKit};
use roomkit_core::rules::transcoding::text_of;

use crate::held::{Hold, Holding, closed, deferred, kit_of, room, spawn_close};
use crate::store::StuckStore;
use crate::support::channel;

fn hold_first_call_after_its_delivery(kit: &RoomKit<StuckStore>) {
    kit.store().hold_task.store(true, Ordering::SeqCst);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _context| async move {
            let mut result = HookResult::allow(AllowOptions::default());
            if text_of(&event.content) == "m0" {
                result
                    .tasks
                    .push(Task::new(TaskId::new("tA").unwrap(), room("r"), "Kept"));
            }
            Ok(result)
        },
        HookOptions {
            name: "task".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
}

// Python: tests/test_kit_close.py::test_each_call_the_close_cuts_reads_kit_closed_with_what_it_delivered
// Hold a real side-effect write to establish m0 remains unfinished after
// its send. A handle already completed before close keeps its original end
// (tests/deferred.rs), so merely retaining that handle proves no cut.
#[tokio::test(start_paused = true)]
async fn each_unfinished_call_keeps_what_it_delivered_when_close_cuts_it() {
    let held = Holding::new("ws", [("m1", Hold::ForEver)]);
    let kit = kit_of(StuckStore::default(), &[&held], &["r"]).await;
    hold_first_call_after_its_delivery(&kit);
    let first = deferred(&kit, "m0", "r").await.delivery.unwrap();
    kit.store().task_started.notified().await;
    assert_eq!(held.sent(), ["m0"]);
    let mut first_wait = pin!(first.wait());
    poll_fn(|cx| {
        assert!(first_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let second = deferred(&kit, "m1", "r").await.delivery.unwrap();
    held.entered("m1").await;
    let third = deferred(&kit, "m2", "r").await.delivery.unwrap();
    let close = spawn_close(&kit);
    let third = third.wait().await.unwrap();
    assert_eq!(third.cancellation_reason.as_deref(), Some("kit_closed"));
    kit.store().release_task.notify_one();
    closed(close).await;
    let results = [
        first_wait.await.unwrap(),
        second.wait().await.unwrap(),
        third,
    ];
    assert_eq!(
        results
            .iter()
            .map(|r| r.cancellation_reason.as_deref())
            .collect::<Vec<_>>(),
        [Some("kit_closed"); 3]
    );
    let statuses: Vec<_> = results
        .iter()
        .map(|r| {
            r.delivery_results
                .iter()
                .map(|(id, r)| (id.clone(), r.status))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        statuses,
        [
            vec![(channel("ws"), DeliveryResultStatus::Sent)],
            vec![(channel("ws"), DeliveryResultStatus::Failed)],
            vec![]
        ]
    );
    let error = results[1].delivery_results[&channel("ws")]
        .error
        .as_ref()
        .unwrap();
    assert_eq!((error.code.as_str(), error.retryable), ("kit_closed", true));
    assert_eq!(held.sent(), ["m0"]);
    assert_eq!(
        kit.store()
            .list_tasks(&room("r"), ListTasksOptions::default())
            .await
            .unwrap()
            .len(),
        1
    );
}
