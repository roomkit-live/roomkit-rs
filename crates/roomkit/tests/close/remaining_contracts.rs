//! RFC §12.10.4: released stores and delivery places held on a cursor read.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::DeliveryResultStatus;
use roomkit::orchestration::{PostAsyncOptions, StatusEntry, StatusLevel};
use roomkit::testing::{EventTap, idle};
use roomkit::{ConversationStore, HookExecution, HookOptions, HookTrigger};
use serde_json::json;

use crate::held::{Hold, Holding, deferred, kit_of};
use crate::store::StuckStore;

// Python: tests/test_kit_close.py::test_work_a_read_starts_is_no_read_once_the_kit_closed
// Isolate the core bus boundary from the source-health adapter in Python.
#[tokio::test]
async fn a_late_status_starts_no_hook_and_reads_no_released_store() {
    let kit = kit_of(StuckStore::default(), &[], &["r"]).await;
    let heard = Arc::new(Mutex::new(Vec::new()));
    let notes = Arc::clone(&heard);
    kit.hook(
        HookTrigger::OnStatusPosted,
        move |entry: StatusEntry, _context| {
            let notes = Arc::clone(&notes);
            async move { notes.lock().unwrap().push(entry.action) }
        },
        HookOptions {
            name: "status".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    let options = PostAsyncOptions {
        metadata: Some([("room_id".to_owned(), json!("r"))].into_iter().collect()),
        ..Default::default()
    };
    let bus = kit.status_bus().clone();
    bus.post_async("src", "before", StatusLevel::Ok, options.clone())
        .await;
    idle(&kit).await;
    assert_eq!(*heard.lock().unwrap(), ["before"]);
    kit.close().await.unwrap();
    assert_eq!(kit.store().closes.lock().unwrap().len(), 1);
    bus.post_async("src", "late", StatusLevel::Ok, options)
        .await;
    idle(&kit).await;
    assert_eq!(*heard.lock().unwrap(), ["before"]);
    assert_eq!(kit.store().reads_after_close.load(Ordering::SeqCst), 0);
}

// Python: tests/test_kit_close.py::test_a_delivery_placed_in_an_idle_lane_runs_one_queued_behind_is_never_sent
// Hold the first place's cursor read itself. In the busy case the second
// place queues behind that active read, before its transport starts.
#[tokio::test(start_paused = true)]
async fn a_place_held_on_its_cursor_gets_grace_but_its_queued_neighbor_does_not() {
    held_cursor_contract().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_place_held_on_its_cursor_keeps_its_grace_on_four_workers() {
    held_cursor_contract().await;
}

async fn held_cursor_contract() {
    for busy in [false, true] {
        let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
        let kit = kit_of(StuckStore::default(), &[&held], &["r"]).await;
        let tap = EventTap::on(&kit, &["delivery_skipped"]);
        *kit.store().held_read.lock().unwrap() = Some("cursor");
        let first = bounded(
            "first deferred",
            deferred(&kit, if busy { "held" } else { "placed" }, "r"),
        )
        .await;
        bounded("cursor read started", kit.store().reading.notified()).await;
        assert_eq!(held.sent(), Vec::<String>::new());
        let queued = if busy {
            Some(bounded("queued deferred", deferred(&kit, "placed", "r")).await)
        } else {
            None
        };
        let mut close = pin!(kit.close());
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if let Some(queued) = queued {
            let handle = queued.delivery.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), handle.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
            assert!(result.delivery_results.is_empty());
        }
        *kit.store().held_read.lock().unwrap() = None;
        kit.store().release_read.notify_one();
        if busy {
            bounded("held send started", held.entered("held")).await;
            held.release.notify_one();
        }
        bounded("close ended", close).await.unwrap();
        let index = first.event.unwrap().index;
        let result = first.delivery.unwrap().wait().await.unwrap();
        assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
        assert_eq!(result.delivery_results.len(), 1);
        assert_eq!(
            result
                .delivery_results
                .get(&crate::support::channel("ws"))
                .unwrap()
                .status,
            DeliveryResultStatus::Sent
        );
        assert_eq!(held.sent(), [if busy { "held" } else { "placed" }]);
        assert_eq!(tap.events(), Vec::<roomkit::FrameworkEvent>::new());
        assert_eq!(
            kit.store()
                .get_delivered_index(&crate::held::room("r"))
                .await
                .unwrap(),
            i64::try_from(index).unwrap()
        );
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> T {
    let result = tokio::time::timeout(Duration::from_secs(10), future).await;
    assert!(result.is_ok(), "timed out: {label}");
    result.unwrap()
}

// The converse: a place that was queued but obtained its delivery claim
// before close is real running work and still advances the cursor.
#[tokio::test(start_paused = true)]
async fn a_queued_set_that_started_before_close_still_advances_its_cursor() {
    let held = Holding::new(
        "ws",
        [
            ("first", Hold::UntilReleased),
            ("second", Hold::UntilReleased),
        ],
    );
    let kit = kit_of(StuckStore::default(), &[&held], &["r"]).await;
    let first = deferred(&kit, "first", "r").await;
    bounded("first send", held.entered("first")).await;
    let second = deferred(&kit, "second", "r").await;
    held.release.notify_one();
    bounded("second send", held.entered("second")).await;
    let first = first.delivery.unwrap().wait().await.unwrap();
    assert_eq!(first.cancellation_reason, None);
    let mut close = pin!(kit.close());
    poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    held.release.notify_one();
    bounded("close", close).await.unwrap();
    let index = second.event.unwrap().index;
    let result = second.delivery.unwrap().wait().await.unwrap();
    assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
    assert_eq!(
        result
            .delivery_results
            .get(&crate::support::channel("ws"))
            .unwrap()
            .status,
        DeliveryResultStatus::Sent
    );
    assert_eq!(held.sent(), ["first", "second"]);
    assert_eq!(
        kit.store()
            .get_delivered_index(&crate::held::room("r"))
            .await
            .unwrap(),
        i64::try_from(index).unwrap()
    );
}
