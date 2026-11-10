//! A cancel that owns the end keeps it even if the work returns afterward.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::HeldTasks;
use crate::tasks::{Marks, Tasks};

// Python: tests/tasks/test_cancelled_delegation.py::test_a_cancel_that_took_a_task_ends_it_once_though_its_worker_finished
// The Python test makes a private worker swallow CancelledError. Here the
// actual held work finishes after its cut while a protected read drains;
// it must not take its completed end back from the cancel that owns it.
#[test]
fn a_cancel_that_took_a_task_ends_it_once_though_its_worker_finished() {
    for threaded in [false, true] {
        let mut builder = if threaded {
            tokio::runtime::Builder::new_multi_thread()
        } else {
            tokio::runtime::Builder::new_current_thread()
        };
        let runtime = builder.worker_threads(2).enable_all().build().unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), cancelled_end_is_kept())
                .await
                .unwrap();
        });
    }
}

async fn cancelled_end_is_kept() {
    let tasks = Arc::new(Tasks::default());
    let held = HeldTasks::default();
    let started = Arc::new(Notify::new());
    let ends = Arc::new(Mutex::new(Vec::new()));
    let (working, entered, finished) =
        (Arc::clone(&tasks), Arc::clone(&started), Arc::clone(&ends));
    let cancelled = Arc::clone(&ends);
    let ended = held
        .hold(
            &tasks,
            "task",
            Box::pin(async move { cancelled.lock().unwrap().push("cancelled") }),
            move |claim| async move {
                let cut = working.task_cuts.of(&Marks::current()).unwrap();
                let _reading = cut.reading();
                let mut signal = cut.signal();
                entered.notify_one();
                signal.wait_for(Option::is_some).await.unwrap();
                // The work returned, but its result must not replace the
                // cancelled end, nor run a second completion.
                if let Some(_end) = claim.take() {
                    finished.lock().unwrap().push("completed");
                }
                finished.lock().unwrap().push("work returned");
            },
        )
        .unwrap();
    started.notified().await;
    assert!(held.cancel("task").await);
    tasks.idle().await;
    assert!(*ended.borrow());
    assert_eq!(*ends.lock().unwrap(), vec!["work returned", "cancelled"]);
    assert!(!held.cancel("task").await);
    assert_eq!(*ends.lock().unwrap(), vec!["work returned", "cancelled"]);
}
