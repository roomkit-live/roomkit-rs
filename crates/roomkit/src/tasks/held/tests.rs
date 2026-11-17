//! A cancel that owns the end keeps it even if the work returns afterward.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::HeldTasks;
use crate::tasks::{Marks, Tasks};
use crate::turns::context::TurnContext;

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

#[tokio::test]
async fn an_owner_drains_only_its_immediate_inline_delegations() {
    let tasks = Arc::new(Tasks::default());
    let inline = HeldTasks::default();
    let background = HeldTasks::default();
    let parent = TurnContext::default().in_turn(Arc::from("parent"));
    let child = parent.clone().in_turn(Arc::from("child"));
    let held = |registry: &HeldTasks, id: &str| {
        registry
            .hold(&tasks, id, Box::pin(async {}), |_| std::future::pending())
            .unwrap()
    };
    let first = parent.clone().scope(async { held(&inline, "first") }).await;
    let other = child.scope(async { held(&inline, "other") }).await;
    let apart = parent.scope(async { held(&background, "apart") }).await;
    inline.cancel_in_turn("parent").await;
    assert!(*first.borrow());
    assert!(!*other.borrow());
    assert!(!*apart.borrow());
    assert_eq!(inline.len(), 1);
    assert_eq!(background.len(), 1);
    inline.close().await;
    background.close().await;
    tasks.idle().await;
}

#[tokio::test]
async fn an_owner_drain_stops_waiting_when_a_child_completion_calls_close() {
    let tasks = Arc::new(Tasks::default());
    let held = HeldTasks::default();
    let entering = Arc::new(Notify::new());
    let close = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (running, reached, closing, released) = (
        Arc::clone(&tasks),
        Arc::clone(&entering),
        Arc::clone(&close),
        Arc::clone(&release),
    );
    let ended = TurnContext::default()
        .in_turn(Arc::from("parent"))
        .scope(async {
            held.hold(
                &tasks,
                "child",
                Box::pin(async move {
                    reached.notify_one();
                    closing.notified().await;
                    running.spare(&Marks::current());
                    released.notified().await;
                }),
                |_| std::future::pending(),
            )
            .unwrap()
        })
        .await;
    let draining = held.clone();
    let wait = tokio::spawn(async move { draining.cancel_in_turn("parent").await });
    entering.notified().await;
    assert!(!wait.is_finished());
    close.notify_one();
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .unwrap()
        .unwrap();
    assert!(!*ended.borrow());
    release.notify_one();
    tasks.idle().await;
}

#[tokio::test]
async fn an_owner_drain_does_not_wait_for_its_own_inline_unit() {
    let tasks = Arc::new(Tasks::default());
    let held = HeldTasks::default();
    let calling = held.clone();
    let ended = TurnContext::default()
        .in_turn(Arc::from("parent"))
        .scope(async {
            held.hold(
                &tasks,
                "child",
                Box::pin(async { panic!("own work was cut") }),
                |claim| async move {
                    calling.cancel_in_turn("parent").await;
                    let _end = claim.take().expect("the current work keeps its completion");
                },
            )
            .unwrap()
        })
        .await;
    tokio::time::timeout(Duration::from_secs(2), super::until_ended(ended))
        .await
        .unwrap();
    tasks.idle().await;
}
