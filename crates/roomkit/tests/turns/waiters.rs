//! A completion waiter owns its wait, not the deferred turn (RFC §10.1).

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use roomkit::model::{EventContent, InboundMessage, TextContent};
use roomkit::{CancelOptions, DeliveryHandle, ProcessInboundOptions, RoomKit};
use roomkit_ai::ToolResult;
use tokio::sync::Notify;

use crate::support::{channel, kit, room};

struct Finished(Arc<AtomicBool>);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn held_turn() -> (RoomKit, DeliveryHandle, Arc<Notify>, Arc<AtomicBool>) {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let (running, opened, ended) = (
        Arc::clone(&started),
        Arc::clone(&release),
        Arc::clone(&finished),
    );
    let kit = kit(move |_, _| {
        let (running, opened, ended) = (
            Arc::clone(&running),
            Arc::clone(&opened),
            Finished(Arc::clone(&ended)),
        );
        async move {
            let _ended = ended;
            running.notify_one();
            opened.notified().await;
            Ok(ToolResult::Text("42".to_owned()))
        }
    })
    .await;
    let message = InboundMessage::new(channel("sms"), "user", TextContent::new("Work"));
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    started.notified().await;
    (kit, result.delivery.unwrap(), release, finished)
}

/// Poll once before dropping: a never-polled future would exercise nothing.
async fn pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

async fn waiting(handle: DeliveryHandle) -> tokio::task::JoinHandle<roomkit::InboundResult> {
    let started = Arc::new(Notify::new());
    let ready = Arc::clone(&started);
    let task = tokio::spawn(async move {
        let mut wait = Box::pin(handle.wait());
        pending(wait.as_mut()).await;
        ready.notify_one();
        wait.await.unwrap()
    });
    started.notified().await;
    task
}

// Python: tests/test_delivery_cancellation.py::test_cancelled_waiter_does_not_cancel_deferred_turn
#[tokio::test(start_paused = true)]
async fn a_cancelled_waiter_does_not_cancel_the_deferred_turn() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (kit, handle, release, finished) = held_turn().await;
        let mut abandoned = Box::pin(handle.wait());
        pending(abandoned.as_mut()).await;
        let mut survivor = Box::pin(handle.wait());
        pending(survivor.as_mut()).await;
        drop(abandoned);
        assert!(!handle.done());
        assert!(!finished.load(Ordering::SeqCst));
        pending(survivor.as_mut()).await;

        release.notify_one();
        let result = survivor.await.unwrap();
        assert_eq!(result.cancellation_reason, None);
        assert!(result.response_events.iter().any(|event| {
            matches!(&event.content, EventContent::Text(text) if text.body == "It is 42.")
        }));
        assert!(finished.load(Ordering::SeqCst));
        assert!(handle.done());
        assert_eq!(handle.wait().await.unwrap(), result);
        kit.close().await.unwrap();
    })
    .await
    .unwrap();
}

// Python: tests/test_delivery_cancellation.py::test_deferred_cancel_is_terminal_and_releases_all_waiters
#[tokio::test(start_paused = true)]
async fn cancellation_releases_every_waiter_with_the_first_reason() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (kit, handle, _release, finished) = held_turn().await;
        let first = waiting(handle.clone()).await;
        let second = waiting(handle.clone()).await;
        let result = handle
            .cancel(CancelOptions {
                reason: "hangup".to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(finished.load(Ordering::SeqCst));
        assert!(handle.done());
        assert_eq!(result.cancellation_reason.as_deref(), Some("hangup"));
        assert!(!result.blocked);
        assert!(result.error.is_none());
        assert_eq!(first.await.unwrap(), result);
        assert_eq!(second.await.unwrap(), result);
        assert_eq!(
            handle.cancel(CancelOptions::default()).await.unwrap(),
            result
        );
        kit.close().await.unwrap();
    })
    .await
    .unwrap();
}
