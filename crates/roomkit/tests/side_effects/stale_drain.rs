//! A stale set still drains its retained writes before its handle ends.

use super::*;
use pretty_assertions::assert_eq;
use std::task::{Context, Waker};
use std::time::Duration;

struct HeldCallback {
    finished: Arc<AtomicBool>,
    reached: Arc<tokio::sync::Notify>,
    opened: Arc<tokio::sync::Notify>,
}

fn hold_callback(kit: &RoomKit<FailingWrites>) -> HeldCallback {
    let created = Arc::new(AtomicBool::new(false));
    let callback_started = Arc::new(tokio::sync::Notify::new());
    let callback_opened = Arc::new(tokio::sync::Notify::new());
    let (seen, reached, opened) = (
        Arc::clone(&created),
        Arc::clone(&callback_started),
        Arc::clone(&callback_opened),
    );
    kit.hook(
        HookTrigger::OnTaskCreated,
        move |_: RoomEvent, _| {
            let (seen, reached, opened) =
                (Arc::clone(&seen), Arc::clone(&reached), Arc::clone(&opened));
            async move {
                reached.notify_one();
                opened.notified().await;
                seen.store(true, Ordering::SeqCst);
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "held_follow_up".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    HeldCallback {
        finished: created,
        reached: callback_started,
        opened: callback_opened,
    }
}

// Python: tests/test_lanes.py::test_a_dropped_set_s_kept_write_holds_its_cascade_s_drain
#[tokio::test(start_paused = true)]
async fn a_stale_sets_retained_write_holds_its_handle_open() {
    let kit = kit_on(Failing::HeldTask, &["sms1"]).await;
    let recorder = RecordingTransport::new(
        channel("ws1"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(recorder.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ws1"), AttachChannelOptions::default())
        .await
        .unwrap();
    gives_task(&kit, "hello", "tA");
    let callback = hold_callback(&kit);
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    let next = room.latest_index.map_or(0, |index| index + 1);
    kit.store()
        .advance_delivered_index(
            &room_id(),
            next,
            store::AdvanceDeliveredIndexOptions { force: true },
        )
        .await
        .unwrap();

    let bound = Duration::from_secs(5);
    let result = tokio::time::timeout(bound, kit.process_inbound(sms("hello"), deferred()))
        .await
        .unwrap()
        .unwrap();
    let handle = result.delivery.unwrap();
    tokio::time::timeout(bound, kit.store().held.0.notified())
        .await
        .unwrap();
    assert!(!handle.done());
    assert!(!callback.finished.load(Ordering::SeqCst));
    let mut waiting = std::pin::pin!(handle.wait());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    kit.store().held.1.notify_one();
    tokio::time::timeout(bound, callback.reached.notified())
        .await
        .unwrap();
    // ON_TASK_CREATED is an async observer: the handle waits for the
    // retained write, not for the observer's body (RFC §9.1, §10.1 step 15).
    let finished = tokio::time::timeout(bound, waiting).await.unwrap().unwrap();
    assert_eq!(
        finished.cancellation_reason.as_deref(),
        Some("delivery_stale")
    );
    assert!(handle.done());
    assert_eq!(recorder.texts(), Vec::<String>::new());
    assert!(
        kit.store()
            .get_task(&TaskId::new("tA").unwrap())
            .await
            .unwrap()
            .is_some()
    );
    assert!(!callback.finished.load(Ordering::SeqCst));
    let mut closing = std::pin::pin!(kit.close());
    // Run the shutdown until its only pending observer blocks it. On the
    // paused clock the timer advances after runnable work has parked;
    // checking the first poll alone would only observe shutdown's spawn.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), closing.as_mut())
            .await
            .is_err()
    );
    callback.opened.notify_one();
    tokio::time::timeout(bound, closing).await.unwrap().unwrap();
    assert!(callback.finished.load(Ordering::SeqCst));
}
