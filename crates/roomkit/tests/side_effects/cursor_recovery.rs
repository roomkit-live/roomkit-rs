//! RFC §13.5: a failed cursor write is owed, never a lost delivery.
//! Store failures and two workers' lanes need a native fixture.

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::testing::EventTap;
use roomkit::{InMemoryLockManager, RoomLockManager};

use super::*;

fn worker(
    store: &Arc<FailingWrites>,
    locks: &Arc<dyn RoomLockManager>,
    gap: Duration,
) -> (RoomKit<FailingWrites>, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions {
        store: Arc::clone(store),
        lock_manager: Some(Arc::clone(locks)),
        delivery_gap_timeout: gap,
        ..RoomKitOptions::with_store(FailingWrites::new(Failing::Cursor))
    });
    let target = RecordingTransport::new(
        channel("ws1"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(target.clone()).unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("sms1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    (kit, target)
}

async fn prepare(kit: &RoomKit<FailingWrites>) {
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms1", "ws1"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
}

async fn ordinary(kit: &RoomKit<FailingWrites>, body: &str) -> roomkit::InboundResult {
    kit.process_inbound(
        sms(body),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn owed_cursor(failing: Failing) {
    let store = Arc::new(FailingWrites::new(failing));
    let locks: Arc<dyn RoomLockManager> = Arc::new(InMemoryLockManager::default());
    // Python fixes the lane's polling backoff at 5 ms for 30/300 ms gaps.
    // The public Rust kit polls up to 250 ms: keep both gaps above that
    // interval, and the debtor's gap shorter than the other worker's.
    let (first, first_target) = worker(&store, &locks, Duration::from_millis(300));
    let (other, other_target) = worker(&store, &locks, Duration::from_secs(3));
    prepare(&first).await;
    let first_skips = EventTap::on(&first, &["delivery_skipped"]);
    let other_skips = EventTap::on(&other, &["delivery_skipped"]);

    store.armed.store(true, Ordering::SeqCst);
    let a0 = ordinary(&first, "A0").await;
    let first_index = a0.event.as_ref().unwrap().index;
    assert_eq!(first_target.texts(), ["A0"]);
    assert_eq!(
        store.get_delivered_index(&room_id()).await.unwrap(),
        i64::try_from(first_index).unwrap() - 1,
        "the delivery completed but its cursor write failed"
    );
    let b1 = other.process_inbound(sms("B1"), deferred()).await.unwrap();
    let a2 = first.process_inbound(sms("A2"), deferred()).await.unwrap();
    assert_eq!(b1.event.as_ref().unwrap().index, first_index + 1);
    assert_eq!(a2.event.as_ref().unwrap().index, first_index + 2);
    let (other_delivery, own_delivery) = (b1.delivery.unwrap(), a2.delivery.unwrap());
    let (b1, a2) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(other_delivery.wait(), own_delivery.wait())
    })
    .await
    .unwrap();
    let (b1, a2) = (b1.unwrap(), a2.unwrap());
    roomkit::testing::idle(&first).await;
    roomkit::testing::idle(&other).await;
    assert_eq!(
        (
            a0.cancellation_reason,
            b1.cancellation_reason,
            a2.cancellation_reason
        ),
        (None, None, None)
    );
    assert_eq!(first_target.texts(), ["A0", "A2"]);
    assert_eq!(other_target.texts(), ["B1"]);
    assert_eq!(first_skips.types(), Vec::<String>::new());
    assert_eq!(other_skips.types(), Vec::<String>::new());
    if failing == Failing::CursorTwice {
        assert_eq!(store.failed_index.load(Ordering::SeqCst), first_index);
    }
    assert_eq!(
        store.get_delivered_index(&room_id()).await.unwrap(),
        i64::try_from(first_index + 2).unwrap()
    );
    first.close().await.unwrap();
    other.close().await.unwrap();
}

// Python: tests/test_lanes.py::test_a_lane_owing_a_cursor_write_skips_no_index_another_process_holds
#[tokio::test(start_paused = true)]
async fn a_lane_owing_a_cursor_write_skips_no_index_another_process_holds() {
    owed_cursor(Failing::Cursor).await;
}

// Python: tests/test_lanes.py::test_a_lane_whose_owed_write_fails_again_skips_no_index_another_process_holds
#[tokio::test(start_paused = true)]
async fn a_lane_whose_owed_write_fails_again_skips_no_index_another_process_holds() {
    owed_cursor(Failing::CursorTwice).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owed_cursor_recovers_on_four_workers() {
    owed_cursor(Failing::Cursor).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owed_cursor_failing_twice_recovers_on_four_workers() {
    owed_cursor(Failing::CursorTwice).await;
}
