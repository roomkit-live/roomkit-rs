//! Delivery lanes across processes (RFC §13.5 "Delivery lanes across
//! processes"), as the Python implementation's `tests/test_lanes.py` and
//! `tests/test_lanes_multiworker.py` prove them: a room's delivery cursor
//! orders the sets of several kits sharing a store, and a hole no lane runs
//! is skipped after the gap timeout, never one a lane still runs.
//!
//! Each test runs on tokio's paused clock: a set waiting on the cursor
//! reads it again after 50 ms, then up to every 250 ms, as Python's lane,
//! and the paused clock runs those waits at once.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventSource, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::store::{AdvanceDeliveredIndexOptions, CommitEventOptions};
use roomkit::testing::{EventTap, RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, InMemoryStore,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn message(from: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel(from), "u", TextContent::new(text))
}

/// A kit on `store`, its `src` transport and its `recorder`, registered.
fn worker(store: &Arc<InMemoryStore>, gap: Duration) -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions {
        store: Arc::clone(store),
        delivery_gap_timeout: gap,
        ..Default::default()
    });
    let options = RecordingTransportOptions::default;
    kit.register_channel(RecordingTransport::new(
        channel("src"),
        ChannelType::SMS,
        options(),
    ))
    .unwrap();
    let recorder = RecordingTransport::new(channel("recorder"), ChannelType::WEBSOCKET, options());
    kit.register_channel(recorder.clone()).unwrap();
    (kit, recorder)
}

/// Creates `r1` through `kit` with `src` and `recorder` attached.
async fn room(kit: &RoomKit) {
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["src", "recorder"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
}

/// Commits `text` straight to `store`, as a process that stopped before
/// delivering it: its index is a hole no lane runs.
async fn lost(store: &InMemoryStore, text: &str) -> u64 {
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(channel("src"), ChannelType::SMS),
        TextContent::new(text),
    );
    let committed = store
        .commit_event(&room_id(), event, CommitEventOptions::default())
        .await
        .unwrap();
    committed.index
}

// Python: tests/test_lanes.py::test_gap_blocks_then_resumes_when_another_worker_advances
#[tokio::test(start_paused = true)]
async fn a_set_waits_on_a_hole_until_another_lane_moves_the_cursor() {
    let store = Arc::new(InMemoryStore::new());
    let (kit, recorder) = worker(&store, Duration::from_secs(10));
    room(&kit).await;
    let skipped = EventTap::on(&kit, &["delivery_skipped"]);
    let hole = lost(&store, "in flight elsewhere").await;
    let sending = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(message("src", "after"), ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(recorder.texts().is_empty(), "the set waits on the hole");
    // The lane that runs the hole's set moves the cursor past it.
    let options = AdvanceDeliveredIndexOptions::default();
    assert!(
        store
            .advance_delivered_index(&room_id(), hole, options)
            .await
            .unwrap()
    );
    let result = sending.await.unwrap();
    assert_eq!(recorder.texts(), ["after"]);
    assert_eq!(skipped.types(), Vec::<String>::new(), "nothing was skipped");
    let delivered = store.get_delivered_index(&room_id()).await.unwrap();
    assert_eq!(
        delivered,
        i64::try_from(result.event.unwrap().index).unwrap()
    );
    kit.close().await.unwrap();
}

// Python: tests/test_lanes.py::test_gap_skips_after_timeout_and_emits_delivery_skipped
#[tokio::test(start_paused = true)]
async fn a_hole_no_lane_runs_is_skipped_after_the_gap_timeout() {
    let store = Arc::new(InMemoryStore::new());
    let (kit, recorder) = worker(&store, Duration::from_millis(50));
    room(&kit).await;
    let skipped = EventTap::on(&kit, &["delivery_skipped"]);
    let hole = lost(&store, "lost").await;
    kit.process_inbound(message("src", "after"), ProcessInboundOptions::default())
        .await
        .unwrap();
    assert_eq!(recorder.texts(), ["after"]);
    let events = skipped.events();
    assert_eq!(events.len(), 1, "one skip: {:?}", skipped.types());
    let data = &events[0].data;
    assert_eq!(
        (data["from_index"].as_u64(), data["to_index"].as_u64()),
        (Some(hole), Some(hole))
    );
    kit.close().await.unwrap();
}
