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

use std::collections::HashMap;
use std::sync::Mutex;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventSource, EventType, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::store::{AdvanceDeliveredIndexOptions, CommitEventOptions, ListEventsOptions};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, EventTap, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, InMemoryStore,
    ProcessInboundOptions, RoomKit, RoomKitOptions, SendEventOptions,
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

// Python: tests/test_lanes.py::test_gap_skips_after_timeout_and_emits_delivery_skipped,
// its handlers run as Python's lane runs them, never holding the room's
// delivery behind them.
#[tokio::test(start_paused = true)]
async fn a_skip_handler_that_sends_into_the_room_holds_no_delivery() {
    let store = Arc::new(InMemoryStore::new());
    let (kit, recorder) = worker(&store, Duration::from_millis(50));
    room(&kit).await;
    let handler_kit = kit.clone();
    kit.on("delivery_skipped", move |_event| {
        let kit = handler_kit.clone();
        async move {
            let options = SendEventOptions::default();
            let notice = TextContent::new("notice: messages lost");
            kit.send_event(&room_id(), &channel("src"), notice, options)
                .await
                .unwrap();
        }
    })
    .unwrap();
    lost(&store, "lost").await;
    let sending = kit.process_inbound(message("src", "after"), ProcessInboundOptions::default());
    tokio::time::timeout(Duration::from_secs(60), sending)
        .await
        .expect("the inbound call returns")
        .unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(recorder.texts(), ["after", "notice: messages lost"]);
    kit.close().await.unwrap();
}

// Python: tests/test_lanes.py::test_stale_plan_is_dropped_when_cursor_already_passed_it
#[tokio::test(start_paused = true)]
async fn a_set_whose_index_another_lane_skipped_is_dropped() {
    let store = Arc::new(InMemoryStore::new());
    let (kit, recorder) = worker(&store, Duration::from_secs(10));
    room(&kit).await;
    let hole = lost(&store, "lost").await;
    let sending = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(message("src", "mine"), ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Another process's lane skips the hole and this set's own index.
    let options = AdvanceDeliveredIndexOptions { force: true };
    assert!(
        store
            .advance_delivered_index(&room_id(), hole + 1, options)
            .await
            .unwrap()
    );
    let result = sending.await.unwrap();
    assert_eq!(
        recorder.texts(),
        Vec::<String>::new(),
        "the stale set is dropped"
    );
    assert_eq!(
        result.cancellation_reason.as_deref(),
        Some("delivery_stale")
    );
    kit.close().await.unwrap();
}

// Python: tests/test_lanes.py::test_unindexed_entries_run_at_their_anchor_without_moving_cursor
#[tokio::test(start_paused = true)]
async fn an_instruction_s_set_waits_for_the_hole_before_it() {
    let store = Arc::new(InMemoryStore::new());
    let (kit, _recorder) = worker(&store, Duration::from_secs(10));
    let agent = EchoAgent::new(channel("ai"), "answer", EchoAgentOptions::default());
    kit.register_channel(agent).unwrap();
    room(&kit).await;
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    let skipped = EventTap::on(&kit, &["delivery_skipped"]);
    let hole = lost(&store, "lost").await;
    let sending = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let mut instruction = message("src", "Introduce yourself.");
            instruction.event_type = EventType::Instruction;
            instruction.addressed_to = Some(vec![channel("ai")]);
            kit.process_inbound(instruction, ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        answers(&store).await,
        0,
        "the instruction waits for the hole"
    );
    sending.await.unwrap();
    assert_eq!(
        answers(&store).await,
        1,
        "the agent answered once the hole was skipped"
    );
    let events = skipped.events();
    assert_eq!(events.len(), 1, "one skip: {:?}", skipped.types());
    assert_eq!(events[0].data["from_index"].as_u64(), Some(hole));
    kit.close().await.unwrap();
}

/// How many events the agent `ai` stored in `r1`.
async fn answers(store: &InMemoryStore) -> usize {
    let options = ListEventsOptions {
        limit: u64::MAX,
        ..Default::default()
    };
    let events = store.list_events(&room_id(), options).await.unwrap();
    events
        .iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .count()
}

// Python: tests/test_lanes_multiworker.py::test_two_kits_interleaved_deliveries_follow_index_order
#[tokio::test(start_paused = true)]
async fn two_kits_interleaved_deliveries_follow_index_order() {
    let store = Arc::new(InMemoryStore::new());
    let log: Arc<Mutex<Vec<(&'static str, String)>>> = Arc::default();
    let kits: Vec<RoomKit> = ["A", "B"]
        .into_iter()
        .map(|tag| {
            let (kit, _recorder) = worker(&store, Duration::from_secs(30));
            let log = Arc::clone(&log);
            kit.on("delivery_succeeded", move |event| {
                let log = Arc::clone(&log);
                async move {
                    if event.channel_id == Some(channel("recorder")) {
                        let id = event.event_id.map(|id| id.to_string()).unwrap_or_default();
                        log.lock().unwrap().push((tag, id));
                    }
                }
            })
            .unwrap();
            kit
        })
        .collect();
    room(&kits[0]).await;
    let sends: Vec<_> = (0..10)
        .map(|i| {
            let kit = kits[i % 2].clone();
            tokio::spawn(async move {
                let text = format!("m{i}");
                kit.process_inbound(message("src", &text), ProcessInboundOptions::default())
                    .await
                    .unwrap()
            })
        })
        .collect();
    for send in sends {
        send.await.unwrap();
    }
    for kit in &kits {
        roomkit::testing::idle(kit).await;
    }
    let options = ListEventsOptions {
        limit: u64::MAX,
        ..Default::default()
    };
    let indexes: HashMap<String, u64> = store
        .list_events(&room_id(), options)
        .await
        .unwrap()
        .into_iter()
        .map(|event| (event.id.to_string(), event.index))
        .collect();
    let log = log.lock().unwrap().clone();
    let delivered: Vec<u64> = log.iter().map(|(_, id)| indexes[id]).collect();
    let mut sorted = delivered.clone();
    sorted.sort_unstable();
    assert_eq!(delivered, sorted, "the deliveries follow the index order");
    assert_eq!(delivered.len(), 10);
    let tags: std::collections::BTreeSet<_> = log.iter().map(|(tag, _)| *tag).collect();
    assert_eq!(tags.len(), 2, "both kits delivered");
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    let cursor = store.get_delivered_index(&room_id()).await.unwrap();
    assert_eq!(Some(u64::try_from(cursor).unwrap()), room.latest_index);
    for kit in kits {
        kit.close().await.unwrap();
    }
}
