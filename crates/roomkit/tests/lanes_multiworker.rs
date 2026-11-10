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
use roomkit::model::{ChannelBinding, ChannelCapabilities, ChannelOutput, RoomContext};
use roomkit::model::{
    ChannelId, ChannelType, EventSource, EventType, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::store::{AdvanceDeliveredIndexOptions, CommitEventOptions, ListEventsOptions};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, EventTap, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    InMemoryLockManager, InMemoryStore, ProcessInboundOptions, RoomKit, RoomKitOptions,
    RoomLockManager, SendEventOptions,
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
// with a handler that sends into the room: its call is not held behind the
// set that emitted the skip, which waits for the handlers once it left its
// place (Python's lane awaits them before it runs the set, the handler's
// call into the room not waiting for its delivery there).
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

// Python: tests/test_lanes_multiworker.py::test_ai_chain_completes_before_return_and_cursor_catches_up
#[tokio::test]
async fn an_ai_cascades_cursor_is_current_when_the_call_returns() {
    let store = Arc::new(InMemoryStore::new());
    let (kit, recorder) = worker(&store, Duration::from_secs(10));
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "answer",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    room(&kit).await;
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();

    let result = kit
        .process_inbound(message("src", "hi"), ProcessInboundOptions::default())
        .await
        .unwrap();
    // These in-memory reads are ready immediately: no idle, sleep, spawn
    // or scheduler yield may complete the delivery on the caller's behalf.
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    let cursor = store.get_delivered_index(&room_id()).await.unwrap();
    let answer = result.response_events.last().unwrap();
    assert_eq!(room.latest_index, Some(answer.index));
    assert_eq!(cursor, i64::try_from(answer.index).unwrap());
    assert_eq!(answer.source.channel_id, channel("ai"));
    assert_eq!(recorder.texts(), ["hi", "answer"]);
    assert_eq!(recorder.deliveries().last().unwrap().event.id, answer.id);
    assert_eq!(result.cancellation_reason, None);
    kit.close().await.unwrap();
}

/// Starts `text`'s inbound call into `r1` through `kit`, as an instruction
/// to the agent `ai` when `instruction` is set.
fn send(
    kit: &RoomKit,
    text: &str,
    instruction: bool,
) -> tokio::task::JoinHandle<roomkit::InboundResult> {
    let kit = kit.clone();
    let mut inbound = message("src", text);
    if instruction {
        inbound.event_type = EventType::Instruction;
        inbound.addressed_to = Some(vec![channel("ai")]);
    }
    tokio::spawn(async move {
        kit.process_inbound(inbound, ProcessInboundOptions::default())
            .await
            .unwrap()
    })
}

/// An instruction's set takes no turn from the cursor: had it waited for
/// the indexes up to the room's latest when its turn came, a set of
/// another process committed after it would hold the set behind it in its
/// lane, until that process skipped the set as lost (review of RMK-1153).
#[tokio::test(start_paused = true)]
async fn an_instruction_holds_no_later_set_behind_another_process() {
    let store = Arc::new(InMemoryStore::new());
    let (a, recorder) = worker(&store, Duration::from_secs(600));
    let (b, _) = worker(&store, Duration::from_secs(5));
    for kit in [&a, &b] {
        let agent = EchoAgent::new(channel("ai"), "answer", EchoAgentOptions::default());
        kit.register_channel(agent).unwrap();
    }
    room(&a).await;
    let skipped = [
        EventTap::on(&a, &["delivery_skipped"]),
        EventTap::on(&b, &["delivery_skipped"]),
    ];
    let hole = lost(&store, "lost").await;
    let step = Duration::from_millis(200);
    let first = send(&a, "m1", false);
    tokio::time::sleep(step).await;
    a.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    let instruction = send(&a, "Introduce yourself.", true);
    tokio::time::sleep(step).await;
    let second = send(&a, "m2", false);
    tokio::time::sleep(step).await;
    let other = send(&b, "m3", false);
    tokio::time::sleep(step).await;
    let options = AdvanceDeliveredIndexOptions::default();
    assert!(
        store
            .advance_delivered_index(&room_id(), hole, options)
            .await
            .unwrap()
    );
    for call in [first, instruction, other] {
        call.await.unwrap();
    }
    let second = second.await.unwrap();
    assert_eq!(second.cancellation_reason, None, "A's m2 is delivered");
    assert!(recorder.texts().contains(&"m2".to_owned()));
    assert_eq!(
        skipped.iter().map(|tap| tap.events().len()).sum::<usize>(),
        0,
        "no live index is skipped"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
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

/// A transport whose deliveries wait for `gate`, `started` notified as the
/// first one starts: a long provider round trip.
struct Slow {
    inner: RecordingTransport,
    started: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}

impl Channel for Slow {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id().clone()
    }

    fn channel_type(&self) -> ChannelType {
        Channel::channel_type(&self.inner)
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.inner.capabilities()
    }

    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.inner.handle_inbound(message, context).await
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.started.notify_one();
        drop(self.gate.acquire().await);
        self.inner.deliver(event, binding, context).await
    }
}

// Python: tests/test_lanes.py::test_claim_blocks_gap_clock_while_owner_is_executing
#[tokio::test(start_paused = true)]
async fn the_claim_blocks_the_gap_clock_while_its_owner_delivers() {
    let store = Arc::new(InMemoryStore::new());
    let locks: Arc<dyn RoomLockManager> = Arc::new(InMemoryLockManager::default());
    let kit = |gap| {
        RoomKit::new(RoomKitOptions {
            store: Arc::clone(&store),
            delivery_gap_timeout: gap,
            lock_manager: Some(Arc::clone(&locks)),
            ..Default::default()
        })
    };
    // B's gap timeout is shorter than A's delivery: without the claim
    // shared through the lock manager, B would skip the index A delivers.
    let (a, b) = (kit(Duration::from_secs(30)), kit(Duration::from_millis(20)));
    let (started, gate) = (Arc::default(), Arc::new(tokio::sync::Semaphore::new(0)));
    let options = RecordingTransportOptions::default;
    let slow = Slow {
        inner: RecordingTransport::new(channel("recorder"), ChannelType::WEBSOCKET, options()),
        started: Arc::clone(&started),
        gate: Arc::clone(&gate),
    };
    let recorder = RecordingTransport::new(channel("recorder"), ChannelType::WEBSOCKET, options());
    let source = || RecordingTransport::new(channel("src"), ChannelType::SMS, options());
    a.register_channel(source()).unwrap();
    a.register_channel(slow).unwrap();
    b.register_channel(source()).unwrap();
    b.register_channel(recorder.clone()).unwrap();
    let skips = EventTap::on(&b, &["delivery_skipped"]);
    room(&a).await;
    let first = {
        let a = a.clone();
        tokio::spawn(async move {
            a.process_inbound(message("src", "m0"), ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    // A delivers index 0, its claim held.
    started.notified().await;
    let second = {
        let b = b.clone();
        tokio::spawn(async move {
            b.process_inbound(message("src", "m1"), ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    // Five times B's gap timeout.
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.add_permits(1);
    first.await.unwrap();
    second.await.unwrap();
    for kit in [&a, &b] {
        roomkit::testing::idle(kit).await;
    }
    assert_eq!(skips.types(), Vec::<String>::new());
    assert_eq!(recorder.texts(), ["m1"]);
    for kit in [a, b] {
        kit.close().await.unwrap();
    }
}
