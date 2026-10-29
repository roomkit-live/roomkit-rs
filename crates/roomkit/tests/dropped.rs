//! A call whose caller stops waiting while the store commits its event
//! (RFC §13.6, decision 1 of step 2b, gap G-076): a commit is never within
//! the call's bound, so it is answered, the call is decided, and the event
//! is delivered whoever waits. A store whose commit waits at a gate holds
//! the call there while its caller goes.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventId, EventSource, EventStatus, EventUpdates, HookResult, Identity,
    IdentityId, InboundMessage, Observation, Participant, ParticipantId, Room, RoomContext,
    RoomEvent, RoomId, RoomUpdates, Task, TextContent, Timestamp,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CommitEventOptions,
    ConversationStore, CreateRoomOptions, EventFilter, EventQuery, FrameworkEvent, HookExecution,
    HookSpec, HookTrigger, InMemoryStore, ProcessInboundOptions, RoomCounters, RoomFilter, RoomKit,
    RoomKitError, RoomKitOptions, SendEventOptions, StoreError, TargetUpdate,
};
use tokio::sync::{Notify, broadcast};

#[path = "support/warnings.rs"]
mod warnings;

use warnings::Warnings;

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// An in-memory store whose first commit of an event that is neither the
/// framework's own lifecycle record (gap G-046) nor `First`, the work the
/// close tests run beside it, tells `committing` it started, then waits for
/// `go`, as a slow database does; that commit then
/// fails when `fails` is set, as a database gone does, or panics when
/// `panics` is, as a broken driver does.
#[derive(Debug, Default)]
struct GatedStore {
    store: InMemoryStore,
    started: AtomicBool,
    committing: Arc<Notify>,
    go: Arc<Notify>,
    fails: AtomicBool,
    panics: AtomicBool,
}

impl ConversationStore for GatedStore {
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        self.store.create_room(room).await
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        self.store.update_room(room_id, updates).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.store.get_room(room_id).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        force: bool,
    ) -> Result<bool, StoreError> {
        self.store
            .advance_delivered_index(room_id, index, force)
            .await
    }

    async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, StoreError> {
        self.store.list_rooms(filter).await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_binding(channel_id, participant_id, identity_id, filter)
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_participant(channel_id, participant_id, identity_id, filter)
            .await
    }

    async fn store_binding(&self, binding: ChannelBinding) -> Result<(), StoreError> {
        self.store.store_binding(binding).await
    }

    async fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.store.delete_binding(room_id, channel_id).await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.store.list_bindings(room_id).await
    }

    async fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> Result<Option<RoomContext>, StoreError> {
        self.store.load_context(room_id, recent_events).await
    }

    async fn create_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.store.create_participant(participant).await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.store.get_participant(room_id, participant_id).await
    }

    async fn update_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.store.update_participant(participant).await
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.store.delete_participant(room_id, participant_id).await
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.store.list_participants(room_id).await
    }

    async fn create_identity(&self, identity: Identity) -> Result<(), StoreError> {
        self.store.create_identity(identity).await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.store.get_identity(identity_id).await
    }

    async fn resolve_identity(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<Option<Identity>, StoreError> {
        self.store
            .resolve_identity(channel_type, address, organization_id)
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.store
            .link_address(identity_id, channel_type, address, organization_id)
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.store
            .unlink_address(identity_id, channel_type, address, organization_id)
            .await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        key: &str,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.store
            .get_event_by_idempotency_key(room_id, channel_id, key)
            .await
    }

    async fn commit(
        &self,
        event: RoomEvent,
        counters: RoomCounters,
        target: Option<TargetUpdate>,
    ) -> Result<(), StoreError> {
        let ungated = event.source.channel_type == ChannelType::SYSTEM
            || roomkit::text_of(&event.content) == "First";
        if !ungated && !self.started.swap(true, Ordering::SeqCst) {
            self.committing.notify_one();
            self.go.notified().await;
            if self.fails.load(Ordering::SeqCst) {
                return Err(StoreError::Value("the database is gone".to_owned()));
            }
            assert!(
                !self.panics.load(Ordering::SeqCst),
                "the store's driver broke"
            );
        }
        self.store.commit(event, counters, target).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.store.get_event(event_id).await
    }

    async fn update_event(
        &self,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<RoomEvent, StoreError> {
        self.store.update_event(event_id, updates).await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store
            .delete_event(room_id, event_id, cascade_replies)
            .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        filter: Option<EventFilter>,
    ) -> Result<u64, StoreError> {
        self.store.get_event_count(room_id, filter).await
    }

    async fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Result<(), StoreError> {
        self.store
            .persist_side_effects(room_id, tasks, observations)
            .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        query: &EventQuery,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.list_events(room_id, query).await
    }

    async fn list_tasks(&self, room_id: &RoomId) -> Result<Vec<Task>, StoreError> {
        self.store.list_tasks(room_id).await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.store.list_observations(room_id).await
    }
}

/// A kit over a gated store, with an SMS sender and a WebSocket reader
/// attached to `room_1`.
async fn gated_room() -> (RoomKit<GatedStore>, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions::with_store(GatedStore::default()));
    let ws = RecordingTransport::new(channel("ws"), ChannelType::WEBSOCKET);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    (kit, ws)
}

/// Drops `call` once the store's commit started, lets the commit go on,
/// and waits for the kit's background work to be over.
async fn drop_during_the_commit<T>(kit: &RoomKit<GatedStore>, call: impl Future<Output = T>) {
    let store = kit.store();
    tokio::select! {
        _ = call => panic!("the call ended before its commit"),
        () = store.committing.notified() => {}
    }
    store.go.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(kit))
        .await
        .unwrap();
}

/// The room's events but its lifecycle records (gap G-046), as `status
/// text`, and whether `events` reports each one's end.
async fn delivered_and_processed(
    kit: &RoomKit<GatedStore>,
    events: &mut broadcast::Receiver<FrameworkEvent>,
) -> (Vec<(EventStatus, String)>, bool) {
    let mut timeline = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap();
    timeline.retain(|event| event.source.channel_type != ChannelType::SYSTEM);
    let mut processed = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let FrameworkEvent::EventProcessed { event_id, .. } = event {
            processed.push(event_id);
        }
    }
    let all_processed = timeline.iter().all(|event| processed.contains(&event.id));
    let lines = timeline
        .iter()
        .map(|event| (event.status, roomkit::text_of(&event.content)))
        .collect();
    (lines, all_processed)
}

#[tokio::test]
async fn a_message_dropped_during_its_commit_is_delivered() {
    let (kit, ws) = gated_room().await;
    let mut events = kit.subscribe();
    let room = room_id();
    let message = InboundMessage::new(channel("sms"), "+15551234567", TextContent::new("Bonjour"));
    drop_during_the_commit(
        &kit,
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        ),
    )
    .await;
    assert_eq!(
        (delivered_and_processed(&kit, &mut events).await, ws.texts()),
        (
            (vec![(EventStatus::Delivered, "Bonjour".to_owned())], true),
            vec!["Bonjour".to_owned()]
        )
    );
}

#[tokio::test]
async fn an_injected_event_dropped_during_its_commit_is_delivered() {
    let (kit, ws) = gated_room().await;
    let mut events = kit.subscribe();
    let room = room_id();
    let event = SendEventOptions::default();
    drop_during_the_commit(
        &kit,
        kit.send_event(&room, &channel("sms"), TextContent::new("Bonjour"), event),
    )
    .await;
    assert_eq!(
        (delivered_and_processed(&kit, &mut events).await, ws.texts()),
        (
            (vec![(EventStatus::Delivered, "Bonjour".to_owned())], true),
            vec!["Bonjour".to_owned()]
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> EventQuery {
    EventQuery {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..EventQuery::default()
    }
}

/// `Slow` into `room_1`, its commit at the store's gate, on a task of its
/// own, and the gate opened `after` it.
async fn slow_commit(
    kit: &RoomKit<GatedStore>,
    after: Duration,
) -> tokio::task::JoinHandle<Result<roomkit::InboundResult, RoomKitError>> {
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message =
                InboundMessage::new(channel("sms"), "+15551234567", TextContent::new("Slow"));
            let options = ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            };
            kit.process_inbound(message, options).await
        })
    };
    tokio::time::timeout(BOUND, kit.store().committing.notified())
        .await
        .unwrap();
    let go = Arc::clone(&kit.store().go);
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        go.notify_one();
    });
    call
}

/// Whether the room holds `text`.
async fn stored(kit: &RoomKit<GatedStore>, text: &str) -> bool {
    kit.store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .iter()
        .any(|event| roomkit::text_of(&event.content) == text)
}

/// The time `kit`'s close takes.
async fn closing(kit: &RoomKit<GatedStore>) -> Duration {
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    started.elapsed()
}

/// RFC §13.6, plan step 3: a commit decided for a caller that left ends
/// within the close's bounds, 5 s for the writes, then 5 s of drain, and is
/// stored; the close waits for it, as Python's does
/// (`tasks/probes/step5-lot1/close_writes.out`: a write of 7 s, the close
/// 6.8 s, stored).
#[tokio::test(start_paused = true)]
async fn a_commit_ending_within_the_close_s_bounds_is_stored() {
    let (kit, _ws) = gated_room().await;
    let call = slow_commit(&kit, Duration::from_secs(7)).await;
    call.abort();
    let took = closing(&kit).await;
    assert_eq!(
        (
            took >= Duration::from_secs(7) && took < Duration::from_secs(8),
            stored(&kit, "Slow").await
        ),
        (true, true)
    );
}

/// Plan step 3, the write bounds' first row: a commit that ends within the
/// close's first wait is delivered, its event's place in the room's lane
/// taken before the lanes are sealed, as Python's decided write covers the
/// commit and the lane's enqueue (`inbound_locked.py` `_write_decision`).
/// On threads and real time, twenty calls each closed while its commit
/// runs: none is left undelivered (`lr2_probe_rs.out`: on e310aa7, four
/// of four were).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_ending_within_the_first_wait_is_delivered() {
    let mut undelivered = 0;
    for _ in 0..20 {
        let (kit, ws) = gated_room().await;
        let call = slow_commit(&kit, Duration::from_millis(20)).await;
        closing(&kit).await;
        let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
        let answered = matches!(ended, Ok(result) if result.cancellation_reason.is_none());
        undelivered += usize::from(!(answered && ws.texts() == ["Slow"]));
    }
    assert_eq!(undelivered, 0);
}

/// Plan step 3: a commit longer than the close's bounds does not hold the
/// close past them, and is never cancelled: it lands after. Its run is the
/// drain's work until it is committed (RMK-1010 item 41, broadened), so the
/// channels close at 10 s and the third wait ends at 15 s, the close's cap
/// for an inbound commit (Python's drain does not wait for the run, and
/// its close returns at 10.0 s, `close_writes.out`; it aligns).
#[tokio::test(start_paused = true)]
async fn a_commit_past_the_close_s_bounds_does_not_hold_it() {
    let (kit, _ws) = gated_room().await;
    let call = slow_commit(&kit, Duration::from_secs(20)).await;
    call.abort();
    let took = closing(&kit).await;
    let at_close = stored(&kit, "Slow").await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert_eq!(
        (
            took >= Duration::from_secs(15) && took < Duration::from_secs(16),
            at_close,
            stored(&kit, "Slow").await
        ),
        (true, false, true)
    );
}

/// A caller still waiting for a call whose commit outlives the drain reads
/// what the write came to once it ends: its event, stored and never
/// delivered, `kit_closed`, as Python's caller does (`close_writes.out`: a
/// write of 12 s, the caller answered at 12.0 s, `kit_closed`); its record
/// stored `BLOCKED`, with the hook's reason, as the call decided reads it
/// (`lr2_probe_blocked_rs.out`: the hook's name before RMK-1010's third
/// pass). The run is the drain's until it is committed (RMK-1010 item 41,
/// broadened): it is cut at 10 s, and the close's third wait sees the
/// write end, at 12 s, where Python's close returns at 10.0 s.
#[tokio::test(start_paused = true)]
async fn a_caller_waiting_on_a_commit_past_the_close_reads_its_event() {
    let mut read = Vec::new();
    for blocks in [false, true] {
        let (kit, ws) = gated_room().await;
        if blocks {
            kit.before_broadcast("moderator", |_, _| async {
                Ok(HookResult::block("spam detected", BlockOptions::default()))
            })
            .unwrap();
        }
        let call = slow_commit(&kit, Duration::from_secs(12)).await;
        let took = closing(&kit).await;
        let ended = tokio::time::timeout(Duration::from_secs(30), call)
            .await
            .unwrap()
            .unwrap();
        read.push((
            took >= Duration::from_secs(12) && took < Duration::from_secs(13),
            ended.map(|result| {
                (
                    result.event.map(|event| roomkit::text_of(&event.content)),
                    result.reason,
                    result.cancellation_reason,
                )
            }),
            ws.texts().is_empty(),
        ));
    }
    let slow = Some("Slow".to_owned());
    assert_eq!(
        read,
        [
            (
                true,
                Ok((slow.clone(), None, Some("kit_closed".to_owned()))),
                true
            ),
            (
                true,
                Ok((slow, Some("spam detected".to_owned()), None)),
                true
            )
        ]
    );
}

/// A waiting caller whose commit fails reads the store's error, the write
/// ending within the close's first wait or past the close, as Python's
/// caller raises it (`close_failed_write_py.out`: failing writes of 3 s
/// and 12 s, the caller raising at 3.0 s and 12.0 s).
#[tokio::test(start_paused = true)]
async fn a_waiting_caller_whose_commit_fails_reads_the_store_s_error() {
    let mut read = Vec::new();
    for seconds in [3, 12] {
        let (kit, _ws) = gated_room().await;
        kit.store().fails.store(true, Ordering::SeqCst);
        let call = slow_commit(&kit, Duration::from_secs(seconds)).await;
        closing(&kit).await;
        let ended = tokio::time::timeout(Duration::from_secs(30), call)
            .await
            .unwrap()
            .unwrap();
        read.push(ended.err());
    }
    let refused = || Some(RoomKitError::Value("the database is gone".to_owned()));
    assert_eq!(read, vec![refused(), refused()]);
}

/// RMK-1010 item 4: the close names the writes it leaves running at each
/// of its waits, at WARN, and a write that fails once its caller is gone
/// is logged at ERROR, whether its run still runs or the close aborted it,
/// as Python's `wait_for_held` and `run_to_its_end` log them
/// (`close_failed_write_py.out`: a failing write of 12 s, the caller gone,
/// a warning at each of Python's three waits and the error; Rust's drain
/// waits for the run until it is cut at 10 s, RMK-1010 item 41 broadened,
/// so its third wait sees the write fail, two warnings;
/// `fixpass3/gone_failed_write_py.out`: the error, the kit not closing; a
/// caller waiting on a failing write of 3 s: nothing logged).
#[tokio::test(start_paused = true)]
async fn the_writes_left_running_and_failing_are_logged() {
    let mut logged = Vec::new();
    for (seconds, waiting, closes) in [(12, false, true), (1, false, false), (3, true, true)] {
        let (kit, _ws) = gated_room().await;
        kit.store().fails.store(true, Ordering::SeqCst);
        let (lines, guard) = Warnings::capture();
        let call = slow_commit(&kit, Duration::from_secs(seconds)).await;
        if !waiting {
            call.abort();
        }
        if closes {
            closing(&kit).await;
        }
        tokio::time::sleep(Duration::from_secs(seconds + 1)).await;
        drop(guard);
        let at = |level: &str, text: &str| {
            let lines = lines.containing(text);
            lines.iter().filter(|line| line.starts_with(level)).count()
        };
        logged.push((
            at("WARN", "closing with decided writes still running"),
            at("ERROR", "the writes a gone caller left failed"),
        ));
    }
    assert_eq!(logged, [(2, 1), (0, 1), (0, 0)]);
}

/// A record from the WebSocket channel, for `commit_event`.
fn record(text: &str) -> RoomEvent {
    let source = EventSource::new(channel("ws"), ChannelType::WEBSOCKET);
    RoomEvent {
        id: "record_1".parse().unwrap(),
        created_at: Timestamp::UNIX_EPOCH,
        ..RoomEvent::new(room_id(), source, TextContent::new(text))
    }
}

/// Python's third wait for the writes, 5 s from the channels' close
/// (`framework.py` `_end_what_the_channels_started` at 6e420487): a
/// `commit_event` whose write takes 7 s, which no draining run waits on,
/// is stored when the close returns, the close taking about 7 s
/// (`step5-lot1-fixpass3-review/py_close_probe_rows.out`: 6.8 s, stored;
/// on e8c9984, 5.0 s and not stored).
#[tokio::test(start_paused = true)]
async fn a_commit_event_ending_within_the_close_s_third_wait_is_stored() {
    let (kit, _ws) = gated_room().await;
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let options = CommitEventOptions::default();
            kit.commit_event(&room_id(), record("Slow"), options).await
        })
    };
    tokio::time::timeout(BOUND, kit.store().committing.notified())
        .await
        .unwrap();
    let go = Arc::clone(&kit.store().go);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(7)).await;
        go.notify_one();
    });
    let took = closing(&kit).await;
    let at_close = stored(&kit, "Slow").await;
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert_eq!(
        (
            took >= Duration::from_secs(7) && took < Duration::from_secs(8),
            at_close,
            ended.is_ok()
        ),
        (true, true, true)
    );
}

/// A transport whose delivery of `First` takes `delivery`, which keeps its
/// room's lane busy, and whose close takes `close`; it keeps the texts it
/// delivered.
struct Busy {
    delivery: Duration,
    close: Duration,
    delivered: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Busy {
    /// Bound to `room_1` on `kit`; the texts it will have delivered.
    async fn bound(
        kit: &RoomKit<GatedStore>,
        delivery: Duration,
        close: Duration,
    ) -> Arc<std::sync::Mutex<Vec<String>>> {
        let delivered = Arc::default();
        let busy = Self {
            delivery,
            close,
            delivered: Arc::clone(&delivered),
        };
        kit.register_channel(busy).unwrap();
        let options = AttachChannelOptions::default();
        kit.attach_channel(&room_id(), &channel("busy"), options)
            .await
            .unwrap();
        delivered
    }
}

impl Channel for Busy {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("busy"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let text = roomkit::text_of(&event.content);
        if text == "First" {
            tokio::time::sleep(self.delivery).await;
        }
        self.delivered.lock().unwrap().push(text);
        Ok(ChannelOutput::default())
    }

    async fn close(&self) {
        tokio::time::sleep(self.close).await;
    }
}

/// The third wait's 5 s count from the end of the channels' close, as
/// Python's: a delivery keeps the lane busy 8 s into the close and the
/// channel's close takes 3 s, so the channels are closed at 11 s, and a
/// `commit_event` whose write takes 14 s is stored when the close returns,
/// at 14 s (`fixpass5/py_slow_channel_close.out`: 13.8 s, stored; counted
/// from the close's start, 10 s, or from before the channels' close, 13 s,
/// the record not stored).
#[tokio::test(start_paused = true)]
async fn the_third_wait_counts_from_the_channels_close() {
    let (kit, _ws) = gated_room().await;
    Busy::bound(&kit, Duration::from_secs(8), Duration::from_secs(3)).await;
    let first = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message =
                InboundMessage::new(channel("sms"), "+15551234567", TextContent::new("First"));
            let options = ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            };
            kit.process_inbound(message, options).await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let options = CommitEventOptions::default();
            kit.commit_event(&room_id(), record("Slow"), options).await
        })
    };
    tokio::time::timeout(BOUND, kit.store().committing.notified())
        .await
        .unwrap();
    let go = Arc::clone(&kit.store().go);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(14)).await;
        go.notify_one();
    });
    let took = closing(&kit).await;
    let at_close = stored(&kit, "Slow").await;
    let first = tokio::time::timeout(BOUND, first).await.unwrap().unwrap();
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert_eq!(
        (
            took >= Duration::from_secs(14) && took < Duration::from_secs(15),
            at_close,
            first.map(|result| result.cancellation_reason),
            ended.is_ok()
        ),
        (true, true, Ok(None), true)
    );
}

/// `text` from the SMS sender into `room`.
fn sms_into(room: RoomId, text: &str) -> (InboundMessage, ProcessInboundOptions) {
    let message = InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room),
        ..Default::default()
    };
    (message, options)
}

/// `Slow` into `room_1` on a task of its own, its commit gated and the gate
/// opened `after` it starts, whenever that is: the close may begin first.
fn slow_into_room_1(
    kit: &RoomKit<GatedStore>,
    after: Duration,
) -> tokio::task::JoinHandle<Result<roomkit::InboundResult, RoomKitError>> {
    let (committing, go) = (
        Arc::clone(&kit.store().committing),
        Arc::clone(&kit.store().go),
    );
    tokio::spawn(async move {
        committing.notified().await;
        tokio::time::sleep(after).await;
        go.notify_one();
    });
    let kit = kit.clone();
    tokio::spawn(async move {
        let (message, options) = sms_into(room_id(), "Slow");
        kit.process_inbound(message, options).await
    })
}

/// A run that commits still runs the delivery set of an entry it adopted:
/// the drain waits for the run, that set getting its grace, as Python's
/// lane drains it.
/// Each row: what `Slow`'s hook does, how long `First`'s delivery and
/// `Slow`'s commit take; the close begins while the hook runs, or once the
/// commit runs. The close's length, whether `First` was delivered when it
/// returned, and `Slow`'s `cancellation_reason` (light review 4, rows c3,
/// c2 and c: on f7ef360 the set was cut at the drain, at 3.1 s, 2.9 s and
/// 5 s; Python `py_lr4_c3.out`, `py_lr4_c2.out`, `py_lr4_rows.out`: 3.8 s,
/// 3.8 s and 6.8 s, delivered, `kit_closed`). Row c12, the commit of 12 s:
/// the run is the drain's until it is cut, at 10 s (RMK-1010 item 41,
/// broadened), and the third wait sees the write end, the close taking
/// 11.8 s; Python's drain does not wait for the run, 10.8 s
/// (`fixpass5/py_lr4_c12.out`).
#[tokio::test(start_paused = true)]
async fn an_adopted_set_keeps_the_drain_s_grace_while_its_run_commits() {
    let mut read = Vec::new();
    for (row, locked, delivery, hook, commit) in [
        ("c3", true, 4_000, 3_000, 100),
        ("c2", false, 4_000, 3_000, 100),
        ("c", false, 6_000, 0, 7_000),
        ("c12", false, 6_000, 0, 12_000),
    ] {
        let (kit, _ws) = gated_room().await;
        let delivered = Busy::bound(&kit, Duration::from_millis(delivery), Duration::ZERO).await;
        side_entry_hook(&kit, locked, Duration::from_millis(hook));
        let call = slow_into_room_1(&kit, Duration::from_millis(commit));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let took = closing(&kit).await;
        let first = delivered.lock().unwrap().contains(&"First".to_owned());
        let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
        read.push((
            row,
            took.as_millis() / 100,
            first,
            ended.ok().map(|result| result.cancellation_reason),
        ));
    }
    let kit_closed = Some(Some("kit_closed".to_owned()));
    assert_eq!(
        read,
        [
            ("c3", 38, true, kit_closed.clone()),
            ("c2", 38, true, kit_closed.clone()),
            ("c", 68, true, kit_closed.clone()),
            ("c12", 118, true, kit_closed),
        ]
    );
}

/// An agent whose turn on `First` answers `Answer` after `turn`.
struct Agent {
    turn: Duration,
}

impl Channel for Agent {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("agent"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if roomkit::text_of(&event.content) != "First" {
            return Ok(ChannelOutput::default());
        }
        tokio::time::sleep(self.turn).await;
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(channel("agent"), ChannelType::AI)
        };
        let answer = RoomEvent {
            id: event.id.clone(),
            created_at: event.created_at,
            ..RoomEvent::new(event.room_id.clone(), source, TextContent::new("Answer"))
        };
        Ok(ChannelOutput {
            responded: true,
            response_events: vec![answer],
            ..ChannelOutput::empty()
        })
    }
}

/// RMK-1010 item 41, broadened: an inbound run is the drain's work until
/// it is committed, whatever holds it. `Slow`'s hook makes `First`, under
/// the lock or off it; an agent's turn on `First` answers after 3 s, and
/// the answer waits for the room's lock behind `Slow`'s section, which
/// commits for 9 s: the answer is stored when the close returns, at 9 s,
/// as Python stores it (light review 5, row r, `py_lr5_r.out`: 8.9 s,
/// `Answer` stored; on f9f285c the drain cut it). With a commit of 12 s,
/// past the drain's grace, neither stores it (Python 11.9 s).
#[tokio::test(start_paused = true)]
async fn an_answer_behind_a_committing_run_is_stored() {
    let mut read = Vec::new();
    for (locked, commit) in [(true, 9_000), (false, 9_000), (true, 12_000)] {
        let (kit, _ws) = gated_room().await;
        let agent = Agent {
            turn: Duration::from_secs(3),
        };
        kit.register_channel(agent).unwrap();
        let options = AttachChannelOptions::default();
        kit.attach_channel(&room_id(), &channel("agent"), options)
            .await
            .unwrap();
        side_entry_hook(&kit, locked, Duration::ZERO);
        let call = slow_into_room_1(&kit, Duration::from_millis(commit));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let took = closing(&kit).await;
        let answered = stored(&kit, "Answer").await;
        let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
        read.push((
            took.as_millis() / 100,
            answered,
            ended.ok().map(|result| result.cancellation_reason),
        ));
    }
    let kit_closed = Some(Some("kit_closed".to_owned()));
    assert_eq!(
        read,
        [
            (89, true, kit_closed.clone()),
            (89, true, kit_closed.clone()),
            (119, false, kit_closed),
        ]
    );
}

/// A `BEFORE_BROADCAST` hook of `Slow` that makes an entry `First` into
/// its own room, a `send_event` under the lock when `locked`, else an
/// off-lock deferred `process_inbound`, then runs `more`.
fn side_entry_hook(kit: &RoomKit<GatedStore>, locked: bool, more: Duration) {
    let mut spec = HookSpec::new("side", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = locked;
    let hook_kit = kit.clone();
    kit.hook(spec, move |event: RoomEvent, _| {
        let kit = hook_kit.clone();
        async move {
            if roomkit::text_of(&event.content) != "Slow" {
                return Ok(HookResult::allow(AllowOptions::default()));
            }
            if locked {
                let options = SendEventOptions::default();
                kit.send_event(
                    &room_id(),
                    &channel("sms"),
                    TextContent::new("First"),
                    options,
                )
                .await?;
            } else {
                let (message, mut options) = sms_into(room_id(), "First");
                options.defer_delivery = true;
                kit.process_inbound(message, options).await?;
            }
            tokio::time::sleep(more).await;
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
}

/// A run in its caller's own task, which the caller keeps but stops
/// polling once its write started (a `select!` whose other branch awaits
/// the close): the close waits for the write, ended at 1 s, not for the run
/// to take its outcome (light review 3, probe p7: the close waited its
/// first 5 s on e8c9984).
#[tokio::test(start_paused = true)]
async fn a_run_its_caller_stops_polling_holds_the_close_for_its_write_only() {
    let (kit, _ws) = gated_room().await;
    let room = room_id();
    let mut commit = pin!(kit.commit_event(&room, record("Slow"), CommitEventOptions::default()));
    tokio::select! {
        _ = commit.as_mut() => panic!("the commit ended before its write"),
        () = kit.store().committing.notified() => {}
    }
    let go = Arc::clone(&kit.store().go);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        go.notify_one();
    });
    let took = closing(&kit).await;
    let at_close = stored(&kit, "Slow").await;
    let ended = commit.await.map(|event| roomkit::text_of(&event.content));
    assert_eq!(
        (took < Duration::from_secs(2), at_close, ended.ok()),
        (true, true, Some("Slow".to_owned()))
    );
}

/// A commit whose write panics is answered as any call that panicked: the
/// caller reads the store's failure, a panic, as it did before the writes ran
/// as tasks of their own (647277b).
#[tokio::test(start_paused = true)]
async fn a_commit_whose_write_panics_reads_as_a_panic() {
    let (kit, _ws) = gated_room().await;
    kit.store().panics.store(true, Ordering::SeqCst);
    let call = slow_commit(&kit, Duration::ZERO).await;
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    let read = match ended {
        Err(RoomKitError::Store(failure)) => Some(failure.to_string()),
        _ => None,
    };
    assert_eq!(read.as_deref(), Some("the call panicked"));
}

/// `Bonjour` into `room_1` once `kit`'s `BEFORE_BROADCAST` hook is one that
/// signals `hanging` and never answers, then the kit's close: whether the
/// caller reads `KitClosed`, and whether the room holds `Bonjour`.
async fn closed_while_its_hook_hangs(kit: &RoomKit<GatedStore>, hanging: &Notify) -> (bool, bool) {
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message =
                InboundMessage::new(channel("sms"), "+15551234567", TextContent::new("Bonjour"));
            let options = ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            };
            kit.process_inbound(message, options).await
        })
    };
    tokio::time::timeout(BOUND, hanging.notified())
        .await
        .unwrap();
    closing(kit).await;
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    (
        matches!(ended, Err(RoomKitError::KitClosed)),
        stored(kit, "Bonjour").await,
    )
}

/// RMK-1010 item 41: a call the close cuts before its own commit reads
/// `KitClosed`, nothing of it stored, though its run committed another
/// event first: here an entry its hook injected into the room, adopted and
/// committed ahead of the call's event. The caller never reads that event
/// as its own.
#[tokio::test(start_paused = true)]
async fn a_call_cut_before_its_commit_does_not_read_an_adopted_entry_s() {
    let (kit, _ws) = gated_room().await;
    kit.store().started.store(true, Ordering::SeqCst);
    let hanging = Arc::new(Notify::new());
    let hook_kit = kit.clone();
    let signal = Arc::clone(&hanging);
    kit.before_broadcast("inject_then_hang", move |event, _| {
        let kit = hook_kit.clone();
        let hanging = Arc::clone(&signal);
        async move {
            if event.source.channel_id == channel("sms") {
                let injected = TextContent::new("Heads up");
                let options = SendEventOptions::default();
                kit.send_event(&room_id(), &channel("ws"), injected, options)
                    .await
                    .unwrap();
                hanging.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let ended = closed_while_its_hook_hangs(&kit, &hanging).await;
    assert_eq!(
        (ended, stored(&kit, "Heads up").await),
        ((true, false), true)
    );
}

/// Item 41 again: the run's step 2 attached the message's channel, which
/// commits the binding's lifecycle record (gap G-046), before the hook
/// that never answers; the caller does not read that record as its event.
#[tokio::test(start_paused = true)]
async fn a_call_cut_before_its_commit_does_not_read_its_channel_s_record() {
    let kit = RoomKit::new(RoomKitOptions::with_store(GatedStore::default()));
    kit.store().started.store(true, Ordering::SeqCst);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    let hanging = Arc::new(Notify::new());
    let signal = Arc::clone(&hanging);
    kit.before_broadcast("hang", move |_, _| {
        let hanging = Arc::clone(&signal);
        async move {
            hanging.notify_one();
            std::future::pending::<()>().await;
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let ended = closed_while_its_hook_hangs(&kit, &hanging).await;
    let attached = kit.store().list_bindings(&room_id()).await.unwrap().len();
    assert_eq!((ended, attached), ((true, false), 1));
}
