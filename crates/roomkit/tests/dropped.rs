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
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventSource, EventStatus, EventUpdates,
    HookResult, Identity, IdentityId, InboundMessage, Observation, Participant, ParticipantId,
    Room, RoomContext, RoomEvent, RoomId, RoomUpdates, Task, TextContent, Timestamp,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, CommitEventOptions, ConversationStore, CreateRoomOptions, EventFilter,
    EventQuery, FrameworkEvent, InMemoryStore, ProcessInboundOptions, RoomCounters, RoomFilter,
    RoomKit, RoomKitError, RoomKitOptions, SendEventOptions, StoreError, TargetUpdate,
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

/// A waiting caller whose commit fails within the close's grace reads the
/// store's error, as Python's caller raises it (`close_failed_write_py.out`:
/// a failing write of 3 s, the caller raising at 3.0 s); one whose write
/// outlasts the grace is cut, and reads as a message cut before its
/// commit, nothing of it stored (RFC §12.10.4; on fefbfac it read the
/// store's error at 12 s).
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
        read.push(ended.map(|result| (result.blocked, result.reason)));
    }
    assert_eq!(
        read,
        vec![
            Err(RoomKitError::Value("the database is gone".to_owned())),
            Ok((true, Some("kit_closed".to_owned())))
        ]
    );
}

/// RMK-1010 item 4, RFC §12.10.4: a write that fails once its caller is
/// gone is logged at ERROR, the kit closing or not, as Python's
/// `run_to_its_end` logs it (`fixpass3/gone_failed_write_py.out`); one the
/// close cuts once its grace ran out ends with no failure to log; a caller
/// waiting on a failing write reads it, nothing logged. On fefbfac the
/// close named the writes it left running at its waits, at WARN.
#[tokio::test(start_paused = true)]
async fn the_writes_left_running_and_failing_are_logged() {
    let mut logged = Vec::new();
    let rows = [
        (12, false, true),
        (1, false, false),
        (3, false, true),
        (3, true, true),
    ];
    for (seconds, waiting, closes) in rows {
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
            at("WARN", "closing with"),
            at("ERROR", "the writes a gone caller left failed"),
        ));
    }
    assert_eq!(logged, [(0, 0), (0, 1), (0, 1), (0, 0)]);
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

/// A run in its caller's own task, which the caller keeps but stops
/// polling once its write started (a `select!` whose other branch awaits
/// the close): the close gives it its grace, a call it tracks, then its
/// unwinds, which it cannot take unpolled, 8 s in all, and names it in its
/// warning; its write, ended at 1 s, is stored, and the call, polled
/// again, reads its commit back (RFC §12.10.4). On fefbfac the close
/// waited for the write alone, 1 s.
#[tokio::test(start_paused = true)]
async fn a_run_its_caller_stops_polling_holds_the_close_its_grace() {
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
    let (lines, guard) = Warnings::capture();
    let took = closing(&kit).await;
    drop(guard);
    let at_close = stored(&kit, "Slow").await;
    let ended = commit.await.map(|event| roomkit::text_of(&event.content));
    assert_eq!(
        (
            took >= Duration::from_secs(5) && took <= Duration::from_secs(8),
            lines.containing("a call").len(),
            at_close,
            ended.ok()
        ),
        (true, 1, true, Some("Slow".to_owned()))
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
/// caller reads its call cut before its commit, blocked, `kit_closed`, with
/// no event, and whether the room holds `Bonjour`.
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
    let cut = ended.is_ok_and(|result| {
        result.blocked && result.reason.as_deref() == Some("kit_closed") && result.event.is_none()
    });
    (cut, stored(kit, "Bonjour").await)
}

/// RMK-1010 item 41, RFC §12.10.4: a call the close cuts before its own
/// commit reads as one cut so, nothing of it stored, though its run
/// committed another event first: here an entry its hook injected into the
/// room, adopted and committed ahead of the call's event. The caller never
/// reads that event as its own.
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
