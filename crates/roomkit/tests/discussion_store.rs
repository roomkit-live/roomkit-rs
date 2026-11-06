//! A discussion whose store refuses or holds what it asks (RFC §19.7.5): its
//! driver backs off, as Python's `_drive`, rather than give and end a turn at
//! once, and a stop waits for the plan under way. A queue the store holds
//! outlives the process that wrote it (rule 16).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};

use roomkit::model::*;
use roomkit::orchestration::DiscussionOptions;
use roomkit::store;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, Discussion, HookOptions,
    HookTrigger, InMemoryStore, InstallStrategyOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, RoomStrategyIsCurrentOptions, RoomStrategyOptions, SpeakQueueChange,
    SpeakQueueEvent, StoreError, UninstallStrategyOptions,
};

/// Refuses the write of a queue that gave at least `refuse_from` turns, and
/// holds one such write, or one read of an event, until it is told.
#[derive(Default)]
struct FlakyStore {
    store: InMemoryStore,
    refuse_from: Arc<AtomicU64>,
    refused: Arc<Notify>,
    held: Arc<AtomicBool>,
    reading: Arc<Notify>,
    read_on: Arc<Notify>,
    reads: Arc<AtomicU32>,
    write_held: Arc<AtomicBool>,
    writing: Arc<Notify>,
    write_on: Arc<Notify>,
    /// Holds the next write of a discussion's configuration until told.
    config_held: Arc<AtomicBool>,
    config_writing: Arc<Notify>,
    config_on: Arc<Notify>,
}

macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $answer:ty;)*) => {
        $(
            async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
                self.store.$name($($argument),*).await
            }
        )*
    };
}

impl ConversationStore for FlakyStore {
    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.held.swap(false, Ordering::SeqCst) {
            self.reading.notify_one();
            self.read_on.notified().await;
        }
        self.store.get_event(event_id).await
    }

    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: store::PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        let given = patch
            .get("_speak_queue")
            .and_then(|queue| queue.get("turns_given"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if given > 0 && self.write_held.swap(false, Ordering::SeqCst) {
            self.writing.notify_one();
            self.write_on.notified().await;
        }
        let configures = patch
            .get("_discussion")
            .is_some_and(|config| !config.is_null());
        if configures && self.config_held.swap(false, Ordering::SeqCst) {
            self.config_writing.notify_one();
            self.config_on.notified().await;
        }
        let refuse_from = self.refuse_from.load(Ordering::SeqCst);
        if refuse_from > 0 && given >= refuse_from {
            self.refused.notify_one();
            return Err(StoreError::Value("the store refuses the write".to_owned()));
        }
        self.store
            .patch_room_metadata(room_id, patch, options)
            .await
    }

    forward! {
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
        get_room(room_id: &RoomId) -> Option<Room>;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        advance_delivered_index(
            room_id: &RoomId,
            index: u64,
            options: store::AdvanceDeliveredIndexOptions
        ) -> bool;
        add_binding(binding: ChannelBinding) -> ChannelBinding;
        get_binding(room_id: &RoomId, channel_id: &ChannelId) -> Option<ChannelBinding>;
        update_binding(binding: ChannelBinding) -> ChannelBinding;
        remove_binding(room_id: &RoomId, channel_id: &ChannelId) -> bool;
        list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
        add_participant(participant: Participant) -> Participant;
        get_participant(room_id: &RoomId, participant_id: &ParticipantId) -> Option<Participant>;
        update_participant(participant: Participant) -> Participant;
        delete_participant(room_id: &RoomId, participant_id: &ParticipantId) -> ();
        create_identity(identity: Identity) -> Identity;
        get_identity(identity_id: &IdentityId) -> Option<Identity>;
        resolve_identity(
            channel_type: &ChannelType,
            address: &str,
            options: store::ResolveIdentityOptions
        ) -> Option<Identity>;
        link_address(
            identity_id: &IdentityId,
            channel_type: &ChannelType,
            address: &str,
            options: store::LinkAddressOptions
        ) -> ();
        check_idempotency(room_id: &RoomId, key: &str, channel_id: Option<&ChannelId>) -> bool;
        commit_event(
            room_id: &RoomId,
            event: RoomEvent,
            options: store::CommitEventOptions
        ) -> RoomEvent;
        update_event(event: RoomEvent) -> RoomEvent;
        delete_event(
            room_id: &RoomId,
            event_id: &EventId,
            options: store::DeleteEventOptions
        ) -> Vec<RoomEvent>;
        get_event_count(room_id: &RoomId, options: store::GetEventCountOptions) -> u64;
        list_events(room_id: &RoomId, options: store::ListEventsOptions) -> Vec<RoomEvent>;
        add_task(task: Task) -> Task;
        add_observation(observation: Observation) -> Observation;
        list_tasks(room_id: &RoomId, options: store::ListTasksOptions) -> Vec<Task>;
        list_observations(room_id: &RoomId) -> Vec<Observation>;
        get_task(task_id: &TaskId) -> Option<Task>;
        update_task(task: Task) -> Task;
        add_event(event: RoomEvent) -> RoomEvent;
        mark_read(room_id: &RoomId, channel_id: &ChannelId, event_id: &EventId) -> ();
        mark_all_read(room_id: &RoomId, channel_id: &ChannelId) -> ();
        get_unread_count(room_id: &RoomId, channel_id: &ChannelId) -> u64;
        list_read_markers(room_id: &RoomId) -> std::collections::BTreeMap<ChannelId, u64>;
        find_latest_room(
            participant_id: &ParticipantId,
            options: store::FindLatestRoomOptions
        ) -> Option<Room>;
        find_room_id_by_channel(
            channel_id: &ChannelId,
            options: store::FindRoomIdByChannelOptions
        ) -> Option<RoomId>;
        get_thread_summaries(
            room_id: &RoomId,
            root_event_ids: &[EventId]
        ) -> std::collections::BTreeMap<EventId, ThreadSummary>;
    }
}

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

/// A room of the discussion of `a` the store's handles reach, `ops` bound.
async fn installed(store: FlakyStore, options: DiscussionOptions) -> RoomKit<FlakyStore> {
    let room = RoomId::new("r1").unwrap();
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    kit.register_channel(RecordingTransport::new(
        id("ops"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        id("a"),
        "a answers",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let created = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.create_room(created).await.unwrap();
    kit.attach_channel(&room, &id("ops"), AttachChannelOptions::default())
        .await
        .unwrap();
    let discussion = Discussion::new(vec![id("a").into()], options).unwrap();
    kit.install_strategy(
        &room,
        Arc::new(discussion),
        InstallStrategyOptions::default(),
    )
    .await
    .unwrap();
    kit
}

fn say(text: &str) -> InboundMessage {
    InboundMessage::new(id("ops"), "oncall", TextContent::new(text))
}

fn hello() -> InboundMessage {
    say("@a hello")
}

/// The turns `kit`'s hooks are told of, given and ended, in order.
fn turns_told(kit: &RoomKit<FlakyStore>) -> watch::Receiver<Vec<SpeakQueueChange>> {
    let (telling, told) = watch::channel(Vec::new());
    kit.hook(
        HookTrigger::OnSpeakQueue,
        move |event: SpeakQueueEvent, _context: RoomContext| {
            if matches!(
                event.change,
                SpeakQueueChange::TurnGiven | SpeakQueueChange::TurnEnded
            ) {
                telling.send_modify(|told| told.push(event.change));
            }
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    told
}

// Python: orchestration/strategies/discussion/_driver.py `_drive`, its failures
// back off; tests/orchestration/test_discussion_edges.py::test_a_failure_reading_the_queue_is_retried
#[tokio::test]
async fn a_queue_that_cannot_be_written_backs_off_and_tells_no_turn() {
    let store = FlakyStore::default();
    let refuse_from = Arc::clone(&store.refuse_from);
    let asked = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&asked);
    let options = DiscussionOptions {
        done: Some(Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { false })
        })),
        ..DiscussionOptions::default()
    };
    let kit = installed(store, options).await;
    let told = turns_told(&kit);

    refuse_from.store(1, Ordering::SeqCst);
    drop(
        kit.process_inbound(hello(), ProcessInboundOptions::default())
            .await,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        asked.load(Ordering::SeqCst) >= 1,
        "the plan was never tried"
    );
    assert!(
        asked.load(Ordering::SeqCst) <= 3,
        "done was asked {} times in 200 ms",
        asked.load(Ordering::SeqCst)
    );
    assert!(told.borrow().is_empty(), "no turn was given");
    refuse_from.store(0, Ordering::SeqCst);
    kit.close().await.unwrap();
}

// Python: _driver.py `_planned_turn` keeps `_seen_version` once the queue's edit
// is stored: a refused plan is planned again, by the wake of a message that lands
// on the number the refused write would have had, without waiting for a poll.
#[tokio::test]
async fn a_message_after_a_refused_plan_is_planned_at_its_wake() {
    let store = FlakyStore::default();
    let (refuse_from, refused) = (Arc::clone(&store.refuse_from), Arc::clone(&store.refused));
    let kit = installed(store, DiscussionOptions::default()).await;
    let mut told = turns_told(&kit);
    drop(
        kit.process_inbound(say("@a one"), ProcessInboundOptions::default())
            .await,
    );
    idle(&kit).await;
    assert_eq!(
        *told.borrow(),
        [SpeakQueueChange::TurnGiven, SpeakQueueChange::TurnEnded]
    );

    refuse_from.store(2, Ordering::SeqCst);
    drop(
        kit.process_inbound(say("@a two"), ProcessInboundOptions::default())
            .await,
    );
    tokio::time::timeout(Duration::from_secs(5), refused.notified())
        .await
        .expect("the plan tries its write");
    refuse_from.store(0, Ordering::SeqCst);
    drop(
        kit.process_inbound(say("@a three"), ProcessInboundOptions::default())
            .await,
    );

    // Under the back-off's first second and the poll's.
    let given_again = told.wait_for(|told| told.len() > 2);
    tokio::time::timeout(Duration::from_millis(500), given_again)
        .await
        .expect("the message's wake plans the turn")
        .unwrap();
    kit.close().await.unwrap();
}

// Python: _driver.py `_planned_turn`, a stop while the plan reads gives no turn.
#[tokio::test]
async fn an_uninstall_during_a_plan_s_read_ends_and_gives_no_turn() {
    let store = FlakyStore::default();
    let (held, reading, read_on, reads) = (
        Arc::clone(&store.held),
        Arc::clone(&store.reading),
        Arc::clone(&store.read_on),
        Arc::clone(&store.reads),
    );
    let kit = installed(store, DiscussionOptions::default()).await;
    let room = RoomId::new("r1").unwrap();
    let told = turns_told(&kit);
    // The first read of the turn's message is the plan's.
    held.store(true, Ordering::SeqCst);
    let saying = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(hello(), ProcessInboundOptions::default())
                .await
        })
    };
    reading.notified().await;

    let uninstalling = {
        let (kit, room) = (kit.clone(), room.clone());
        tokio::spawn(async move {
            kit.uninstall_strategy(&room, UninstallStrategyOptions::default())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    read_on.notify_one();
    tokio::time::timeout(Duration::from_secs(5), uninstalling)
        .await
        .expect("the uninstall ends, the plan's loop with it")
        .unwrap()
        .unwrap();
    drop(saying.await);
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(told.borrow().is_empty(), "no turn was given");
    assert!(reads.load(Ordering::SeqCst) < 10, "the plan read on");
    kit.close().await.unwrap();
}

// Python: _driver.py `_planned_turn` plans under the room's lock, which a stop
// waits for: the turn's given told first, then its end.
#[tokio::test]
async fn an_uninstall_during_a_plan_s_write_tells_the_turn_given_then_its_end() {
    let store = FlakyStore::default();
    let (write_held, writing, write_on) = (
        Arc::clone(&store.write_held),
        Arc::clone(&store.writing),
        Arc::clone(&store.write_on),
    );
    let kit = installed(store, DiscussionOptions::default()).await;
    let room = RoomId::new("r1").unwrap();
    let told = turns_told(&kit);
    write_held.store(true, Ordering::SeqCst);
    let saying = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(hello(), ProcessInboundOptions::default())
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), writing.notified())
        .await
        .expect("the plan writes its queue");

    let uninstalling = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.uninstall_strategy(&room, UninstallStrategyOptions::default())
                .await
        })
    };
    // The stop past its driver, the plan's write still held: only then is this
    // an uninstall during the write.
    tokio::time::sleep(Duration::from_millis(10)).await;
    write_on.notify_one();
    tokio::time::timeout(Duration::from_secs(5), uninstalling)
        .await
        .expect("the uninstall ends once the plan stored its queue")
        .unwrap()
        .unwrap();
    drop(saying.await);
    idle(&kit).await;

    assert_eq!(
        *told.borrow(),
        [SpeakQueueChange::TurnGiven, SpeakQueueChange::TurnEnded]
    );
    kit.close().await.unwrap();
}

// Python: _driver.py `cut`, which reads the turn the driver clears as its
// task is cancelled, and `_abandon`'s wait on the turn's cleanup: a
// listen_only racing an uninstall cuts a turn the stop already let go of, and
// returns once it is.
#[tokio::test]
async fn a_listen_only_racing_an_uninstall_returns_once_the_turn_is_cut() {
    let store = FlakyStore::default();
    let (write_held, writing, write_on) = (
        Arc::clone(&store.write_held),
        Arc::clone(&store.writing),
        Arc::clone(&store.write_on),
    );
    let kit = installed(store, DiscussionOptions::default()).await;
    let room = RoomId::new("r1").unwrap();
    write_held.store(true, Ordering::SeqCst);
    let saying = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(hello(), ProcessInboundOptions::default())
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), writing.notified())
        .await
        .expect("the plan writes its queue");

    let listening = {
        let (kit, room) = (kit.clone(), room.clone());
        tokio::spawn(async move {
            let started = tokio::time::Instant::now();
            let options = roomkit::ListenOnlyOptions::default();
            let listened = kit.listen_only(&room, &[id("a")], options).await;
            (listened, started.elapsed())
        })
    };
    tokio::task::yield_now().await;
    let uninstalling = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.uninstall_strategy(&room, UninstallStrategyOptions::default())
                .await
        })
    };
    tokio::task::yield_now().await;
    write_on.notify_one();
    let (listened, took) = tokio::time::timeout(Duration::from_secs(10), listening)
        .await
        .unwrap()
        .unwrap();
    assert!(uninstalling.await.unwrap().unwrap());
    drop(saying.await);

    listened.unwrap();
    assert!(took < Duration::from_secs(1), "listen_only took {took:?}");
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_discussion_edges.py::test_listen_only_on_a_turn_given_but_not_started_cuts_it,
// bounded as its `wait_for(held.wait(), 5)`: the cut waits for the turn's end,
// which comes at once, never for its whole bound.
#[tokio::test]
async fn listen_only_on_a_turn_given_returns_once_it_is_cut() {
    let kit = installed(FlakyStore::default(), DiscussionOptions::default()).await;
    let (telling, mut told) = watch::channel(None);
    let weak = kit.downgrade();
    kit.hook(
        HookTrigger::OnSpeakQueue,
        move |event: SpeakQueueEvent, _context: RoomContext| {
            let (kit, telling) = (weak.upgrade(), telling.clone());
            async move {
                let Some(kit) = kit.filter(|_| event.change == SpeakQueueChange::TurnGiven) else {
                    return;
                };
                let started = tokio::time::Instant::now();
                let options = roomkit::ListenOnlyOptions::default();
                let listened = kit.listen_only(&event.room_id, &[id("a")], options).await;
                telling.send_replace(Some((listened.is_ok(), started.elapsed())));
            }
        },
        HookOptions::default(),
    )
    .unwrap();

    kit.process_inbound(hello(), ProcessInboundOptions::default())
        .await
        .unwrap();
    let (listened, took) =
        tokio::time::timeout(Duration::from_secs(10), told.wait_for(Option::is_some))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    idle(&kit).await;

    assert!(listened);
    assert!(took < Duration::from_secs(1), "listen_only took {took:?}");
    let room = RoomId::new("r1").unwrap();
    let events = kit
        .store()
        .list_events(&room, store::ListEventsOptions::default())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.source.channel_id != id("a"))
    );
    kit.close().await.unwrap();
}

/// A kit on `store` holding `ops`, `a` answering `@b your turn` and `b`
/// answering `b says`.
fn kit_on(store: &Arc<InMemoryStore>, b_says: &str) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        store: Arc::clone(store),
        ..RoomKitOptions::default()
    });
    kit.register_channel(RecordingTransport::new(
        id("ops"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    for (agent, says) in [("a", "@b your turn"), ("b", b_says)] {
        let echo = EchoAgent::new(id(agent), says, EchoAgentOptions::default());
        kit.register_channel(echo).unwrap();
    }
    kit
}

/// The discussion of `a` and `b`, its turns stopped at `max_depth`.
fn discussion(max_depth: Option<u32>) -> Arc<Discussion> {
    let options = DiscussionOptions {
        max_depth,
        ..DiscussionOptions::default()
    };
    Arc::new(Discussion::new(vec![id("a").into(), id("b").into()], options).unwrap())
}

// Python: tests/orchestration/test_strategy_discussion.py::test_the_queue_outlives_the_process_but_not_who_speaks
#[tokio::test]
async fn the_queue_outlives_the_process_but_not_who_speaks() {
    let room = RoomId::new("r1").unwrap();
    let store = Arc::new(InMemoryStore::default());
    let kit = kit_on(&store, "b");
    let created = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.create_room(created).await.unwrap();
    kit.attach_channel(&room, &id("ops"), AttachChannelOptions::default())
        .await
        .unwrap();
    let options = InstallStrategyOptions::default();
    kit.install_strategy(&room, discussion(Some(2)), options)
        .await
        .unwrap();
    kit.process_inbound(say("@a start"), ProcessInboundOptions::default())
        .await
        .unwrap();
    idle(&kit).await;
    let queue = kit.speak_queue(&room, roomkit::SpeakQueueOptions::default());
    // b's turn at depth 2 is stopped by max_depth=2: b stays queued.
    assert_eq!(queue.map(|queue| queue.queue), Some(vec![id("b")]));
    kit.close().await.unwrap();

    let again = kit_on(&store, "b resumes");
    let options = InstallStrategyOptions::default();
    again
        .install_strategy(&room, discussion(None), options)
        .await
        .unwrap();
    let restored = again
        .speak_queue(&room, roomkit::SpeakQueueOptions::default())
        .unwrap();

    assert_eq!(
        (restored.queue, restored.speaking, restored.waiting),
        (vec![id("b")], None, true)
    );
    again.close().await.unwrap();
}

// Python: core/mixins/discussion.py `_discussion_is_current`, which reads the
// discussion an install registers once its configuration is stored: an
// install under way is not current yet, and room_strategy_is_current leaves
// it be.
#[tokio::test]
async fn an_install_under_way_is_not_current_and_stays() {
    let store = FlakyStore::default();
    let (held, writing, on) = (
        Arc::clone(&store.config_held),
        Arc::clone(&store.config_writing),
        Arc::clone(&store.config_on),
    );
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    kit.register_channel(RecordingTransport::new(
        id("ops"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        id("a"),
        "a answers",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("r1").unwrap();
    let created = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.create_room(created).await.unwrap();
    held.store(true, Ordering::SeqCst);
    let installing = {
        let (kit, room) = (kit.clone(), room.clone());
        tokio::spawn(async move {
            let discussion =
                Discussion::new(vec![id("a").into()], DiscussionOptions::default()).unwrap();
            let options = InstallStrategyOptions::default();
            kit.install_strategy(&room, Arc::new(discussion), options)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), writing.notified())
        .await
        .expect("the install stores its configuration");

    let kind = std::any::TypeId::of::<Discussion>();
    let options = RoomStrategyIsCurrentOptions::default();
    let during = kit.room_strategy_is_current(&room, kind, options).await;
    on.notify_one();
    installing.await.unwrap().unwrap();

    assert!(!during.unwrap());
    assert!(
        kit.room_strategy(&room, RoomStrategyOptions::default())
            .is_some()
    );
    let options = RoomStrategyIsCurrentOptions::default();
    assert!(
        kit.room_strategy_is_current(&room, kind, options)
            .await
            .unwrap()
    );
    kit.close().await.unwrap();
}
