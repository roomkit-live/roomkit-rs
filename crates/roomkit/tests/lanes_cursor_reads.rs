//! A delivery set whose room's cursor cannot be read (RFC §13.5 "Delivery
//! lanes across processes"): the set reads it again after the wait it
//! gives a hole, as Python's lane retries a round that failed, and is
//! never delivered out of the cursor's order meanwhile. A set with no index
//! whose anchor cannot be read waits for no other process, and the failure
//! is its call's error (RFC §10.1.1 rule 5).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent, EventId,
    EventType, HookResult, Identity, IdentityId, InboundMessage, Observation, Participant,
    ParticipantId, Room, RoomContext, RoomEvent, RoomId, Task, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions, HookOptions,
    HookTrigger, InMemoryStore, ProcessInboundOptions, RoomKit, RoomKitOptions, StoreError,
    StoreFailure, store,
};
use tokio::sync::{Notify, Semaphore};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// An in-memory store whose cursor reads fail while `down` is set, and
/// whose next plain read of a room fails once `room_read_fails` is set, its
/// contexts read all the same: the read that anchors a set with no index.
#[derive(Debug, Default)]
struct CursorReadsFail {
    store: InMemoryStore,
    down: AtomicBool,
    room_read_fails: AtomicBool,
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

impl ConversationStore for CursorReadsFail {
    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new("cursor down")));
        }
        self.store.get_delivered_index(room_id).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        if self.room_read_fails.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new(
                "the store is unreachable",
            )));
        }
        self.store.get_room(room_id).await
    }

    async fn load_room_context(
        &self,
        room_id: &RoomId,
    ) -> Result<(Option<Room>, Vec<ChannelBinding>, Vec<Participant>), StoreError> {
        self.store.load_room_context(room_id).await
    }

    forward! {
        room_exists(room_id: &RoomId) -> bool;
        update_room_if(
            room: Room,
            status: roomkit::model::RoomStatus,
            paused_at: Option<roomkit::model::Timestamp>,
            last_activity_at: Option<roomkit::model::Timestamp>
        ) -> Option<Room>;
        patch_room_metadata(
            room_id: &RoomId,
            patch: roomkit::model::Metadata,
            options: store::PatchRoomMetadataOptions
        ) -> Option<Room>;
        get_private_state(room_id: &RoomId, key: &str) -> Option<roomkit::model::Metadata>;
        set_private_state(
            room_id: &RoomId,
            key: &str,
            value: Option<roomkit::model::Metadata>
        ) -> ();
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
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
        get_event(event_id: &EventId) -> Option<RoomEvent>;
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
        get_task(task_id: &roomkit::model::TaskId) -> Option<Task>;
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
        ) -> std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>;
    }
}

// Python: src/roomkit/core/lanes.py, RoomDeliveryLane._run (no Python test): a
// round that failed is retried after the lane's backoff.
#[tokio::test(start_paused = true)]
async fn a_set_reads_a_cursor_that_failed_again_before_it_runs() {
    let options = RoomKitOptions::with_store(CursorReadsFail::default());
    let store = Arc::clone(&options.store);
    let kit = RoomKit::new(options);
    let recorder = RecordingTransport::new(
        channel("recorder"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let sms = RecordingTransport::new(
        channel("src"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(recorder.clone()).unwrap();
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
    store.down.store(true, Ordering::SeqCst);
    let sending = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message = InboundMessage::new(channel("src"), "u", TextContent::new("m"));
            kit.process_inbound(message, ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        recorder.texts().is_empty(),
        "nothing runs while the cursor cannot be read"
    );
    store.down.store(false, Ordering::SeqCst);
    sending.await.unwrap();
    assert_eq!(recorder.texts(), ["m"]);
    kit.close().await.unwrap();
}

/// A transport that holds the delivery of `m0` until `gate` lets it go,
/// and writes what it delivers into `order`.
struct Gated {
    held: Arc<Notify>,
    gate: Arc<Semaphore>,
    order: Arc<Mutex<Vec<String>>>,
}

impl Channel for Gated {
    fn channel_id(&self) -> ChannelId {
        channel("out")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("nothing is sent through out".into()))
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let EventContent::Text(text) = &event.content else {
            return Ok(ChannelOutput::empty());
        };
        if text.body == "m0" {
            self.held.notify_one();
            drop(self.gate.acquire().await.unwrap());
        }
        self.order.lock().unwrap().push(text.body.clone());
        Ok(ChannelOutput::empty())
    }
}

/// An agent `ai` that answers nothing and writes `instruction` into `order`
/// at the turn an instruction gives it.
struct Directed {
    order: Arc<Mutex<Vec<String>>>,
}

impl Channel for Directed {
    fn channel_id(&self) -> ChannelId {
        channel("ai")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("nothing sends an agent a message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if event.r#type == EventType::Instruction {
            self.order.lock().unwrap().push("instruction".to_owned());
        }
        ready(Ok(ChannelOutput::empty()))
    }
}

/// Has `store` fail its next plain read of a room once an instruction's
/// `BEFORE_BROADCAST` hooks run, under the room's lock: the read that
/// anchors its set comes next. The notify tells the hook ran.
fn fail_the_anchor_read(
    kit: &RoomKit<CursorReadsFail>,
    store: &Arc<CursorReadsFail>,
) -> Arc<Notify> {
    let hooked = Arc::new(Notify::new());
    let arming = (Arc::clone(store), Arc::clone(&hooked));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (store, hooked) = arming.clone();
            async move {
                if event.r#type == EventType::Instruction {
                    store.room_read_fails.store(true, Ordering::SeqCst);
                    hooked.notify_one();
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "arm".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    hooked
}

/// A kit on a store whose anchor read fails once armed, with `src`, the
/// gated `out`, the agent `ai` and the room `r1`: what its deliveries and
/// turns write in order, the transport's notify and gate, and the notify of
/// the hook that arms the failure.
struct Rig {
    kit: RoomKit<CursorReadsFail>,
    store: Arc<CursorReadsFail>,
    order: Arc<Mutex<Vec<String>>>,
    held: Arc<Notify>,
    gate: Arc<Semaphore>,
    hooked: Arc<Notify>,
}

async fn rig() -> Rig {
    let options = RoomKitOptions::with_store(CursorReadsFail::default());
    let store = Arc::clone(&options.store);
    let kit = RoomKit::new(options);
    let order = Arc::new(Mutex::new(Vec::new()));
    let held = Arc::new(Notify::new());
    let gate = Arc::new(Semaphore::new(0));
    kit.register_channel(RecordingTransport::new(
        channel("src"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(Gated {
        held: Arc::clone(&held),
        gate: Arc::clone(&gate),
        order: Arc::clone(&order),
    })
    .unwrap();
    kit.register_channel(Directed {
        order: Arc::clone(&order),
    })
    .unwrap();
    let hooked = fail_the_anchor_read(&kit, &store);
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["src", "out", "ai"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    Rig {
        kit,
        store,
        order,
        held,
        gate,
        hooked,
    }
}

/// `text` from `src`, as an instruction to `ai` when `instruction` is set.
fn inbound(text: &str, instruction: bool) -> InboundMessage {
    let mut message = InboundMessage::new(channel("src"), "u", TextContent::new(text));
    if instruction {
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![channel("ai")]);
    }
    message
}

/// The read of the room that anchors an instruction's set fails: the call
/// returns, the failure is its cascade's error (RFC §10.1.1 rule 5), which
/// a deferred call's handle reports once the set ran, and the set runs
/// waiting for no other process, behind the sets this process placed
/// before it (RFC §13.5).
// Python: tests/test_lanes_multiworker.py::test_an_instruction_whose_anchor_read_fails_reports_it_and_keeps_its_place
#[tokio::test]
async fn an_instruction_whose_anchor_read_fails_reports_it_and_keeps_its_place() {
    let bound = Duration::from_secs(5);
    let deferred = || ProcessInboundOptions {
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    for defer in [false, true] {
        let rig = rig().await;
        let kit = rig.kit.clone();
        let zero = tokio::spawn(async move {
            kit.process_inbound(inbound("m0", false), ProcessInboundOptions::default())
                .await
                .unwrap()
        });
        tokio::time::timeout(bound, rig.held.notified())
            .await
            .unwrap();
        let mut queued = Vec::new();
        for text in ["m1", "m2"] {
            let one = rig.kit.process_inbound(inbound(text, false), deferred());
            queued.push(one.await.unwrap());
        }
        let instruction = inbound("Introduce yourself", true);
        let result = if defer {
            let decided = rig.kit.process_inbound(instruction, deferred());
            let decided = decided.await.unwrap();
            assert_eq!(decided.error, None, "the decision carries no failure");
            rig.gate.add_permits(1);
            let delivery = decided.delivery.unwrap();
            let waited = tokio::time::timeout(bound, delivery.wait());
            waited.await.unwrap().unwrap()
        } else {
            let kit = rig.kit.clone();
            let call = tokio::spawn(async move {
                kit.process_inbound(instruction, ProcessInboundOptions::default())
                    .await
                    .unwrap()
            });
            tokio::time::timeout(bound, rig.hooked.notified())
                .await
                .unwrap();
            // The instruction's set is placed once its section let the lock go.
            let lock = rig.kit.lock_manager().locked(&room_id()).await.unwrap();
            lock.release().await.unwrap();
            rig.gate.add_permits(1);
            tokio::time::timeout(bound, call).await.unwrap().unwrap()
        };
        tokio::time::timeout(bound, zero).await.unwrap().unwrap();
        for one in queued {
            let delivery = one.delivery.unwrap();
            let waited = tokio::time::timeout(bound, delivery.wait());
            waited.await.unwrap().unwrap();
        }
        assert!(!result.blocked);
        let error = result.error.expect("the failed read is the call's error");
        assert!(
            error.message.contains("the store is unreachable"),
            "{error:?} (deferred: {defer})"
        );
        assert_eq!(
            *rig.order.lock().unwrap(),
            ["m0", "m1", "m2", "instruction"],
            "deferred: {defer}"
        );
        rig.kit.close().await.unwrap();
    }
}

/// A set whose anchor read failed still runs under the room's delivery
/// claim (RFC §13.5: "The sets of one room run under its delivery claim"):
/// another process's set holding it holds the set back until it is over,
/// as Python's lane runs such a set in a claimed round.
// Python: src/roomkit/core/lanes.py, RoomDeliveryLane._claimed_round (no
// Python test): every entry runs under the claim.
#[tokio::test(start_paused = true)]
async fn a_set_whose_anchor_read_failed_runs_under_the_room_s_claim() {
    let a = rig().await;
    let locks = a.kit.lock_manager();
    let b = RoomKit::new(RoomKitOptions {
        store: Arc::clone(&a.store),
        lock_manager: Some(locks),
        ..RoomKitOptions::with_store(CursorReadsFail::default())
    });
    let gate = Arc::new(Semaphore::new(0));
    let held = Arc::new(Notify::new());
    b.register_channel(RecordingTransport::new(
        channel("src"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    b.register_channel(Gated {
        held: Arc::clone(&held),
        gate: Arc::clone(&gate),
        order: Arc::clone(&a.order),
    })
    .unwrap();
    b.register_channel(Directed {
        order: Arc::clone(&a.order),
    })
    .unwrap();
    // The other process delivers `m0`, holding the room's claim meanwhile.
    let other = b.clone();
    let zero = tokio::spawn(async move {
        other
            .process_inbound(inbound("m0", false), ProcessInboundOptions::default())
            .await
            .unwrap()
    });
    let bound = Duration::from_secs(5);
    tokio::time::timeout(bound, held.notified()).await.unwrap();
    let kit = a.kit.clone();
    let instruction = tokio::spawn(async move {
        kit.process_inbound(
            inbound("Introduce yourself", true),
            ProcessInboundOptions::default(),
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(bound, a.hooked.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        *a.order.lock().unwrap(),
        Vec::<String>::new(),
        "the set waits for the claim"
    );
    gate.add_permits(1);
    tokio::time::timeout(bound, zero).await.unwrap().unwrap();
    let result = tokio::time::timeout(bound, instruction)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.error.is_some(),
        "the failed read is the call's error"
    );
    assert_eq!(*a.order.lock().unwrap(), ["m0", "instruction"]);
    a.kit.close().await.unwrap();
    b.close().await.unwrap();
}
