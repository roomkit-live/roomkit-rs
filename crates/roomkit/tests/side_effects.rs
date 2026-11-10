//! The side effects a delivery set stores (RFC §10.1 step 15) when the
//! store fails one of their writes, and when the room's lane drops the set
//! as stale or as the kit closes: what no shared scenario can script, a
//! store that fails, another worker's lane, a close past its grace
//! (Python's `tests/test_hook_side_effects.py`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(clippy::panic, reason = "a store's driver that breaks panics")]

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventDirection, EventId, EventSource, HookResult, Identity, IdentityId, InboundMessage,
    Metadata, Observation, ObservationId, Participant, ParticipantId, Room, RoomContext, RoomEvent,
    RoomId, Task, TaskId, TaskStatus, TextContent,
};
use roomkit::store::PatchRoomMetadataOptions;
use roomkit::tasks::WaitOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    DelegateOptions, HookExecution, HookOptions, HookTrigger, InMemoryStore, ProcessInboundOptions,
    RoomKit, RoomKitOptions, StoreError, StoreFailure, store,
};

#[path = "support/warnings.rs"]
mod warnings;

#[path = "side_effects/claim_release.rs"]
mod claim_release;
#[path = "side_effects/cursor_recovery.rs"]
mod cursor_recovery;
#[path = "side_effects/execution_failure.rs"]
mod execution_failure;
#[path = "side_effects/stale_drain.rs"]
mod stale_drain;

use warnings::Warnings;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// The write a [`FailingWrites`] store fails.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Failing {
    /// The task `t1`.
    Task,
    /// The observation `o1`.
    Observation,
    /// The room's metadata `stage`.
    Metadata,
    /// The observation `o1`, by a panic.
    PanickingObservation,
    /// The room's delivery cursor's next write once `armed`, once.
    Cursor,
    /// The next two ordinary cursor writes once `armed`.
    CursorTwice,
    /// The room's delivery cursor's second write once `armed`, once.
    SecondCursor,
    /// None: the write of the task `tA` waits until the test opens it,
    /// telling it once it started.
    HeldTask,
}

/// An in-memory store that fails the write `failing` names.
#[derive(Debug)]
struct FailingWrites {
    store: InMemoryStore,
    failing: Failing,
    /// For [`Failing::Cursor`]: the next cursor write fails, then none.
    armed: AtomicBool,
    /// For [`Failing::SecondCursor`] and [`Failing::CursorTwice`]: writes since `armed`,
    /// and the index of the one that failed.
    cursor_writes: AtomicUsize,
    failed_index: AtomicU64,
    /// For [`Failing::HeldTask`]: told once `tA`'s write started, and
    /// what lets it go on.
    held: (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>),
}

impl FailingWrites {
    fn new(failing: Failing) -> Self {
        Self {
            store: InMemoryStore::default(),
            failing,
            armed: AtomicBool::new(false),
            cursor_writes: AtomicUsize::new(0),
            failed_index: AtomicU64::new(0),
            held: Default::default(),
        }
    }
}

/// The store's failure, as a database gone answers.
fn down() -> StoreError {
    StoreError::Unavailable(StoreFailure::new("store down"))
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

impl ConversationStore for FailingWrites {
    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        if self.failing == Failing::Task && task.id.as_str() == "t1" {
            return Err(down());
        }
        if self.failing == Failing::HeldTask && task.id.as_str() == "tA" {
            let (reached, opened) = &self.held;
            reached.notify_one();
            opened.notified().await;
        }
        self.store.add_task(task).await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        if observation.id.as_str() == "o1" {
            match self.failing {
                Failing::Observation => return Err(down()),
                Failing::PanickingObservation => panic!("the store's driver broke"),
                Failing::Task
                | Failing::Metadata
                | Failing::Cursor
                | Failing::CursorTwice
                | Failing::SecondCursor
                | Failing::HeldTask => {}
            }
        }
        self.store.add_observation(observation).await
    }

    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        if self.failing == Failing::Metadata && patch.contains_key("stage") {
            return Err(down());
        }
        self.store
            .patch_room_metadata(room_id, patch, options)
            .await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: store::AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        if self.failing == Failing::Cursor && self.armed.swap(false, Ordering::SeqCst) {
            return Err(down());
        }
        if self.failing == Failing::CursorTwice
            && self.armed.load(Ordering::SeqCst)
            && !options.force
            && self.cursor_writes.fetch_add(1, Ordering::SeqCst) < 2
        {
            self.failed_index.store(index, Ordering::SeqCst);
            return Err(down());
        }
        if self.failing == Failing::SecondCursor
            && self.armed.load(Ordering::SeqCst)
            && self.cursor_writes.fetch_add(1, Ordering::SeqCst) == 1
        {
            self.armed.store(false, Ordering::SeqCst);
            self.failed_index.store(index, Ordering::SeqCst);
            return Err(down());
        }
        self.store
            .advance_delivered_index(room_id, index, options)
            .await
    }

    forward! {
        get_room(room_id: &RoomId) -> Option<Room>;
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        get_delivered_index(room_id: &RoomId) -> i64;
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
        ) -> std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>;
    }
}

/// A transport whose delivery writes the room's `stage` then its `owner`,
/// Python's `UpdatingChannel("crm", {"stage": "qualified", "owner": "ana"})`.
struct Crm;

impl Channel for Crm {
    fn channel_id(&self) -> ChannelId {
        channel("crm")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(crm_updates()))
    }
}

/// The CRM's updates as its output carries them: `stage` then `owner`.
fn crm_updates() -> ChannelOutput {
    let mut output = ChannelOutput::default();
    output
        .metadata_updates
        .insert("stage".to_owned(), "qualified".into());
    output
        .metadata_updates
        .insert("owner".to_owned(), "ana".into());
    output
}

/// An agent that answers `done`, its output carrying the CRM's updates:
/// Python's `_AnsweringUpdater("worker", ...)`.
struct Worker;

impl Channel for Worker {
    fn channel_id(&self) -> ChannelId {
        channel("worker")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(channel("worker"), ChannelType::AI)
        };
        let answer = RoomEvent::new(event.room_id, source, TextContent::new("done"));
        ready(Ok(ChannelOutput {
            responded: true,
            response_events: vec![answer],
            ..crm_updates()
        }))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// The line a failed metadata write logs, its keys sorted.
const METADATA_LINE: &str = "Failed to write the room metadata updates ['owner', 'stage']";

/// A hook's two tasks and two observations, `t1`, `t2`, `o1`, `o2`, with
/// which it blocks the event when `blocks`.
fn triage(blocks: bool) -> HookResult {
    let tasks = (1..=2)
        .map(|i| {
            let id = TaskId::new(format!("t{i}")).unwrap();
            Task::new(id, room_id(), format!("Call back {i}"))
        })
        .collect();
    let observations = (1..=2)
        .map(|i| {
            let id = ObservationId::new(format!("o{i}")).unwrap();
            Observation::new(id, room_id(), channel("sms1"), format!("note {i}"))
        })
        .collect();
    if blocks {
        let options = BlockOptions {
            tasks: Some(tasks),
            observations: Some(observations),
            ..BlockOptions::default()
        };
        return HookResult::block("spam", options);
    }
    HookResult {
        tasks,
        observations,
        ..HookResult::allow(AllowOptions::default())
    }
}

/// A kit on a store failing `failing`, binding `channels` in its room
/// `r1`.
async fn kit_on(failing: Failing, channels: &[&str]) -> RoomKit<FailingWrites> {
    let store = FailingWrites::new(failing);
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    let sms = RecordingTransport::new(
        channel("sms1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(Crm).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for bound in channels {
        kit.attach_channel(&room_id(), &channel(bound), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

/// A kit on a store failing `failing`, its room `r1` binding `sms1` and
/// the CRM, a hook giving [`triage`]'s side effects, and the ids of the
/// tasks whose `ON_TASK_CREATED` ran.
async fn triaging_room(
    failing: Failing,
    blocks: bool,
) -> (RoomKit<FailingWrites>, Arc<Mutex<Vec<String>>>) {
    let kit = kit_on(failing, &["sms1", "crm"]).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| async move { Ok::<_, ChannelError>(triage(blocks)) },
        HookOptions {
            name: "triage".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let created = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&created);
    kit.hook(
        HookTrigger::OnTaskCreated,
        move |event: RoomEvent, _| {
            let task_id = event.metadata.get("task_id").and_then(|id| id.as_str());
            seen.lock().unwrap().extend(task_id.map(str::to_owned));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "follow_up".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    (kit, created)
}

/// `ids` sorted, as owned strings.
fn sorted(ids: impl IntoIterator<Item = impl ToString>) -> Vec<String> {
    let mut ids: Vec<String> = ids.into_iter().map(|id| id.to_string()).collect();
    ids.sort();
    ids
}

// Python: tests/test_hook_side_effects.py::TestSideEffectWriteFailures::test_a_failed_side_effect_write_is_logged_and_the_call_goes_on
// (RMK-1498): a side effect the store fails to write is one ERROR line, the
// others still written, a blocked event's too; the call goes on, its error
// its turn's, and only a task stored fires ON_TASK_CREATED.
#[tokio::test]
async fn a_failed_side_effect_write_is_logged_and_the_call_goes_on() {
    let cases = [
        (Failing::Task, false),
        (Failing::Observation, false),
        (Failing::Metadata, false),
        (Failing::Task, true),
        // A store's driver that panics fails that write alone, as a
        // Python store's exception, whatever its class, does.
        (Failing::PanickingObservation, false),
    ];
    for (failing, blocks) in cases {
        let (kit, created) = triaging_room(failing, blocks).await;
        let (warnings, capturing) = Warnings::capture();
        let message = InboundMessage::new(channel("sms1"), "+15550001", TextContent::new("hi"));
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        let result = kit.process_inbound(message, options).await.unwrap();
        roomkit::testing::idle(&kit).await;
        drop(capturing);
        let store = kit.store();
        let tasks = store
            .list_tasks(&room_id(), store::ListTasksOptions::default())
            .await
            .unwrap();
        let tasks = sorted(tasks.into_iter().map(|task| task.id));
        let observations = store.list_observations(&room_id()).await.unwrap();
        let observations = sorted(observations.into_iter().map(|o| o.id));
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        let stage = room.metadata.get("stage").and_then(|stage| stage.as_str());
        let created = sorted(created.lock().unwrap().clone());
        kit.close().await.unwrap();

        let case = format!("{failing:?} blocks={blocks}");
        let expected = match failing {
            Failing::Task => "Failed to persist task t1",
            Failing::Observation | Failing::PanickingObservation => {
                "Failed to persist observation o1"
            }
            Failing::Metadata => METADATA_LINE,
            Failing::Cursor | Failing::CursorTwice | Failing::SecondCursor | Failing::HeldTask => {
                unreachable!("no case fails the cursor or holds a write")
            }
        };
        let lines = warnings.containing("ERROR");
        assert_eq!(
            (
                result.error.is_none(),
                result.blocked,
                lines.len(),
                lines.iter().all(|line| line.contains(expected)),
            ),
            (true, blocks, 1, true),
            "{case}: {result:?} {lines:?}"
        );
        let all_tasks = if failing == Failing::Task {
            sorted(["t2"])
        } else {
            sorted(["t1", "t2"])
        };
        let all_observations = if matches!(
            failing,
            Failing::Observation | Failing::PanickingObservation
        ) {
            sorted(["o2"])
        } else {
            sorted(["o1", "o2"])
        };
        let all_stage = (failing != Failing::Metadata && !blocks).then_some("qualified");
        assert_eq!(
            (tasks, created, observations, stage),
            (all_tasks.clone(), all_tasks, all_observations, all_stage),
            "{case}"
        );
    }
}

// Python: tests/test_hook_side_effects.py::TestSideEffectWriteFailures::test_a_delegated_turn_s_failed_metadata_write_is_logged
// (RMK-1498): the metadata updates a task's child room is given, by a
// transport shared into it or by the agent's own output, are written as a
// room's are, a failure one ERROR line, and the task still completes.
#[tokio::test]
async fn a_delegated_turn_s_failed_metadata_write_is_logged() {
    for shared in [true, false] {
        let kit = if shared {
            let kit = kit_on(Failing::Metadata, &["crm"]).await;
            let worker = EchoAgent::new(channel("worker"), "done", EchoAgentOptions::default());
            kit.register_channel(worker).unwrap();
            kit
        } else {
            let kit = kit_on(Failing::Metadata, &[]).await;
            kit.register_channel(Worker).unwrap();
            kit
        };
        let (warnings, capturing) = Warnings::capture();
        let options = DelegateOptions {
            share_channels: shared.then(|| vec![channel("crm")]),
            wait: true,
            ..DelegateOptions::default()
        };
        let task = kit
            .delegate(&room_id(), &channel("worker"), "go", options)
            .await
            .unwrap();
        let result = task.wait(WaitOptions::default()).await.unwrap();
        roomkit::testing::idle(&kit).await;
        drop(capturing);
        let child = kit
            .store()
            .get_room(&task.child_room_id)
            .await
            .unwrap()
            .unwrap();
        kit.close().await.unwrap();

        let lines = warnings.containing("ERROR");
        assert_eq!(
            (
                result.status,
                lines.len(),
                lines.iter().all(|line| line.contains(METADATA_LINE)),
                child.metadata.get("stage"),
            ),
            (TaskStatus::Completed, 1, true, None),
            "shared={shared}: {:?}",
            warnings.containing("")
        );
    }
}

// Python: tests/test_hook_side_effects.py::TestDroppedSetSideEffects::test_a_set_the_lane_drops_as_stale_still_stores_its_hooks_side_effects
// (RMK-1499): a delivery set the room's lane drops before it ran, its index
// declared lost by another worker, still stores the task its event's hook
// gave before the call returns, and the task's ON_TASK_CREATED fires, as
// after a set that ran.
#[tokio::test]
async fn a_set_the_lane_drops_as_stale_still_stores_its_hooks_side_effects() {
    let kit = RoomKit::new(RoomKitOptions::default());
    for (id, kind) in [("sms1", ChannelType::SMS), ("ws1", ChannelType::WEBSOCKET)] {
        let transport =
            RecordingTransport::new(channel(id), kind, RecordingTransportOptions::default());
        kit.register_channel(transport).unwrap();
    }
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for bound in ["sms1", "ws1"] {
        kit.attach_channel(&room_id(), &channel(bound), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            let id = TaskId::new("t1").unwrap();
            let task = Task::new(id, room_id(), "Call the customer back");
            Ok::<_, ChannelError>(HookResult {
                tasks: vec![task],
                ..HookResult::allow(AllowOptions::default())
            })
        },
        HookOptions {
            name: "triage".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let created = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&created);
    kit.hook(
        HookTrigger::OnTaskCreated,
        move |event: RoomEvent, _| {
            let task_id = event.metadata.get("task_id").and_then(|id| id.as_str());
            seen.lock().unwrap().extend(task_id.map(str::to_owned));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "follow_up".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();

    // Another worker's lane declares the next index lost before this
    // process delivers it.
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    let next = room.latest_index.map_or(0, |index| index + 1);
    let options = store::AdvanceDeliveredIndexOptions { force: true };
    kit.store()
        .advance_delivered_index(&room_id(), next, options)
        .await
        .unwrap();
    let message = InboundMessage::new(
        channel("sms1"),
        "+15550001",
        TextContent::new("Call me back"),
    );
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();
    let tasks = kit
        .store()
        .list_tasks(&room_id(), store::ListTasksOptions::default())
        .await
        .unwrap();
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();

    assert_eq!(
        (
            result.cancellation_reason.as_deref(),
            sorted(tasks.into_iter().map(|task| task.id)),
            created.lock().unwrap().clone(),
        ),
        (
            Some("delivery_stale"),
            sorted(["t1"]),
            vec!["t1".to_owned()]
        )
    );
}

/// The text of `event`, when it is a text.
fn text_of(event: &RoomEvent) -> Option<&str> {
    if let EventContent::Text(text) = &event.content {
        Some(text.body.as_str())
    } else {
        None
    }
}

/// The message `body` from `+15550001` on `sms1`.
fn sms(body: &str) -> InboundMessage {
    InboundMessage::new(channel("sms1"), "+15550001", TextContent::new(body))
}

/// Room `r1` on `kit` binding `sms1`, a recording SMS transport, and
/// `ws1`, registered.
async fn sms_and<S: ConversationStore>(kit: &RoomKit<S>, ws1: impl Channel + 'static) {
    let sms1 = RecordingTransport::new(
        channel("sms1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms1).unwrap();
    kit.register_channel(ws1).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for bound in ["sms1", "ws1"] {
        kit.attach_channel(&room_id(), &channel(bound), AttachChannelOptions::default())
            .await
            .unwrap();
    }
}

/// A locked hook on `kit` giving the task `id` to the message `text`.
fn gives_task<S: ConversationStore>(kit: &RoomKit<S>, text: &'static str, id: &'static str) {
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| async move {
            let tasks = (text_of(&event) == Some(text))
                .then(|| Task::new(TaskId::new(id).unwrap(), room_id(), "Call back"))
                .into_iter()
                .collect();
            Ok::<_, ChannelError>(HookResult {
                tasks,
                ..HookResult::allow(AllowOptions::default())
            })
        },
        HookOptions {
            name: "triage".to_owned(),
            priority: 1,
            ..Default::default()
        },
    )
    .unwrap();
}

/// The ids of the tasks whose `ON_TASK_CREATED` ran on `kit`.
fn notes_created<S: ConversationStore>(kit: &RoomKit<S>) -> Arc<Mutex<Vec<String>>> {
    let created = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&created);
    kit.hook(
        HookTrigger::OnTaskCreated,
        move |event: RoomEvent, _| {
            let task_id = event.metadata.get("task_id").and_then(|id| id.as_str());
            seen.lock().unwrap().extend(task_id.map(str::to_owned));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "follow_up".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    created
}

/// The texts whose AFTER_BROADCAST observer ran.
fn aftermath(kit: &RoomKit) -> Arc<Mutex<Vec<String>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let after = Arc::clone(&seen);
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            after
                .lock()
                .unwrap()
                .extend(text_of(&event).map(str::to_owned));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "after".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    seen
}

/// `kit`'s call of the message `hello` into `r1`, started in a task.
fn hello_from<S: ConversationStore>(
    kit: &RoomKit<S>,
) -> tokio::task::JoinHandle<Result<roomkit::InboundResult, roomkit::RoomKitError>> {
    let caller = kit.clone();
    tokio::spawn(async move {
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        caller.process_inbound(sms("hello"), options).await
    })
}

/// The options of a call into `r1` that defers its delivery.
fn deferred() -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id()),
        defer_delivery: true,
        ..Default::default()
    }
}

/// A transport whose delivery of `Hold on` never answers, telling the test
/// when it started.
struct Stuck {
    reached: Arc<tokio::sync::Notify>,
}

impl Channel for Stuck {
    fn channel_id(&self) -> ChannelId {
        channel("ws1")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let held = text_of(&event) == Some("Hold on");
        let reached = Arc::clone(&self.reached);
        async move {
            if held {
                reached.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(ChannelOutput::default())
        }
    }
}

// Python: tests/test_hook_side_effects.py::TestDroppedSetSideEffects::test_a_dropped_set_s_task_is_stored_while_its_call_outlasts_the_close_s_grace
// (RMK-1514): hello's off-lock hook makes two deferred calls into its
// room, `Hold on`, whose transport never answers, then `Call me back`,
// whose hook gives `t1` and whose set waits in the room's lane behind the
// first. The kit's close drops that set and stores its task within its
// grace, though the set ahead of it, an entry of the same call, outlasts
// the grace and is cut; the task's ON_TASK_CREATED fires.
#[tokio::test(start_paused = true)]
async fn a_dropped_set_s_task_is_stored_while_its_call_outlasts_the_close_s_grace() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let reached = Arc::new(tokio::sync::Notify::new());
    let stuck = Stuck {
        reached: Arc::clone(&reached),
    };
    sms_and(&kit, stuck).await;
    let made = Arc::new(tokio::sync::Notify::new());
    let (hook_kit, hook_made) = (kit.clone(), Arc::clone(&made));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, made) = (hook_kit.clone(), Arc::clone(&hook_made));
            async move {
                if text_of(&event) == Some("hello") {
                    for body in ["Hold on", "Call me back"] {
                        kit.process_inbound(sms(body), deferred()).await?;
                    }
                    made.notify_one();
                }
                Ok::<_, ChannelError>(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "adopts".to_owned(),
            needs_lock: false,
            ..Default::default()
        },
    )
    .unwrap();
    gives_task(&kit, "Call me back", "t1");
    let created = notes_created(&kit);

    let hello = hello_from(&kit);
    let bound = std::time::Duration::from_secs(5);
    tokio::time::timeout(bound, async {
        tokio::join!(reached.notified(), made.notified())
    })
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    drop(tokio::time::timeout(bound, hello).await.unwrap());
    let tasks = kit
        .store()
        .list_tasks(&room_id(), store::ListTasksOptions::default())
        .await
        .unwrap();

    assert_eq!(
        (
            sorted(tasks.into_iter().map(|task| task.id)),
            created.lock().unwrap().clone()
        ),
        (sorted(["t1"]), vec!["t1".to_owned()])
    );
}

// Python: tests/test_hook_side_effects.py::TestDroppedSetSideEffects::test_an_adopted_call_dropped_as_stale_reads_its_task_stored
// (RMK-1514): hello's off-lock hook has another worker declare the room's
// next index lost, then makes a deferred call, `Call me back`, whose hook
// gives `t1`: the room's lane drops its set as stale, and its handle,
// waited from that hook, answers `delivery_stale` once `t1` is stored.
#[tokio::test]
async fn an_adopted_call_dropped_as_stale_reads_its_task_stored() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let ws1 = RecordingTransport::new(
        channel("ws1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    sms_and(&kit, ws1).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (hook_kit, hook_seen) = (kit.clone(), Arc::clone(&seen));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, seen) = (hook_kit.clone(), Arc::clone(&hook_seen));
            async move {
                if text_of(&event) != Some("hello") {
                    return Ok::<_, ChannelError>(HookResult::allow(AllowOptions::default()));
                }
                // Another worker's lane declares the next index lost.
                let room = kit.store().get_room(&room_id()).await?.unwrap();
                let next = room.latest_index.map_or(0, |index| index + 1);
                let options = store::AdvanceDeliveredIndexOptions { force: true };
                kit.store()
                    .advance_delivered_index(&room_id(), next, options)
                    .await?;
                let side = kit.process_inbound(sms("Call me back"), deferred()).await?;
                let waited = side.delivery.unwrap().wait().await?;
                let tasks = kit
                    .store()
                    .list_tasks(&room_id(), store::ListTasksOptions::default())
                    .await?;
                seen.lock().unwrap().push((
                    waited.cancellation_reason,
                    sorted(tasks.into_iter().map(|task| task.id)),
                ));
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "adopts".to_owned(),
            needs_lock: false,
            ..Default::default()
        },
    )
    .unwrap();
    gives_task(&kit, "Call me back", "t1");

    let result = hello_from(&kit).await.unwrap().unwrap();
    kit.close().await.unwrap();

    assert_eq!(
        (result.cancellation_reason, seen.lock().unwrap().clone()),
        (
            None,
            vec![(Some("delivery_stale".to_owned()), sorted(["t1"]))]
        )
    );
}

// Python: tests/test_hook_side_effects.py::TestDroppedSetSideEffects::test_a_set_placed_first_in_an_idle_lane_runs_at_the_close
// (RMK-1514, from the review of its second half): hello's off-lock hook
// makes a deferred call, `first`, whose hook gives `tA`, then, once `first`
// delivered and its write of `tA` started, `second`, placed first in the
// idle lane. The kit's close starts while `tA`'s write waits: `second`'s
// set runs from its placement, delivered within the grace, as the lane
// drops only what waits behind another place (RFC §12.10.4).
#[tokio::test]
async fn a_set_placed_first_in_an_idle_lane_runs_at_the_close() {
    let kit = kit_on(Failing::HeldTask, &["sms1"]).await;
    let (reached, opened) = kit.store().held.clone();
    let ws1 = RecordingTransport::new(
        channel("ws1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(ws1.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ws1"), AttachChannelOptions::default())
        .await
        .unwrap();
    let made = Arc::new(tokio::sync::Notify::new());
    let (hook_kit, hook_made, hook_reached) =
        (kit.clone(), Arc::clone(&made), Arc::clone(&reached));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, made, reached) = (
                hook_kit.clone(),
                Arc::clone(&hook_made),
                Arc::clone(&hook_reached),
            );
            async move {
                if text_of(&event) == Some("hello") {
                    kit.process_inbound(sms("first"), deferred()).await?;
                    reached.notified().await;
                    kit.process_inbound(sms("second"), deferred()).await?;
                    made.notify_one();
                }
                Ok::<_, ChannelError>(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "adopts".to_owned(),
            needs_lock: false,
            ..Default::default()
        },
    )
    .unwrap();
    gives_task(&kit, "first", "tA");

    let hello = hello_from(&kit);
    let bound = std::time::Duration::from_secs(5);
    tokio::time::timeout(bound, made.notified()).await.unwrap();
    let closer = kit.clone();
    let close = tokio::spawn(async move { closer.close().await });
    // The close starts and its runs read it, then `tA`'s write goes on.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    opened.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(60), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(tokio::time::timeout(bound, hello).await.unwrap());

    assert_eq!(ws1.texts(), ["first", "second"]);
}

/// Hooks on `kit`: one giving the task `t1` to the message `Call me back`,
/// `t2` to `Call me later`, and what each later `ON_TASK_CREATED` and `AFTER_BROADCAST` reads, as
/// `task_created t1` and `after_broadcast <text>`.
fn hooks_noting_what_follows(kit: &RoomKit<FailingWrites>) -> Arc<Mutex<Vec<String>>> {
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| async move {
            let body = if let EventContent::Text(text) = &event.content {
                Some(text.body.as_str())
            } else {
                None
            };
            let task = match body {
                Some("Call me back") => Some("t1"),
                Some("Call me later") => Some("t2"),
                Some(_) | None => None,
            };
            let tasks = task
                .map(|id| {
                    Task::new(
                        TaskId::new(id).unwrap(),
                        room_id(),
                        "Call the customer back",
                    )
                })
                .into_iter()
                .collect();
            Ok::<_, ChannelError>(HookResult {
                tasks,
                ..HookResult::allow(AllowOptions::default())
            })
        },
        HookOptions {
            name: "triage".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let later = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&later);
    kit.hook(
        HookTrigger::OnTaskCreated,
        move |event: RoomEvent, _| {
            let task_id = event.metadata.get("task_id").and_then(|id| id.as_str());
            seen.lock()
                .unwrap()
                .extend(task_id.map(|id| format!("task_created {id}")));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "follow_up".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let seen = Arc::clone(&later);
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let body = if let EventContent::Text(text) = &event.content {
                Some(text.body.clone())
            } else {
                None
            };
            seen.lock()
                .unwrap()
                .extend(body.map(|body| format!("after_broadcast {body}")));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    later
}

// Python: tests/test_hook_side_effects.py::TestARoundThatFailsMidway::test_a_set_whose_cursor_write_fails_once_it_ran
// (RMK-1515, HITL b7ba30c9): the store fails the room's delivery
// cursor write once, after the event's delivery set ran. The cursor's
// writer logs it and the set goes on (cursors.rs catch_up): its aftermath
// runs, the hook's task stored, its ON_TASK_CREATED and the event's
// AFTER_BROADCAST fired, nothing cancelled. The index stays this
// process's: the room's next message is delivered at once, the cursor
// moved past both, nothing skipped. Python's lane, which dropped that
// set's aftermath and skipped the index, now does the same.
#[tokio::test]
async fn a_set_whose_cursor_write_fails_once_it_ran() {
    let kit = kit_on(Failing::Cursor, &["sms1", "crm"]).await;
    let later = hooks_noting_what_follows(&kit);
    let skipped = roomkit::testing::EventTap::on(&kit, &["delivery_skipped"]);
    let options = || ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };

    // The room's records are accounted for: the next cursor write is the
    // one after the message's set ran.
    kit.store().armed.store(true, Ordering::SeqCst);
    let (warnings, _capturing) = Warnings::capture();
    let message = InboundMessage::new(
        channel("sms1"),
        "+15550001",
        TextContent::new("Call me back"),
    );
    let first = kit.process_inbound(message, options()).await.unwrap();
    let tasks = kit
        .store()
        .list_tasks(&room_id(), store::ListTasksOptions::default())
        .await
        .unwrap();
    roomkit::testing::idle(&kit).await;
    let after_first = sorted(later.lock().unwrap().clone());
    let message = InboundMessage::new(channel("sms1"), "+15550001", TextContent::new("Hi"));
    let second = kit.process_inbound(message, options()).await.unwrap();
    roomkit::testing::idle(&kit).await;
    let cursor = kit.store().get_delivered_index(&room_id()).await.unwrap();
    let latest = kit
        .store()
        .get_room(&room_id())
        .await
        .unwrap()
        .unwrap()
        .latest_index;
    let failed = (
        kit.store().armed.load(Ordering::SeqCst),
        warnings
            .containing("the room's delivery cursor could not be moved")
            .len(),
    );
    kit.close().await.unwrap();

    // The cursor write failed once, logged as a WARN.
    assert_eq!(failed, (false, 1));

    assert_eq!(
        (
            first.cancellation_reason.as_deref(),
            sorted(tasks.into_iter().map(|task| task.id)),
            after_first,
        ),
        (
            None,
            sorted(["t1"]),
            sorted(["after_broadcast Call me back", "task_created t1"]),
        )
    );
    assert_eq!(
        (
            second.cancellation_reason.as_deref(),
            skipped.types(),
            u64::try_from(cursor).ok(),
            latest,
        ),
        (None, Vec::<String>::new(), latest, latest)
    );
}

/// A transport whose delivery of `Call me back` waits until the test
/// opens it.
struct Gated {
    reached: Arc<tokio::sync::Notify>,
    opened: Arc<tokio::sync::Notify>,
}

impl Channel for Gated {
    fn channel_id(&self) -> ChannelId {
        channel("ws1")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let held =
            matches!(&event.content, EventContent::Text(text) if text.body == "Call me back");
        let (reached, opened) = (Arc::clone(&self.reached), Arc::clone(&self.opened));
        async move {
            if held {
                reached.notify_one();
                opened.notified().await;
            }
            Ok(ChannelOutput::default())
        }
    }
}

// Python: tests/test_hook_side_effects.py::TestARoundThatFailsMidway::test_two_sets_whose_second_cursor_write_fails_both_keep_their_aftermath
// (RMK-1515, the review's case): the second message is sent while the
// first's delivery waits, and only the second's cursor write fails. Both
// sets keep their aftermath, neither is cancelled, the room's next message
// is delivered, and the cursor catches up with all three.
#[tokio::test]
async fn two_sets_whose_second_cursor_write_fails_both_keep_their_aftermath() {
    let kit = kit_on(Failing::SecondCursor, &["sms1"]).await;
    let (reached, opened) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let gated = Gated {
        reached: Arc::clone(&reached),
        opened: Arc::clone(&opened),
    };
    kit.register_channel(gated).unwrap();
    kit.attach_channel(&room_id(), &channel("ws1"), AttachChannelOptions::default())
        .await
        .unwrap();
    let later = hooks_noting_what_follows(&kit);
    let skipped = roomkit::testing::EventTap::on(&kit, &["delivery_skipped"]);
    let send = |body: &'static str| {
        let kit = kit.clone();
        async move {
            let message = InboundMessage::new(channel("sms1"), "+15550001", TextContent::new(body));
            let options = ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            };
            kit.process_inbound(message, options).await.unwrap()
        }
    };

    // The room's records are accounted for: the next cursor writes are the
    // messages' own.
    kit.store().armed.store(true, Ordering::SeqCst);
    let (warnings, _capturing) = Warnings::capture();
    let first = tokio::spawn(send("Call me back"));
    tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let latest = || async {
        let room = kit.store().get_room(&room_id()).await.unwrap();
        room.and_then(|room| room.latest_index)
    };
    let before = latest().await;
    let second = tokio::spawn(send("Call me later"));
    // The second message is committed, its set placed behind the first's,
    // whose delivery waits.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while latest().await <= before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    opened.notify_one();
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    let tasks = kit
        .store()
        .list_tasks(&room_id(), store::ListTasksOptions::default())
        .await
        .unwrap();
    roomkit::testing::idle(&kit).await;
    let third = send("Hi").await;
    roomkit::testing::idle(&kit).await;
    let cursor = kit.store().get_delivered_index(&room_id()).await.unwrap();
    let latest = latest().await;
    let failed = (
        kit.store().failed_index.load(Ordering::SeqCst),
        warnings
            .containing("the room's delivery cursor could not be moved")
            .len(),
    );
    kit.close().await.unwrap();

    // The second message's cursor write failed, the first's went through.
    let second_index = second.event.as_ref().map(|event| event.index);
    assert_eq!((Some(failed.0), failed.1), (second_index, 1));
    assert_eq!(
        (
            [
                first.cancellation_reason.as_deref(),
                second.cancellation_reason.as_deref(),
                third.cancellation_reason.as_deref(),
            ],
            sorted(tasks.into_iter().map(|task| task.id)),
            sorted(later.lock().unwrap().clone()),
        ),
        (
            [None, None, None],
            sorted(["t1", "t2"]),
            sorted([
                "after_broadcast Call me back",
                "after_broadcast Call me later",
                "after_broadcast Hi",
                "task_created t1",
                "task_created t2",
            ]),
        )
    );
    assert_eq!(
        (skipped.types(), u64::try_from(cursor).ok()),
        (Vec::<String>::new(), latest)
    );
}
