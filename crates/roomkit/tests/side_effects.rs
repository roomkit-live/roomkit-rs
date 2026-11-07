//! The side effects a delivery set stores (RFC §10.1 step 15) when the
//! store fails one of their writes: what no shared scenario can script, a
//! store that fails (Python's `tests/test_hook_side_effects.py`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventId, HookResult, Identity,
    IdentityId, InboundMessage, Metadata, Observation, ObservationId, Participant, ParticipantId,
    Room, RoomContext, RoomEvent, RoomId, Task, TaskId, TextContent,
};
use roomkit::store::PatchRoomMetadataOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    HookExecution, HookOptions, HookTrigger, InMemoryStore, ProcessInboundOptions, RoomKit,
    RoomKitOptions, StoreError, StoreFailure, store,
};

#[path = "support/warnings.rs"]
mod warnings;

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
}

/// An in-memory store that fails the write `failing` names.
#[derive(Debug)]
struct FailingWrites {
    store: InMemoryStore,
    failing: Failing,
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
        self.store.add_task(task).await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        if self.failing == Failing::Observation && observation.id.as_str() == "o1" {
            return Err(down());
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

    forward! {
        get_room(room_id: &RoomId) -> Option<Room>;
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        get_delivered_index(room_id: &RoomId) -> i64;
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

/// A transport whose delivery writes the room's `stage`, Python's
/// `UpdatingChannel("crm", {"stage": "qualified"})`.
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
        let mut output = ChannelOutput::default();
        output
            .metadata_updates
            .insert("stage".to_owned(), "qualified".into());
        ready(Ok(output))
    }
}

/// A hook's two tasks and two observations, `t1`, `t2`, `o1`, `o2`.
fn triage() -> HookResult {
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
    HookResult {
        tasks,
        observations,
        ..HookResult::allow(AllowOptions::default())
    }
}

/// A kit on a store failing `failing`, its room `r1` binding `sms1` and
/// the CRM, a hook giving [`triage`]'s side effects, and the ids of the
/// tasks whose `ON_TASK_CREATED` ran.
async fn triaging_room(failing: Failing) -> (RoomKit<FailingWrites>, Arc<Mutex<Vec<String>>>) {
    let store = FailingWrites {
        store: InMemoryStore::default(),
        failing,
    };
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
    for bound in ["sms1", "crm"] {
        kit.attach_channel(&room_id(), &channel(bound), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async { Ok::<_, ChannelError>(triage()) },
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

// Python: tests/test_hook_side_effects.py::TestSideEffectWriteFailures::test_a_failed_side_effect_write_is_logged_and_the_call_goes_on
// (RMK-1498): a side effect the store fails to write is one ERROR line, the
// others still written; the call goes on, its error its turn's, and only a
// task stored fires ON_TASK_CREATED.
#[tokio::test]
async fn a_failed_side_effect_write_is_logged_and_the_call_goes_on() {
    for failing in [Failing::Task, Failing::Observation, Failing::Metadata] {
        let (kit, created) = triaging_room(failing).await;
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
        let ids = |ids: Vec<String>| {
            let mut ids = ids;
            ids.sort();
            ids
        };
        let tasks = store
            .list_tasks(&room_id(), store::ListTasksOptions::default())
            .await
            .unwrap();
        let tasks = ids(tasks.into_iter().map(|task| task.id.to_string()).collect());
        let observations = store.list_observations(&room_id()).await.unwrap();
        let observations = ids(observations.into_iter().map(|o| o.id.to_string()).collect());
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        let stage = room.metadata.get("stage").and_then(|stage| stage.as_str());
        let created = ids(created.lock().unwrap().clone());
        kit.close().await.unwrap();

        let expected = match failing {
            Failing::Task => "Failed to persist task t1",
            Failing::Observation => "Failed to persist observation o1",
            Failing::Metadata => "Failed to write the room metadata updates ['stage']",
        };
        let lines = warnings.containing("ERROR");
        assert_eq!(
            (
                result.error.is_none(),
                result.blocked,
                lines.len(),
                lines.iter().all(|line| line.contains(expected)),
            ),
            (true, false, 1, true),
            "{failing:?}: {result:?} {lines:?}"
        );
        let all_tasks = if failing == Failing::Task {
            vec!["t2"]
        } else {
            vec!["t1", "t2"]
        };
        let all_observations = if failing == Failing::Observation {
            vec!["o2"]
        } else {
            vec!["o1", "o2"]
        };
        let all_stage = (failing != Failing::Metadata).then_some("qualified");
        assert_eq!(
            (tasks, created, observations, stage),
            (
                all_tasks.iter().map(|id| (*id).to_owned()).collect(),
                all_tasks.iter().map(|id| (*id).to_owned()).collect(),
                all_observations.iter().map(|id| (*id).to_owned()).collect(),
                all_stage,
            ),
            "{failing:?}"
        );
    }
}
