//! A `cancel_task` asked while another cancel of the same task ends: one
//! of that cancel's calls only while the task ended `cancelled`, the call
//! could have cancelled it running, and the end has not decided whom it
//! hands the cancellation back to; otherwise answered that the task ended
//! (RFC §23.4): Python's `tests/tasks/test_cancel_joins_an_ending.py`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::hook::AllowOptions;
use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventType, HookResult, Identity, IdentityId,
    Observation, Participant, ParticipantId, Room, RoomEvent, RoomId, Task, TaskId, ThreadSummary,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::json;
use tokio::sync::Notify;

use super::{
    BOUND, CANCELLED, ENDED, Fixture, Lagging, Latch, Worker, answer, cancel, channel, on_bus,
    post_line, room_id, until,
};
use crate::orchestration::StatusLevel;
use crate::store::{self, ConversationStore, InMemoryStore, StoreError};
use crate::testing::idle;
use crate::{CancelTaskOptions, DelegateOptions, HookOptions, RoomKit, RoomKitOptions};

/// An in-memory store whose read of room `r`'s participants, once `hold`
/// armed it, tells `entered` then waits for `go`: the read a task's end
/// makes of the room it tells `ON_TASK_COMPLETED` in, which a store slow
/// to answer holds (Python's `_held_end`).
#[derive(Debug, Default)]
struct HeldStore {
    store: InMemoryStore,
    armed: AtomicBool,
    entered: Notify,
    go: Notify,
}

impl HeldStore {
    fn hold(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn entered(&self) {
        tokio::time::timeout(BOUND, self.entered.notified())
            .await
            .unwrap();
    }
}

/// Forwards each of the store's methods named to the in-memory store.
macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $answer:ty;)*) => {
        $(
            async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
                self.store.$name($($argument),*).await
            }
        )*
    };
}

impl ConversationStore for HeldStore {
    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        if room_id.as_str() == "r" && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.go.notified().await;
        }
        self.store.list_participants(room_id).await
    }

    forward! {
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

/// A room over a store whose read of the room an end makes is held.
async fn held_room(worker: Worker) -> Fixture<HeldStore> {
    Fixture::open(worker, RoomKitOptions::with_store(HeldStore::default())).await
}

/// Waits until `channel_id`'s call joined the cancel of `task_id` under
/// way.
async fn joined<S: ConversationStore>(kit: &RoomKit<S>, task_id: &str, channel_id: &str) {
    let cancels = &kit.inner.task_cancels;
    until(|| {
        cancels
            .get(task_id)
            .is_some_and(|cancel| cancel.asked(channel_id))
    })
    .await;
}

// Python: tests/tasks/test_cancel_joins_an_ending.py::test_an_agent_cancelling_as_the_host_s_cancel_ends_joins_it
#[tokio::test]
async fn an_agent_cancelling_as_the_host_s_cancel_ends_joins_it() {
    let room = held_room(Worker::endless()).await;
    let task = room.running_task("r").await;
    room.kit.store().hold();

    let (kit, id) = (room.kit.clone(), task.id.clone());
    let host =
        tokio::spawn(async move { kit.cancel_task(&id, CancelTaskOptions::default()).await });
    room.kit.store().entered().await;
    let (kit, id) = (room.kit.clone(), task.id.clone());
    let agent = tokio::spawn(async move { cancel(&kit, &id).await });
    joined(&room.kit, &task.id, "speaker").await;
    room.kit.store().go.notify_one();
    let answers = (host.await.unwrap(), agent.await.unwrap());
    idle(&room.kit).await;

    assert_eq!(
        (
            answers,
            room.ended(),
            room.kit.inner.task_cancels.get(&task.id).is_none(),
            room.handed_back()
        ),
        (
            (true, answer(&task.id, "cancelled", CANCELLED)),
            vec!["cancelled".to_owned()],
            true,
            Vec::<String>::new()
        )
    );
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_joins_an_ending.py::test_a_task_that_completed_as_the_host_cancelled_is_answered_at_once
#[tokio::test]
async fn a_task_that_completed_as_the_host_cancelled_is_answered_at_once() {
    let room = held_room(Worker::endless()).await;
    let task = room.running_task("r").await;
    room.kit.store().hold();
    room.worker.release.open();
    room.kit.store().entered().await;
    let (kit, id) = (room.kit.clone(), task.id.clone());
    let host =
        tokio::spawn(async move { kit.cancel_task(&id, CancelTaskOptions::default()).await });
    let cancels = &room.kit.inner.task_cancels;
    until(|| cancels.get(&task.id).is_some()).await;

    let answered = tokio::time::timeout(Duration::from_secs(1), cancel(&room.kit, &task.id)).await;
    room.kit.store().go.notify_one();
    tokio::time::timeout(BOUND, host).await.unwrap().unwrap();

    assert_eq!(answered.ok(), Some(answer(&task.id, "completed", ENDED)));
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_joins_an_ending.py::test_a_call_after_the_hand_back_was_decided_is_answered_ended
#[tokio::test]
async fn a_call_after_the_hand_back_was_decided_is_answered_ended() {
    let (bus, flush) = Lagging::bus();
    let room = Fixture::open(Worker::endless(), on_bus(&bus)).await;
    let (handing_back, go_on) = (Arc::new(Notify::new()), Latch::new());
    let (told, held) = (Arc::clone(&handing_back), go_on.clone());
    let instructions = HookOptions {
        event_types: Some([EventType::Instruction].into()),
        ..HookOptions::default()
    };
    room.kit
        .hook(
            HookTrigger::BeforeBroadcast,
            move |_event: RoomEvent, _| {
                let (told, held) = (Arc::clone(&told), held.clone());
                async move {
                    told.notify_one();
                    held.passed().await;
                    HookResult::allow(AllowOptions::default())
                }
            },
            instructions,
        )
        .unwrap();
    let task = room.running_task("r").await;
    let (kit, id) = (room.kit.clone(), task.id.clone());
    let host =
        tokio::spawn(async move { kit.cancel_task(&id, CancelTaskOptions::default()).await });
    tokio::time::timeout(BOUND, handing_back.notified())
        .await
        .unwrap();

    let answered = tokio::time::timeout(Duration::from_secs(1), cancel(&room.kit, &task.id)).await;
    go_on.open();
    tokio::time::timeout(BOUND, host).await.unwrap().unwrap();
    flush.open();
    idle(&room.kit).await;

    assert_eq!(
        (answered.ok(), room.handed_back()),
        (
            Some(answer(&task.id, "cancelled", ENDED)),
            vec!["cancelled".to_owned()]
        )
    );
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_joins_an_ending.py::test_a_strategy_s_task_the_host_cancels_is_not_the_agent_s_to_join
#[tokio::test]
async fn a_strategy_s_task_the_host_cancels_is_not_the_agent_s_to_join() {
    let room = held_room(Worker::endless()).await;
    let delegation = DelegateOptions {
        post_status: false,
        ..DelegateOptions::default()
    };
    let task = room
        .kit
        .delegate(&room_id("r"), &channel("worker"), "Count.", delegation)
        .await
        .unwrap();
    room.worker.started().await;
    let child = json!({ "child_room_id": task.child_room_id });
    post_line(room.kit.status_bus(), StatusLevel::Info, &task.id, child).await;
    room.kit.store().hold();
    let (kit, id) = (room.kit.clone(), task.id.clone());
    let host =
        tokio::spawn(async move { kit.cancel_task(&id, CancelTaskOptions::default()).await });
    room.kit.store().entered().await;

    let answered = tokio::time::timeout(Duration::from_secs(1), cancel(&room.kit, &task.id)).await;
    room.kit.store().go.notify_one();
    tokio::time::timeout(BOUND, host).await.unwrap().unwrap();

    assert_eq!(answered.ok(), Some(answer(&task.id, "cancelled", ENDED)));
    room.kit.close().await.unwrap();
}
