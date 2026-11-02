//! What `delegate` and the background helpers refuse, and in which words
//! (RFC §23.3, §23.4): a parent room the store fails to read, a hand-back
//! they cannot keep, an agent that is none of the room's, each as the
//! Python implementation's (`core/mixins/delegation.py`,
//! `core/mixins/background.py`, `tasks/background.py`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::atomic::{AtomicBool, Ordering};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    ConversationStore, CreateRoomOptions, DelegateOptions, InMemoryStore, RoomKit, RoomKitError,
    RoomKitOptions, StartTaskOptions, StoreError, StoreFailure, store,
};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r").unwrap()
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

/// An in-memory store whose reads of a room fail once `down` is set, as a
/// database gone does.
#[derive(Debug, Default)]
struct RoomReadsFail {
    store: InMemoryStore,
    down: AtomicBool,
}

impl ConversationStore for RoomReadsFail {
    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new(
                "the database is gone",
            )));
        }
        self.store.get_room(room_id).await
    }

    forward! {
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
        list_participants(room_id: &RoomId) -> Vec<Participant>;
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

/// A kit holding the room `r` and the agent `worker`, its agent nowhere.
async fn kit_with<S: ConversationStore>(store: S) -> RoomKit<S> {
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    let worker = EchoAgent::new(channel("worker"), "Found it.", EchoAgentOptions::default());
    kit.register_channel(worker).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit
}

// Python: core/mixins/delegation.py delegate, whose get_room lets the
// store's failure through; RoomNotFoundError only for a room missing or
// another organization's (no test of its own: RMK-1192).
#[tokio::test]
async fn a_parent_room_the_store_fails_to_read_fails_the_delegation_with_its_error() {
    let kit = kit_with(RoomReadsFail::default()).await;
    kit.store().down.store(true, Ordering::SeqCst);

    let refused = kit
        .delegate(
            &room(),
            &channel("worker"),
            "Find it.",
            DelegateOptions::default(),
        )
        .await;

    let refusal = refused.err().unwrap();
    assert_eq!(
        (
            matches!(refusal, RoomKitError::Store(_)),
            refusal.to_string().contains("the database is gone")
                || std::error::Error::source(&refusal)
                    .is_some_and(|cause| cause.to_string().contains("the database is gone"))
        ),
        (true, true),
        "{refusal:?}"
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_delegation_brief_and_hand_back.py::test_an_unknown_hand_back_and_a_negative_conversation_are_refused
// (the conversation, a u64 here, is never negative); its words, the
// name quoted as Python's `!r` writes it.
#[tokio::test]
async fn an_unknown_hand_back_and_a_post_by_no_agent_of_the_room_are_refused() {
    let kit = kit_with(InMemoryStore::default()).await;
    let unknown = DelegateOptions {
        hand_back: "shout".to_owned(),
        ..DelegateOptions::default()
    };
    let posted = DelegateOptions {
        hand_back: "post".to_owned(),
        ..DelegateOptions::default()
    };

    let words = |outcome: Result<_, RoomKitError>| match outcome {
        Err(RoomKitError::Value(message)) => message,
        other => format!("not a ValueError: {:?}", other.map(|_| ())),
    };
    let refusals = (
        words(
            kit.delegate(&room(), &channel("worker"), "Count.", unknown)
                .await,
        ),
        words(
            kit.delegate(&room(), &channel("worker"), "Count.", posted)
                .await,
        ),
    );

    assert_eq!(
        refusals,
        (
            "hand_back must be one of ('relay', 'post'), not 'shout'".to_owned(),
            "hand_back='post' posts as 'worker', which must be an intelligence channel \
             attached to room 'r'"
                .to_owned()
        )
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_background_tasks.py::test_start_task_refuses_a_channel_that_is_no_agent_of_the_room
#[tokio::test]
async fn start_task_refuses_a_channel_that_is_no_agent_of_the_room() {
    let kit = kit_with(InMemoryStore::default()).await;

    let refused = kit
        .start_task(
            &room(),
            &channel("worker"),
            "Count.",
            StartTaskOptions::default(),
        )
        .await;

    assert_eq!(
        refused.err().map(|refusal| refusal.to_string()),
        Some(RoomKitError::Value("'worker' is not an agent of room 'r'".to_owned()).to_string())
    );
    kit.close().await.unwrap();
}
