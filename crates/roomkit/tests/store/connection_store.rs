//! A minimal backend whose connection entry can be counted independently
//! of the default context loader under test.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::{ConversationStore, InMemoryStore, StoreError, store};

tokio::task_local! {
    static SCOPE_ID: usize;
}

#[derive(Debug, Default)]
pub(super) struct ScopedReads {
    inner: InMemoryStore,
    pub(super) entries: AtomicUsize,
    pub(super) parent_reads: Mutex<Vec<Option<usize>>>,
}

macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),* $(,)?) -> $answer:ty;)*) => {
        $(async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
            self.inner.$name($($argument),*).await
        })*
    };
}

impl ConversationStore for ScopedReads {
    async fn connection<'a, W, T, E>(&'a self, work: W) -> Result<T, E>
    where
        W: Future<Output = Result<T, E>> + Send + 'a,
        T: Send + 'a,
        E: From<StoreError> + Send + 'a,
    {
        let id = self.entries.fetch_add(1, Ordering::Relaxed);
        SCOPE_ID.scope(id, work).await
    }

    async fn get_conversation(
        &self,
        room_id: &RoomId,
        options: store::GetConversationOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        if room_id.as_str() == "parent" {
            self.parent_reads
                .lock()
                .unwrap()
                .push(SCOPE_ID.try_with(|id| *id).ok());
        }
        self.inner.get_conversation(room_id, options).await
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<roomkit::model::Metadata>, StoreError> {
        if room_id.as_str() == "parent" && key == "author_register" {
            self.parent_reads
                .lock()
                .unwrap()
                .push(SCOPE_ID.try_with(|id| *id).ok());
            // Stop before a child room is created and its own context reads begin.
            return Err(StoreError::Value("parent register unavailable".to_owned()));
        }
        self.inner.get_private_state(room_id, key).await
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
            options: store::AdvanceDeliveredIndexOptions,
        ) -> bool;
        add_binding(binding: ChannelBinding) -> ChannelBinding;
        get_binding(
            room_id: &RoomId,
            channel_id: &ChannelId,
        ) -> Option<ChannelBinding>;
        update_binding(binding: ChannelBinding) -> ChannelBinding;
        remove_binding(
            room_id: &RoomId,
            channel_id: &ChannelId,
        ) -> bool;
        list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
        add_participant(participant: Participant) -> Participant;
        get_participant(
            room_id: &RoomId,
            participant_id: &ParticipantId,
        ) -> Option<Participant>;
        update_participant(participant: Participant) -> Participant;
        delete_participant(
            room_id: &RoomId,
            participant_id: &ParticipantId,
        ) -> ();
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_identity(identity: Identity) -> Identity;
        get_identity(identity_id: &IdentityId) -> Option<Identity>;
        resolve_identity(
            channel_type: &ChannelType,
            address: &str,
            options: store::ResolveIdentityOptions,
        ) -> Option<Identity>;
        link_address(
            identity_id: &IdentityId,
            channel_type: &ChannelType,
            address: &str,
            options: store::LinkAddressOptions,
        ) -> ();
        check_idempotency(
            room_id: &RoomId,
            key: &str,
            channel_id: Option<&ChannelId>,
        ) -> bool;
        commit_event(
            room_id: &RoomId,
            event: RoomEvent,
            options: store::CommitEventOptions,
        ) -> RoomEvent;
        get_event(event_id: &EventId) -> Option<RoomEvent>;
        update_event(event: RoomEvent) -> RoomEvent;
        delete_event(
            room_id: &RoomId,
            event_id: &EventId,
            options: store::DeleteEventOptions,
        ) -> Vec<RoomEvent>;
        get_event_count(
            room_id: &RoomId,
            options: store::GetEventCountOptions,
        ) -> u64;
        list_events(
            room_id: &RoomId,
            options: store::ListEventsOptions,
        ) -> Vec<RoomEvent>;
        add_task(task: Task) -> Task;
        add_observation(observation: Observation) -> Observation;
        list_tasks(
            room_id: &RoomId,
            options: store::ListTasksOptions,
        ) -> Vec<Task>;
        list_observations(room_id: &RoomId) -> Vec<Observation>;
        get_task(task_id: &roomkit::model::TaskId) -> Option<Task>;
        update_task(task: Task) -> Task;
        add_event(event: RoomEvent) -> RoomEvent;
        mark_read(
            room_id: &RoomId,
            channel_id: &ChannelId,
            event_id: &EventId,
        ) -> ();
        mark_all_read(
            room_id: &RoomId,
            channel_id: &ChannelId,
        ) -> ();
        get_unread_count(
            room_id: &RoomId,
            channel_id: &ChannelId,
        ) -> u64;
        list_read_markers(room_id: &RoomId) -> std::collections::BTreeMap<ChannelId, u64>;
        find_latest_room(
            participant_id: &ParticipantId,
            options: store::FindLatestRoomOptions,
        ) -> Option<Room>;
        find_room_id_by_channel(
            channel_id: &ChannelId,
            options: store::FindRoomIdByChannelOptions,
        ) -> Option<RoomId>;
        get_thread_summaries(
            room_id: &RoomId,
            root_event_ids: &[EventId],
        ) -> std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>;
    }
}
