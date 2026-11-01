//! A store implementing what Python's `ConversationStore` makes abstract
//! and nothing else, over the in-memory store: the trait's provided
//! methods serve it, as Python's base class serves a store that overrides
//! none of them (RFC §14.1). A test binary includes it by path.

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::{ConversationStore, InMemoryStore, StoreError, store};

/// The in-memory store, behind the methods Python's ABC makes abstract.
#[derive(Debug, Default)]
pub(crate) struct AbstractOnly {
    pub(crate) store: InMemoryStore,
    /// Its `update_event` writes the row as given, its delivery results
    /// included, as a custom store may (Python's `_WritesTheRowAsGiven`);
    /// otherwise it keeps the stored results, as the trait says.
    pub(crate) writes_the_row_as_given: bool,
}

impl ConversationStore for AbstractOnly {
    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.create_room(room).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.store.get_room(room_id).await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.update_room(room).await
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.store.delete_room(room_id).await
    }

    async fn list_rooms(&self, options: store::ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.store.list_rooms(options).await
    }

    async fn find_rooms(&self, options: store::FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.store.find_rooms(options).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: store::AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        self.store
            .advance_delivered_index(room_id, index, options)
            .await
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.store.add_binding(binding).await
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        self.store.get_binding(room_id, channel_id).await
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.store.update_binding(binding).await
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.store.remove_binding(room_id, channel_id).await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.store.list_bindings(room_id).await
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        self.store.add_participant(participant).await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.store.get_participant(room_id, participant_id).await
    }

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
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

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        self.store.create_identity(identity).await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.store.get_identity(identity_id).await
    }

    async fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: store::ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        self.store
            .resolve_identity(channel_type, address, options)
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: store::LinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.store
            .link_address(identity_id, channel_type, address, options)
            .await
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        self.store.check_idempotency(room_id, key, channel_id).await
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: store::CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        self.store.commit_event(room_id, event, options).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.store.get_event(event_id).await
    }

    async fn update_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        let results = event.delivery_results.clone();
        let updated = self.store.update_event(event).await?;
        if self.writes_the_row_as_given {
            self.store
                .record_delivery_results(&updated.room_id, &updated.id, results)
                .await?;
        }
        Ok(updated)
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: store::DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.delete_event(room_id, event_id, options).await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: store::GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        self.store.get_event_count(room_id, options).await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: store::ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.list_events(room_id, options).await
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        self.store.add_task(task).await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        self.store.add_observation(observation).await
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: store::ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        self.store.list_tasks(room_id, options).await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.store.list_observations(room_id).await
    }

    async fn get_task(&self, task_id: &roomkit::model::TaskId) -> Result<Option<Task>, StoreError> {
        self.store.get_task(task_id).await
    }

    async fn update_task(&self, task: Task) -> Result<Task, StoreError> {
        self.store.update_task(task).await
    }

    async fn add_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.store.add_event(event).await
    }

    async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> Result<(), StoreError> {
        self.store.mark_read(room_id, channel_id, event_id).await
    }

    async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.store.mark_all_read(room_id, channel_id).await
    }

    async fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<u64, StoreError> {
        self.store.get_unread_count(room_id, channel_id).await
    }

    async fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> Result<std::collections::BTreeMap<ChannelId, u64>, StoreError> {
        self.store.list_read_markers(room_id).await
    }

    async fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: store::FindLatestRoomOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.store.find_latest_room(participant_id, options).await
    }

    async fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: store::FindRoomIdByChannelOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_channel(channel_id, options)
            .await
    }

    async fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> Result<std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>, StoreError>
    {
        self.store
            .get_thread_summaries(room_id, root_event_ids)
            .await
    }
}
