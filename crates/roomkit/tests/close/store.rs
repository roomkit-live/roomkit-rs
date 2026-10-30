//! A store whose writes may never end, as a stuck database's, and whose
//! reads by id may fail.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventUpdates, Identity, IdentityId,
    Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId, RoomUpdates,
    Task,
};
use roomkit::{
    ConversationStore, EventFilter, EventQuery, InMemoryStore, RoomCounters, RoomFilter,
    StoreError, TargetUpdate, text_of,
};
use tokio::sync::Notify;

/// An in-memory store whose commit of a text, or whose writes of a room,
/// may never end, telling `committing` they started, and whose reads of an
/// event by its id may fail.
#[derive(Debug, Default)]
pub(crate) struct StuckStore {
    store: InMemoryStore,
    pub(crate) committing: Arc<Notify>,
    /// The commit of this text never ends, its row written first when
    /// `true`; it tells `committing` it started.
    hangs: Option<(&'static str, bool)>,
    /// Whether a read of an event by its id fails.
    read_back_fails: bool,
    /// Once set, a write of a room never ends; it tells `committing` it
    /// started.
    pub(crate) rooms_hang: Arc<AtomicBool>,
    /// The writes dropped before they ended, cut: "commit", "room".
    pub(crate) cut: Arc<Mutex<Vec<&'static str>>>,
}

/// Notes a write dropped before it ended.
struct CutNote {
    cut: Arc<Mutex<Vec<&'static str>>>,
    what: &'static str,
}

impl Drop for CutNote {
    fn drop(&mut self) {
        self.cut.lock().unwrap().push(self.what);
    }
}

impl StuckStore {
    /// A store whose commit of `text` never ends, its row written first
    /// when `written`.
    pub(crate) fn hanging(text: &'static str, written: bool) -> Self {
        Self {
            hangs: Some((text, written)),
            ..Self::default()
        }
    }

    /// The same store, whose reads of an event by its id fail.
    pub(crate) fn failing_read_back(self) -> Self {
        Self {
            read_back_fails: true,
            ..self
        }
    }

    /// Whether a write of `what` that never ends was cut.
    pub(crate) fn was_cut(&self, what: &str) -> bool {
        self.cut.lock().unwrap().contains(&what)
    }

    /// Never ends, telling `committing` it started; a drop notes `what`
    /// cut.
    async fn hang(&self, what: &'static str) {
        let _note = CutNote {
            cut: Arc::clone(&self.cut),
            what,
        };
        self.committing.notify_one();
        std::future::pending::<()>().await;
    }
}

impl ConversationStore for StuckStore {
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        self.store.create_room(room).await
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        if self.rooms_hang.load(Ordering::SeqCst) {
            self.hang("room").await;
        }
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
        let text = text_of(&event.content);
        if let Some((hangs, written)) = self.hangs
            && text == hangs
        {
            if written {
                self.store.commit(event, counters, target).await?;
            }
            self.hang("commit").await;
            return Ok(());
        }
        self.store.commit(event, counters, target).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        if self.read_back_fails {
            return Err(StoreError::Value("the store is gone".to_owned()));
        }
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
