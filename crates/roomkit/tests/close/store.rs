//! A store whose writes may never end, as a stuck database's, and whose
//! reads by id may fail.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventId, EventType, Identity, IdentityId,
    Metadata, Observation, Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::store;
use roomkit::{ConversationStore, InMemoryStore, StoreError};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

/// A committed row and whether the store was already released then.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommittedRow {
    pub(crate) kind: EventType,
    pub(crate) after_close: bool,
    pub(crate) error: Option<String>,
}

/// An in-memory store whose commit of a text, or whose writes of a room,
/// may never end, telling `committing` they started, and whose reads of an
/// event by its id may fail.
#[derive(Debug, Default)]
pub(crate) struct StuckStore {
    store: InMemoryStore,
    pub(crate) commits: Mutex<Vec<CommittedRow>>,
    pub(crate) yield_commits: AtomicBool,
    pub(crate) commit_steps: Arc<Mutex<Vec<String>>>,
    /// Suspend each cursor read, including those of a turn's terminal rows.
    pub(crate) cursor_delay: Mutex<Option<std::time::Duration>>,
    pub(crate) delayed_cursors: AtomicUsize,
    pub(crate) committing: Arc<Notify>,
    /// A read held at the store boundary until `release_read` is notified.
    pub(crate) held_read: Mutex<Option<&'static str>>,
    pub(crate) reading: Notify,
    /// Context reads attempted after the store was released.
    pub(crate) reads_after_close: AtomicUsize,
    /// Hold tA after its delivery, keeping that call unfinished.
    pub(crate) hold_task: AtomicBool,
    pub(crate) task_started: Notify,
    pub(crate) release_task: Notify,
    pub(crate) delayed_read: Mutex<Option<(&'static str, std::time::Duration)>>,
    pub(crate) release_read: Notify,
    /// The commit of this text never ends, its row written first when
    /// `true`; it tells `committing` it started.
    hangs: Option<(&'static str, bool)>,
    /// Whether a read of an event by its id fails.
    read_back_fails: bool,
    /// Whether a read of an event by its id never ends.
    read_back_hangs: bool,
    /// Once set, the writes of a room's delivery cursor left before one
    /// never ends.
    pub(crate) cursor_writes_left: Arc<Mutex<Option<usize>>>,
    /// Once set, a write of a room never ends; it tells `committing` it
    /// started.
    pub(crate) rooms_hang: Arc<AtomicBool>,
    /// The writes dropped before they ended, cut: "commit", "room".
    pub(crate) cut: Arc<Mutex<Vec<&'static str>>>,
    /// Its closes, each saying whether the channel `closed_before` says
    /// closed was by then.
    pub(crate) closes: Arc<Mutex<Vec<bool>>>,
    /// Whether a channel was closed, which each close of the store reads.
    pub(crate) closed_before: Arc<AtomicBool>,
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
    async fn commit(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: store::CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        if self.yield_commits.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let stored = self.store.commit_event(room_id, event, options).await?;
        if self.yield_commits.load(Ordering::SeqCst) {
            self.commit_steps
                .lock()
                .unwrap()
                .push(format!("committed {}", text_of(&stored.content)));
        }
        let error = if let EventContent::ToolCall(call) = &stored.content {
            call.error.clone()
        } else {
            None
        };
        self.commits.lock().unwrap().push(CommittedRow {
            kind: stored.r#type,
            after_close: !self.closes.lock().unwrap().is_empty(),
            error,
        });
        Ok(stored)
    }

    async fn read(&self, name: &str) {
        if !self.closes.lock().unwrap().is_empty() {
            self.reads_after_close.fetch_add(1, Ordering::SeqCst);
        }
        let delay = {
            let mut delayed = self.delayed_read.lock().unwrap();
            if delayed.as_ref().is_some_and(|(at, _)| *at == name) {
                delayed.take().map(|(_, delay)| delay)
            } else {
                None
            }
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let held = self
            .held_read
            .lock()
            .unwrap()
            .is_some_and(|held| held == name);
        if held {
            self.reading.notify_one();
            self.release_read.notified().await;
        }
    }

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

    /// The same store, whose reads of an event by its id never end.
    pub(crate) fn hanging_read_back(self) -> Self {
        Self {
            read_back_hangs: true,
            ..self
        }
    }

    /// The same store, whose closes read whether the channel `closed`
    /// says closed was by then.
    pub(crate) fn closing_after(self, closed: &Arc<AtomicBool>) -> Self {
        Self {
            closed_before: Arc::clone(closed),
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
    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.create_room(room).await
    }

    async fn close(&self) {
        let channel_closed = self.closed_before.load(Ordering::SeqCst);
        self.closes.lock().unwrap().push(channel_closed);
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.read("room").await;
        self.store.get_room(room_id).await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        if self.rooms_hang.load(Ordering::SeqCst) {
            self.hang("room").await;
        }
        self.store.update_room(room).await
    }

    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: store::PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        if self.rooms_hang.load(Ordering::SeqCst) {
            self.hang("room").await;
        }
        self.store
            .patch_room_metadata(room_id, patch, options)
            .await
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

    async fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: store::FindRoomIdsByChannelOptions,
    ) -> Result<Vec<RoomId>, StoreError> {
        self.store
            .find_room_ids_by_channel(channel_id, options)
            .await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: store::FindRoomIdByBindingOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_binding(channel_id, participant_id, options)
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: store::FindRoomIdByParticipantOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_participant(channel_id, participant_id, options)
            .await
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        let delay = *self.cursor_delay.lock().unwrap();
        if let Some(delay) = delay {
            self.delayed_cursors.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(delay).await;
        }
        self.read("cursor").await;
        self.store.get_delivered_index(room_id).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: store::AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        let hangs = {
            let mut left = self.cursor_writes_left.lock().unwrap();
            match left.as_mut() {
                Some(0) => true,
                Some(count) => {
                    *count -= 1;
                    false
                }
                None => false,
            }
        };
        if hangs {
            self.hang("cursor").await;
        }
        self.store
            .advance_delivered_index(room_id, index, options)
            .await
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        self.store.get_private_state(room_id, key).await
    }

    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        self.store.set_private_state(room_id, key, value).await
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
        self.read("bindings").await;
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
        self.read("identity").await;
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

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: store::UnlinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.store
            .unlink_address(identity_id, channel_type, address, options)
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

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.store
            .get_event_by_idempotency_key(room_id, key, channel_id)
            .await
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: store::CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        let text = text_of(&event.content);
        if let Some((hangs, written)) = self.hangs
            && text == hangs
        {
            let committed = if written {
                Some(self.commit(room_id, event, options).await?)
            } else {
                None
            };
            self.hang("commit").await;
            return committed.ok_or_else(|| StoreError::Value("never reached".to_owned()));
        }
        self.commit(room_id, event, options).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        if self.read_back_fails {
            return Err(StoreError::Value("the store is gone".to_owned()));
        }
        if self.read_back_hangs {
            std::future::pending::<()>().await;
        }
        self.store.get_event(event_id).await
    }

    async fn update_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.store.update_event(event).await
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
        if self.hold_task.load(Ordering::SeqCst) && task.id.as_str() == "tA" {
            self.task_started.notify_one();
            self.release_task.notified().await;
        }
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
