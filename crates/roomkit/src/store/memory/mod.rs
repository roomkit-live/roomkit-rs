//! The in-memory store (RFC §14.2): for tests, prototypes and single-process
//! deployments.

mod events;
mod identities;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventStatus, EventUpdates, Identity,
    IdentityId, Metadata, Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent,
    RoomId, RoomUpdates, Task,
};
use roomkit_core::ops::{RoomCounters, TargetUpdate};
use roomkit_core::rules::{idempotency, routing};

use self::identities::Identities;
use super::{ConversationStore, EventFilter, RoomFilter, StoreError};

/// The key of a room's private state holding its register of authors (RFC
/// §10.1 step 12, §14.1), which a context carries.
const AUTHOR_REGISTER: &str = "author_register";

/// A store that keeps everything in memory, for one process.
///
/// Every read returns copies and every write copies in, so no caller can
/// change stored state outside the store's methods (RFC §14.4). A room's
/// private state is kept apart from its metadata (RFC §14.1).
#[derive(Debug, Default)]
pub struct InMemoryStore {
    rooms: Mutex<HashMap<RoomId, Record>>,
    identities: Mutex<Identities>,
}

#[derive(Debug)]
struct Record {
    room: Room,
    bindings: Vec<ChannelBinding>,
    participants: Vec<Participant>,
    events: Vec<RoomEvent>,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
    /// The room's private state, by key (RFC §14.1).
    private: BTreeMap<String, Metadata>,
}

impl Record {
    /// Writes `updates` on the room, its private state apart: a value
    /// written or removed under a key removes the metadata's copy under it
    /// (RFC §14.1 `set_private_state`).
    fn update(&mut self, mut updates: RoomUpdates) {
        for (key, value) in std::mem::take(&mut updates.private_state) {
            self.room.metadata.remove(&key);
            match value {
                Some(value) => {
                    self.private.insert(key, value);
                }
                None => {
                    self.private.remove(&key);
                }
            }
        }
        updates.apply(&mut self.room);
    }

    /// Whether the room has a participant with the id.
    fn has_participant(&self, participant_id: &ParticipantId) -> bool {
        self.participants
            .iter()
            .any(|participant| &participant.id == participant_id)
    }

    /// Whether `filter` admits the room.
    fn admitted_by(&self, filter: &RoomFilter) -> bool {
        filter.matches(&self.room, &self.bindings, &self.participants)
    }

    /// Whether the room's binding of `channel_id` names `participant_id` or
    /// `identity_id`.
    fn binds_to(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
    ) -> bool {
        self.bindings.iter().any(|binding| {
            &binding.channel_id == channel_id
                && binding
                    .participant_id
                    .as_ref()
                    .is_some_and(|named| names(named, participant_id, identity_id))
        })
    }

    /// Whether the room holds a participant reached through `channel_id`
    /// whose id is `participant_id` or `identity_id`, or whose identity is
    /// `identity_id`.
    fn reached(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
    ) -> bool {
        self.participants.iter().any(|participant| {
            participant.connected_via.contains(channel_id)
                && (names(&participant.id, participant_id, identity_id)
                    || identity_id
                        .is_some_and(|identity| participant.identity_id.as_ref() == Some(identity)))
        })
    }
}

/// Whether `named`, a stored id, names the sender: their address
/// `participant_id`, or the identity `identity_id` the caller resolved for
/// them. Ids compare as stored.
fn names(
    named: &ParticipantId,
    participant_id: &ParticipantId,
    identity_id: Option<&IdentityId>,
) -> bool {
    named == participant_id
        || identity_id.is_some_and(|identity| named.as_str() == identity.as_str())
}

/// The refusal of `event`'s commit when `events`, its room's timeline,
/// holds an event of the same source channel with the same idempotency key
/// (RFC §14.3); `None` for an event that carries no key, a blank one read
/// as none already (RFC §13.4).
fn taken_key(events: &[RoomEvent], event: &RoomEvent) -> Option<StoreError> {
    let key = event.idempotency_key.as_deref()?;
    let channel_id = &event.source.channel_id;
    let taken = events.iter().any(|stored| {
        stored.idempotency_key.as_deref() == Some(key) && &stored.source.channel_id == channel_id
    });
    taken.then(|| StoreError::IdempotencyKeyTaken {
        room_id: event.room_id.clone(),
        channel_id: channel_id.clone(),
        key: key.to_owned(),
    })
}

/// Moves the delivery `cursor` to `index` from `index - 1`, or from any
/// lower value when `force` (RFC §13.5); whether it moved.
fn advance(cursor: &mut i64, index: u64, force: bool) -> bool {
    let Ok(index) = i64::try_from(index) else {
        return false;
    };
    let moves = *cursor < index && (force || *cursor == index - 1);
    if moves {
        *cursor = index;
    }
    moves
}

impl InMemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The rooms, held only while a method runs; a method never awaits with
    /// them held. A panic while they were held leaves them as it found them
    /// or as one whole write left them, so the store keeps serving.
    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Record>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The identities, held as the rooms are.
    fn identities(&self) -> MutexGuard<'_, Identities> {
        self.identities
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The newest room `filter` admits whose record `found` accepts, as the
    /// default router orders rooms (RFC §10.4).
    fn newest(&self, filter: &RoomFilter, found: impl Fn(&Record) -> bool) -> Option<RoomId> {
        self.rooms()
            .values()
            .filter(|record| record.admitted_by(filter) && found(record))
            .map(|record| &record.room)
            .max_by(|left, right| routing::recency(left, right))
            .map(|room| room.id.clone())
    }

    /// Runs `read` on the room's record.
    fn read<T>(&self, room_id: &RoomId, read: impl FnOnce(&Record) -> T) -> Result<T, StoreError> {
        self.rooms()
            .get(room_id)
            .map(read)
            .ok_or_else(|| StoreError::RoomNotFound(room_id.clone()))
    }

    /// Runs `write` on the room's record.
    fn write<T>(
        &self,
        room_id: &RoomId,
        write: impl FnOnce(&mut Record) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut rooms = self.rooms();
        let record = rooms
            .get_mut(room_id)
            .ok_or_else(|| StoreError::RoomNotFound(room_id.clone()))?;
        write(record)
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the store answers at once; written as async fn, it reads as a store that awaits"
)]
impl ConversationStore for InMemoryStore {
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        let mut rooms = self.rooms();
        if rooms.contains_key(&room.id) {
            return Err(StoreError::RoomExists(room.id));
        }
        rooms.insert(
            room.id.clone(),
            Record {
                room,
                bindings: Vec::new(),
                participants: Vec::new(),
                events: Vec::new(),
                tasks: Vec::new(),
                observations: Vec::new(),
                private: BTreeMap::new(),
            },
        );
        Ok(())
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        self.write(room_id, |record| {
            record.update(updates);
            Ok(record.room.clone())
        })
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        Ok(self
            .rooms()
            .get(room_id)
            .and_then(|record| record.private.get(key).cloned()))
    }

    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        let updates = RoomUpdates {
            private_state: [(key.to_owned(), value)].into(),
            ..RoomUpdates::default()
        };
        if let Some(record) = self.rooms().get_mut(room_id) {
            record.update(updates);
        }
        Ok(())
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        Ok(self
            .rooms()
            .get(room_id)
            .map_or(-1, |record| record.room.delivered_index))
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        force: bool,
    ) -> Result<bool, StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(room_id) else {
            return Ok(false);
        };
        Ok(advance(&mut record.room.delivered_index, index, force))
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        Ok(self.rooms().get(room_id).map(|record| record.room.clone()))
    }

    async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, StoreError> {
        let mut rooms: Vec<Room> = self
            .rooms()
            .values()
            .filter(|record| record.admitted_by(filter))
            .map(|record| record.room.clone())
            .collect();
        rooms
            .sort_by(|left, right| (left.created_at, &left.id).cmp(&(right.created_at, &right.id)));
        Ok(rooms)
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        Ok(self.newest(filter, |record| {
            record.binds_to(channel_id, participant_id, identity_id)
        }))
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        Ok(self.newest(filter, |record| {
            record.reached(channel_id, participant_id, identity_id)
        }))
    }

    async fn store_binding(&self, binding: ChannelBinding) -> Result<(), StoreError> {
        let room_id = binding.room_id.clone();
        self.write(&room_id, |record| {
            record
                .bindings
                .retain(|bound| bound.channel_id != binding.channel_id);
            record.bindings.push(binding);
            Ok(())
        })
    }

    async fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.write(room_id, |record| {
            record
                .bindings
                .retain(|bound| &bound.channel_id != channel_id);
            Ok(())
        })
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.read(room_id, |record| record.bindings.clone())
    }

    async fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> Result<Option<RoomContext>, StoreError> {
        Ok(self.rooms().get(room_id).map(|record| {
            let skip = record.events.len().saturating_sub(recent_events);
            RoomContext {
                room: record.room.clone(),
                bindings: record.bindings.clone(),
                participants: record.participants.clone(),
                recent_events: record.events.iter().skip(skip).cloned().collect(),
                author_register: record.private.get(AUTHOR_REGISTER).cloned(),
            }
        }))
    }

    async fn create_participant(&self, participant: Participant) -> Result<(), StoreError> {
        let room_id = participant.room_id.clone();
        self.write(&room_id, |record| {
            if record.has_participant(&participant.id) {
                return Err(StoreError::ParticipantExists {
                    room_id: room_id.clone(),
                    participant_id: participant.id,
                });
            }
            record.participants.push(participant);
            Ok(())
        })
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.read(room_id, |record| {
            record
                .participants
                .iter()
                .find(|participant| &participant.id == participant_id)
                .cloned()
        })
    }

    async fn update_participant(&self, participant: Participant) -> Result<(), StoreError> {
        let room_id = participant.room_id.clone();
        self.write(&room_id, |record| {
            let Some(stored) = record
                .participants
                .iter_mut()
                .find(|stored| stored.id == participant.id)
            else {
                return Err(StoreError::ParticipantNotFound {
                    room_id: room_id.clone(),
                    participant_id: participant.id,
                });
            };
            *stored = participant;
            Ok(())
        })
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.write(room_id, |record| {
            record
                .participants
                .retain(|participant| &participant.id != participant_id);
            Ok(())
        })
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.read(room_id, |record| record.participants.clone())
    }

    async fn create_identity(&self, identity: Identity) -> Result<(), StoreError> {
        self.identities().create(identity)
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        Ok(self.identities().get(identity_id))
    }

    async fn resolve_identity(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<Option<Identity>, StoreError> {
        Ok(self
            .identities()
            .resolve(&channel_type, address, organization_id))
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.identities()
            .link(identity_id, channel_type, address, organization_id)
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.identities()
            .unlink(identity_id, &channel_type, address, organization_id)
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        key: &str,
    ) -> Result<Option<RoomEvent>, StoreError> {
        // A blank key is no key: it names no event (RFC §13.4).
        if !idempotency::is_key(key) {
            return self.read(room_id, |_| None);
        }
        self.read(room_id, |record| {
            record
                .events
                .iter()
                .find(|event| {
                    event.idempotency_key.as_deref() == Some(key)
                        && channel_id
                            .is_none_or(|channel_id| &event.source.channel_id == channel_id)
                })
                .cloned()
        })
    }

    async fn commit(
        &self,
        mut event: RoomEvent,
        counters: RoomCounters,
        target: Option<TargetUpdate>,
    ) -> Result<(), StoreError> {
        // A blank key is no key: the event is stored with none (RFC §13.4).
        event.idempotency_key = idempotency::key(event.idempotency_key);
        let room_id = event.room_id.clone();
        self.write(&room_id, |record| {
            // The room's count is the next index: a hard-deleted index is
            // never given again (gap G-064).
            if event.index != record.room.event_count {
                return Err(StoreError::IndexTaken {
                    room_id: room_id.clone(),
                    index: event.index,
                });
            }
            if let Some(taken) = taken_key(&record.events, &event) {
                return Err(taken);
            }
            if let Some(TargetUpdate { event_id, updates }) = &target {
                let stored = record
                    .events
                    .iter_mut()
                    .find(|stored| &stored.id == event_id)
                    .ok_or_else(|| StoreError::EventNotFound(event_id.clone()))?;
                updates.apply(stored);
            }
            record.room.event_count = counters.event_count;
            record.room.latest_index = Some(counters.latest_index);
            record.room.timers.last_activity_at = counters.last_activity_at;
            record.update(counters.room);
            record.events.push(event);
            Ok(())
        })
    }

    async fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Result<(), StoreError> {
        self.write(room_id, |record| {
            record.tasks.extend(tasks);
            record.observations.extend(observations);
            Ok(())
        })
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        include_blocked: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.read(room_id, |record| {
            record
                .events
                .iter()
                .filter(|event| include_blocked || event.status != EventStatus::Blocked)
                .cloned()
                .collect()
        })
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        Ok(self.rooms().values().find_map(|record| {
            record
                .events
                .iter()
                .find(|event| &event.id == event_id)
                .cloned()
        }))
    }

    async fn update_event(
        &self,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<RoomEvent, StoreError> {
        let mut rooms = self.rooms();
        let stored = rooms
            .values_mut()
            .find_map(|record| record.events.iter_mut().find(|event| &event.id == event_id))
            .ok_or_else(|| StoreError::EventNotFound(event_id.clone()))?;
        updates.apply(stored);
        Ok(stored.clone())
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.write(room_id, |record| {
            Ok(events::remove(
                &mut record.events,
                event_id,
                cascade_replies,
            ))
        })
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        filter: Option<EventFilter>,
    ) -> Result<u64, StoreError> {
        self.read(room_id, |record| {
            let counted = record
                .events
                .iter()
                .filter(|event| filter.is_none_or(|filter| filter.matches(event)))
                .count();
            u64::try_from(counted).unwrap_or(u64::MAX)
        })
    }

    async fn list_tasks(&self, room_id: &RoomId) -> Result<Vec<Task>, StoreError> {
        self.read(room_id, |record| record.tasks.clone())
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.read(room_id, |record| record.observations.clone())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelCategory, ChannelDirection, ChannelType, EventContent, EventDirection, EventId,
        EventSource, EventType, RoomStatus, RoomTimers, Timestamp,
    };

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn event(index: u64, status: EventStatus) -> RoomEvent {
        let mut event = RoomEvent::new(
            EventId::new(format!("evt_{index}")).unwrap(),
            room_id(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::SMS,
                EventDirection::Inbound,
            ),
            EventContent::text("hi"),
            Timestamp::UNIX_EPOCH,
        );
        event.index = index;
        event.status = status;
        event
    }

    fn counters(index: u64) -> RoomCounters {
        RoomCounters {
            event_count: index + 1,
            latest_index: index,
            last_activity_at: None,
            room: RoomUpdates::default(),
        }
    }

    async fn store_with_room() -> InMemoryStore {
        let store = InMemoryStore::new();
        store
            .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn a_room_id_cannot_be_created_twice() {
        let store = store_with_room().await;
        let again = store
            .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
            .await;
        assert_eq!(again, Err(StoreError::RoomExists(room_id())));
    }

    #[tokio::test]
    async fn a_commit_sets_the_room_s_counters() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Delivered), counters(0), None)
            .await
            .unwrap();
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!((room.event_count, room.latest_index), (1, Some(0)));
    }

    /// RFC §14.1 `advance_delivered_index`, §13.5, as the Python
    /// implementation's stores answer it: a compare-and-set from the index
    /// before, from any lower value when forced, never backwards, nothing
    /// for a room the store does not hold; a commit and a room's update
    /// leave the cursor as stored.
    #[tokio::test]
    async fn the_delivery_cursor_advances_by_compare_and_set_only() {
        let store = store_with_room().await;
        let room = room_id();
        let missing = RoomId::new("room_9").unwrap();
        let start = store.get_delivered_index(&room).await.unwrap();
        let mut moves = Vec::new();
        for (index, force) in [(0, false), (0, false), (2, false), (2, true), (1, true)] {
            moves.push(
                store
                    .advance_delivered_index(&room, index, force)
                    .await
                    .unwrap(),
            );
        }
        store
            .commit(event(0, EventStatus::Delivered), counters(0), None)
            .await
            .unwrap();
        store
            .update_room(&room, RoomUpdates::default())
            .await
            .unwrap();
        assert_eq!(
            (
                start,
                moves,
                store.get_delivered_index(&room).await.unwrap(),
                store
                    .get_room(&room)
                    .await
                    .unwrap()
                    .unwrap()
                    .delivered_index,
                store.get_delivered_index(&missing).await.unwrap(),
                store
                    .advance_delivered_index(&missing, 0, true)
                    .await
                    .unwrap()
            ),
            (-1, vec![true, false, false, true, false], 2, 2, -1, false)
        );
    }

    #[tokio::test]
    async fn an_index_the_room_holds_is_refused() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Delivered), counters(0), None)
            .await
            .unwrap();
        let again = store
            .commit(event(0, EventStatus::Delivered), counters(0), None)
            .await;
        assert_eq!(
            again,
            Err(StoreError::IndexTaken {
                room_id: room_id(),
                index: 0
            })
        );
    }

    #[tokio::test]
    async fn a_timeline_read_leaves_refused_events_out_by_default() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Blocked), counters(0), None)
            .await
            .unwrap();
        store
            .commit(event(1, EventStatus::Delivered), counters(1), None)
            .await
            .unwrap();
        let received = store.list_events(&room_id(), false).await.unwrap();
        assert_eq!(
            received.iter().map(|event| event.index).collect::<Vec<_>>(),
            [1]
        );
    }

    #[tokio::test]
    async fn a_timeline_read_serves_refused_events_when_asked() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Blocked), counters(0), None)
            .await
            .unwrap();
        assert_eq!(store.list_events(&room_id(), true).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_context_holds_the_latest_events_only() {
        let store = store_with_room().await;
        for index in 0..3 {
            store
                .commit(event(index, EventStatus::Delivered), counters(index), None)
                .await
                .unwrap();
        }
        let context = store.load_context(&room_id(), 2).await.unwrap().unwrap();
        assert_eq!(
            context
                .recent_events
                .iter()
                .map(|event| event.index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[tokio::test]
    async fn a_room_update_writes_only_the_fields_it_names() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Delivered), counters(0), None)
            .await
            .unwrap();
        let updates = RoomUpdates {
            status: Some(RoomStatus::Paused),
            ..RoomUpdates::default()
        };
        let room = store.update_room(&room_id(), updates).await.unwrap();
        assert_eq!((room.status, room.event_count), (RoomStatus::Paused, 1));
    }

    #[tokio::test]
    async fn a_commit_writes_the_room_updates_it_carries() {
        let store = store_with_room().await;
        let mut resumed = counters(0);
        resumed.room.status = Some(RoomStatus::Active);
        store
            .update_room(
                &room_id(),
                RoomUpdates {
                    status: Some(RoomStatus::Paused),
                    ..RoomUpdates::default()
                },
            )
            .await
            .unwrap();
        store
            .commit(event(0, EventStatus::Delivered), resumed, None)
            .await
            .unwrap();
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(room.status, RoomStatus::Active);
    }

    #[tokio::test]
    async fn a_deleted_binding_is_gone() {
        let store = store_with_room().await;
        let binding = ChannelBinding::new(
            ChannelId::new("sms").unwrap(),
            room_id(),
            ChannelType::SMS,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        store.store_binding(binding).await.unwrap();
        store
            .delete_binding(&room_id(), &ChannelId::new("sms").unwrap())
            .await
            .unwrap();
        assert_eq!(store.list_bindings(&room_id()).await.unwrap(), []);
    }

    fn jean() -> Participant {
        Participant::new(
            ParticipantId::new("p_jean").unwrap(),
            room_id(),
            ChannelId::new("sms").unwrap(),
            Timestamp::UNIX_EPOCH,
        )
    }

    #[tokio::test]
    async fn a_context_holds_the_room_s_participants() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        let context = store.load_context(&room_id(), 10).await.unwrap().unwrap();
        assert_eq!(context.participants, [jean()]);
    }

    #[tokio::test]
    async fn a_participant_id_cannot_be_created_twice_in_a_room() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        assert_eq!(
            store.create_participant(jean()).await,
            Err(StoreError::ParticipantExists {
                room_id: room_id(),
                participant_id: jean().id,
            })
        );
    }

    #[tokio::test]
    async fn a_participant_is_found_within_its_room() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        assert_eq!(
            store.get_participant(&room_id(), &jean().id).await,
            Ok(Some(jean()))
        );
    }

    #[tokio::test]
    async fn an_updated_participant_is_written_whole() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        let renamed = Participant {
            display_name: Some("Jean Tremblay".to_owned()),
            ..jean()
        };
        store.update_participant(renamed.clone()).await.unwrap();
        assert_eq!(store.list_participants(&room_id()).await, Ok(vec![renamed]));
    }

    #[tokio::test]
    async fn updating_a_participant_the_room_does_not_have_is_refused() {
        let store = store_with_room().await;
        assert_eq!(
            store.update_participant(jean()).await,
            Err(StoreError::ParticipantNotFound {
                room_id: room_id(),
                participant_id: jean().id,
            })
        );
    }

    #[tokio::test]
    async fn a_deleted_participant_is_gone() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        store
            .delete_participant(&room_id(), &jean().id)
            .await
            .unwrap();
        assert_eq!(store.list_participants(&room_id()).await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn listing_rooms_with_timers_armed_skips_the_others() {
        let store = store_with_room().await;
        let mut timed = Room::new(RoomId::new("room_2").unwrap(), Timestamp::UNIX_EPOCH);
        timed.timers = RoomTimers {
            inactive_after_seconds: Some(60),
            ..RoomTimers::default()
        };
        store.create_room(timed).await.unwrap();
        let filter = RoomFilter {
            timers_armed: true,
            ..RoomFilter::default()
        };
        let listed = store.list_rooms(&filter).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|room| room.id.as_str())
                .collect::<Vec<_>>(),
            ["room_2"]
        );
    }

    /// A room `id` created `seconds` after the epoch, its SMS binding
    /// naming `correspondent`.
    async fn room_naming(
        store: &InMemoryStore,
        id: &str,
        seconds: i64,
        correspondent: Option<&str>,
    ) {
        let room_id = RoomId::new(id).unwrap();
        let created = Timestamp::from_unix_millis(seconds * 1000).unwrap();
        store
            .create_room(Room::new(room_id.clone(), created))
            .await
            .unwrap();
        let mut binding = ChannelBinding::new(
            ChannelId::new("sms").unwrap(),
            room_id,
            ChannelType::SMS,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        binding.participant_id = correspondent.map(|id| ParticipantId::new(id).unwrap());
        store.store_binding(binding).await.unwrap();
    }

    fn alice() -> ParticipantId {
        ParticipantId::new("+15550000001").unwrap()
    }

    fn sms() -> ChannelId {
        ChannelId::new("sms").unwrap()
    }

    #[tokio::test]
    async fn the_newest_room_whose_binding_names_the_participant_is_found() {
        let store = InMemoryStore::new();
        room_naming(&store, "older", 10, Some("+15550000001")).await;
        room_naming(&store, "newer", 20, Some("+15550000001")).await;
        room_naming(&store, "newest", 30, Some("+15550000002")).await;
        let found = store
            .find_room_id_by_binding(&sms(), &alice(), None, &RoomFilter::default())
            .await
            .unwrap();
        assert_eq!(found, Some(RoomId::new("newer").unwrap()));
    }

    #[tokio::test]
    async fn a_binding_of_another_channel_finds_no_room() {
        let store = InMemoryStore::new();
        room_naming(&store, "room_1", 10, Some("+15550000001")).await;
        let found = store
            .find_room_id_by_binding(
                &ChannelId::new("whatsapp").unwrap(),
                &alice(),
                None,
                &RoomFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn a_room_the_filter_refuses_is_not_found_by_its_binding() {
        let store = InMemoryStore::new();
        room_naming(&store, "room_1", 10, Some("+15550000001")).await;
        let filter = RoomFilter {
            status: Some(RoomStatus::Paused),
            ..RoomFilter::default()
        };
        let found = store
            .find_room_id_by_binding(&sms(), &alice(), None, &filter)
            .await
            .unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn a_participant_reached_through_the_channel_finds_its_room() {
        let store = store_with_room().await;
        let mut participant = jean();
        participant.identity_id = Some(IdentityId::new("id_jean").unwrap());
        store.create_participant(participant).await.unwrap();
        let by_id = store
            .find_room_id_by_participant(&sms(), &jean().id, None, &RoomFilter::default())
            .await
            .unwrap();
        // Another address, the identity the caller resolved for it.
        let by_identity = store
            .find_room_id_by_participant(
                &sms(),
                &ParticipantId::new("+15559999999").unwrap(),
                Some(&IdentityId::new("id_jean").unwrap()),
                &RoomFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!((by_id, by_identity), (Some(room_id()), Some(room_id())));
    }

    #[tokio::test]
    async fn a_participant_reached_through_another_channel_finds_no_room() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        let found = store
            .find_room_id_by_participant(
                &ChannelId::new("email").unwrap(),
                &jean().id,
                None,
                &RoomFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn listing_the_rooms_a_channel_reaches_skips_the_others() {
        let store = InMemoryStore::new();
        room_naming(&store, "bound", 10, None).await;
        store
            .create_room(Room::new(
                RoomId::new("joined").unwrap(),
                Timestamp::UNIX_EPOCH,
            ))
            .await
            .unwrap();
        store
            .create_participant(Participant::new(
                alice(),
                RoomId::new("joined").unwrap(),
                sms(),
                Timestamp::UNIX_EPOCH,
            ))
            .await
            .unwrap();
        store
            .create_room(Room::new(
                RoomId::new("elsewhere").unwrap(),
                Timestamp::UNIX_EPOCH,
            ))
            .await
            .unwrap();
        let filter = RoomFilter {
            channel_id: Some(sms()),
            ..RoomFilter::default()
        };
        let listed = store.list_rooms(&filter).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|room| room.id.as_str())
                .collect::<Vec<_>>(),
            ["joined", "bound"]
        );
    }

    #[tokio::test]
    async fn listing_rooms_without_a_filter_returns_them_oldest_first() {
        let store = InMemoryStore::new();
        for (id, millis) in [("late", 2_000), ("early", 1_000)] {
            let created = Timestamp::from_unix_millis(millis).unwrap();
            store
                .create_room(Room::new(RoomId::new(id).unwrap(), created))
                .await
                .unwrap();
        }
        let listed = store.list_rooms(&RoomFilter::default()).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|room| room.id.as_str())
                .collect::<Vec<_>>(),
            ["early", "late"]
        );
    }

    /// RFC §14.1, §10.4 step 1 (44dada1), gap G-058: the routing lookups
    /// find the sender by their address or by the identity the caller
    /// resolved for them, the newest room either name finds winning.
    mod routing_lookups {
        use pretty_assertions::assert_eq;
        use roomkit_core::model::{IdentityId, ParticipantId};

        use super::*;

        const ALICE: &str = "+15550000001";

        fn sms() -> ChannelId {
            ChannelId::new("sms").unwrap()
        }

        fn marie() -> IdentityId {
            IdentityId::new("id_marie").unwrap()
        }

        /// A store holding `named`, rooms created a second apart in that
        /// order, each with an SMS binding naming the id it gives.
        async fn bound(named: &[(&str, &str)]) -> InMemoryStore {
            let store = InMemoryStore::new();
            for (offset, (room, correspondent)) in named.iter().enumerate() {
                let room_id = RoomId::new(*room).unwrap();
                let created =
                    Timestamp::from_unix_millis(i64::try_from(offset).unwrap() * 1000).unwrap();
                store
                    .create_room(Room::new(room_id.clone(), created))
                    .await
                    .unwrap();
                let mut binding = ChannelBinding::new(
                    sms(),
                    room_id,
                    ChannelType::SMS,
                    ChannelCategory::Transport,
                    ChannelDirection::Bidirectional,
                    created,
                );
                binding.participant_id = Some(ParticipantId::new(*correspondent).unwrap());
                store.store_binding(binding).await.unwrap();
            }
            store
        }

        async fn by_binding(
            store: &InMemoryStore,
            identity: Option<&IdentityId>,
        ) -> Option<String> {
            let alice = ParticipantId::new(ALICE).unwrap();
            store
                .find_room_id_by_binding(&sms(), &alice, identity, &RoomFilter::default())
                .await
                .unwrap()
                .map(|room| room.to_string())
        }

        #[tokio::test]
        async fn a_binding_naming_the_sender_s_identity_is_found() {
            let store = bound(&[("by_identity", "id_marie")]).await;
            assert_eq!(
                (
                    by_binding(&store, None).await,
                    by_binding(&store, Some(&marie())).await
                ),
                (None, Some("by_identity".to_owned()))
            );
        }

        #[tokio::test]
        async fn the_newest_room_either_name_finds_wins() {
            let identity_newer = bound(&[("by_address", ALICE), ("by_identity", "id_marie")]).await;
            let address_newer = bound(&[("by_identity", "id_marie"), ("by_address", ALICE)]).await;
            assert_eq!(
                (
                    by_binding(&identity_newer, Some(&marie())).await,
                    by_binding(&address_newer, Some(&marie())).await
                ),
                (
                    Some("by_identity".to_owned()),
                    Some("by_address".to_owned())
                )
            );
        }
    }
}
