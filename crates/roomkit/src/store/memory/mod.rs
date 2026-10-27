//! The in-memory store (RFC §14.2): for tests, prototypes and single-process
//! deployments.

mod events;
mod identities;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventUpdates, Identity, IdentityId, Metadata,
    Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId, RoomUpdates,
    Task,
};
use roomkit_core::ops::{RoomCounters, TargetUpdate};
use roomkit_core::rules::{idempotency, routing};

use self::identities::Identities;
use super::{ConversationStore, EventFilter, EventQuery, RoomFilter, StoreError};

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

/// The page of `timeline` `query` asks for, as the Python implementation's
/// in-memory store cuts it: the events between the cursor and the end in
/// index order, those the query admits, then the window, taken from the
/// newest end for a `before_index` cursor and for `newest_first` in offset
/// mode, the page ascending either way.
fn page(timeline: &[RoomEvent], query: &EventQuery) -> Vec<RoomEvent> {
    let mut events: Vec<&RoomEvent> = timeline
        .iter()
        .filter(|event| query.after_index.is_none_or(|after| event.index > after))
        .filter(|event| query.before_index.is_none_or(|before| event.index < before))
        .filter(|event| query.admits(event))
        .collect();
    // Rendered by index whatever the commit order (RFC §14.1); the sort is
    // stable.
    events.sort_by_key(|event| event.index);
    let limit = usize::try_from(query.limit).unwrap_or(usize::MAX);
    let cursor = query.after_index.is_some() || query.before_index.is_some();
    let offset = if cursor {
        0
    } else {
        usize::try_from(query.offset).unwrap_or(usize::MAX)
    };
    let from_newest = query.before_index.is_some() || (!cursor && query.newest_first);
    let (start, end) = if from_newest {
        let end = events.len().saturating_sub(offset);
        (end.saturating_sub(limit), end)
    } else {
        let start = offset.min(events.len());
        (start, start.saturating_add(limit).min(events.len()))
    };
    events
        .get(start..end)
        .unwrap_or_default()
        .iter()
        .map(|event| (*event).clone())
        .collect()
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
            // In its place: the room lists its bindings in the order they
            // were attached.
            match record
                .bindings
                .iter_mut()
                .find(|bound| bound.channel_id == binding.channel_id)
            {
                Some(bound) => *bound = binding,
                None => record.bindings.push(binding),
            }
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
                if events::deletes(&event, event_id) {
                    events::mark_edits_deleted(&mut record.events, event_id);
                }
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
        query: &EventQuery,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        query.validate()?;
        self.read(room_id, |record| page(&record.events, query))
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
        if let Some(filter) = &filter {
            filter.validate()?;
        }
        self.read(room_id, |record| {
            let counted = record
                .events
                .iter()
                .filter(|event| filter.as_ref().is_none_or(|filter| filter.matches(event)))
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
