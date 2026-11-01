//! The in-memory store (RFC §14.2): for tests, prototypes and single-process
//! deployments.

mod events;
mod identities;

use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Metadata, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, RoomStatus, Task, TaskId, ThreadSummary,
    Timestamp,
};
use roomkit_core::rules::{edits, idempotency, lifecycle, routing};

use self::identities::Identities;
use super::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, ConversationStore, DeleteEventOptions,
    FindLatestRoomOptions, FindRoomIdByBindingOptions, FindRoomIdByChannelOptions,
    FindRoomIdByParticipantOptions, FindRoomIdsByChannelOptions, FindRoomsOptions,
    GetEventCountOptions, LinkAddressOptions, ListEventsOptions, ListRoomsOptions,
    ListTasksOptions, PatchRoomMetadataOptions, ResolveIdentityOptions, RoomFilter, StoreError,
    UnlinkAddressOptions, names_by_recipient, names_sender,
};

/// A store that keeps everything in memory, for one process: Python's
/// `InMemoryStore`.
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
    /// The index the room gives out next, past every index it gave out, a
    /// deleted event's or one an added event carried (Python's
    /// `_next_event_index`).
    next_index: u64,
    /// Each channel's read marker, the highest index it read.
    read_markers: BTreeMap<ChannelId, u64>,
}

impl Record {
    /// The record of a new room.
    fn new(room: Room) -> Self {
        Self {
            room,
            bindings: Vec::new(),
            participants: Vec::new(),
            events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            private: BTreeMap::new(),
            next_index: 0,
            read_markers: BTreeMap::new(),
        }
    }

    /// The room's next index, given out (Python's `_reserve_event_index`).
    fn reserve_index(&mut self) -> u64 {
        let index = self.next_index;
        self.next_index = index.saturating_add(1);
        index
    }

    /// Stores `event` at the index it carries, the room's next index kept
    /// past it.
    fn append(&mut self, event: RoomEvent) {
        self.next_index = self.next_index.max(event.index.saturating_add(1));
        self.events.push(event);
    }

    /// Moves `channel_id`'s read marker to `index` when that is higher.
    fn raise_marker(&mut self, channel_id: &ChannelId, index: u64) {
        let marker = self.read_markers.entry(channel_id.clone()).or_insert(index);
        *marker = (*marker).max(index);
    }

    /// The room's binding of `channel_id`.
    fn binding(&self, channel_id: &ChannelId) -> Option<&ChannelBinding> {
        self.bindings
            .iter()
            .find(|binding| &binding.channel_id == channel_id)
    }

    /// Whether the room is `participant_id`'s, as `find_latest_room` reads
    /// it: their participant record, on any channel, or a binding naming
    /// them, of `channel_type` when it names one.
    fn holds(&self, participant_id: &ParticipantId, channel_type: Option<&ChannelType>) -> bool {
        self.participants
            .iter()
            .any(|participant| &participant.id == participant_id)
            || self.bindings.iter().any(|binding| {
                binding.participant_id.as_ref() == Some(participant_id)
                    && channel_type.is_none_or(|kind| &binding.channel_type == kind)
            })
    }

    /// Whether the room holds a participant reached through `channel_id`
    /// who is the sender, by `participant_id`, `identity_id` or the record
    /// of their own the identity step left them pending under there (RFC
    /// §11.2), as Python's `reached_as` reads `participant_names`.
    fn reached(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        identity_id: Option<&IdentityId>,
    ) -> bool {
        let own = routing::own_record_id(channel_id.as_str(), participant_id);
        self.participants.iter().any(|participant| {
            participant.connected_via.contains(channel_id)
                && (names_sender(participant.id.as_str(), participant_id, identity_id)
                    || participant.id.as_str() == own
                    || participant.identity_id.as_ref().is_some_and(|identity| {
                        names_sender(identity.as_str(), participant_id, identity_id)
                    }))
        })
    }
}

/// The page of `timeline` `options` asks for, as the Python implementation's
/// in-memory store cuts it: the events between the cursor and the end in
/// index order, those the options admit, then the window, taken from the
/// newest end for a `before_index` cursor and for `newest_first` in offset
/// mode, the page ascending either way.
fn page(timeline: &[RoomEvent], options: &ListEventsOptions) -> Vec<RoomEvent> {
    let mut events: Vec<&RoomEvent> = timeline
        .iter()
        .filter(|event| options.after_index.is_none_or(|after| event.index > after))
        .filter(|event| {
            options
                .before_index
                .is_none_or(|before| event.index < before)
        })
        .filter(|event| options.admits(event))
        .collect();
    // Rendered by index whatever the commit order (RFC §14.1); the sort is
    // stable.
    events.sort_by_key(|event| event.index);
    let limit = usize::try_from(options.limit).unwrap_or(usize::MAX);
    let cursor = options.after_index.is_some() || options.before_index.is_some();
    let offset = if cursor {
        0
    } else {
        usize::try_from(options.offset).unwrap_or(usize::MAX)
    };
    let from_newest = options.before_index.is_some() || (!cursor && options.newest_first);
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

/// `rooms` from `offset`, at most `limit` of them, as a Python slice cuts
/// them.
fn window(rooms: Vec<Room>, offset: u64, limit: u64) -> Vec<Room> {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    rooms.into_iter().skip(offset).take(limit).collect()
}

/// The refusal of `event`'s commit when `events`, its room's timeline,
/// holds another event of the same source channel with the same
/// idempotency key (RFC §14.3); `None` for an event that carries no key, a
/// blank one read as none already (RFC §13.4).
fn taken_key(events: &[RoomEvent], event: &RoomEvent) -> Option<StoreError> {
    let key = event.idempotency_key.as_deref()?;
    let channel_id = &event.source.channel_id;
    let taken = events.iter().any(|stored| {
        stored.id != event.id
            && stored.idempotency_key.as_deref() == Some(key)
            && &stored.source.channel_id == channel_id
    });
    taken.then(|| StoreError::IdempotencyKeyTaken {
        room_id: event.room_id.clone(),
        channel_id: channel_id.clone(),
        key: key.to_owned(),
    })
}

/// Writes `room` whole over the record's, keeping the counters the store
/// manages: a caller's stale copy never rewinds them (RFC §14.3).
fn write_room(record: &mut Record, room: &mut Room) {
    room.event_count = record.room.event_count;
    room.latest_index = record.room.latest_index;
    room.delivered_index = record.room.delivered_index;
    record.room.clone_from(room);
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

/// Writes `target` whole on its stored copy among `events`, keeping the
/// stored `delivery_results` (RFC §14.3), and marks its edits deleted when
/// `event` deletes it (RFC §10.3).
///
/// # Errors
///
/// [`StoreError::EventNotFound`] for a target `events` no longer holds.
fn write_target(
    events: &mut [RoomEvent],
    event: &RoomEvent,
    mut target: RoomEvent,
) -> Result<(), StoreError> {
    let stored = events
        .iter_mut()
        .find(|stored| stored.id == target.id)
        .ok_or_else(|| StoreError::EventNotFound(target.id.clone()))?;
    target.delivery_results = mem::take(&mut stored.delivery_results);
    *stored = target;
    let target_id = stored.id.clone();
    if events::deletes(event, &target_id) {
        events::mark_edits_deleted(events, &target_id);
    }
    Ok(())
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

    /// Every room `admits` accepts, in the order every store pages them: by
    /// creation, then by id.
    fn rooms_in_order(&self, admits: impl Fn(&Room) -> bool) -> Vec<Room> {
        let mut rooms: Vec<Room> = self
            .rooms()
            .values()
            .map(|record| &record.room)
            .filter(|room| admits(room))
            .cloned()
            .collect();
        rooms.sort_by(routing::recency);
        rooms
    }

    /// The newest room `filter` admits whose record `found` accepts, as the
    /// default router orders rooms (RFC §10.4).
    fn newest(&self, filter: &RoomFilter, found: impl Fn(&Record) -> bool) -> Option<RoomId> {
        self.rooms()
            .values()
            .filter(|record| filter.admits(&record.room) && found(record))
            .map(|record| &record.room)
            .max_by(|left, right| routing::recency(left, right))
            .map(|room| room.id.clone())
    }

    /// Runs `read` on the room's record; `None` for a room the store lacks.
    fn read<T>(&self, room_id: &RoomId, read: impl FnOnce(&Record) -> T) -> Option<T> {
        self.rooms().get(room_id).map(read)
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

    /// Stores `binding` in place of the room's binding of its channel.
    fn put_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        super::written::binding(&binding)?;
        let room_id = binding.room_id.clone();
        self.write(&room_id, |record| {
            // In its place: the room lists its bindings in the order they
            // were attached.
            match record
                .bindings
                .iter_mut()
                .find(|bound| bound.channel_id == binding.channel_id)
            {
                Some(bound) => bound.clone_from(&binding),
                None => record.bindings.push(binding.clone()),
            }
            Ok(binding)
        })
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the store answers at once; written as async fn, it reads as a store that awaits"
)]
impl ConversationStore for InMemoryStore {
    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        let mut rooms = self.rooms();
        if rooms.contains_key(&room.id) {
            return Err(StoreError::RoomAlreadyExists(room.id));
        }
        rooms.insert(room.id.clone(), Record::new(room.clone()));
        Ok(room)
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        Ok(self.read(room_id, |record| record.room.clone()))
    }

    async fn room_exists(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        Ok(self.rooms().contains_key(room_id))
    }

    async fn update_room(&self, mut room: Room) -> Result<Room, StoreError> {
        let room_id = room.id.clone();
        self.write(&room_id, |record| {
            write_room(record, &mut room);
            Ok(room)
        })
    }

    /// One write under the store's lock: no commit falls between the
    /// comparison and the write.
    async fn update_room_if(
        &self,
        mut room: Room,
        status: RoomStatus,
        paused_at: Option<Timestamp>,
        last_activity_at: Option<Timestamp>,
    ) -> Result<Option<Room>, StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(&room.id) else {
            return Ok(None);
        };
        if !super::room_holds(&record.room, status, paused_at, last_activity_at) {
            return Ok(None);
        }
        write_room(record, &mut room);
        Ok(Some(room))
    }

    /// One write under the store's lock: no commit falls between the read
    /// and the write.
    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(room_id) else {
            return Ok(None);
        };
        for key in &options.unset {
            record.room.metadata.remove(key);
        }
        record.room.metadata.extend(patch);
        record.room.updated_at = super::written::now();
        Ok(Some(record.room.clone()))
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        Ok(self.rooms().remove(room_id).is_some())
    }

    async fn list_rooms(&self, options: ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        let rooms = self.rooms_in_order(|_| true);
        Ok(window(rooms, options.offset, options.limit))
    }

    async fn find_rooms(&self, options: FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        let FindRoomsOptions {
            organization_id,
            status,
            metadata_filter,
            limit,
            offset,
        } = options;
        let rooms = self.rooms_in_order(|room| {
            organization_id
                .as_ref()
                .is_none_or(|organization| room.organization_id.as_ref() == Some(organization))
                && status.is_none_or(|status| room.status == status)
                && metadata_filter.as_ref().is_none_or(|filter| {
                    filter
                        .iter()
                        .all(|(key, value)| room.metadata.get(key) == Some(value))
                })
        });
        Ok(window(rooms, offset, limit))
    }

    async fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: FindLatestRoomOptions,
    ) -> Result<Option<Room>, StoreError> {
        let FindLatestRoomOptions {
            channel_type,
            status,
        } = options;
        let statuses = status.map(|status| vec![status]);
        let filter = RoomFilter {
            statuses,
            organization_id: None,
        };
        let room_id = self.newest(&filter, |record| {
            record.holds(participant_id, channel_type.as_ref())
        });
        Ok(room_id.and_then(|room_id| self.read(&room_id, |record| record.room.clone())))
    }

    async fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdByChannelOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        let ids = FindRoomIdsByChannelOptions {
            room_filter: Some(RoomFilter {
                statuses: options.status.map(|status: RoomStatus| vec![status]),
                organization_id: None,
            }),
            limit: 1,
            group: None,
        };
        let found = self.find_room_ids_by_channel(channel_id, ids).await?;
        Ok(found.into_iter().next())
    }

    async fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdsByChannelOptions,
    ) -> Result<Vec<RoomId>, StoreError> {
        let filter = options.room_filter.unwrap_or_default();
        let mut matched: Vec<Room> =
            self.rooms()
                .values()
                .filter(|record| {
                    record.binding(channel_id).is_some_and(|binding| {
                        options.group.is_none_or(|group| binding.group == group)
                    }) && filter.admits(&record.room)
                })
                .map(|record| record.room.clone())
                .collect();
        matched.sort_by(routing::recency);
        Ok(matched
            .into_iter()
            .take(options.limit)
            .map(|room| room.id)
            .collect())
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByBindingOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        let filter = options.room_filter.unwrap_or_default();
        let identity_id = options.identity_id.as_ref();
        let recipient_key = options.recipient_key.as_deref();
        Ok(self.newest(&filter, |record| {
            record.binding(channel_id).is_some_and(|binding| {
                binding
                    .participant_id
                    .as_ref()
                    .is_some_and(|named| names_sender(named.as_str(), participant_id, identity_id))
                    || names_by_recipient(binding, participant_id, recipient_key)
            })
        }))
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByParticipantOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        let filter = options.room_filter.unwrap_or_default();
        Ok(self.newest(&filter, |record| {
            record.reached(channel_id, participant_id, options.identity_id.as_ref())
        }))
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        Ok(self
            .read(room_id, |record| record.room.delivered_index)
            .unwrap_or(-1))
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(room_id) else {
            return Ok(false);
        };
        Ok(advance(
            &mut record.room.delivered_index,
            index,
            options.force,
        ))
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        Ok(self
            .read(room_id, |record| record.private.get(key).cloned())
            .flatten())
    }

    /// Kept apart from the metadata, whose copy under `key` goes in the
    /// same write (RFC §14.1).
    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(room_id) else {
            return Ok(());
        };
        record.room.metadata.remove(key);
        match value {
            Some(value) => record.private.insert(key.to_owned(), value),
            None => record.private.remove(key),
        };
        Ok(())
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.put_binding(binding)
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        Ok(self
            .read(room_id, |record| record.binding(channel_id).cloned())
            .flatten())
    }

    async fn binding_exists(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        Ok(self
            .read(room_id, |record| record.binding(channel_id).is_some())
            .unwrap_or(false))
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.put_binding(binding)
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(room_id) else {
            return Ok(false);
        };
        let held = record.bindings.len();
        record
            .bindings
            .retain(|bound| &bound.channel_id != channel_id);
        Ok(record.bindings.len() < held)
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        Ok(self
            .read(room_id, |record| record.bindings.clone())
            .unwrap_or_default())
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        let room_id = participant.room_id.clone();
        self.write(&room_id, |record| {
            if record
                .participants
                .iter()
                .any(|stored| stored.id == participant.id)
            {
                return Err(StoreError::ParticipantExists {
                    room_id: room_id.clone(),
                    participant_id: participant.id,
                });
            }
            record.participants.push(participant.clone());
            Ok(participant)
        })
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        Ok(self
            .read(room_id, |record| {
                record
                    .participants
                    .iter()
                    .find(|participant| &participant.id == participant_id)
                    .cloned()
            })
            .flatten())
    }

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
        let mut rooms = self.rooms();
        let stored = rooms.get_mut(&participant.room_id).and_then(|record| {
            record
                .participants
                .iter_mut()
                .find(|stored| stored.id == participant.id)
        });
        let Some(stored) = stored else {
            return Err(StoreError::ParticipantNotFound {
                room_id: participant.room_id,
                participant_id: participant.id,
            });
        };
        stored.clone_from(&participant);
        Ok(participant)
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        if let Some(record) = self.rooms().get_mut(room_id) {
            record
                .participants
                .retain(|participant| &participant.id != participant_id);
        }
        Ok(())
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        Ok(self
            .read(room_id, |record| record.participants.clone())
            .unwrap_or_default())
    }

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        self.identities().create(identity.clone())?;
        Ok(identity)
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        Ok(self.identities().get(identity_id))
    }

    async fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        Ok(self
            .identities()
            .resolve(channel_type, address, options.organization_id.as_deref()))
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: LinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.identities().link(
            identity_id,
            channel_type.clone(),
            address,
            options.organization_id.as_deref(),
        )
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: UnlinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.identities().unlink(
            identity_id,
            channel_type,
            address,
            options.organization_id.as_deref(),
        )
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        let found = self
            .get_event_by_idempotency_key(room_id, key, channel_id)
            .await?;
        Ok(found.is_some())
    }

    /// With no channel, the event of lowest index any channel of the room
    /// committed the key with, as Python's store reads it (§22.1).
    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<Option<RoomEvent>, StoreError> {
        // A blank key is no key: it names no event (RFC §13.4).
        if !idempotency::is_key(key) {
            return Ok(None);
        }
        Ok(self
            .read(room_id, |record| {
                record
                    .events
                    .iter()
                    .filter(|event| {
                        event.idempotency_key.as_deref() == Some(key)
                            && channel_id
                                .is_none_or(|channel_id| &event.source.channel_id == channel_id)
                    })
                    .min_by_key(|event| event.index)
                    .cloned()
            })
            .flatten())
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
        options: CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        let CommitEventOptions {
            mut target,
            activity,
        } = options;
        super::written::event(&mut event)?;
        if let Some(target) = &mut target {
            super::written::event(target)?;
        }
        // A blank key is no key: the event is stored with none (RFC §13.4).
        event.idempotency_key = idempotency::key(event.idempotency_key);
        let now = activity.then(super::written::now);
        self.write(room_id, |record| {
            if let Some(taken) = taken_key(&record.events, &event) {
                return Err(taken);
            }
            if let Some(target) = target {
                write_target(&mut record.events, &event, target)?;
            }
            // The room's next index: a hard-deleted index is never given
            // again (gap G-064), nor one an added event carried.
            event.index = record.reserve_index();
            let room = &mut record.room;
            room.event_count = room.event_count.saturating_add(1);
            room.latest_index = Some(event.index);
            if let Some(now) = now {
                lifecycle::commit_activity(room, now);
            }
            record.append(event.clone());
            Ok(event)
        })
    }

    async fn add_event(&self, mut event: RoomEvent) -> Result<RoomEvent, StoreError> {
        super::written::event(&mut event)?;
        // A blank key is no key: the event is stored with none (RFC §13.4).
        event.idempotency_key = idempotency::key(event.idempotency_key);
        let room_id = event.room_id.clone();
        self.write(&room_id, |record| {
            if let Some(taken) = taken_key(&record.events, &event) {
                return Err(taken);
            }
            record.append(event.clone());
            Ok(event)
        })
    }

    /// One write under the store's lock: the index is reserved as the
    /// event is stored.
    async fn add_event_auto_index(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
    ) -> Result<RoomEvent, StoreError> {
        super::written::event(&mut event)?;
        event.idempotency_key = idempotency::key(event.idempotency_key);
        self.write(room_id, |record| {
            if let Some(taken) = taken_key(&record.events, &event) {
                return Err(taken);
            }
            event.index = record.reserve_index();
            record.append(event.clone());
            Ok(event)
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

    async fn update_event(&self, mut event: RoomEvent) -> Result<RoomEvent, StoreError> {
        super::written::event(&mut event)?;
        let mut rooms = self.rooms();
        let stored = rooms
            .get_mut(&event.room_id)
            .and_then(|record| {
                record
                    .events
                    .iter_mut()
                    .find(|stored| stored.id == event.id)
            })
            .ok_or_else(|| StoreError::EventNotFound(event.id.clone()))?;
        event.delivery_results = mem::take(&mut stored.delivery_results);
        stored.clone_from(&event);
        Ok(event)
    }

    async fn record_delivery_results(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        results: Metadata,
    ) -> Result<(), StoreError> {
        if let Some(record) = self.rooms().get_mut(room_id)
            && let Some(stored) = record.events.iter_mut().find(|event| &event.id == event_id)
        {
            stored.delivery_results = results;
        }
        Ok(())
    }

    /// One write under the store's lock: the target and its edits' marks.
    async fn write_change_target(
        &self,
        room_id: &RoomId,
        mut target: RoomEvent,
    ) -> Result<(), StoreError> {
        super::written::event(&mut target)?;
        let mut rooms = self.rooms();
        let events = rooms
            .get_mut(room_id)
            .map(|record| &mut record.events)
            .ok_or_else(|| StoreError::EventNotFound(target.id.clone()))?;
        let stored = events
            .iter_mut()
            .find(|stored| stored.id == target.id)
            .ok_or_else(|| StoreError::EventNotFound(target.id.clone()))?;
        target.delivery_results = mem::take(&mut stored.delivery_results);
        let deleted = edits::is_deleted(&target);
        let target_id = target.id.clone();
        *stored = target;
        if deleted {
            events::mark_edits_deleted(events, &target_id);
        }
        Ok(())
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        let mut rooms = self.rooms();
        let Some(record) = rooms.get_mut(room_id) else {
            return Ok(Vec::new());
        };
        Ok(events::remove(
            &mut record.events,
            event_id,
            options.cascade_replies,
        ))
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        let filter = options.event_filter;
        if let Some(filter) = &filter {
            filter.validate()?;
        }
        let counted = self
            .read(room_id, |record| {
                record
                    .events
                    .iter()
                    .filter(|event| filter.as_ref().is_none_or(|filter| filter.matches(event)))
                    .count()
            })
            .unwrap_or_default();
        Ok(u64::try_from(counted).unwrap_or(u64::MAX))
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        options.validate()?;
        Ok(self
            .read(room_id, |record| page(&record.events, &options))
            .unwrap_or_default())
    }

    async fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> Result<BTreeMap<EventId, ThreadSummary>, StoreError> {
        Ok(self
            .read(room_id, |record| {
                events::thread_summaries(&record.events, root_event_ids)
            })
            .unwrap_or_default())
    }

    async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> Result<(), StoreError> {
        if let Some(record) = self.rooms().get_mut(room_id) {
            let read = record.events.iter().find(|event| &event.id == event_id);
            if let Some(index) = read.map(|event| event.index) {
                record.raise_marker(channel_id, index);
            }
        }
        Ok(())
    }

    async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        if let Some(record) = self.rooms().get_mut(room_id) {
            let highest = record.events.iter().map(|event| event.index).max();
            if let Some(index) = highest {
                record.raise_marker(channel_id, index);
            }
        }
        Ok(())
    }

    async fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<u64, StoreError> {
        let unread = self.read(room_id, |record| {
            let marker = record.read_markers.get(channel_id).copied();
            record
                .events
                .iter()
                .filter(|event| marker.is_none_or(|read| event.index > read))
                .count()
        });
        Ok(u64::try_from(unread.unwrap_or_default()).unwrap_or(u64::MAX))
    }

    async fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> Result<BTreeMap<ChannelId, u64>, StoreError> {
        Ok(self
            .read(room_id, |record| record.read_markers.clone())
            .unwrap_or_default())
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        let room_id = task.room_id.clone();
        self.write(&room_id, |record| {
            record.tasks.push(task.clone());
            Ok(task)
        })
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        let room_id = observation.room_id.clone();
        self.write(&room_id, |record| {
            record.observations.push(observation.clone());
            Ok(observation)
        })
    }

    async fn get_task(&self, task_id: &TaskId) -> Result<Option<Task>, StoreError> {
        Ok(self.rooms().values().find_map(|record| {
            record
                .tasks
                .iter()
                .find(|task| &task.id == task_id)
                .cloned()
        }))
    }

    async fn update_task(&self, task: Task) -> Result<Task, StoreError> {
        let mut rooms = self.rooms();
        let stored = rooms
            .values_mut()
            .find_map(|record| record.tasks.iter_mut().find(|stored| stored.id == task.id))
            .ok_or_else(|| StoreError::TaskNotFound(task.id.clone()))?;
        stored.clone_from(&task);
        Ok(task)
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        Ok(self
            .read(room_id, |record| {
                record
                    .tasks
                    .iter()
                    .filter(|task| options.status.is_none_or(|status| task.status == status))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        Ok(self
            .read(room_id, |record| record.observations.clone())
            .unwrap_or_default())
    }
}
