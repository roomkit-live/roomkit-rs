//! Where rooms and their timelines live (RFC §14): Python's `roomkit.store`,
//! its `ConversationStore` and `InMemoryStore`, with the keyword arguments
//! of the store's methods as the options structs of this module.
//!
//! The store is authoritative: the core keeps no room state between
//! operations and reads what it decides on through the store, so a store
//! shared by several processes stays correct for the core; the kit's room
//! lock holds within one process, so two kits on one store need a
//! distributed lock (RFC §13.5), which 0.1 does not have
//! (docs/architecture.md).

pub(crate) mod error;
mod memory;
mod options;
pub(crate) mod query;
pub(crate) mod rooms;

use std::future::Future;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventType, Identity, IdentityId, Metadata,
    Observation, Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit_core::rules::addresses::written_recipient;
use roomkit_core::rules::routing;

pub use self::memory::InMemoryStore;
pub use self::options::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, DeleteEventOptions,
    FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions, FindRoomIdsByChannelOptions,
    FindRoomsOptions, GetConversationOptions, GetEventCountOptions, LinkAddressOptions,
    ListRoomsOptions, ListTasksOptions, PatchRoomMetadataOptions, ResolveIdentityOptions,
    UnlinkAddressOptions,
};
pub use self::query::ListEventsOptions;

pub(crate) use self::error::{StoreError, StoreFailure};
pub(crate) use self::query::EventFilter;
pub(crate) use self::rooms::RoomFilter;
use self::rooms::rooms_read_by;

/// The checks a store runs on what it writes: a value its model refuses,
/// which a struct literal can hold since it runs no check, is refused with
/// [`StoreError::Value`], nothing written, so that a later read of the
/// record never fails on it (RFC §6.4 media URLs, a rate limit not above
/// 0). The Python implementation refuses such a value when its model is
/// built; Rust refuses it at each door that writes it. Outside the public
/// API: every store of the workspace runs them, and a third-party store
/// runs none unless it calls them.
#[doc(hidden)]
pub mod written {
    use std::future::Future;

    use roomkit_core::model::{ChannelBinding, RoomEvent, Timestamp};
    use roomkit_core::ops::Environment;

    use super::StoreError;
    use crate::environment::SystemEnvironment;

    tokio::task_local! {
        /// The instant of the write the kit runs, from the kit's clock.
        static WRITTEN_AT: Timestamp;
    }

    /// An event a store writes, a commit's and its target, each content as
    /// its constructor writes it (a link's scheme in lower case).
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Value`] for a content its model refuses.
    pub fn event(event: &mut RoomEvent) -> Result<(), StoreError> {
        Ok(event.check()?)
    }

    /// A binding a store writes.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Value`] for a rate limit its model refuses.
    pub fn binding(binding: &ChannelBinding) -> Result<(), StoreError> {
        Ok(binding.check()?)
    }

    /// The store's clock: the instant an `activity` commit writes as the
    /// room's last activity (RFC §5.1), and a metadata patch as its
    /// `updated_at`, as the Python implementation's store reads its own. It
    /// is the kit's clock while the kit writes, so that a kit on a test's
    /// clock reads its rooms' timers on the same one.
    pub fn now() -> Timestamp {
        WRITTEN_AT
            .try_with(|at| *at)
            .unwrap_or_else(|_| SystemEnvironment.now())
    }

    /// Runs `write`, a store's write the kit runs, its [`now`] `at`.
    pub(crate) async fn at<F: Future>(at: Timestamp, write: F) -> F::Output {
        WRITTEN_AT.scope(at, write).await
    }
}

/// How many events a read of a whole timeline asks the store for at a time.
const TIMELINE_PAGE: u64 = 500;

/// The whole timeline of what the room `room_id` received, in index order,
/// read a page at a time, as the Python implementation reads it where it
/// needs it whole (a register's rebuild, the default router's step 3).
pub(crate) async fn whole_timeline<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
) -> Result<Vec<RoomEvent>, StoreError> {
    let mut events: Vec<RoomEvent> = Vec::new();
    loop {
        let options = ListEventsOptions {
            limit: TIMELINE_PAGE,
            after_index: events.last().map(|event| event.index),
            ..ListEventsOptions::default()
        };
        let page = store.list_events(room_id, options).await?;
        let full = u64::try_from(page.len()).is_ok_and(|read| read >= TIMELINE_PAGE);
        events.extend(page);
        if !full {
            return Ok(events);
        }
    }
}

/// Whether `named`, a stored id, names the sender known by
/// `participant_id` and the identity `identity_id` the caller resolved, as
/// Python's `sender_names` reads them (RFC §10.4 step 1).
fn names_sender(named: &str, participant_id: &str, identity_id: Option<&IdentityId>) -> bool {
    named == participant_id || identity_id.is_some_and(|identity| named == identity.as_str())
}

/// Whether `binding` names `address` through its recipient, as Python's
/// `names_by_recipient` reads it (RFC §5.7, §10.4 step 1): a binding with a
/// recipient and no correspondent, and not `group`, is its recipient's
/// conversation.
fn names_by_recipient(
    binding: &ChannelBinding,
    address: &str,
    recipient_key: Option<&str>,
) -> bool {
    let Some(key) = recipient_key else {
        return false;
    };
    if binding.participant_id.is_some() || binding.group {
        return false;
    }
    written_recipient(&binding.metadata, key).is_some_and(|recipient| recipient == address)
}

/// A conversation store (RFC §14.1): Python's `ConversationStore`, the
/// operations the pipeline and the kit use, by Python's names. Implement
/// the methods with `async fn`; a store implements what Python's ABC makes
/// abstract, and may override what it provides.
///
/// The store is authoritative: the core keeps no room state between
/// operations and reads what it decides on through the store, so a store
/// shared by several processes stays correct for the core; the kit's room
/// lock holds within one process, so two kits on one store need a
/// distributed lock (RFC §13.5), which 0.1 does not have
/// (docs/architecture.md).
///
/// The room metadata keys under `roomkit:` belong to the implementation
/// (RFC §10.6.5): the framework writes them, and host code that writes the
/// store directly must leave them alone.
pub trait ConversationStore: Send + Sync + 'static {
    /// Stores a new room, and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::RoomAlreadyExists`] for an id the store holds (RFC
    /// §14.3).
    fn create_room(&self, room: Room) -> impl Future<Output = Result<Room, StoreError>> + Send;

    /// The room `room_id`, if the store holds it.
    fn get_room(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Option<Room>, StoreError>> + Send;

    /// Writes `room` whole, keyed by its id, and returns it as stored. The
    /// counters the store manages, `event_count`, `latest_index` and
    /// `delivered_index`, keep their stored values, as Python's stores keep
    /// them: a caller's stale copy never rewinds them (RFC §14.3). A caller
    /// writes the rest from a fresh read under the room's lock: a whole
    /// write from a stale copy would undo the activity a commit just wrote.
    ///
    /// # Errors
    ///
    /// [`StoreError::RoomNotFound`] for a room the store lacks.
    fn update_room(&self, room: Room) -> impl Future<Output = Result<Room, StoreError>> + Send;

    /// Removes the keys `unset` names from the room's metadata, then writes
    /// `patch` on it, its `updated_at` the store's clock: the partial write
    /// of RFC §10.1 step 15, which a commit cannot lose; the room as it
    /// leaves it, `None` for a room the store lacks. Provided as Python
    /// provides it, a read then [`update_room`](Self::update_room), which
    /// is not atomic: a store shared across processes overrides it with
    /// one partial write.
    fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: PatchRoomMetadataOptions,
    ) -> impl Future<Output = Result<Option<Room>, StoreError>> + Send {
        async move {
            let Some(mut room) = self.get_room(room_id).await? else {
                return Ok(None);
            };
            for key in &options.unset {
                room.metadata.remove(key);
            }
            room.metadata.extend(patch);
            room.updated_at = written::now();
            Ok(Some(self.update_room(room).await?))
        }
    }

    /// Deletes the room `room_id` and everything it holds; whether the store
    /// held it.
    fn delete_room(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// A page of every room, as Python's `list_rooms(offset=0, limit=50)`.
    fn list_rooms(
        &self,
        options: ListRoomsOptions,
    ) -> impl Future<Output = Result<Vec<Room>, StoreError>> + Send;

    /// A page of the rooms `options` names: of an organization, in a
    /// status, whose metadata holds every key and value `metadata_filter`
    /// names.
    fn find_rooms(
        &self,
        options: FindRoomsOptions,
    ) -> impl Future<Output = Result<Vec<Room>, StoreError>> + Send;

    /// The rooms binding `channel_id` among those `room_filter` admits, the
    /// oldest first, at most `limit`, of a `group` binding or not when
    /// `group` names it. Provided as Python provides it, over
    /// [`find_rooms`](Self::find_rooms) and
    /// [`get_binding`](Self::get_binding).
    fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdsByChannelOptions,
    ) -> impl Future<Output = Result<Vec<RoomId>, StoreError>> + Send {
        async move {
            let filter = options.room_filter.unwrap_or_default();
            let mut matched: Vec<Room> = Vec::new();
            for room in rooms_read_by(self, &filter).await? {
                let binding = self.get_binding(&room.id, channel_id).await?;
                if binding
                    .is_some_and(|binding| options.group.is_none_or(|group| binding.group == group))
                {
                    matched.push(room);
                }
            }
            matched.sort_by(routing::recency);
            Ok(matched
                .into_iter()
                .take(options.limit)
                .map(|room| room.id)
                .collect())
        }
    }

    /// The newest room whose binding of `channel_id` names the sender, by
    /// `participant_id` or the identity the caller resolved, or names the
    /// sender's address through its recipient, among the rooms the filter
    /// admits (RFC §10.4 step 1). Provided as Python provides it.
    fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByBindingOptions,
    ) -> impl Future<Output = Result<Option<RoomId>, StoreError>> + Send {
        async move {
            let ids = FindRoomIdsByChannelOptions {
                room_filter: options.room_filter,
                limit: usize::MAX,
                group: None,
            };
            let room_ids = self.find_room_ids_by_channel(channel_id, ids).await?;
            for room_id in room_ids.into_iter().rev() {
                let Some(binding) = self.get_binding(&room_id, channel_id).await? else {
                    continue;
                };
                let named = binding.participant_id.as_ref().is_some_and(|named| {
                    names_sender(named.as_str(), participant_id, options.identity_id.as_ref())
                });
                if named
                    || names_by_recipient(
                        &binding,
                        participant_id,
                        options.recipient_key.as_deref(),
                    )
                {
                    return Ok(Some(room_id));
                }
            }
            Ok(None)
        }
    }

    /// The newest room holding a participant reached through `channel_id`
    /// who is the sender, by `participant_id` or the identity the caller
    /// resolved, among the rooms the filter admits (RFC §10.4 step 1).
    /// Provided as Python provides it.
    fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByParticipantOptions,
    ) -> impl Future<Output = Result<Option<RoomId>, StoreError>> + Send {
        async move {
            let filter = options.room_filter.unwrap_or_default();
            let mut best: Option<Room> = None;
            for room in rooms_read_by(self, &filter).await? {
                if best
                    .as_ref()
                    .is_some_and(|best| routing::recency(&room, best).is_le())
                {
                    continue;
                }
                let reached = self
                    .list_participants(&room.id)
                    .await?
                    .iter()
                    .any(|participant| {
                        participant.connected_via.contains(channel_id)
                            && (names_sender(
                                participant.id.as_str(),
                                participant_id,
                                options.identity_id.as_ref(),
                            ) || participant.identity_id.as_ref().is_some_and(|identity| {
                                names_sender(
                                    identity.as_str(),
                                    participant_id,
                                    options.identity_id.as_ref(),
                                )
                            }))
                    });
                if reached {
                    best = Some(room);
                }
            }
            Ok(best.map(|room| room.id))
        }
    }

    /// The room's delivery cursor (RFC §13.5): the index of the last event
    /// its delivery reached, `-1` before any, and for a room the store
    /// lacks.
    fn get_delivered_index(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<i64, StoreError>> + Send {
        async move {
            let room = self.get_room(room_id).await?;
            Ok(room.map_or(-1, |room| room.delivered_index))
        }
    }

    /// Moves the room's delivery cursor to `index` from `index - 1`, or from
    /// any lower value with `force`, as one compare-and-set (RFC §14.1);
    /// whether it moved. Required: a read-then-write default could move it
    /// twice (RFC §14.1).
    fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: AdvanceDeliveredIndexOptions,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// The room's private state under `key` (RFC §14.1), which its metadata
    /// never shows: `None` when nothing is stored there, or the room is
    /// unknown. Provided, as Python provides it, over the metadata; a store
    /// keeping private state apart overrides both.
    fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> impl Future<Output = Result<Option<Metadata>, StoreError>> + Send {
        async move {
            let room = self.get_room(room_id).await?;
            Ok(room.and_then(|room| room.metadata.get(key)?.as_object().cloned()))
        }
    }

    /// Writes the room's private state under `key`, `None` removing it;
    /// nothing for an unknown room (RFC §14.1). Provided, as Python provides
    /// it, through [`patch_room_metadata`](Self::patch_room_metadata).
    fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async move {
            let (patch, unset) = match value {
                Some(value) => (
                    Metadata::from_iter([(key.to_owned(), value.into())]),
                    Vec::new(),
                ),
                None => (Metadata::new(), vec![key.to_owned()]),
            };
            let options = PatchRoomMetadataOptions { unset };
            self.patch_room_metadata(room_id, patch, options).await?;
            Ok(())
        }
    }

    /// Stores `binding`, replacing the room's binding of its channel, and
    /// returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::RoomNotFound`] for a room the store lacks;
    /// [`StoreError::Value`] for a binding its model refuses.
    fn add_binding(
        &self,
        binding: ChannelBinding,
    ) -> impl Future<Output = Result<ChannelBinding, StoreError>> + Send;

    /// The room's binding of `channel_id`, if any.
    fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> impl Future<Output = Result<Option<ChannelBinding>, StoreError>> + Send;

    /// Writes `binding`, keyed by its room and channel, and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::RoomNotFound`] for a room the store lacks;
    /// [`StoreError::Value`] for a binding its model refuses.
    fn update_binding(
        &self,
        binding: ChannelBinding,
    ) -> impl Future<Output = Result<ChannelBinding, StoreError>> + Send;

    /// Removes the room's binding of `channel_id`; whether it held one.
    fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// The room's bindings; none for a room the store lacks.
    fn list_bindings(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<ChannelBinding>, StoreError>> + Send;

    /// The room, its bindings and its participants, as one read (RFC
    /// §10.1 step 3): `(None, [], [])` for a room the store lacks. Provided
    /// as Python provides it.
    #[expect(
        clippy::type_complexity,
        reason = "Python's (room, bindings, participants), a named alias would be a name Python lacks"
    )]
    fn load_room_context(
        &self,
        room_id: &RoomId,
    ) -> impl Future<
        Output = Result<(Option<Room>, Vec<ChannelBinding>, Vec<Participant>), StoreError>,
    > + Send {
        async move {
            let Some(room) = self.get_room(room_id).await? else {
                return Ok((None, Vec::new(), Vec::new()));
            };
            let bindings = self.list_bindings(room_id).await?;
            let participants = self.list_participants(room_id).await?;
            Ok((Some(room), bindings, participants))
        }
    }

    /// The room's conversation: its message events, a blocked one
    /// included, the newest `limit` in index order, or with `after_index`
    /// the first `limit` after it. Provided as Python provides it, over
    /// [`list_events`](Self::list_events).
    fn get_conversation(
        &self,
        room_id: &RoomId,
        options: GetConversationOptions,
    ) -> impl Future<Output = Result<Vec<RoomEvent>, StoreError>> + Send {
        async move {
            let messages = EventFilter {
                event_types: Some(vec![EventType::Message]),
                include_blocked: true,
                ..EventFilter::default()
            };
            let read = ListEventsOptions {
                limit: options.limit,
                after_index: options.after_index,
                event_filter: Some(messages),
                newest_first: options.after_index.is_none(),
                ..ListEventsOptions::default()
            };
            self.list_events(room_id, read).await
        }
    }

    /// Stores `participant` and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::RoomNotFound`] for a room the store lacks;
    /// [`StoreError::ParticipantExists`] for an id the room holds.
    fn add_participant(
        &self,
        participant: Participant,
    ) -> impl Future<Output = Result<Participant, StoreError>> + Send;

    /// The room's participant `participant_id`, if any.
    fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> impl Future<Output = Result<Option<Participant>, StoreError>> + Send;

    /// Writes `participant` whole, keyed by its room and id (RFC §14.1),
    /// and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::ParticipantNotFound`] for a participant the room lacks.
    fn update_participant(
        &self,
        participant: Participant,
    ) -> impl Future<Output = Result<Participant, StoreError>> + Send;

    /// Removes the room's participant `participant_id`.
    fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room's participants; none for a room the store lacks.
    fn list_participants(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<Participant>, StoreError>> + Send;

    /// Stores `identity`, replacing one under its id, and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::AddressLinked`] for an address another identity of its
    /// organization holds (RFC §5.6).
    fn create_identity(
        &self,
        identity: Identity,
    ) -> impl Future<Output = Result<Identity, StoreError>> + Send;

    /// The identity `identity_id`, if any.
    fn get_identity(
        &self,
        identity_id: &IdentityId,
    ) -> impl Future<Output = Result<Option<Identity>, StoreError>> + Send;

    /// The identity of the organization `options` names that `address` on
    /// `channel_type` belongs to, if any (RFC §5.6, §17.2).
    fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: ResolveIdentityOptions,
    ) -> impl Future<Output = Result<Option<Identity>, StoreError>> + Send;

    /// Links `address` on `channel_type` to the identity `identity_id`, of
    /// the organization `options` names (RFC §5.6).
    ///
    /// # Errors
    ///
    /// [`StoreError::IdentityNotFound`] for an identity the organization
    /// lacks; [`StoreError::AddressLinked`] for an address another of its
    /// identities holds.
    fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: LinkAddressOptions,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Unlinks `address` on `channel_type` from the identity `identity_id`
    /// (RFC §5.6, §14.1). Provided as Python provides it: refused by name,
    /// for a store that does not unlink addresses (Python's
    /// `NotImplementedError`).
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn unlink_address(
        &self,
        _identity_id: &IdentityId,
        _channel_type: &ChannelType,
        _address: &str,
        _options: UnlinkAddressOptions,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async {
            let store = std::any::type_name::<Self>();
            let name = store.rsplit("::").next().unwrap_or(store);
            Err(StoreError::Value(format!(
                "{name} does not unlink identity addresses (RFC §14.1)"
            )))
        }
    }

    /// Whether the room holds an event committed with `key`, from
    /// `channel_id` when it names one (RFC §13.4, §14.3). Required: a
    /// redelivery seen with no event is answered as a blocked duplicate,
    /// never processed again.
    fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// The event the room committed with `key`, from `channel_id` when it
    /// names one (RFC §13.4). Provided as Python provides it: `None`.
    fn get_event_by_idempotency_key(
        &self,
        _room_id: &RoomId,
        _key: &str,
        _channel_id: Option<&ChannelId>,
    ) -> impl Future<Output = Result<Option<RoomEvent>, StoreError>> + Send {
        async { Ok(None) }
    }

    /// Commits `event` to `room_id` as one atomic write (RFC §10.1 step 12,
    /// §14.3): its index the room's event count, the room's counters
    /// bumped, the change's `target` written whole as it leaves it, keeping
    /// its stored `delivery_results`, a delete's target's edits marked
    /// deleted, and, for an `activity` commit, the room's last activity
    /// moved to the commit's instant, the store's clock (the kit's while
    /// the kit commits), and a `PAUSED` room resumed
    /// ([`commit_activity`](roomkit_core::rules::lifecycle::commit_activity),
    /// RFC §5.1); the event as stored. A blank idempotency key is stored as
    /// none (RFC §13.4). Required: a read-then-write default would let the
    /// counters and the timeline diverge (RFC §14.3).
    ///
    /// # Errors
    ///
    /// [`StoreError::IdempotencyKeyTaken`] for a key its channel holds in
    /// the room, nothing written; [`StoreError::EventNotFound`] for a
    /// target the room no longer holds; [`StoreError::Value`] for a content
    /// its model refuses.
    fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: CommitEventOptions,
    ) -> impl Future<Output = Result<RoomEvent, StoreError>> + Send;

    /// The event `event_id`, if any.
    fn get_event(
        &self,
        event_id: &EventId,
    ) -> impl Future<Output = Result<Option<RoomEvent>, StoreError>> + Send;

    /// Writes `event` whole, keyed by its id, keeping the stored
    /// `delivery_results` (RFC §14.3), and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::EventNotFound`] for an event the store lacks;
    /// [`StoreError::Value`] for a content its model refuses.
    fn update_event(
        &self,
        event: RoomEvent,
    ) -> impl Future<Output = Result<RoomEvent, StoreError>> + Send;

    /// Hard-deletes the room's event `event_id` (RFC §10.3), its replies
    /// one level deep with `cascade_replies`, and always the edits and
    /// deletes of what it removes; the events removed in §10.3's order, the
    /// target, its replies by index, then the changes by index, none when
    /// the room lacks it. The room's counters stay as they are.
    fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: DeleteEventOptions,
    ) -> impl Future<Output = Result<Vec<RoomEvent>, StoreError>> + Send;

    /// How many of the room's events `event_filter` admits, every row of
    /// its timeline, a refused one included, without one; `0` for a room
    /// the store lacks. Once events are hard-deleted, the count is lower
    /// than the room's `event_count`, which counts every index given out
    /// (gap G-064).
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] for a filter its model refuses.
    fn get_event_count(
        &self,
        room_id: &RoomId,
        options: GetEventCountOptions,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// A page of the room's timeline ([`ListEventsOptions`]); none for a
    /// room the store lacks.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] for both cursors, or a filter its model
    /// refuses.
    fn list_events(
        &self,
        room_id: &RoomId,
        options: ListEventsOptions,
    ) -> impl Future<Output = Result<Vec<RoomEvent>, StoreError>> + Send;

    /// Stores `task` and returns it (RFC §10.1 step 15).
    fn add_task(&self, task: Task) -> impl Future<Output = Result<Task, StoreError>> + Send;

    /// Stores `observation` and returns it (RFC §10.1 step 15).
    fn add_observation(
        &self,
        observation: Observation,
    ) -> impl Future<Output = Result<Observation, StoreError>> + Send;

    /// The room's tasks, of the status `options` names; none for a room the
    /// store lacks.
    fn list_tasks(
        &self,
        room_id: &RoomId,
        options: ListTasksOptions,
    ) -> impl Future<Output = Result<Vec<Task>, StoreError>> + Send;

    /// The room's observations; none for a room the store lacks.
    fn list_observations(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<Observation>, StoreError>> + Send;

    /// Releases what the store holds, a file or a pool: Python's
    /// `close()`, which the kit's [`close`](crate::RoomKit::close) calls
    /// once no work can reach the store any more (RFC §12.10.4). An
    /// operation after it may fail. Nothing to release by default.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}
