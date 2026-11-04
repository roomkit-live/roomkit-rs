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

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Mutex, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventId, EventType, Identity, IdentityId,
    Metadata, Observation, Participant, ParticipantId, Room, RoomEvent, RoomId, RoomStatus, Task,
    TaskId, ThreadSummary, Timestamp, Verification, VerificationRequest,
};
use roomkit_core::rules::addresses::written_recipient;
use roomkit_core::rules::edits;
use roomkit_core::rules::routing;

pub use self::memory::InMemoryStore;
pub use self::options::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, DeleteEventOptions, FindLatestRoomOptions,
    FindRoomIdByBindingOptions, FindRoomIdByChannelOptions, FindRoomIdByParticipantOptions,
    FindRoomIdsByChannelOptions, FindRoomsOptions, GetConversationOptions, GetEventCountOptions,
    GetTimelineOptions, GetVerificationOptions, LinkAddressOptions, ListRoomsOptions,
    ListTasksOptions, PatchRoomMetadataOptions, ResolveIdentityOptions, UnlinkAddressOptions,
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

/// The name of the store type `S`, as Python's `type(store).__name__`
/// reads it: its path and its generic arguments left out
/// (`Wrapper<roomkit::InMemoryStore>` is `Wrapper`).
fn store_name<S: ?Sized>() -> &'static str {
    let full = std::any::type_name::<S>();
    let bare = full.split('<').next().unwrap_or(full);
    bare.rsplit("::").next().unwrap_or(bare)
}

/// Says once per store type that `S` keeps each room's private state in
/// its metadata, as Python's `_warn_private_state_in_metadata` says it once
/// per store: a trait's provided method holds no state of the store's own,
/// so the store types that warned are kept for the process.
fn warn_private_state_in_metadata<S: ?Sized>() {
    static WARNED: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());
    let store = std::any::type_name::<S>();
    let first = WARNED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(store);
    if !first {
        return;
    }
    let name = store_name::<S>();
    tracing::warn!(
        target: "roomkit::store",
        "{name} keeps each room's private state, the register of authors among it, in \
         the room's metadata, which clients may read: the register's digests name the \
         room's senders to whoever tries every phone number or name. Implement \
         get_private_state() and set_private_state() to keep it apart."
    );
}

/// The refusal of a store that does not keep verifications (RFC §11.7,
/// Level 2), named by its type as Python's `_verification_unsupported`
/// names it.
fn verification_unsupported<S: ?Sized>() -> StoreError {
    let name = store_name::<S>();
    StoreError::Value(format!(
        "{name} does not store identity verifications (RFC §11.7)"
    ))
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

/// Whether `room` is still in `status`, with `paused_at` and its timers'
/// `last_activity_at`: the state a timer transition was computed from, as
/// Python's `room_holds` reads it.
pub(crate) fn room_holds(
    room: &Room,
    status: RoomStatus,
    paused_at: Option<Timestamp>,
    last_activity_at: Option<Timestamp>,
) -> bool {
    room.status == status
        && room.paused_at == paused_at
        && room.timers.last_activity_at == last_activity_at
}

/// Marks every edit of `target_id` in the room deleted, through the store's
/// own reads and writes, as Python's default `_mark_edits_deleted` does: an
/// edit is told by its content's target, whatever its type.
async fn mark_edits_deleted<S: ConversationStore + ?Sized>(
    store: &S,
    room_id: &RoomId,
    target_id: &EventId,
) -> Result<(), StoreError> {
    let every_row = EventFilter {
        include_blocked: true,
        ..EventFilter::default()
    };
    let mut offset = 0;
    loop {
        let read = ListEventsOptions {
            offset,
            limit: TIMELINE_PAGE,
            event_filter: Some(every_row.clone()),
            ..ListEventsOptions::default()
        };
        let page = store.list_events(room_id, read).await?;
        if page.is_empty() {
            return Ok(());
        }
        offset = offset.saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        for mut edit in page {
            let edits_target = matches!(
                &edit.content,
                EventContent::Edit(content) if &content.target_event_id == target_id
            );
            if edits_target {
                edit.metadata.insert(edits::DELETED.to_owned(), true.into());
                store.update_event(edit).await?;
            }
        }
    }
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
    /// Python `is_process_local`: whether the store lives and dies with this
    /// process, so that no other process can share it. A persistent store,
    /// the default, is shared as soon as a deployment runs a second process:
    /// the kit warns when one is paired with an in-process lock (RFC §13.5).
    /// Hidden as Python's class attribute is out of its signature list.
    #[doc(hidden)]
    fn is_process_local(&self) -> bool {
        false
    }

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

    /// Whether the store holds the room `room_id`, read without the room:
    /// the yes or no inbound routing asks on every message. Provided as
    /// Python provides it, over [`get_room`](Self::get_room); a store
    /// overrides it with an existence query.
    fn room_exists(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        async move { Ok(self.get_room(room_id).await?.is_some()) }
    }

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

    /// Writes `room` as [`update_room`](Self::update_room) does, only if the
    /// stored room is still in `status`, with `paused_at` and
    /// `last_activity_at` as its timers' last activity: a compare-and-set,
    /// the write a timer transition computed from a room read earlier makes,
    /// which a commit that resumed the room or moved its activity meanwhile
    /// must not lose (RFC §5.1). Returns the room written, or `None` with
    /// nothing written when the stored room moved or is gone. Provided as
    /// Python provides it, a read, a comparison then a write, which is not
    /// atomic: a store whose commits can run beside it overrides it with one
    /// transaction, as the workspace's stores do.
    ///
    /// # Errors
    ///
    /// As [`update_room`](Self::update_room).
    fn update_room_if(
        &self,
        room: Room,
        status: RoomStatus,
        paused_at: Option<Timestamp>,
        last_activity_at: Option<Timestamp>,
    ) -> impl Future<Output = Result<Option<Room>, StoreError>> + Send {
        async move {
            let Some(stored) = self.get_room(&room.id).await? else {
                return Ok(None);
            };
            if !room_holds(&stored, status, paused_at, last_activity_at) {
                return Ok(None);
            }
            Ok(Some(self.update_room(room).await?))
        }
    }

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
                room.metadata.shift_remove(key);
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

    /// The newest room of the participant `participant_id`, the last
    /// created, the greatest id by code point breaking a tie, in the status
    /// `options` names: a room holding their participant record, on any
    /// channel, or binding a channel to them, of the channel type `options`
    /// names; `None` when none.
    fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: FindLatestRoomOptions,
    ) -> impl Future<Output = Result<Option<Room>, StoreError>> + Send;

    /// A room binding `channel_id`, in the status `options` names: the
    /// oldest, as [`find_room_ids_by_channel`](Self::find_room_ids_by_channel)
    /// orders them, as the Python implementation's stores answer; `None`
    /// when none.
    fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdByChannelOptions,
    ) -> impl Future<Output = Result<Option<RoomId>, StoreError>> + Send;

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
    /// unknown. Provided, as Python provides it, over the metadata, which
    /// clients may read, saying so once per store type with a warning; a
    /// store keeping private state apart overrides both.
    fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> impl Future<Output = Result<Option<Metadata>, StoreError>> + Send {
        async move {
            warn_private_state_in_metadata::<Self>();
            let room = self.get_room(room_id).await?;
            Ok(room.and_then(|room| room.metadata.get(key)?.as_object().cloned()))
        }
    }

    /// Writes the room's private state under `key`, `None` removing it;
    /// nothing for an unknown room (RFC §14.1). Provided, as Python provides
    /// it, through [`patch_room_metadata`](Self::patch_room_metadata), with
    /// [`get_private_state`](Self::get_private_state)'s warning.
    fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async move {
            warn_private_state_in_metadata::<Self>();
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

    /// Whether the room `room_id` binds `channel_id`, read without the
    /// binding: routing's yes or no (RFC §10.1 step 2). Provided as Python
    /// provides it, over [`get_binding`](Self::get_binding); a store
    /// overrides it with an existence query.
    fn binding_exists(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        async move { Ok(self.get_binding(room_id, channel_id).await?.is_some()) }
    }

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

    /// The room's whole activity: the events it received, of every type,
    /// a refused one only when `options.event_filter` asks for it, the
    /// oldest `limit` in index order, the newest with `newest_first`, or
    /// with `after_index` the first `limit` after it. Provided as Python
    /// provides it, over [`list_events`](Self::list_events).
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] for a filter its model refuses.
    fn get_timeline(
        &self,
        room_id: &RoomId,
        options: GetTimelineOptions,
    ) -> impl Future<Output = Result<Vec<RoomEvent>, StoreError>> + Send {
        async move {
            let read = ListEventsOptions {
                limit: options.limit,
                after_index: options.after_index,
                event_filter: options.event_filter,
                newest_first: options.newest_first,
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
            let name = store_name::<Self>();
            Err(StoreError::Value(format!(
                "{name} does not unlink identity addresses (RFC §14.1)"
            )))
        }
    }

    /// The verification of the identity `identity_id` within the
    /// organization `options` names, if it has one (RFC §11.7.1, §14.1).
    /// Provided as Python provides it: refused by name, for a store that
    /// does not keep verifications (Level 2).
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn get_verification(
        &self,
        _identity_id: &IdentityId,
        _options: GetVerificationOptions,
    ) -> impl Future<Output = Result<Option<Verification>, StoreError>> + Send {
        async { Err(verification_unsupported::<Self>()) }
    }

    /// Stores `verification`, replacing the one its identity holds in its
    /// organization, and returns it (RFC §11.7.1). Provided as Python
    /// provides it: refused by name.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn put_verification(
        &self,
        _verification: Verification,
    ) -> impl Future<Output = Result<Verification, StoreError>> + Send {
        async { Err(verification_unsupported::<Self>()) }
    }

    /// Every verification VERIFIED with an expiry, across organizations,
    /// read when the framework starts to arm the timers that end them
    /// (RFC §11.7.5). Provided as Python provides it: refused by name.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn list_verifications_in_force(
        &self,
    ) -> impl Future<Output = Result<Vec<Verification>, StoreError>> + Send {
        async { Err(verification_unsupported::<Self>()) }
    }

    /// Stores a new verification request and returns it (RFC §11.7.2).
    /// Provided as Python provides it: refused by name.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn create_verification_request(
        &self,
        _request: VerificationRequest,
    ) -> impl Future<Output = Result<VerificationRequest, StoreError>> + Send {
        async { Err(verification_unsupported::<Self>()) }
    }

    /// The verification request `request_id`, if any (RFC §11.7.2).
    /// Provided as Python provides it: refused by name.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn get_verification_request(
        &self,
        _request_id: &str,
    ) -> impl Future<Output = Result<Option<VerificationRequest>, StoreError>> + Send {
        async { Err(verification_unsupported::<Self>()) }
    }

    /// Replaces the stored verification request of `request`'s id, and
    /// returns it (RFC §11.7.2). Provided as Python provides it: refused by
    /// name.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`] by default.
    fn update_verification_request(
        &self,
        _request: VerificationRequest,
    ) -> impl Future<Output = Result<VerificationRequest, StoreError>> + Send {
        async { Err(verification_unsupported::<Self>()) }
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

    /// Stores `event` as it is, its index the caller's: the room's counters
    /// stay as they are, which only [`commit_event`](Self::commit_event)
    /// moves, and the room's next index is kept past it. Python's
    /// `add_event`, a write below the pipeline's commit.
    ///
    /// # Errors
    ///
    /// [`StoreError::RoomNotFound`] for a room the store lacks, nothing
    /// written (decided 2026-10-31, RMK-1058);
    /// [`StoreError::IdempotencyKeyTaken`] for a key its channel holds in
    /// the room for another event; [`StoreError::Value`] for a content its
    /// model refuses.
    fn add_event(
        &self,
        event: RoomEvent,
    ) -> impl Future<Output = Result<RoomEvent, StoreError>> + Send;

    /// Stores `event` at the room's next index, and returns it so: the room's
    /// counters stay as they are. Provided as Python provides it, a count
    /// then [`add_event`](Self::add_event), which is not atomic and reuses
    /// a deleted index: a store overrides it with one write that reserves
    /// the index, as the Python implementation's stores do.
    ///
    /// # Errors
    ///
    /// As [`add_event`](Self::add_event).
    fn add_event_auto_index(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
    ) -> impl Future<Output = Result<RoomEvent, StoreError>> + Send {
        async move {
            let every_row = GetEventCountOptions { event_filter: None };
            event.index = self.get_event_count(room_id, every_row).await?;
            self.add_event(event).await
        }
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

    /// Writes `results`, the outcome of the event's delivery set, on the
    /// event as stored, and nothing else of it (RFC §10.3, §14.3): the one
    /// write of `delivery_results`, which every rewrite of the row keeps.
    /// An event the room `room_id` does not hold is left alone. Provided as
    /// Python provides it: the stored event written back with the results
    /// through [`update_event`](Self::update_event), then read back, which
    /// serves a store whose `update_event` writes the row as given; over one
    /// that keeps the stored results, as its contract says, it fails saying
    /// so rather than record nothing: such a store overrides this method.
    ///
    /// # Errors
    ///
    /// [`StoreError::Value`], nothing written, when `update_event` kept the
    /// stored results (Python's `NotImplementedError`).
    fn record_delivery_results(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        results: Metadata,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async move {
            let Some(mut stored) = self.get_event(event_id).await? else {
                return Ok(());
            };
            if &stored.room_id != room_id {
                return Ok(());
            }
            stored.delivery_results.clone_from(&results);
            self.update_event(stored).await?;
            let written = self.get_event(event_id).await?;
            if written.is_some_and(|written| written.delivery_results == results) {
                return Ok(());
            }
            let name = store_name::<Self>();
            Err(StoreError::Value(format!(
                "{name} keeps delivery_results on update_event: override record_delivery_results to write them"
            )))
        }
    }

    /// Writes `target` as the edit or the delete the persistence policy
    /// keeps out of the timeline changed it (RFC §10.3): the target's row
    /// alone, its stored delivery results kept, no row of the change, the
    /// room's counters unmoved, and when the target is now deleted
    /// (`metadata.deleted`) every edit of it marked deleted too, as
    /// [`commit_event`](Self::commit_event) does with a change it stores.
    /// Provided as Python provides it, through
    /// [`update_event`](Self::update_event) and the store's reads, which is
    /// not atomic: a store overrides it with one transaction.
    ///
    /// # Errors
    ///
    /// [`StoreError::EventNotFound`], nothing written, for a target the room
    /// `room_id` no longer holds.
    fn write_change_target(
        &self,
        room_id: &RoomId,
        mut target: RoomEvent,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async move {
            let Some(stored) = self
                .get_event(&target.id)
                .await?
                .filter(|stored| &stored.room_id == room_id)
            else {
                return Err(StoreError::EventNotFound(target.id));
            };
            target.delivery_results = stored.delivery_results;
            let deleted = edits::is_deleted(&target);
            let target_id = target.id.clone();
            self.update_event(target).await?;
            if deleted {
                mark_edits_deleted(self, room_id, &target_id).await?;
            }
            Ok(())
        }
    }

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

    /// The summary of each thread of `root_event_ids` that has replies in
    /// the room, by its root: how many replies it received and the instant
    /// of the last, what a "N replies" affordance shows. A root with no
    /// reply has none, and a reply the room refused is not counted, as a
    /// default timeline read of the thread does not serve it (RFC §14.1).
    fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> impl Future<Output = Result<BTreeMap<EventId, ThreadSummary>, StoreError>> + Send;

    /// Stores `task` and returns it (RFC §10.1 step 15).
    fn add_task(&self, task: Task) -> impl Future<Output = Result<Task, StoreError>> + Send;

    /// Stores `observation` and returns it (RFC §10.1 step 15).
    fn add_observation(
        &self,
        observation: Observation,
    ) -> impl Future<Output = Result<Observation, StoreError>> + Send;

    /// The task `task_id`, whichever room holds it; `None` when the store
    /// holds none.
    fn get_task(
        &self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Task>, StoreError>> + Send;

    /// Writes `task` whole, keyed by its id, and returns it.
    ///
    /// # Errors
    ///
    /// [`StoreError::TaskNotFound`] for a task the store does not hold,
    /// nothing written (RFC §14.3): an update is no creation, which
    /// [`add_task`](Self::add_task) makes.
    fn update_task(&self, task: Task) -> impl Future<Output = Result<Task, StoreError>> + Send;

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

    /// Marks the room's event `event_id` read on `channel_id`: the
    /// channel's marker, a high-water mark on the events' index, moves to
    /// the event's when that is higher, never back. An event that is not
    /// the room's, another room's or none, marks nothing.
    fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Marks every event of the room read on `channel_id`: its marker moves
    /// to the room's highest index, never back. A room with no event gets
    /// no marker, so its first event, index 0, arrives unread.
    fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// How many of the room's events `channel_id` has not read: those whose
    /// index is above its marker, every one when it has none.
    fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Every channel's read marker in the room, the highest index it read;
    /// a channel with none is absent. With one channel per member, what a
    /// "seen by" receipt is made of. None for a room the store lacks, whose
    /// markers went with it.
    fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<BTreeMap<ChannelId, u64>, StoreError>> + Send;

    /// Releases what the store holds, a file or a pool: Python's
    /// `close()`, which the kit's [`close`](crate::RoomKit::close) calls
    /// once no work can reach the store any more (RFC §12.10.4). An
    /// operation after it may fail. Nothing to release by default.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryStore, store_name};

    struct Wrapper<T>(T);

    /// A store is named as Python's `type(store).__name__`, a generic one
    /// by its bare type.
    #[test]
    fn a_store_is_named_by_its_bare_type() {
        assert_eq!(
            (
                store_name::<InMemoryStore>(),
                store_name::<Wrapper<InMemoryStore>>()
            ),
            ("InMemoryStore", "Wrapper")
        );
    }
}
