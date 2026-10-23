//! Where rooms and their timelines live (RFC §14).
//!
//! The store is authoritative: the core keeps no room state between
//! operations and reads what it decides on through the store, so a store
//! shared by several processes stays correct (docs/architecture.md).

mod memory;

use std::future::Future;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, ErrorRecord, EventId, EventStatus, EventUpdates,
    Identity, IdentityId, Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent,
    RoomId, RoomStatus, RoomUpdates, Task,
};
use roomkit_core::ops::{RoomCounters, TargetUpdate};
use roomkit_core::rules::timers;
use thiserror::Error;

pub use self::memory::InMemoryStore;

/// Why a store refused or failed a request.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// A room with the id exists already.
    #[error("room {0} exists already")]
    RoomExists(RoomId),
    /// No room has the id.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The room has a participant with the id already (RFC §5.5).
    #[error("room {room_id} has a participant {participant_id} already")]
    ParticipantExists {
        /// The room.
        room_id: RoomId,
        /// The participant.
        participant_id: ParticipantId,
    },
    /// The room has no participant with the id.
    #[error("room {room_id} has no participant {participant_id}")]
    ParticipantNotFound {
        /// The room.
        room_id: RoomId,
        /// The participant.
        participant_id: ParticipantId,
    },
    /// An identity with the id exists already.
    #[error("identity {0} exists already")]
    IdentityExists(IdentityId),
    /// No identity of the organization has the id.
    #[error("no identity {0} in the organization")]
    IdentityNotFound(IdentityId),
    /// Another identity of the organization uses the address on channels of
    /// the type already (RFC §14.1, gap G-060).
    #[error("the {channel_type:?} address {address} belongs to identity {identity_id} already")]
    AddressLinked {
        /// The channel type.
        channel_type: ChannelType,
        /// The address.
        address: String,
        /// The identity that uses it.
        identity_id: IdentityId,
    },
    /// No event has the id (RFC §14.1 `update_event`), or the target of
    /// an edit or a delete is gone by its commit.
    #[error("event {0} does not exist")]
    EventNotFound(EventId),
    /// The commit's index is not the room's next one (RFC §8.1): another
    /// writer committed first, or the index was given out already, a
    /// hard-deleted one included, which is never given again.
    #[error("index {index} is not room {room_id}'s next index")]
    IndexTaken {
        /// The room.
        room_id: RoomId,
        /// The index.
        index: u64,
    },
    /// The store could not be reached or failed.
    #[error("the store failed: {0}")]
    Unavailable(String),
}

impl StoreError {
    /// The error as the core records it.
    pub fn to_record(&self) -> ErrorRecord {
        let error_type = match self {
            Self::RoomExists(_) => "RoomExists",
            Self::RoomNotFound(_) => "RoomNotFound",
            Self::ParticipantExists { .. } => "ParticipantExists",
            Self::ParticipantNotFound { .. } => "ParticipantNotFound",
            Self::IdentityExists(_) => "IdentityExists",
            Self::IdentityNotFound(_) => "IdentityNotFound",
            Self::AddressLinked { .. } => "AddressLinked",
            Self::EventNotFound(_) => "EventNotFound",
            Self::IndexTaken { .. } => "IndexTaken",
            Self::Unavailable(_) => "StoreUnavailable",
        };
        ErrorRecord::new(error_type, self.to_string())
    }
}

/// Which rooms [`ConversationStore::list_rooms`] returns; an empty filter
/// returns every room.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomFilter {
    /// Only the rooms of this organization.
    pub organization_id: Option<String>,
    /// Only the rooms in this status.
    pub status: Option<RoomStatus>,
    /// Only the rooms with a timer that can still fall due: an ACTIVE room
    /// with an inactivity timer, a PAUSED room with a close timer (RFC §5.1).
    pub timers_armed: bool,
    /// Only the rooms this channel reaches: those binding it, and those
    /// holding a participant reached through it (RFC §5.5 `connected_via`),
    /// the rooms the default router reads (RFC §10.4).
    pub channel_id: Option<ChannelId>,
}

impl RoomFilter {
    /// Whether the room `room`, with its `bindings` and `participants`,
    /// passes the filter.
    pub fn matches(
        &self,
        room: &Room,
        bindings: &[ChannelBinding],
        participants: &[Participant],
    ) -> bool {
        let reached = self.channel_id.as_ref().is_none_or(|channel_id| {
            bindings
                .iter()
                .any(|binding| &binding.channel_id == channel_id)
                || participants
                    .iter()
                    .any(|participant| participant.connected_via.contains(channel_id))
        });
        self.organization_id
            .as_ref()
            .is_none_or(|organization| room.organization_id.as_ref() == Some(organization))
            && self.status.is_none_or(|status| room.status == status)
            && (!self.timers_armed || timers::armed(room))
            && reached
    }
}

/// Which of a room's events a count stands for (RFC §14.1 `EventFilter`):
/// the rows [`ConversationStore::list_events`] serves under the same
/// criteria.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventFilter {
    /// Whether the events stored `BLOCKED` count; a timeline read serves
    /// what the room received by default.
    pub include_blocked: bool,
}

impl EventFilter {
    /// Whether `event` passes the filter.
    pub fn matches(&self, event: &RoomEvent) -> bool {
        self.include_blocked || event.status != EventStatus::Blocked
    }
}

/// A conversation store (RFC §14.1): the operations the pipeline and the kit
/// use. Implement the methods with `async fn`.
///
/// The room metadata keys under `roomkit:` belong to the implementation
/// (RFC §10.6.5): the framework writes them through [`update_room`], and
/// host code that writes the store directly must leave them alone.
///
/// [`update_room`]: ConversationStore::update_room
pub trait ConversationStore: Send + Sync + 'static {
    /// Stores a new room.
    fn create_room(&self, room: Room) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Writes the fields `updates` names on the stored room, keeping every
    /// other one as stored, and returns the room as it now stands (RFC §14.1
    /// `update_room(room_id, updates)`). The write is atomic: a commit can
    /// neither fall in the middle of it nor be lost by it, whether or not
    /// its caller holds the room's lock.
    fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> impl Future<Output = Result<Room, StoreError>> + Send;

    /// The room `room_id`, if it exists.
    fn get_room(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Option<Room>, StoreError>> + Send;

    /// The rooms `filter` admits, oldest first (RFC §14.1 `list_rooms`).
    fn list_rooms(
        &self,
        filter: &RoomFilter,
    ) -> impl Future<Output = Result<Vec<Room>, StoreError>> + Send;

    /// The newest room `filter` admits whose binding of `channel_id` names
    /// `participant_id` as its correspondent (RFC §14.1
    /// `find_room_id_by_binding`, the filter carrying its status and the
    /// caller's organization, gap G-058). Ids compare as stored. Rooms are
    /// ordered as the default router orders them
    /// ([`rules::routing::recency`](roomkit_core::rules::routing::recency)):
    /// by creation, then by id. Step 1 reads ACTIVE and PAUSED rooms (gap
    /// G-045): a router built on this query asks it for each status, and
    /// keeps the newer of the rooms it answers.
    fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        filter: &RoomFilter,
    ) -> impl Future<Output = Result<Option<RoomId>, StoreError>> + Send;

    /// The newest room `filter` admits holding a participant reached through
    /// `channel_id` whose id is `participant_id`, or whose identity's is
    /// (RFC §14.1 `find_room_id_by_participant`, given the channel by gap
    /// G-058). Ids compare as stored; rooms are ordered, and step 1 asks
    /// for each status, as in
    /// [`find_room_id_by_binding`](Self::find_room_id_by_binding).
    fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        filter: &RoomFilter,
    ) -> impl Future<Output = Result<Option<RoomId>, StoreError>> + Send;

    /// Stores a binding, replacing the room's binding of the same channel.
    fn store_binding(
        &self,
        binding: ChannelBinding,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Removes the room's binding of `channel_id`; removing a binding the
    /// room does not have changes nothing (RFC §14.1 `delete_binding`).
    fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room's bindings.
    fn list_bindings(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<ChannelBinding>, StoreError>> + Send;

    /// The room with its bindings, its participants and its latest
    /// `recent_events` events, refused ones included (RFC §10.1 step 3);
    /// `None` when the room does not exist.
    fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> impl Future<Output = Result<Option<RoomContext>, StoreError>> + Send;

    /// Stores a new participant of its room (RFC §14.1
    /// `create_participant`). A participant is one record per room and id
    /// (RFC §5.5): an id the room has is refused with
    /// [`StoreError::ParticipantExists`].
    fn create_participant(
        &self,
        participant: Participant,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room's participant `participant_id`, if it has one (RFC §14.1
    /// `get_participant`, named within its room by gap G-054).
    fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> impl Future<Output = Result<Option<Participant>, StoreError>> + Send;

    /// Writes `participant` whole, in place of its room's record of its id
    /// (RFC §14.1 `update_participant`, gap G-054). A participant the room
    /// does not have is refused with [`StoreError::ParticipantNotFound`].
    fn update_participant(
        &self,
        participant: Participant,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Removes the room's participant `participant_id`; removing one the
    /// room does not have changes nothing (RFC §14.1 `delete_participant`).
    fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room's participants, in the order they were created (RFC §14.1
    /// `list_participants`).
    fn list_participants(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<Participant>, StoreError>> + Send;

    /// Stores a new identity (RFC §14.1 `create_identity`). An id taken is
    /// refused with [`StoreError::IdentityExists`], and an address another
    /// identity of its organization uses with [`StoreError::AddressLinked`]
    /// (gap G-060).
    fn create_identity(
        &self,
        identity: Identity,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The identity `identity_id`, if any (RFC §14.1 `get_identity`).
    fn get_identity(
        &self,
        identity_id: &IdentityId,
    ) -> impl Future<Output = Result<Option<Identity>, StoreError>> + Send;

    /// The identity of `organization_id` that uses `address` on channels of
    /// `channel_type`, if any (RFC §14.1 `resolve_identity`, §17.2). An
    /// organization of `None` resolves the identities of no organization
    /// (gap G-060). Addresses compare as stored: link them in their
    /// channel's form, a phone number in E.164 (RFC §5.7).
    fn resolve_identity(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> impl Future<Output = Result<Option<Identity>, StoreError>> + Send;

    /// Links `address` on channels of `channel_type` to the identity
    /// `identity_id` of `organization_id` (RFC §14.1 `link_address`). An
    /// identity of another organization is refused with
    /// [`StoreError::IdentityNotFound`], and an address another identity of
    /// the organization uses with [`StoreError::AddressLinked`]; linking an
    /// address the identity uses changes nothing.
    fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The event the room committed from `channel_id` with idempotency
    /// `key` (RFC §13.4, gap G-023).
    fn find_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        key: &str,
    ) -> impl Future<Output = Result<Option<RoomEvent>, StoreError>> + Send;

    /// Stores `event`, sets the room's counters, writes the room updates
    /// they carry (a paused room's resume) and the update an edit or a
    /// delete makes to its target, as one atomic commit (RFC §10.1 step 12,
    /// §14.3). Refuses with [`StoreError::IndexTaken`] an index the room has
    /// given out already: one its timeline holds, or one a hard delete
    /// removed, since the counters count both (RFC §8.1, gap G-064). Refuses
    /// with [`StoreError::EventNotFound`] a target the room no longer holds,
    /// writing nothing.
    fn commit(
        &self,
        event: RoomEvent,
        counters: RoomCounters,
        target: Option<TargetUpdate>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The event `event_id`, whichever room holds it, refused ones included
    /// (RFC §14.1 `get_event`).
    fn get_event(
        &self,
        event_id: &EventId,
    ) -> impl Future<Output = Result<Option<RoomEvent>, StoreError>> + Send;

    /// Writes the fields `updates` names on the stored event, keeping every
    /// other one, and returns the event as it now stands (RFC §14.1
    /// `update_event(event_id, updates)`). An event no room holds is refused
    /// with [`StoreError::EventNotFound`].
    fn update_event(
        &self,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> impl Future<Output = Result<RoomEvent, StoreError>> + Send;

    /// Removes the room's event `event_id` for good, with, when
    /// `cascade_replies` is set, the events whose thread it roots
    /// (`parent_event_id`, a reply's replies included), and always the edits
    /// and deletes of every event removed (gap G-063). Returns the events
    /// removed, the target first (RFC §14.1 `delete_event(room_id, event_id,
    /// cascade_replies)`, which names their ids; the records let the kit fire
    /// `ON_EVENT_DELETED` for each, gap G-064). An event the room does not
    /// hold removes nothing. The room's counters are left as they are.
    fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> impl Future<Output = Result<Vec<RoomEvent>, StoreError>> + Send;

    /// How many of the room's events `filter` admits, or how many rows the
    /// timeline holds, the refused ones included, with no filter (RFC §14.1
    /// `get_event_count`). Once events are hard-deleted, the count is lower
    /// than the room's `event_count`, which counts every index given out
    /// (gap G-064).
    fn get_event_count(
        &self,
        room_id: &RoomId,
        filter: Option<EventFilter>,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Stores side effects (RFC §10.1 step 15).
    fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room's timeline in index order: the events the room received,
    /// and the refused ones when `include_blocked` is set (RFC §14.1).
    fn list_events(
        &self,
        room_id: &RoomId,
        include_blocked: bool,
    ) -> impl Future<Output = Result<Vec<RoomEvent>, StoreError>> + Send;

    /// The room's tasks.
    fn list_tasks(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<Task>, StoreError>> + Send;

    /// The room's observations.
    fn list_observations(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<Observation>, StoreError>> + Send;
}
