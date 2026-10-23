//! Where rooms and their timelines live (RFC §14).
//!
//! The store is authoritative: the core keeps no room state between
//! operations and reads what it decides on through the store, so a store
//! shared by several processes stays correct (docs/architecture.md).

mod memory;

use std::future::Future;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ErrorRecord, Observation, Participant, ParticipantId, Room,
    RoomContext, RoomEvent, RoomId, RoomStatus, RoomUpdates, Task,
};
use roomkit_core::ops::RoomCounters;
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
    /// The room's timeline holds the index already (RFC §8.1): another
    /// writer committed first.
    #[error("room {room_id} holds index {index} already")]
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
}

impl RoomFilter {
    /// Whether `room` passes the filter.
    pub fn matches(&self, room: &Room) -> bool {
        self.organization_id
            .as_ref()
            .is_none_or(|organization| room.organization_id.as_ref() == Some(organization))
            && self.status.is_none_or(|status| room.status == status)
            && (!self.timers_armed || timers::armed(room))
    }
}

/// A conversation store (RFC §14.1): the operations the pipeline and the kit
/// use. Implement the methods with `async fn`.
///
/// The room metadata keys under `roomkit.` belong to the implementation
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

    /// The event the room committed from `channel_id` with idempotency
    /// `key` (RFC §13.4, gap G-023).
    fn find_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        key: &str,
    ) -> impl Future<Output = Result<Option<RoomEvent>, StoreError>> + Send;

    /// Stores `event`, sets the room's counters and writes the room updates
    /// they carry (a paused room's resume), as one atomic commit (RFC
    /// §14.3). Refuses with [`StoreError::IndexTaken`] an index the room's
    /// timeline holds already (RFC §8.1).
    fn commit(
        &self,
        event: RoomEvent,
        counters: RoomCounters,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

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
