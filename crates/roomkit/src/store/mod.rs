//! Where rooms and their timelines live (RFC §14).
//!
//! The store is authoritative: the core keeps no room state between
//! operations and reads what it decides on through the store, so a store
//! shared by several processes stays correct (docs/architecture.md).

mod memory;

use std::future::Future;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ErrorRecord, Observation, Room, RoomContext, RoomEvent, RoomId, Task,
};
use roomkit_core::ops::RoomCounters;
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
            Self::IndexTaken { .. } => "IndexTaken",
            Self::Unavailable(_) => "StoreUnavailable",
        };
        ErrorRecord::new(error_type, self.to_string())
    }
}

/// A conversation store (RFC §14.1): the operations the pipeline and the kit
/// use. Implement the methods with `async fn`.
pub trait ConversationStore: Send + Sync + 'static {
    /// Stores a new room.
    fn create_room(&self, room: Room) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Replaces a room's record.
    fn update_room(&self, room: Room) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room `room_id`, if it exists.
    fn get_room(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Option<Room>, StoreError>> + Send;

    /// Stores a binding, replacing the room's binding of the same channel.
    fn store_binding(
        &self,
        binding: ChannelBinding,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The room's bindings.
    fn list_bindings(
        &self,
        room_id: &RoomId,
    ) -> impl Future<Output = Result<Vec<ChannelBinding>, StoreError>> + Send;

    /// The room with its bindings, participants and latest `recent_events`
    /// events, refused ones included; `None` when the room does not exist.
    fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> impl Future<Output = Result<Option<RoomContext>, StoreError>> + Send;

    /// The event the room committed from `channel_id` with idempotency
    /// `key` (RFC §13.4, gap G-023).
    fn find_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        key: &str,
    ) -> impl Future<Output = Result<Option<RoomEvent>, StoreError>> + Send;

    /// Stores `event` and sets the room's counters as one atomic commit
    /// (RFC §14.3). Refuses with [`StoreError::IndexTaken`] an index the
    /// room's timeline holds already (RFC §8.1).
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
