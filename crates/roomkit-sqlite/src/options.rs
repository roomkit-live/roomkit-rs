//! What the store's own methods take: Python's keyword arguments, with its
//! defaults.

use std::path::PathBuf;

use roomkit::model::RoomId;

/// What [`SqliteStore::new`](crate::SqliteStore::new) takes: Python's
/// `SQLiteStore(path='roomkit.db')`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqliteStoreOptions {
    /// The file, a leading `~` read as the home directory.
    pub path: PathBuf,
}

impl Default for SqliteStoreOptions {
    fn default() -> Self {
        Self {
            path: PathBuf::from("roomkit.db"),
        }
    }
}

/// What [`SqliteStore::search_events`](crate::SqliteStore::search_events)
/// takes: Python's `*, room_id=None, limit=20`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchEventsOptions {
    /// Only the events of this room.
    pub room_id: Option<RoomId>,
    /// How many events at most.
    pub limit: u64,
}

impl Default for SearchEventsOptions {
    fn default() -> Self {
        Self {
            room_id: None,
            limit: 20,
        }
    }
}
