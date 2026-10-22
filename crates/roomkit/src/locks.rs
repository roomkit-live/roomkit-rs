//! Per-room mutual exclusion, in one process (RFC §13.5).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// One asynchronous mutex per room, created on first use.
///
/// The kit keeps two sets of them: the room locks the operations ask for
/// through `AcquireLock`, and the room lanes that run one inbound operation
/// per room at a time (RFC §10.2). Both hold within one process only; a
/// store shared across processes needs a distributed lock (RFC §13.5).
#[derive(Debug, Default)]
pub(crate) struct RoomMutexes {
    rooms: Mutex<HashMap<RoomId, Arc<AsyncMutex<()>>>>,
}

impl RoomMutexes {
    /// Waits for the room's mutex and returns its guard; dropping the guard
    /// releases it.
    pub(crate) async fn lock(&self, room_id: &RoomId) -> OwnedMutexGuard<()> {
        let mutex = {
            let mut rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(rooms.entry(room_id.clone()).or_default())
        };
        mutex.lock_owned().await
    }
}
