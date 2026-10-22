//! Per-room mutual exclusion, in one process (RFC §13.5).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// One asynchronous mutex per room, created on first use and dropped once
/// nobody holds or waits for it, so rooms that come and go (or ids that name
/// no room) leave nothing behind.
///
/// The kit keeps two sets of them: the room locks the operations ask for
/// through `AcquireLock`, and the room lanes that run one inbound operation
/// per room at a time (RFC §10.2). Both hold within one process only; a
/// store shared across processes needs a distributed lock (RFC §13.5).
#[derive(Debug, Default)]
pub(crate) struct RoomMutexes {
    rooms: Mutex<HashMap<RoomId, Arc<AsyncMutex<()>>>>,
}

/// A held room mutex; dropping it releases the mutex.
#[derive(Debug)]
pub(crate) struct RoomGuard<'mutexes> {
    guard: Option<OwnedMutexGuard<()>>,
    room_id: RoomId,
    mutexes: &'mutexes RoomMutexes,
}

impl RoomMutexes {
    /// Waits for the room's mutex and returns its guard.
    pub(crate) async fn lock(&self, room_id: &RoomId) -> RoomGuard<'_> {
        let mutex = Arc::clone(self.rooms().entry(room_id.clone()).or_default());
        RoomGuard {
            guard: Some(mutex.lock_owned().await),
            room_id: room_id.clone(),
            mutexes: self,
        }
    }

    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Arc<AsyncMutex<()>>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for RoomGuard<'_> {
    fn drop(&mut self) {
        // Release the mutex first: its guard holds a reference to it.
        drop(self.guard.take());
        let mut rooms = self.mutexes.rooms();
        // A waiter cloned the mutex under this same lock before waiting, so a
        // mutex only the map still holds has neither holder nor waiter.
        let unused = rooms
            .get(&self.room_id)
            .is_some_and(|mutex| Arc::strong_count(mutex) == 1);
        if unused {
            rooms.remove(&self.room_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    #[tokio::test]
    async fn a_released_mutex_leaves_nothing_behind() {
        let mutexes = RoomMutexes::default();
        drop(mutexes.lock(&room_id()).await);
        assert!(
            mutexes.rooms().is_empty(),
            "the room's mutex should be gone"
        );
    }

    #[tokio::test]
    async fn the_last_holder_removes_a_mutex_another_task_waited_for() {
        let mutexes = Arc::new(RoomMutexes::default());
        let held = mutexes.lock(&room_id()).await;
        let waiter = {
            let mutexes = Arc::clone(&mutexes);
            tokio::spawn(async move {
                drop(mutexes.lock(&room_id()).await);
            })
        };
        // Let the waiter clone the mutex and start waiting.
        tokio::task::yield_now().await;
        drop(held);
        waiter.await.unwrap();
        assert!(
            mutexes.rooms().is_empty(),
            "the last holder removes the mutex"
        );
    }

    #[tokio::test]
    async fn a_held_mutex_excludes_a_second_holder() {
        let mutexes = RoomMutexes::default();
        let _held = mutexes.lock(&room_id()).await;
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            mutexes.lock(&room_id()),
        )
        .await;
        assert!(second.is_err(), "the second lock should wait");
    }
}
