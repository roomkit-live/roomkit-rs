//! Per-room mutual exclusion, in one process (RFC §13.5).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// One asynchronous mutex per room, created on first use and dropped once
/// nobody holds or waits for it, so rooms that come and go (or ids that name
/// no room) leave nothing behind.
///
/// The kit keeps them as the room locks the operations ask for through
/// `AcquireLock`. They hold within one process only; a store shared across
/// processes needs a distributed lock (RFC §13.5).
#[derive(Debug, Default)]
pub(crate) struct RoomMutexes {
    rooms: Arc<Mutex<Rooms>>,
}

/// The rooms' mutexes, and how many callers wait for each.
#[derive(Debug, Default)]
struct Rooms {
    mutexes: HashMap<RoomId, Arc<AsyncMutex<()>>>,
    waiting: HashMap<RoomId, usize>,
}

/// A held room mutex; dropping it releases the mutex.
///
/// The guard exists from the moment its caller starts waiting, holding a
/// handle on the mutex, so a wait that is cancelled before it holds the
/// mutex goes through the same drop and leaves nothing behind. It holds
/// the rooms' table itself, so one taken with [`RoomMutexes::lock_owned`]
/// outlives the borrow it was taken through.
#[derive(Debug)]
pub(crate) struct RoomGuard<'mutexes> {
    guard: Option<OwnedMutexGuard<()>>,
    /// The waiter's handle on the mutex, until it holds it.
    waiting: Option<Arc<AsyncMutex<()>>>,
    room_id: RoomId,
    rooms: Arc<Mutex<Rooms>>,
    mutexes: PhantomData<&'mutexes RoomMutexes>,
}

impl RoomMutexes {
    /// Waits for the room's mutex and returns its guard.
    pub(crate) async fn lock(&self, room_id: &RoomId) -> RoomGuard<'_> {
        self.acquire(room_id).await
    }

    /// [`lock`](Self::lock), its guard owned: code that hands the lock it
    /// holds to a run of the driver (a keyed delivery's section, RFC
    /// §22.1).
    pub(crate) async fn lock_owned(&self, room_id: &RoomId) -> RoomGuard<'static> {
        self.acquire(room_id).await
    }

    async fn acquire<'guard>(&self, room_id: &RoomId) -> RoomGuard<'guard> {
        let mutex = {
            let mut rooms = lock(&self.rooms);
            *rooms.waiting.entry(room_id.clone()).or_default() += 1;
            Arc::clone(rooms.mutexes.entry(room_id.clone()).or_default())
        };
        let mut guard = RoomGuard {
            guard: None,
            waiting: Some(Arc::clone(&mutex)),
            room_id: room_id.clone(),
            rooms: Arc::clone(&self.rooms),
            mutexes: PhantomData,
        };
        // Cancelled during this wait, the pending lock future, declared after
        // `guard`, is dropped first, then `guard`, which finds the mutex held
        // by the map alone when nobody else uses it.
        guard.guard = Some(mutex.lock_owned().await);
        guard.waiting = None;
        unwait(&mut lock(&guard.rooms), &guard.room_id);
        guard
    }

    /// How many callers wait for the room's mutex now. Outside the kit's
    /// work: the conformance runner's `wait_for_lock` step.
    pub(crate) fn waiting(&self, room_id: &RoomId) -> usize {
        lock(&self.rooms).waiting.get(room_id).copied().unwrap_or(0)
    }

    #[cfg(test)]
    fn rooms(&self) -> MutexGuard<'_, Rooms> {
        lock(&self.rooms)
    }
}

fn lock(rooms: &Mutex<Rooms>) -> MutexGuard<'_, Rooms> {
    rooms.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Counts one waiter of `room_id`'s mutex less.
fn unwait(rooms: &mut Rooms, room_id: &RoomId) {
    if let Some(waiting) = rooms.waiting.get_mut(room_id) {
        *waiting -= 1;
        if *waiting == 0 {
            rooms.waiting.remove(room_id);
        }
    }
}

impl Drop for RoomGuard<'_> {
    fn drop(&mut self) {
        // Release the mutex first, and a waiter's handle on it: each holds a
        // reference to it.
        drop(self.guard.take());
        let cancelled = self.waiting.take().is_some();
        let mut rooms = lock(&self.rooms);
        if cancelled {
            unwait(&mut rooms, &self.room_id);
        }
        // A waiter cloned the mutex under this same lock before waiting, so a
        // mutex only the map still holds has neither holder nor waiter.
        let unused = rooms
            .mutexes
            .get(&self.room_id)
            .is_some_and(|mutex| Arc::strong_count(mutex) == 1);
        if unused {
            rooms.mutexes.remove(&self.room_id);
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
            mutexes.rooms().mutexes.is_empty(),
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
        assert_eq!(mutexes.waiting(&room_id()), 1, "the waiter is counted");
        drop(held);
        waiter.await.unwrap();
        assert!(
            mutexes.rooms().mutexes.is_empty(),
            "the last holder removes the mutex"
        );
        assert_eq!(mutexes.waiting(&room_id()), 0, "the holder waits no more");
    }

    /// The holder lets the mutex go while a waiter still holds its handle;
    /// the waiter is then cancelled before it ever holds the mutex.
    #[tokio::test]
    async fn a_waiter_cancelled_before_it_holds_the_mutex_leaves_nothing_behind() {
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
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(
            mutexes.rooms().mutexes.is_empty(),
            "the cancelled waiter removes the mutex"
        );
        assert_eq!(mutexes.waiting(&room_id()), 0, "a cancelled waiter waits no more");
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
