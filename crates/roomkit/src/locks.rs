//! Per-room mutual exclusion, in one process (RFC §13.5).
//!
//! Each lock records the unit of the kit's work that holds it and of those
//! that wait for it, so that the kit's close drops the work waiting on a
//! lock a spared unit holds (RFC §12.10.4).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::tasks::{Marks, Unit};

/// One asynchronous mutex per room, created on first use and dropped once
/// nobody holds or waits for it, so rooms that come and go (or ids that name
/// no room) leave nothing behind.
///
/// The kit keeps them as the room locks the operations ask for through
/// `AcquireLock`. They hold within one process only; a store shared across
/// processes needs a distributed lock (RFC §13.5).
#[derive(Debug, Default)]
pub(crate) struct RoomMutexes {
    rooms: Mutex<HashMap<RoomId, Arc<AsyncMutex<()>>>>,
    /// Who holds and waits for each room's lock.
    takers: Mutex<Takers>,
}

/// The work holding and waiting for the rooms' locks, by room.
#[derive(Debug, Default)]
struct Takers {
    /// The key the next taker is recorded under.
    next: u64,
    rooms: HashMap<RoomId, Vec<Taker>>,
}

/// One taker of a room's lock: the unit of its work, and whether it holds
/// the lock or waits for it.
#[derive(Debug)]
struct Taker {
    key: u64,
    unit: Option<Unit>,
    holds: bool,
}

/// A held room mutex; dropping it releases the mutex.
///
/// The guard exists from the moment its caller starts waiting, holding a
/// handle on the mutex, so a wait that is cancelled before it holds the
/// mutex goes through the same drop and leaves nothing behind.
#[derive(Debug)]
pub(crate) struct RoomGuard<'mutexes> {
    guard: Option<OwnedMutexGuard<()>>,
    /// The waiter's handle on the mutex, until it holds it.
    waiting: Option<Arc<AsyncMutex<()>>>,
    room_id: RoomId,
    /// The guard's record among the room's takers.
    taker: u64,
    mutexes: &'mutexes RoomMutexes,
}

impl RoomMutexes {
    /// Waits for the room's mutex and returns its guard, for the unit of
    /// the work the current code runs in.
    pub(crate) async fn lock(&self, room_id: &RoomId) -> RoomGuard<'_> {
        let mutex = Arc::clone(self.rooms().entry(room_id.clone()).or_default());
        let taker = self.takes(room_id, Marks::current().innermost());
        let mut guard = RoomGuard {
            guard: None,
            waiting: Some(Arc::clone(&mutex)),
            room_id: room_id.clone(),
            taker,
            mutexes: self,
        };
        // Cancelled during this wait, the pending lock future, declared after
        // `guard`, is dropped first, then `guard`, which finds the mutex held
        // by the map alone when nobody else uses it.
        guard.guard = Some(mutex.lock_owned().await);
        guard.waiting = None;
        self.holds(room_id, taker);
        guard
    }

    /// The units of the work waiting for a room's lock a unit in `spared`
    /// holds, but those `spared` holds: their wait ends only once that unit
    /// lets the lock go.
    pub(crate) fn queued_behind(&self, spared: &HashSet<Unit>) -> HashSet<Unit> {
        let is_spared = |unit: Option<Unit>| unit.is_some_and(|unit| spared.contains(&unit));
        self.takers()
            .rooms
            .values()
            .filter(|takers| {
                takers
                    .iter()
                    .any(|taker| taker.holds && is_spared(taker.unit))
            })
            .flatten()
            .filter(|taker| !taker.holds)
            .filter_map(|taker| taker.unit.filter(|unit| !spared.contains(unit)))
            .collect()
    }

    /// Whether a unit in `spared` holds `room_id`'s lock.
    pub(crate) fn held_by(&self, room_id: &RoomId, spared: &HashSet<Unit>) -> bool {
        self.takers().rooms.get(room_id).is_some_and(|takers| {
            takers
                .iter()
                .any(|taker| taker.holds && taker.unit.is_some_and(|unit| spared.contains(&unit)))
        })
    }

    /// Records a taker of `room_id`'s lock in `unit`, waiting.
    fn takes(&self, room_id: &RoomId, unit: Option<Unit>) -> u64 {
        let mut takers = self.takers();
        let key = takers.next;
        takers.next = key.wrapping_add(1);
        takers
            .rooms
            .entry(room_id.clone())
            .or_default()
            .push(Taker {
                key,
                unit,
                holds: false,
            });
        key
    }

    /// The taker `key` of `room_id`'s lock holds it.
    fn holds(&self, room_id: &RoomId, key: u64) {
        let mut takers = self.takers();
        let taker = takers
            .rooms
            .get_mut(room_id)
            .and_then(|takers| takers.iter_mut().find(|taker| taker.key == key));
        if let Some(taker) = taker {
            taker.holds = true;
        }
    }

    /// The taker `key` of `room_id`'s lock is gone.
    fn leaves(&self, room_id: &RoomId, key: u64) {
        let mut takers = self.takers();
        if let Some(room) = takers.rooms.get_mut(room_id) {
            room.retain(|taker| taker.key != key);
            if room.is_empty() {
                takers.rooms.remove(room_id);
            }
        }
    }

    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Arc<AsyncMutex<()>>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn takers(&self) -> MutexGuard<'_, Takers> {
        self.takers.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for RoomGuard<'_> {
    fn drop(&mut self) {
        // Release the mutex first, and a waiter's handle on it: each holds a
        // reference to it.
        drop(self.guard.take());
        drop(self.waiting.take());
        self.mutexes.leaves(&self.room_id, self.taker);
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
            mutexes.rooms().is_empty(),
            "the cancelled waiter removes the mutex"
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
