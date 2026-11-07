//! Per-room mutual exclusion (RFC §13.5): the in-memory lock manager's
//! mutexes, and the kit's hold on its lock manager.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use roomkit_core::model::RoomId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::error::RoomKitError;
use crate::lock_manager::{RoomLock, RoomLockManager};

/// One asynchronous mutex per room, created on first use and dropped once
/// nobody holds or waits for it, so rooms that come and go (or ids that name
/// no room) leave nothing behind.
///
/// The `InMemoryLockManager`'s locks. They hold within one process only; a
/// store shared across processes needs a distributed lock (RFC §13.5).
#[derive(Debug, Default)]
pub(crate) struct RoomMutexes {
    rooms: Arc<Mutex<Rooms>>,
}

/// The rooms' mutexes, and how many callers wait for each.
#[derive(Debug, Default)]
struct Rooms {
    mutexes: HashMap<RoomId, Arc<AsyncMutex<()>>>,
    waiting: HashMap<RoomId, usize>,
    /// How many mutexes nobody holds or waits for are kept, the least
    /// recently asked for dropped first past it, as Python's
    /// `InMemoryLockManager(max_locks)`; none kept at 0.
    retained: usize,
    /// When each room's mutex was last asked for, by a count of asks, and
    /// the rooms by that count, the least recently asked for first.
    asked: HashMap<RoomId, u64>,
    order: BTreeMap<u64, RoomId>,
    asks: u64,
}

impl Rooms {
    /// Drops the mutexes nobody holds or waits for, the least recently
    /// asked for first, while more than `retained` are kept, as Python's
    /// `_evict`: a held one stays, the bound passed meanwhile.
    fn evict(&mut self) {
        let Some(over) = self.mutexes.len().checked_sub(self.retained) else {
            return;
        };
        let mutexes = &self.mutexes;
        let unused: Vec<RoomId> = self
            .order
            .values()
            .filter(|room_id| {
                mutexes
                    .get(*room_id)
                    .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
            })
            .take(over)
            .cloned()
            .collect();
        for room_id in unused {
            self.forget(&room_id);
        }
    }

    /// Counts an ask for `room_id`'s mutex, the most recent one.
    fn ask(&mut self, room_id: &RoomId) {
        self.asks += 1;
        if let Some(before) = self.asked.insert(room_id.clone(), self.asks) {
            self.order.remove(&before);
        }
        self.order.insert(self.asks, room_id.clone());
    }

    /// Drops `room_id`'s mutex and its place in the order of asks.
    fn forget(&mut self, room_id: &RoomId) {
        self.mutexes.remove(room_id);
        if let Some(asked) = self.asked.remove(room_id) {
            self.order.remove(&asked);
        }
    }
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
    /// Waits for the room's mutex and returns its guard, owned: the
    /// in-memory lock manager's lock.
    pub(crate) async fn lock_owned(&self, room_id: &RoomId) -> RoomGuard<'static> {
        self.acquire(room_id).await
    }

    async fn acquire<'guard>(&self, room_id: &RoomId) -> RoomGuard<'guard> {
        let mutex = {
            let mut rooms = lock(&self.rooms);
            *rooms.waiting.entry(room_id.clone()).or_default() += 1;
            rooms.ask(room_id);
            let created = !rooms.mutexes.contains_key(room_id);
            let mutex = Arc::clone(rooms.mutexes.entry(room_id.clone()).or_default());
            if created {
                rooms.evict();
            }
            mutex
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

    /// How many callers wait for the room's mutex now.
    #[cfg(test)]
    pub(crate) fn waiting(&self, room_id: &RoomId) -> usize {
        lock(&self.rooms).waiting.get(room_id).copied().unwrap_or(0)
    }

    /// Mutexes that keep up to `retained` of those nobody holds or waits
    /// for, as Python's `InMemoryLockManager(max_locks)`.
    pub(crate) fn retaining(retained: usize) -> Self {
        Self {
            rooms: Arc::new(Mutex::new(Rooms {
                retained,
                ..Rooms::default()
            })),
        }
    }

    /// How many rooms' mutexes are kept now: those held or waited for, and
    /// those retained.
    pub(crate) fn size(&self) -> usize {
        lock(&self.rooms).mutexes.len()
    }

    #[cfg(test)]
    fn rooms(&self) -> MutexGuard<'_, Rooms> {
        lock(&self.rooms)
    }
}

/// The kit's hold on a lock manager's locks of one kind (RFC §13.5): it
/// counts the callers waiting for each key, whatever the manager, for the
/// conformance runner's `wait_for_lock` step.
pub(crate) struct KitLocks {
    manager: Arc<dyn RoomLockManager>,
    waiting: Arc<Mutex<HashMap<RoomId, usize>>>,
    /// Whether the keys are room ids, which hold no NUL character: one
    /// leads the keys of the kit's other locks in a shared manager.
    rooms: bool,
}

impl KitLocks {
    pub(crate) fn new(manager: Arc<dyn RoomLockManager>) -> Self {
        Self {
            manager,
            waiting: Arc::default(),
            rooms: false,
        }
    }

    /// The kit's hold on its rooms' locks, Python's `RoomLocks`: a room id
    /// holding a NUL character takes none, wherever it came from (a room
    /// written to the store straight), as `create_room` refuses it (RFC
    /// §13.5).
    pub(crate) fn rooms(manager: Arc<dyn RoomLockManager>) -> Self {
        Self {
            rooms: true,
            ..Self::new(manager)
        }
    }

    /// The manager the locks are taken from.
    pub(crate) fn manager(&self) -> &Arc<dyn RoomLockManager> {
        &self.manager
    }

    /// Waits for `key`'s lock from the manager. A wait dropped before it
    /// holds the lock counts as a waiter no more. A room's lock is refused,
    /// the manager never asked, for a room id holding a NUL character, as
    /// Python's `ValueError`.
    pub(crate) async fn lock(&self, key: &RoomId) -> Result<HeldLock, RoomKitError> {
        if self.rooms && key.as_str().contains('\0') {
            return Err(RoomKitError::Value(
                "A room id holds no NUL character".to_owned(),
            ));
        }
        let _waiter = Waiter::new(&self.waiting, key);
        let lock = self.manager.locked(key).await?;
        Ok(HeldLock {
            lock: Some(lock),
            key: key.clone(),
        })
    }

    /// How many callers wait for `key`'s lock now. Outside the kit's work:
    /// the conformance runner's `wait_for_lock` step.
    pub(crate) fn waiting(&self, key: &RoomId) -> usize {
        waiters(&self.waiting).get(key).copied().unwrap_or(0)
    }

    /// Where a run counts the acquires its operation holds back behind
    /// another of its own sections among the waiters, none yet.
    pub(crate) fn held_back(&self) -> HeldBack {
        HeldBack {
            waiting: Arc::clone(&self.waiting),
            told: Vec::new(),
            waiters: Vec::new(),
        }
    }
}

/// The acquires of a room's lock one run's operation holds back behind
/// another of its own sections (`Operation::locks_held_back`), counted
/// among the room's waiters as a caller waiting for the manager is, until
/// the run tells others or ends: a section of the operation waits for that
/// lock as surely, though the manager never sees it.
pub(crate) struct HeldBack {
    waiting: Arc<Mutex<HashMap<RoomId, usize>>>,
    /// What the operation last told, by room.
    told: Vec<(RoomId, usize)>,
    waiters: Vec<Waiter>,
}

impl HeldBack {
    /// Counts `held_back`, by room, in place of what was told before.
    pub(crate) fn tell(&mut self, held_back: Vec<(RoomId, usize)>) {
        if held_back == self.told {
            return;
        }
        self.waiters.clear();
        for (room_id, count) in &held_back {
            for _ in 0..*count {
                self.waiters.push(Waiter::new(&self.waiting, room_id));
            }
        }
        self.told = held_back;
    }
}

impl fmt::Debug for HeldBack {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeldBack")
            .field("told", &self.told)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for KitLocks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("KitLocks").finish_non_exhaustive()
    }
}

/// A lock the kit holds from its lock manager: released by
/// [`release`](Self::release), whose failure its caller reports, or by its
/// drop, which releases it through the manager all the same and logs a
/// failure, as Python's `ReleaseTolerantLockManager` logs every one.
pub(crate) struct HeldLock {
    lock: Option<Box<dyn RoomLock>>,
    key: RoomId,
}

impl HeldLock {
    /// Releases the lock; the future answers whether the release failed.
    pub(crate) fn release(
        mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send>> {
        match self.lock.take() {
            Some(lock) => lock.release(),
            None => Box::pin(std::future::ready(Ok(()))),
        }
    }
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        let Some(lock) = self.lock.take() else {
            return;
        };
        // Outside a runtime, no release can run: the lock's own drop lets
        // it go, as it did before the kit released through the manager.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            drop(lock);
            return;
        };
        // A release over at once (the in-memory lock's) is over before the
        // drop returns, as the lock's own drop was; one that waits on I/O
        // goes on in a task of its own.
        let mut release = lock.release();
        let key = self.key.clone();
        match release
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => log_failed_release(&key, result),
            Poll::Pending => {
                drop(runtime.spawn(async move { log_failed_release(&key, release.await) }));
            }
        }
    }
}

/// Logs a release that failed: what the section decided stands (RFC
/// §13.5), as Python's `_log_failed_release`.
fn log_failed_release(key: &RoomId, released: Result<(), RoomKitError>) {
    if let Err(error) = released {
        tracing::warn!(target: "roomkit::locks", key = %key, %error, "releasing the lock failed");
    }
}

impl fmt::Debug for HeldLock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("HeldLock").finish_non_exhaustive()
    }
}

/// One caller counted among a key's waiters until its drop.
struct Waiter {
    waiting: Arc<Mutex<HashMap<RoomId, usize>>>,
    key: RoomId,
}

impl Waiter {
    fn new(waiting: &Arc<Mutex<HashMap<RoomId, usize>>>, key: &RoomId) -> Self {
        *waiters(waiting).entry(key.clone()).or_default() += 1;
        Self {
            waiting: Arc::clone(waiting),
            key: key.clone(),
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut waiting = waiters(&self.waiting);
        if let Some(count) = waiting.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                waiting.remove(&self.key);
            }
        }
    }
}

fn waiters(waiting: &Mutex<HashMap<RoomId, usize>>) -> MutexGuard<'_, HashMap<RoomId, usize>> {
    waiting.lock().unwrap_or_else(PoisonError::into_inner)
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
        if unused && rooms.retained == 0 {
            rooms.forget(&self.room_id);
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
        drop(mutexes.lock_owned(&room_id()).await);
        assert!(
            mutexes.rooms().mutexes.is_empty(),
            "the room's mutex should be gone"
        );
    }

    #[tokio::test]
    async fn the_last_holder_removes_a_mutex_another_task_waited_for() {
        let mutexes = Arc::new(RoomMutexes::default());
        let held = mutexes.lock_owned(&room_id()).await;
        let waiter = {
            let mutexes = Arc::clone(&mutexes);
            tokio::spawn(async move {
                drop(mutexes.lock_owned(&room_id()).await);
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
        let held = mutexes.lock_owned(&room_id()).await;
        let waiter = {
            let mutexes = Arc::clone(&mutexes);
            tokio::spawn(async move {
                drop(mutexes.lock_owned(&room_id()).await);
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
        assert_eq!(
            mutexes.waiting(&room_id()),
            0,
            "a cancelled waiter waits no more"
        );
    }

    #[tokio::test]
    async fn a_held_mutex_excludes_a_second_holder() {
        let mutexes = RoomMutexes::default();
        let _held = mutexes.lock_owned(&room_id()).await;
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            mutexes.lock_owned(&room_id()),
        )
        .await;
        assert!(second.is_err(), "the second lock should wait");
    }

    // Python: tests/test_locks.py::TestInMemoryLockManager::test_lru_eviction,
    // the order: the least recently asked for goes first.
    #[tokio::test]
    async fn the_least_recently_asked_for_mutex_goes_first() {
        let mutexes = RoomMutexes::retaining(2);
        for id in ["r1", "r2", "r1", "r3"] {
            drop(mutexes.lock_owned(&RoomId::new(id).unwrap()).await);
        }
        let rooms = mutexes.rooms();
        let kept = |id: &str| rooms.mutexes.contains_key(&RoomId::new(id).unwrap());
        assert!(kept("r1") && kept("r3") && !kept("r2"));
    }

    /// A manager whose release needs the runtime's timer.
    struct Timed;

    struct TimedLock;

    impl RoomLockManager for Timed {
        fn locked<'a>(
            &'a self,
            _room_id: &'a RoomId,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>>
        {
            let lock: Box<dyn RoomLock> = Box::new(TimedLock);
            Box::pin(std::future::ready(Ok(lock)))
        }
    }

    impl RoomLock for TimedLock {
        fn release(
            self: Box<Self>,
        ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + 'static>> {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                Ok(())
            })
        }
    }

    /// A lock dropped once no runtime runs lets go through its own drop,
    /// never polling a release that needs the runtime (light review of
    /// RMK-1153's fixes: it panicked).
    #[test]
    fn a_lock_dropped_outside_a_runtime_lets_go_without_its_release() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let locks = KitLocks::new(Arc::new(Timed));
        let held = runtime.block_on(locks.lock(&room_id())).unwrap();
        drop(runtime);
        drop(held);
    }
}
