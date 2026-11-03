//! The lock manager the kit takes its room locks from (RFC §13.5), Python's
//! `RoomLockManager` and its in-process default, `InMemoryLockManager`.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::RoomId;

use crate::error::RoomKitError;
use crate::locks::{RoomGuard, RoomMutexes};

/// A future a lock manager answers with.
type LockFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, RoomKitError>> + Send + 'a>>;

/// Python `RoomLockManager`: where the kit takes each room's lock, one
/// section of the room at a time (RFC §13.5). Implement it to plug in a
/// lock that holds across processes (a Postgres advisory lock, Redis); the
/// kit takes it as `Arc<dyn RoomLockManager>`
/// ([`RoomKitOptions::lock_manager`](crate::RoomKitOptions::lock_manager)).
///
/// Python's `async with manager.locked(room_id):` is a guard here:
/// `let lock = manager.locked(&room_id).await?; ...; lock.release().await?;`.
/// A manager need not be reentrant: the kit grants a lock to the code that
/// runs under it (a hook, a tool calling the kit back) without asking the
/// manager again (RFC §9.5.1).
pub trait RoomLockManager: Any + Send + Sync {
    /// Python `locked(room_id)`: waits for `room_id`'s lock and returns it,
    /// held until its [`RoomLock::release`] or its drop.
    ///
    /// The wait MUST be cancellation-safe: the kit bounds it with
    /// `process_timeout` (RFC §13.6), and a wait dropped before it holds
    /// the lock leaves nothing taken.
    #[expect(
        clippy::type_complexity,
        reason = "the boxed future of a trait object, written out as MemoryProvider's"
    )]
    fn locked<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>>;

    /// Python `close()`: releases what the manager holds (a connection
    /// pool). The kit's close calls it; nothing by default. It MUST be
    /// idempotent.
    fn close(&self) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + '_>> {
        Box::pin(ready(Ok(())))
    }
}

/// A room lock a [`RoomLockManager`] granted: Python's `async with` block,
/// held until [`release`](Self::release), or until its drop, which releases
/// it too (a section cancelled before its release).
pub trait RoomLock: Send + Sync {
    /// Releases the lock. A release that fails (a lease lost) changes
    /// nothing the section decided or wrote: the kit logs it and reports it
    /// on the operation's result (RFC §13.5).
    fn release(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + 'static>>;
}

/// The default of `InMemoryLockManager.max_locks`.
const MAX_LOCKS: usize = 1024;

/// Python `InMemoryLockManager(max_locks=1024)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InMemoryLockManagerOptions {
    /// Python `max_locks=1024`: the most locks kept that nobody holds or
    /// waits for. This manager keeps none: a room's lock goes once its last
    /// holder or waiter lets it go, so the bound always holds.
    pub max_locks: usize,
}

impl Default for InMemoryLockManagerOptions {
    fn default() -> Self {
        Self {
            max_locks: MAX_LOCKS,
        }
    }
}

/// Python `InMemoryLockManager`: one lock per room, in this process (the
/// kit's default). Two kits handed the same manager share their rooms'
/// locks, and their delivery claims and verifications too, each kind in a
/// map of its own, so that no room id names another kind's key (RFC §13.5).
pub struct InMemoryLockManager {
    options: InMemoryLockManagerOptions,
    rooms: RoomMutexes,
    /// The maps of the kit's other kinds of locks, by kind.
    key_spaces: Mutex<HashMap<&'static str, Arc<InMemoryLockManager>>>,
}

impl InMemoryLockManager {
    /// Python `InMemoryLockManager(max_locks=1024)`.
    #[must_use]
    pub fn new(options: InMemoryLockManagerOptions) -> Self {
        Self {
            options,
            rooms: RoomMutexes::default(),
            key_spaces: Mutex::new(HashMap::new()),
        }
    }

    /// Python `size`: how many rooms' locks the manager keeps now, those
    /// held or waited for.
    #[must_use]
    pub fn size(&self) -> usize {
        self.rooms.size()
    }

    /// The map of its own for the locks of `kind` (a room's delivery
    /// claim, an identity's verification), shared by every kit that shares
    /// this manager, as Python's `_key_space`.
    pub(crate) fn key_space(&self, kind: &'static str) -> Arc<Self> {
        let mut spaces = self
            .key_spaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            spaces
                .entry(kind)
                .or_insert_with(|| Arc::new(Self::new(self.options))),
        )
    }
}

impl Default for InMemoryLockManager {
    fn default() -> Self {
        Self::new(InMemoryLockManagerOptions::default())
    }
}

impl fmt::Debug for InMemoryLockManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryLockManager")
            .field("max_locks", &self.options.max_locks)
            .field("size", &self.size())
            .finish_non_exhaustive()
    }
}

impl RoomLockManager for InMemoryLockManager {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> LockFuture<'a, Box<dyn RoomLock>> {
        Box::pin(async move {
            let lock: Box<dyn RoomLock> =
                Box::new(MemoryLock(self.rooms.lock_owned(room_id).await));
            Ok(lock)
        })
    }
}

/// A room's in-process lock: its release never fails.
struct MemoryLock(
    #[expect(dead_code, reason = "held for its drop, which releases the lock")] RoomGuard<'static>,
);

impl RoomLock for MemoryLock {
    fn release(self: Box<Self>) -> LockFuture<'static, ()> {
        drop(self);
        Box::pin(ready(Ok(())))
    }
}

/// `manager`'s locks of `kind`: an in-memory manager's map of their own;
/// any other manager itself, each key led by a NUL character, which no room
/// id holds (`create_room` refuses one), as Python's `_key_space`.
pub(crate) fn key_space(
    manager: &Arc<dyn RoomLockManager>,
    kind: &'static str,
) -> Arc<dyn RoomLockManager> {
    let any: &dyn Any = &**manager;
    match any.downcast_ref::<InMemoryLockManager>() {
        Some(memory) => memory.key_space(kind),
        None => Arc::new(NulLedKeys(Arc::clone(manager))),
    }
}

/// What leads the key of a lock of another kind than a room's in a shared
/// manager: no room id holds it.
const OTHER_KIND: char = '\0';

/// A shared manager's locks of a kind other than its rooms': the same
/// manager, each key led by a NUL character (`\0__delivery__:r1`,
/// `\0verification:org-a:alice`), as Python's `_NulLedKeys`. The kit
/// closes the manager itself.
struct NulLedKeys(Arc<dyn RoomLockManager>);

impl RoomLockManager for NulLedKeys {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> LockFuture<'a, Box<dyn RoomLock>> {
        Box::pin(async move {
            let key = RoomId::new(format!("{OTHER_KIND}{room_id}"))?;
            self.0.locked(&key).await
        })
    }
}

/// A lock manager of no in-memory kind, as one shared across processes,
/// for the tests: the in-memory locks behind a type of its own.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct SharedLockManager(InMemoryLockManager);

#[cfg(test)]
impl RoomLockManager for SharedLockManager {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> LockFuture<'a, Box<dyn RoomLock>> {
        self.0.locked(room_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    fn room(id: &str) -> RoomId {
        RoomId::new(id).unwrap()
    }

    // Python: tests/test_locks.py::TestInMemoryLockManager::test_serialization
    #[tokio::test]
    async fn test_serialization() {
        let manager = Arc::new(InMemoryLockManager::default());
        let inside = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..5)
            .map(|_| {
                let (manager, inside, most) =
                    (Arc::clone(&manager), Arc::clone(&inside), Arc::clone(&most));
                tokio::spawn(async move {
                    let lock = manager.locked(&room("r1")).await.unwrap();
                    let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(now, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    inside.fetch_sub(1, Ordering::SeqCst);
                    lock.release().await.unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(most.load(Ordering::SeqCst), 1);
    }

    // Python: tests/test_locks.py::TestInMemoryLockManager::test_different_rooms_do_not_serialize
    #[tokio::test]
    async fn test_different_rooms_do_not_serialize() {
        let manager = InMemoryLockManager::default();
        let first = manager.locked(&room("r1")).await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), manager.locked(&room("r2")))
            .await
            .expect("r2's lock waits on no other room's")
            .unwrap();
        // Python: test_different_rooms_different_locks.
        assert_eq!(manager.size(), 2);
        first.release().await.unwrap();
        second.release().await.unwrap();
    }

    // Python: tests/test_locks.py::TestInMemoryLockManager::test_lru_eviction,
    // test_held_lock_not_evicted: a lock nobody holds goes at once, a held
    // one stays.
    #[tokio::test]
    async fn test_held_lock_not_evicted() {
        let manager = InMemoryLockManager::new(InMemoryLockManagerOptions { max_locks: 2 });
        let held = manager.locked(&room("r1")).await.unwrap();
        for other in ["r2", "r3"] {
            manager
                .locked(&room(other))
                .await
                .unwrap()
                .release()
                .await
                .unwrap();
        }
        assert_eq!(manager.size(), 1);
        drop(held);
        assert_eq!(manager.size(), 0);
    }

    // Python: tests/test_locks.py::test_reentrance_is_scoped_to_manager
    #[tokio::test]
    async fn test_reentrance_is_scoped_to_manager() {
        let (first, second) = (
            InMemoryLockManager::default(),
            InMemoryLockManager::default(),
        );
        let one = first.locked(&room("r1")).await.unwrap();
        let other = tokio::time::timeout(Duration::from_secs(5), second.locked(&room("r1")))
            .await
            .expect("another manager's lock of r1 is its own")
            .unwrap();
        drop((one, other));
    }

    // Python: tests/test_locks.py::test_a_lock_of_another_kind_is_no_room_lock
    #[tokio::test]
    async fn test_a_lock_of_another_kind_is_no_room_lock() {
        let rooms: Arc<dyn RoomLockManager> = Arc::new(InMemoryLockManager::default());
        let claims = key_space(&rooms, "delivery");
        assert!(Arc::ptr_eq(&claims, &key_space(&rooms, "delivery")));
        let claim = claims.locked(&room("r1")).await.unwrap();
        let section = tokio::time::timeout(Duration::from_secs(5), rooms.locked(&room("r1")))
            .await
            .expect("a claim keyed like a room holds no room's lock")
            .unwrap();
        drop((claim, section));
    }
}
