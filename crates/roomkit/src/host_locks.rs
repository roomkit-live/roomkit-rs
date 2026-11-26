//! A host's live room locks, outside a driver pass (RFC §9.5.1, §13.5).
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::thread::ThreadId;

use crate::context::Lease;
use crate::model::RoomId;
use crate::{RoomKitError, RoomLock, RoomLockManager};
use tokio::sync::watch;

tokio::task_local! {
    static DRIVER_ACQUIRE: ();
    static INHERITED: Vec<Weak<Grant>>;
}

// A root future in Runtime::block_on has no Tokio task id. It stays on the
// calling thread. Spawned tasks have their own id even on that same thread.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Owner {
    Task(tokio::task::Id),
    Root(ThreadId, tokio::runtime::Id),
}

fn owner() -> Option<Owner> {
    tokio::task::try_id().map(Owner::Task).or_else(|| {
        Some(Owner::Root(
            std::thread::current().id(),
            tokio::runtime::Handle::try_current().ok()?.id(),
        ))
    })
}

type Manager = Weak<dyn RoomLockManager>;

// Only managers installed on kits can lend a lock to a kit. A weak reference
// also keeps a dropped manager's address from naming a different manager.
static MANAGERS: Mutex<Vec<Manager>> = Mutex::new(Vec::new());
static HAS_GRANTS: AtomicBool = AtomicBool::new(false);
static GRANTS: Mutex<Vec<Weak<Grant>>> = Mutex::new(Vec::new());

pub(crate) fn register(manager: &Arc<dyn RoomLockManager>) {
    let weak = Arc::downgrade(manager);
    let mut managers = MANAGERS.lock().unwrap_or_else(PoisonError::into_inner);
    managers.retain(|m| m.strong_count() != 0);
    if !managers.iter().any(|m| m.ptr_eq(&weak)) {
        managers.push(weak);
    }
}

struct Grant {
    owner: Owner,
    manager: Manager,
    lease: Lease,
}

fn current() -> Vec<Weak<Grant>> {
    if !HAS_GRANTS.load(Ordering::Acquire) {
        return Vec::new();
    }
    let mut grants = INHERITED.try_with(Clone::clone).unwrap_or_default();
    if let Some(owner) = owner() {
        let held = GRANTS.lock().unwrap_or_else(PoisonError::into_inner);
        for grant in held
            .iter()
            .filter(|g| g.upgrade().is_some_and(|g| g.owner == owner))
        {
            if !grants.iter().any(|inherited| inherited.ptr_eq(grant)) {
                grants.push(Weak::clone(grant));
            }
        }
    }
    grants
}

pub(crate) fn keep<F: Future>(future: F) -> impl Future<Output = F::Output> {
    INHERITED.scope(current(), future)
}

pub(crate) fn driver<F: Future>(future: F) -> impl Future<Output = F::Output> {
    DRIVER_ACQUIRE.scope((), future)
}

pub(crate) fn lease(manager: &dyn RoomLockManager, room: &RoomId) -> Option<Lease> {
    current()
        .iter()
        .filter_map(Weak::upgrade)
        .find(|g| {
            std::ptr::addr_eq(g.manager.as_ptr(), std::ptr::from_ref(manager))
                && g.lease.room_id() == room
                && g.lease.is_live()
        })
        .map(|g| g.lease.clone())
}

/// What a [`RoomLockManager`] wraps its acquisition in, `acquire` the
/// future that takes the room's lock, so that a host holding its guard can
/// call the kit for that room (RFC §9.5.1, §13.5). When the calling task
/// already holds `room`'s lock from `manager`, it takes nothing again and
/// gives a guard whose release does nothing; else it takes the lock and
/// marks it held by the calling task, which lends it to the kit's calls
/// that task makes while the guard lives. The kit's own acquisition, and a
/// lock of another kind than a room's (a key that holds a NUL), are taken
/// as they are. A manager installed on no kit lends nothing.
///
/// The built-in [`InMemoryLockManager`](crate::InMemoryLockManager) wraps
/// its acquisition in it; a manager written outside this crate (a Postgres
/// advisory lock) does the same in its [`locked`](RoomLockManager::locked):
/// `Box::pin(roomkit::track_host_room_lock(self, room_id, acquire))`.
///
/// Python's managers do it through `roomkit.core.locks._has_room_lock` and
/// `_mark_room_locked`.
///
/// # Errors
///
/// Returns what `acquire` returns.
pub async fn track_host_room_lock(
    manager: &dyn RoomLockManager,
    room: &RoomId,
    acquire: impl Future<Output = Result<Box<dyn RoomLock>, RoomKitError>>,
) -> Result<Box<dyn RoomLock>, RoomKitError> {
    // NUL keys belong to other lock namespaces (such as delivery claims).
    // A raw guard on one cannot grant access to a room (RFC §13.5).
    if room.as_str().contains('\0') || DRIVER_ACQUIRE.try_with(|()| ()).is_ok() {
        return acquire.await;
    }
    if lease(manager, room).is_some() {
        return Ok(Box::new(Borrowed));
    }
    let guard = acquire.await?;
    let registration = MANAGERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|m| std::ptr::addr_eq(m.as_ptr(), std::ptr::from_ref(manager)))
        .cloned();
    let (Some(manager), Some(owner)) = (registration, owner()) else {
        return Ok(guard);
    };
    let (lease, live) = Lease::host(room.clone());
    let grant = Arc::new(Grant {
        owner,
        manager,
        lease,
    });
    {
        let mut held = GRANTS.lock().unwrap_or_else(PoisonError::into_inner);
        held.push(Arc::downgrade(&grant));
        HAS_GRANTS.store(true, Ordering::Release);
    }
    Ok(Box::new(HostGuard {
        guard,
        mark: Mark { grant, _live: live },
    }))
}

struct Mark {
    grant: Arc<Grant>,
    _live: watch::Sender<()>,
}

impl Drop for Mark {
    fn drop(&mut self) {
        let weak = Arc::downgrade(&self.grant);
        let mut held = GRANTS.lock().unwrap_or_else(PoisonError::into_inner);
        held.retain(|other| !other.ptr_eq(&weak));
        HAS_GRANTS.store(!held.is_empty(), Ordering::Release);
    }
}

struct HostGuard {
    mark: Mark,
    guard: Box<dyn RoomLock>,
}

impl RoomLock for HostGuard {
    fn release(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send>> {
        let Self { guard, mark } = *self;
        drop(mark);
        guard.release()
    }
}

struct Borrowed;

impl RoomLock for Borrowed {
    fn release(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send>> {
        Box::pin(ready(Ok(())))
    }
}
