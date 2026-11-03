//! The kit's lock manager and delivery gap a scenario's `setup.kit` names
//! (RFC §13.5): README, "Setup", `lock_manager` and
//! `delivery_gap_timeout_ms`; "Steps", `lock_releases`.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use roomkit::model::{ErrorRecord, RoomId};
use roomkit::{
    InMemoryLockManager, RoomKit, RoomKitError, RoomKitOptions, RoomLock, RoomLockManager,
};
use serde_json::Value;

/// The framework event of a delivery set's gap skip (RFC §8.2, §13.5).
pub(crate) const LOCK_EVENTS: [&str; 1] = ["delivery_skipped"];

/// `options` with the kit's lock manager and `delivery_gap_timeout` a
/// scenario's `setup.kit` names.
pub(crate) fn kit_options<S>(mut options: RoomKitOptions<S>, kit: &Value) -> RoomKitOptions<S> {
    if let Some(timeout) = kit["delivery_gap_timeout_ms"].as_u64() {
        options.delivery_gap_timeout = Duration::from_millis(timeout);
    }
    if let Some(error) = kit["lock_manager"]["release_error"].as_str() {
        options.lock_manager = Some(Arc::new(ScriptedLockManager {
            inner: InMemoryLockManager::default(),
            release_error: error.to_owned(),
            failing: Arc::default(),
        }));
    }
    options
}

/// Sets the releases of the kit's scripted lock manager failing or
/// succeeding, from the next one on: `false` when the scenario's kit has
/// none.
pub(crate) fn set_lock_releases<S>(kit: &RoomKit<S>, releases: &str) -> bool {
    let manager: Arc<dyn Any + Send + Sync> = kit.lock_manager();
    let Ok(scripted) = manager.downcast::<ScriptedLockManager>() else {
        return false;
    };
    scripted.failing.store(releases == "fail", Ordering::SeqCst);
    true
}

/// The in-memory lock manager, whose release fails while `failing` is set,
/// as a distributed lock that finds its lease expired at release.
struct ScriptedLockManager {
    inner: InMemoryLockManager,
    release_error: String,
    failing: Arc<AtomicBool>,
}

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, RoomKitError>> + Send + 'a>>;

impl RoomLockManager for ScriptedLockManager {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> Answer<'a, Box<dyn RoomLock>> {
        Box::pin(async move {
            let lock: Box<dyn RoomLock> = Box::new(ScriptedLock {
                lock: self.inner.locked(room_id).await?,
                release_error: self.release_error.clone(),
                failing: Arc::clone(&self.failing),
            });
            Ok(lock)
        })
    }
}

struct ScriptedLock {
    lock: Box<dyn RoomLock>,
    release_error: String,
    failing: Arc<AtomicBool>,
}

impl RoomLock for ScriptedLock {
    fn release(self: Box<Self>) -> Answer<'static, ()> {
        let failing = self.failing.load(Ordering::SeqCst);
        let Self {
            lock,
            release_error,
            ..
        } = *self;
        Box::pin(async move {
            lock.release().await?;
            if failing {
                return Err(RoomKitError::Lock(ErrorRecord::new(
                    "LockReleaseFailedError",
                    release_error,
                )));
            }
            Ok(())
        })
    }
}
