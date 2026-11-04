//! Python: `tests/test_postgres_lock.py::test_warns_persistent_store_with_in_memory_lock`
//! and `test_no_warning_for_in_memory_store`: a store other processes may
//! share, paired with an in-process lock, is told at the kit's construction
//! (RFC §13.5). Python's SQLite store is one too. No file is opened.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::model::RoomId;
use roomkit::{
    InMemoryLockManager, RoomKit, RoomKitError, RoomKitOptions, RoomLock, RoomLockManager,
};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};
use warnings::Warnings;

fn store() -> SqliteStore {
    SqliteStore::new(SqliteStoreOptions::default())
}

const NAMED: &str = "SqliteStore is paired with InMemoryLockManager";

#[tokio::test]
async fn a_shared_store_with_the_default_lock_is_warned_of() {
    let (warnings, _capture) = Warnings::capture();
    let _kit = RoomKit::new(RoomKitOptions::with_store(store()));
    assert_eq!(warnings.containing(NAMED).len(), 1);
}

#[tokio::test]
async fn a_shared_store_with_an_in_memory_lock_given_is_warned_of() {
    let (warnings, _capture) = Warnings::capture();
    let mut options = RoomKitOptions::with_store(store());
    options.lock_manager = Some(Arc::new(InMemoryLockManager::default()));
    let _kit = RoomKit::new(options);
    assert_eq!(warnings.containing(NAMED).len(), 1);
}

#[tokio::test]
async fn the_in_memory_store_is_not_warned_of() {
    let (warnings, _capture) = Warnings::capture();
    let _kit = RoomKit::new(RoomKitOptions::default());
    assert_eq!(warnings.containing("InMemoryLockManager").len(), 0);
}

/// A manager other than the in-process one, as a distributed lock is.
struct Elsewhere(InMemoryLockManager);

impl RoomLockManager for Elsewhere {
    fn locked<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>> {
        self.0.locked(room_id)
    }
}

// Python: core/framework.py `isinstance(lock_manager, InMemoryLockManager)`.
#[tokio::test]
async fn a_shared_store_with_another_lock_is_not_warned_of() {
    let (warnings, _capture) = Warnings::capture();
    let mut options = RoomKitOptions::with_store(store());
    options.lock_manager = Some(Arc::new(Elsewhere(InMemoryLockManager::default())));
    let _kit = RoomKit::new(options);
    assert_eq!(warnings.containing("InMemoryLockManager").len(), 0);
}
