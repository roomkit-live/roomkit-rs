//! A backend across processes serves a SQLite-backed kit's executor.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../../roomkit/tests/support/served_executor.rs"]
mod served_executor;
mod support;

use roomkit::{RoomKit, RoomKitOptions};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

#[tokio::test]
async fn a_turn_across_processes_runs_on_the_sqlite_executor() {
    let file = support::TempFile::new("served-executor");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    served_executor::exercise(RoomKit::new(RoomKitOptions::with_store(store))).await;
}
