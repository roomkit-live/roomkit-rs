//! A Redis backend serves a SQLite-backed kit's executor.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../../roomkit/tests/support/served_executor.rs"]
mod served_executor;
mod support;

use roomkit::{RoomKit, RoomKitOptions};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

#[tokio::test]
async fn a_redis_turn_runs_on_the_sqlite_executor() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    let file = support::TempFile::new("served-executor");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    served_executor::exercise(&url, RoomKit::new(RoomKitOptions::with_store(store))).await;
}
