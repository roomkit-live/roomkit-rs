//! The store contract (RFC §14) on the SQLite store:
//! `roomkit::testing::check_conversation_store`, the in-memory store's.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../support/mod.rs"]
mod support;

use std::ops::Deref;

use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;

/// A SQLite store on a file of its own, removed with it.
struct Store {
    store: SqliteStore,
    _file: TempFile,
}

impl Deref for Store {
    type Target = SqliteStore;

    fn deref(&self) -> &SqliteStore {
        &self.store
    }
}

/// An empty store, on a new file.
async fn store() -> Store {
    let file = TempFile::new("store");
    Store {
        store: SqliteStore::new(SqliteStoreOptions {
            path: file.path().into(),
        }),
        _file: file,
    }
}

#[tokio::test]
async fn the_sqlite_store_keeps_the_store_contract() {
    roomkit::testing::check_conversation_store(store).await;
}
