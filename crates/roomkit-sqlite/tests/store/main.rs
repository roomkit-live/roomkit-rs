//! The store suite (RFC §14), the in-memory store's, on the SQLite store:
//! the suite's files are roomkit's (crates/roomkit/tests/store/suite),
//! included by path.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../../../roomkit/tests/store/suite/mod.rs"]
mod suite;
#[path = "../support/mod.rs"]
mod support;

use std::ops::Deref;

use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;

/// A SQLite store on a file of its own, removed with it.
pub(crate) struct Store {
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
#[expect(
    clippy::unused_async,
    reason = "the shared suite awaits its store, which another store may open at once"
)]
pub(crate) async fn store() -> Store {
    let file = TempFile::new("store");
    Store {
        store: SqliteStore::new(SqliteStoreOptions {
            path: file.path().into(),
        }),
        _file: file,
    }
}
