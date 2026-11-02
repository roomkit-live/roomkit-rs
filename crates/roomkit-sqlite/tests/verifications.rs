//! Identity verification on the SQLite store (RFC §11.7): what a kit
//! does with a store that cannot answer as it starts.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use roomkit::{ConversationStore, RoomKit, RoomKitOptions};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;

/// Python: `tests/test_verification_lifecycle.py::test_a_kit_whose_store_cannot_answer_still_starts`.
/// Resuming the verifications in force never stops a kit from starting: a
/// store that cannot answer, closed here, is logged and passed over, a
/// verification whose time passed reading EXPIRED all the same (RFC
/// §11.7.5).
#[tokio::test]
async fn a_kit_whose_store_cannot_answer_still_starts() {
    let file = TempFile::new("unanswering");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    store.close().await;
    let kit = RoomKit::new(RoomKitOptions::with_store(store));

    let started = kit.start().await;

    assert!(started.is_ok(), "{started:?}");
    drop(kit.close().await);
}
