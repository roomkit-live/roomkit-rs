//! The store suite (RFC §14) on the in-memory store.
//!
//! The suite, in `suite/`, is written for any store: roomkit-sqlite runs
//! it on its own (crates/roomkit-sqlite/tests/store/main.rs).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod abstract_only;
mod suite;

use roomkit::InMemoryStore;

/// The store the suite runs on.
pub(crate) type Store = InMemoryStore;

/// An empty store, opened as a store that awaits its I/O is.
#[expect(
    clippy::unused_async,
    reason = "the suite opens every store the same way, some awaiting"
)]
pub(crate) async fn store() -> Store {
    InMemoryStore::new()
}
