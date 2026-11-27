//! The store contract (RFC §14) on the in-memory store, and the store's
//! own behaviours.
//!
//! The contract's checks are `roomkit::testing::check_conversation_store`:
//! the stores of roomkit-providers run them on theirs.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod abstract_only;
mod abstract_only_store;
mod connection;
mod connection_store;
mod held_index;
mod received_events;
mod room_filter;

use roomkit::InMemoryStore;
use roomkit::testing::check_conversation_store;

#[tokio::test]
async fn the_in_memory_store_keeps_the_store_contract() {
    check_conversation_store(|| async { Box::new(InMemoryStore::new()) }).await;
}
