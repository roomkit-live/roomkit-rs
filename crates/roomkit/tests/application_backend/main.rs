//! An application supplies the transport of a kit backed by memory.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
mod scenario;

#[tokio::test]
async fn application_turn_backend() {
    scenario::run(roomkit::InMemoryStore::default()).await;
}
