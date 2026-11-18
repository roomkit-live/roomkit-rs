//! An application supplies the transport of a kit backed by memory.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
mod scenario;

#[tokio::test]
async fn application_turn_backend() {
    scenario::run(roomkit::InMemoryStore::default()).await;
}

#[tokio::test]
async fn retaining_the_owner_port_does_not_keep_an_abandoned_kit_alive() {
    scenario::abandon(roomkit::InMemoryStore::default()).await;
}
