//! Run the public application turn backend contract on SQLite.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "../../roomkit/tests/application_backend/scenario.rs"]
mod scenario;
mod support;

fn store(file: &support::TempFile) -> roomkit_sqlite::SqliteStore {
    roomkit_sqlite::SqliteStore::new(roomkit_sqlite::SqliteStoreOptions {
        path: file.path().into(),
    })
}

#[tokio::test]
async fn application_turn_backend() {
    let file = support::TempFile::new("application-backend");
    scenario::run(store(&file)).await;
}

#[tokio::test]
async fn retaining_the_owner_port_does_not_keep_an_abandoned_kit_alive() {
    let file = support::TempFile::new("abandoned-owner");
    scenario::abandon(store(&file)).await;
}
