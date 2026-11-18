//! Run the public application turn backend contract on SQLite.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "../../roomkit/tests/application_backend/scenario.rs"]
mod scenario;
mod support;

#[tokio::test]
async fn application_turn_backend() {
    let file = support::TempFile::new("application-backend");
    let store = roomkit_sqlite::SqliteStore::new(roomkit_sqlite::SqliteStoreOptions {
        path: file.path().into(),
    });
    scenario::run(store).await;
}
