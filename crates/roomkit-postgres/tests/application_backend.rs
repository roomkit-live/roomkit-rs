//! Run the public application turn backend contract on an isolated Postgres schema.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "../../roomkit/tests/application_backend/scenario.rs"]
mod scenario;
mod support;

#[tokio::test]
async fn application_turn_backend() {
    if support::dsn().is_some() {
        let schema = support::TempSchema::new().await;
        scenario::run(schema.initialized_store().await).await;
    }
}
