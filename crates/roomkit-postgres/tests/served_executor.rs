//! A Redis backend serves a PostgreSQL-backed kit's executor.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../../roomkit/tests/support/served_executor.rs"]
mod served_executor;
mod support;

use roomkit::{RoomKit, RoomKitOptions};

#[tokio::test]
async fn a_redis_turn_runs_on_the_postgres_executor() {
    let (Ok(url), Some(_)) = (std::env::var("REDIS_URL"), support::dsn()) else {
        return;
    };
    let schema = support::TempSchema::new().await;
    let store = schema.initialized_store().await;
    served_executor::exercise(&url, RoomKit::new(RoomKitOptions::with_store(store))).await;
}
