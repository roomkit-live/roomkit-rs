//! The acquisition timeout becomes a fresh release budget at the owning scope's exit.

use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Manager, Pool};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
use tokio::time::{Instant, timeout};
use tokio_postgres::{Client, NoTls};

use super::fixture::{backend_pid, room};
use super::support::{TempSchema, dsn, sessions_blocked_by};

const BUDGET: Duration = Duration::from_secs(5);
const TEST_LIMIT: Duration = Duration::from_secs(8);

async fn blocked_fixture(schema: &TempSchema) -> (PostgresStore, Pool, Client, i32) {
    let pool = Pool::builder(Manager::new(schema.dsn().parse().unwrap(), NoTls))
        .max_size(1)
        .build()
        .unwrap();
    let store = PostgresStore::new(PostgresStoreOptions {
        pool: Some(pool.clone()),
        // The cancellation request never receives its response.
        cancel_query: Some(Arc::new(|_| Box::pin(std::future::pending()))),
        ..Default::default()
    });
    store.init(InitOptions::default()).await.unwrap();
    let pid = backend_pid(&pool).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; LOCK TABLE rooms IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    (store, pool, holder, pid)
}

async fn cancel_read(store: &PostgresStore, schema: &TempSchema, holder: &Client) {
    let id = room();
    tokio::select! {
        result = store.get_room(&id) => panic!("read escaped its lock: {result:?}"),
        _ = sessions_blocked_by(schema, holder) => {},
    }
}

#[tokio::test]
async fn the_owning_exit_starts_a_fresh_budget_for_pending_cleanup() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, holder, initial_pid) = blocked_fixture(&schema).await;
    let mut exiting = None;
    let scope = store.connection(async {
        store
            .connection(async {
                cancel_read(&store, &schema, &holder).await;
                Ok::<_, StoreError>(())
            })
            .await?;
        tokio::time::sleep(BUDGET + Duration::from_millis(100)).await;
        assert_eq!(pool.status().size, 1);
        assert_eq!(pool.status().available, 0);
        exiting = Some(Instant::now());
        Err::<(), _>(StoreError::Value("body sentinel".to_owned()))
    });
    let error = timeout(BUDGET + TEST_LIMIT, scope)
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, StoreError::Unavailable(cause) if cause.is::<tokio::time::error::Elapsed>())
    );
    assert!(exiting.unwrap().elapsed() >= BUDGET);
    assert_eq!(pool.status().size, 0);
    assert_ne!(backend_pid(&pool).await, initial_pid);
    holder.batch_execute("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn dropping_the_owner_expires_a_stalled_cleanup_and_replaces_its_connection() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, holder, initial_pid) = blocked_fixture(&schema).await;
    let id = room();
    let mut scope = Box::pin(store.connection(store.get_room(&id)));
    tokio::select! {
        result = &mut scope => panic!("read escaped its lock: {result:?}"),
        _ = sessions_blocked_by(&schema, &holder) => {},
    }
    let dropped = Instant::now();
    drop(scope);
    assert_eq!(pool.status().available, 0);
    let replacement = timeout(TEST_LIMIT, backend_pid(&pool)).await.unwrap();
    assert!(dropped.elapsed() >= BUDGET);
    assert_ne!(replacement, initial_pid);
    holder.batch_execute("ROLLBACK").await.unwrap();
}
