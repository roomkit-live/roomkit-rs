//! An acknowledged cancel still has to drain `ReadyForQuery` within the release budget.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Connect, Manager, ManagerConfig, Pool};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, Error, NoTls};

use super::fixture::{backend_pid, room};
use super::support::{TempSchema, dsn, sessions_blocked_by};

/// Stop polling the main socket on request. `CancelToken` uses a separate socket
/// to the same PostgreSQL instance and can still get its acknowledgement.
#[derive(Clone, Default)]
struct PauseDriver {
    request: Arc<Notify>,
    paused: Arc<Notify>,
}

impl Connect for PauseDriver {
    fn connect(
        &self,
        config: &Config,
    ) -> Pin<Box<dyn Future<Output = Result<(Client, JoinHandle<()>), Error>> + Send + '_>> {
        let config = config.clone();
        let pause = self.clone();
        Box::pin(async move {
            let (client, connection) = config.connect(NoTls).await?;
            let task = tokio::spawn(async move {
                tokio::pin!(connection);
                tokio::select! {
                    _ = &mut connection => {},
                    () = pause.request.notified() => {
                        pause.paused.notify_one();
                        std::future::pending::<()>().await;
                    },
                }
            });
            Ok((client, task))
        })
    }
}

async fn paused_store(schema: &TempSchema) -> (PostgresStore, Pool, PauseDriver, Arc<Notify>) {
    let pause = PauseDriver::default();
    let pool = Pool::builder(Manager::from_connect(
        schema.dsn().parse().unwrap(),
        pause.clone(),
        ManagerConfig::default(),
    ))
    .max_size(1)
    .build()
    .unwrap();
    let acknowledged = Arc::new(Notify::new());
    let sent = Arc::clone(&acknowledged);
    let store = PostgresStore::new(PostgresStoreOptions {
        pool: Some(pool.clone()),
        cancel_query: Some(Arc::new(move |token| {
            let sent = Arc::clone(&sent);
            Box::pin(async move {
                token
                    .cancel_query(NoTls)
                    .await
                    .map_err(StoreError::unavailable)?;
                sent.notify_one();
                Ok(())
            })
        })),
        ..Default::default()
    });
    store.init(InitOptions::default()).await.unwrap();
    (store, pool, pause, acknowledged)
}

#[tokio::test]
async fn the_release_budget_also_bounds_ready_for_query_after_acknowledged_cancel() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, pause, acknowledged) = paused_store(&schema).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; LOCK TABLE rooms IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let mut blocked = Vec::new();
    let scope = store.connection(async {
        let id = room();
        let mut read = Box::pin(store.get_room(&id));
        tokio::select! {
            result = &mut read => panic!("read escaped its lock: {result:?}"),
            waiters = sessions_blocked_by(&schema, &holder) => blocked = waiters,
        }
        pause.request.notify_one();
        pause.paused.notified().await;
        drop(read);
        acknowledged.notified().await;
        Ok::<_, StoreError>(())
    });
    let error = tokio::time::timeout(Duration::from_secs(8), scope)
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, StoreError::Unavailable(cause) if cause.is::<tokio::time::error::Elapsed>())
    );
    assert_eq!(pool.status().size, 0);
    assert_eq!(blocked.len(), 1);
    assert_ne!(backend_pid(&pool).await, blocked[0]);
    holder.batch_execute("ROLLBACK").await.unwrap();
}
