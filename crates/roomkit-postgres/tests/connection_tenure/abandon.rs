//! Cancellation must finish on the retained connection before it is reused.

use roomkit::model::Room;
use roomkit::{ConversationStore, StoreError};
use serde_json::json;

use super::fixture::room;
use super::support::{TempSchema, dsn, sessions_blocked_by};

#[tokio::test]
async fn dropping_the_outer_block_keeps_cleanup_and_its_lease_alive() {
    dropped_during_cleanup(false).await;
}

#[tokio::test]
async fn dropping_the_exit_barrier_keeps_cleanup_and_its_lease_alive() {
    dropped_during_cleanup(true).await;
}

async fn dropped_during_cleanup(at_exit: bool) {
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let canceller: roomkit_postgres::PostgresCancelQuery = {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        Arc::new(move |token| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                token
                    .cancel_query(NoTls)
                    .await
                    .map_err(StoreError::unavailable)
            })
        })
    };
    let (store, pool, counter) = super::fixture::counted_with(&schema, canceller).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    let body = async {
        let mut cut = Room::new(room());
        cut.metadata.insert("cut".to_owned(), json!(true));
        let mut write = Box::pin(store.update_room(cut));
        if at_exit {
            tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                _ = sessions_blocked_by(&schema, &holder) => {},
            }
            drop(write);
        } else {
            write.await?;
        }
        Ok::<_, StoreError>(())
    };
    let mut scope = Box::pin(store.connection(body));
    if at_exit {
        tokio::select! {
            result = &mut scope => panic!("exit escaped cleanup: {result:?}"),
            () = entered.notified() => {},
        }
    } else {
        tokio::select! {
            result = &mut scope => panic!("write escaped its lock: {result:?}"),
            _ = sessions_blocked_by(&schema, &holder) => {},
        }
    }
    drop(scope);
    if !at_exit {
        entered.notified().await;
    }
    assert_eq!(counter.pids().len(), 1);
    assert_eq!(pool.status().size, 1);
    assert_eq!(pool.status().available, 0);
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while pool.status().available != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let pids = counter.pids();
    let state: String = holder
        .query_one(
            "SELECT state FROM pg_stat_activity WHERE pid = $1",
            &[pids.first().unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(state, "idle");
    holder.batch_execute("COMMIT").await.unwrap();
    assert!(
        store
            .get_room(&room())
            .await
            .unwrap()
            .unwrap()
            .metadata
            .is_empty()
    );
}

#[tokio::test]
async fn dropping_a_polled_block_outside_the_runtime_still_cancels_its_write() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&calls);
    let canceller: roomkit_postgres::PostgresCancelQuery = Arc::new(move |token| {
        let counting = Arc::clone(&counting);
        Box::pin(async move {
            counting.fetch_add(1, Ordering::Relaxed);
            token
                .cancel_query(NoTls)
                .await
                .map_err(StoreError::unavailable)
        })
    });
    let (store, pool, counter) = super::fixture::counted_with(&schema, canceller).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    let mut scope = Box::pin(store.connection(async {
        let mut cut = Room::new(room());
        cut.metadata.insert("cut".to_owned(), json!(true));
        store.update_room(cut).await
    }));
    let blocked = tokio::select! {
        result = &mut scope => panic!("write escaped its lock: {result:?}"),
        blocked = sessions_blocked_by(&schema, &holder) => blocked,
    };
    assert_eq!(blocked, counter.pids());
    std::thread::scope(|threads| {
        threads
            .spawn(move || {
                assert!(tokio::runtime::Handle::try_current().is_err());
                drop(scope);
            })
            .join()
            .unwrap();
    });
    let drained = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while pool.status().available != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    holder.batch_execute("COMMIT").await.unwrap();
    super::support::until_ended(&schema, &blocked).await;
    let found = store.get_room(&room()).await.unwrap().unwrap();
    assert!(
        found.metadata.is_empty(),
        "an abandoned write reached PostgreSQL: {:?}",
        found.metadata
    );
    assert!(drained.is_ok(), "the lease was not safely returned");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
