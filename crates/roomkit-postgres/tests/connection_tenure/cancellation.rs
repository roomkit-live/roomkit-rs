//! Cancellation must finish on the retained connection before it is reused.

use roomkit::model::Room;
use roomkit::{ConversationStore, StoreError};
use serde_json::json;

use super::fixture::{counted, room};
use super::support::{TempSchema, dsn, sessions_blocked_by};

#[tokio::test]
async fn a_cancelled_write_keeps_the_same_connection_for_the_next_call() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, counter) = counted(&schema).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    store
        .connection(async {
            let mut cut = Room::new(room());
            cut.metadata.insert("cut".to_owned(), json!(true));
            let mut write = Box::pin(store.update_room(cut));
            let blocked = tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                blocked = sessions_blocked_by(&schema, &holder) => blocked,
            };
            assert_eq!(blocked, counter.pids());
            drop(write);
            let found = store.get_room(&room()).await?.unwrap();
            assert!(found.metadata.is_empty());
            assert_eq!(counter.pids(), blocked);
            let state: String = holder
                .query_one(
                    "SELECT state FROM pg_stat_activity WHERE pid = $1",
                    &[&blocked[0]],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(state, "idle");
            holder.batch_execute("COMMIT").await.unwrap();
            let mut next = found;
            next.metadata.insert("next".to_owned(), json!(true));
            assert_eq!(
                store.update_room(next).await?.metadata.get("next"),
                Some(&json!(true))
            );
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
    assert_eq!(counter.pids().len(), 1);
    assert_eq!(pool.status().available, pool.status().size);
    assert_eq!(
        store
            .get_room(&room())
            .await
            .unwrap()
            .unwrap()
            .metadata
            .get("cut"),
        None
    );
}

#[tokio::test]
async fn returning_without_another_call_still_drains_the_cancelled_write() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, counter) = counted(&schema).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    store
        .connection(async {
            let mut cut = Room::new(room());
            cut.metadata.insert("cut".to_owned(), json!(true));
            let mut write = Box::pin(store.update_room(cut));
            tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                _ = sessions_blocked_by(&schema, &holder) => {},
            }
            drop(write);
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
    assert_eq!(pool.status().available, pool.status().size);
    let pids = counter.pids();
    assert_eq!(pids.len(), 1);
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
async fn cancellation_rolls_back_a_transaction_before_reusing_the_connection() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, _, counter) = counted(&schema).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    store
        .connection(async {
            let mut cut = Room::new(room());
            cut.metadata.insert("cut".to_owned(), json!(true));
            let mut write =
                Box::pin(store.update_room_if(cut, roomkit::model::RoomStatus::Active, None, None));
            tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                _ = sessions_blocked_by(&schema, &holder) => {},
            }
            drop(write);
            assert!(store.get_room(&room()).await?.unwrap().metadata.is_empty());
            let pids = counter.pids();
            assert_eq!(pids.len(), 1);
            let row = holder
                .query_one(
                    "SELECT state, xact_start IS NULL FROM pg_stat_activity WHERE pid = $1",
                    &[pids.first().unwrap()],
                )
                .await
                .unwrap();
            assert_eq!(row.get::<_, String>(0), "idle");
            assert!(row.get::<_, bool>(1));
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
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
