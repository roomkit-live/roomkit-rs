//! Cancellation must finish on the retained connection before it is reused.

use roomkit::model::Room;
use roomkit::{ConversationStore, StoreError};
use serde_json::json;

use super::fixture::room;
use super::support::{TempSchema, dsn, sessions_blocked_by};

#[tokio::test]
async fn cleanup_failure_closes_the_connection_and_takes_precedence() {
    use std::sync::Arc;
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    for body_fails in [false, true] {
        let schema = TempSchema::new().await;
        let canceller: roomkit_postgres::PostgresCancelQuery = Arc::new(|token| {
            Box::pin(async move {
                token
                    .cancel_query(NoTls)
                    .await
                    .map_err(StoreError::unavailable)?;
                Err(StoreError::Value("cleanup sentinel".to_owned()))
            })
        });
        let (store, pool, counter) = super::fixture::counted_with(&schema, canceller).await;
        let holder = schema.client().await;
        holder
            .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
            .await
            .unwrap();
        let result = store
            .connection(async {
                store
                    .create_room(Room::new(
                        roomkit::model::RoomId::new("before-cleanup").unwrap(),
                    ))
                    .await?;
                let mut cut = Room::new(room());
                cut.metadata.insert("cut".to_owned(), json!(true));
                let mut write = Box::pin(store.update_room(cut));
                tokio::select! {
                    result = &mut write => panic!("write escaped its lock: {result:?}"),
                    _ = sessions_blocked_by(&schema, &holder) => {},
                }
                drop(write);
                if body_fails {
                    return Err(StoreError::Value("body sentinel".to_owned()));
                }
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(result, StoreError::Value("cleanup sentinel".to_owned()));
        assert_eq!(counter.pids().len(), 1);
        assert_eq!(pool.status().size, 0);
        holder.batch_execute("COMMIT").await.unwrap();
        assert!(
            store
                .room_exists(&roomkit::model::RoomId::new("before-cleanup").unwrap())
                .await
                .unwrap()
        );
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
}

#[tokio::test]
async fn a_cleanup_error_reaches_the_next_call_once_with_its_original_variant() {
    use std::sync::Arc;
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let canceller: roomkit_postgres::PostgresCancelQuery = Arc::new(|token| {
        Box::pin(async move {
            token
                .cancel_query(NoTls)
                .await
                .map_err(StoreError::unavailable)?;
            Err(StoreError::Value("cleanup sentinel".to_owned()))
        })
    });
    let (store, pool, counter) = super::fixture::counted_with(&schema, canceller).await;
    let holder = schema.client().await;
    holder
        .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
        .await
        .unwrap();
    let result = store
        .connection(async {
            let mut cut = Room::new(room());
            cut.metadata.insert("cut".to_owned(), json!(true));
            let mut write = Box::pin(store.update_room(cut));
            tokio::select! {
                result = &mut write => panic!("write escaped its lock: {result:?}"),
                _ = sessions_blocked_by(&schema, &holder) => {},
            }
            drop(write);
            assert_eq!(
                store.get_room(&room()).await.unwrap_err(),
                StoreError::Value("cleanup sentinel".to_owned())
            );
            assert_eq!(
                store.get_room(&room()).await.unwrap_err(),
                StoreError::Value("the scoped connection is closed".to_owned())
            );
            Ok::<_, StoreError>(17)
        })
        .await
        .unwrap();
    assert_eq!(result, 17);
    assert_eq!(counter.pids().len(), 1);
    assert_eq!(pool.status().size, 0);
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
