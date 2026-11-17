use std::sync::Arc;

use roomkit::model::{Room, RoomId};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::{PostgresStore, PostgresStoreOptions};
use tokio::sync::Barrier;

use super::fixture::{counted, room};
use super::support::{TempSchema, dsn};

#[tokio::test]
async fn a_block_and_its_nested_clone_pay_one_checkout() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, counter) = counted(&schema).await;
    let cloned = store.clone();
    store
        .connection(async {
            assert!(store.get_room(&room()).await?.is_some());
            cloned
                .connection(async {
                    assert_eq!(cloned.list_bindings(&room()).await?, Vec::new());
                    Ok::<_, StoreError>(())
                })
                .await?;
            assert_eq!(store.list_participants(&room()).await?, Vec::new());
            let (found, bindings, participants) = store.load_room_context(&room()).await?;
            assert!(found.is_some());
            assert!(bindings.is_empty() && participants.is_empty());
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
    assert_eq!(counter.pids().len(), 1);
    assert_eq!(pool.status().available, pool.status().size);
    counter.reset();
    store.get_room(&room()).await.unwrap();
    store.list_bindings(&room()).await.unwrap();
    store.list_participants(&room()).await.unwrap();
    assert_eq!(counter.pids().len(), 3);
}

#[tokio::test]
async fn different_stores_mask_and_restore_the_previous_binding() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (first, first_pool, first_count) = counted(&schema).await;
    let other_schema = TempSchema::new().await;
    let (second, second_pool, second_count) = counted(&other_schema).await;
    first
        .connection(async {
            first.get_room(&room()).await?;
            second
                .connection(async {
                    assert_eq!(first_pool.status().available, 0);
                    assert_eq!(second_pool.status().available, 0);
                    let inner = first
                        .connection(async {
                            let pids = first_count.pids();
                            assert_eq!(pids.len(), 2);
                            assert_ne!(pids[0], pids[1]);
                            assert_ne!(pids[1], second_count.pids()[0]);
                            first
                                .create_room(Room::new(RoomId::new("written").unwrap()))
                                .await?;
                            Err::<(), StoreError>(StoreError::Value("body sentinel".to_owned()))
                        })
                        .await
                        .unwrap_err();
                    assert_eq!(inner, StoreError::Value("body sentinel".to_owned()));
                    assert!(!second.room_exists(&RoomId::new("written").unwrap()).await?);
                    assert_eq!(second_count.pids().len(), 1);
                    Ok::<_, StoreError>(())
                })
                .await?;
            assert!(first.room_exists(&RoomId::new("written").unwrap()).await?);
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
    assert_eq!(first_count.pids().len(), 2);
    assert_eq!(second_count.pids().len(), 1);
    assert_eq!(first_pool.status().available, first_pool.status().size);
    assert_eq!(second_pool.status().available, second_pool.status().size);
}

#[tokio::test]
async fn concurrent_blocks_have_distinct_connections() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, counter) = counted(&schema).await;
    let ready = Arc::new(Barrier::new(2));
    let run = || {
        let store = store.clone();
        let ready = Arc::clone(&ready);
        tokio::spawn(async move {
            store
                .connection(async {
                    store.get_room(&room()).await?;
                    ready.wait().await;
                    store.list_bindings(&room()).await?;
                    Ok::<_, StoreError>(())
                })
                .await
        })
    };
    let first = run();
    let second = run();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let pids = counter.pids();
    assert_eq!(pids.len(), 2);
    assert_ne!(pids[0], pids[1]);
    assert_eq!(pool.status().available, pool.status().size);
}

#[tokio::test]
async fn an_external_pool_without_a_canceller_refuses_before_polling_the_body() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (_, pool, counter) = counted(&schema).await;
    let store = PostgresStore::new(PostgresStoreOptions {
        pool: Some(pool),
        ..PostgresStoreOptions::default()
    });
    let result = store
        .connection(async {
            panic!("the body must not run without a cancellation capability");
            #[expect(
                unreachable_code,
                reason = "fix the type of a body that must never run"
            )]
            Ok::<_, StoreError>(())
        })
        .await;
    assert_eq!(
        result.unwrap_err(),
        StoreError::Value(
            "PostgresStore.connection() requires cancel_query for an external pool".to_owned()
        )
    );
    assert_eq!(counter.pids(), Vec::<i32>::new());
}

#[tokio::test]
async fn cancelled_acquisition_leaves_every_pool_slot_available() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (store, pool, _) = counted(&schema).await;
    let held = [
        pool.get().await.unwrap(),
        pool.get().await.unwrap(),
        pool.get().await.unwrap(),
    ];
    let entered = std::sync::atomic::AtomicBool::new(false);
    let mut scope = Box::pin(store.connection(async {
        entered.store(true, std::sync::atomic::Ordering::Relaxed);
        store.get_room(&room()).await
    }));
    assert!(matches!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(scope.as_mut().poll(cx))).await,
        std::task::Poll::Pending
    ));
    assert!(!entered.load(std::sync::atomic::Ordering::Relaxed));
    drop(scope);
    drop(held);
    assert_eq!(pool.status().available, 3);
    assert!(
        store
            .connection(store.get_room(&room()))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn an_unscoped_call_of_another_store_uses_its_own_pool() {
    if dsn().is_none() {
        return;
    }
    let first_schema = TempSchema::new().await;
    let (first, _, first_count) = counted(&first_schema).await;
    let second_schema = TempSchema::new().await;
    let (second, _, second_count) = counted(&second_schema).await;
    let only_second = RoomId::new("only-second").unwrap();
    second
        .create_room(Room::new(only_second.clone()))
        .await
        .unwrap();
    second_count.reset();
    first
        .connection(async {
            assert!(second.get_room(&only_second).await?.is_some());
            assert!(first.get_room(&only_second).await?.is_none());
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
    assert_eq!(first_count.pids().len(), 1);
    assert_eq!(second_count.pids().len(), 1);
    assert_ne!(first_count.pids(), second_count.pids());
}
