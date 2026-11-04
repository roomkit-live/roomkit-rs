//! The lock across processes on a real database: Python's
//! `tests/test_postgres_lock.py` and `tests/test_postgres_multiprocess.py`.
//! Each test skips itself without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, InMemoryLockManager,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, RoomLockManager,
};
use roomkit_postgres::{
    PostgresAdvisoryLockManager, PostgresAdvisoryLockManagerOptions, PostgresStore,
};
use tokio::sync::oneshot;

use crate::support::{TempSchema, dsn};

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// A manager of its own pool on the database, initialized.
async fn manager(schema: &TempSchema) -> PostgresAdvisoryLockManager {
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        dsn: Some(schema.dsn()),
        max_size: 4,
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    locks.init().await.unwrap();
    locks
}

/// How many advisory locks the server holds, and how many sessions wait for
/// one.
async fn advisory_locks(schema: &TempSchema) -> (i64, i64) {
    let row = schema
        .client()
        .await
        .query_one(
            "SELECT count(*) FILTER (WHERE granted), count(*) FILTER (WHERE NOT granted) \
             FROM pg_locks WHERE locktype = 'advisory'",
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

/// Waits until the server shows `wanted`: an advisory lock's state is seen
/// nowhere else, and a closed session's goes a moment later.
async fn until_locks_are(schema: &TempSchema, wanted: (i64, i64)) {
    let mut seen = advisory_locks(schema).await;
    for _ in 0..400 {
        if seen == wanted {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        seen = advisory_locks(schema).await;
    }
    assert_eq!(seen, wanted);
}

/// Python: `test_locked_raises_without_init`.
#[tokio::test]
async fn a_lock_before_init_fails_and_says_so() {
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions::default());
    let Err(refused) = locks.locked(&room("r1")).await else {
        panic!("a lock was granted before init");
    };
    assert!(matches!(refused, RoomKitError::Runtime(_)), "{refused:?}");
    assert!(refused.to_string().contains("init() must be called"));
}

/// A DSN holds a password: neither the options nor the manager write one.
#[test]
fn no_debug_form_writes_the_dsn() {
    let options = PostgresAdvisoryLockManagerOptions {
        dsn: Some("postgresql://user:hunter2@host/db".to_owned()),
        ..PostgresAdvisoryLockManagerOptions::default()
    };
    assert!(!format!("{options:?}").contains("hunter2"));
    assert!(!format!("{:?}", PostgresAdvisoryLockManager::new(options)).contains("hunter2"));
}

/// Python: `test_locked_takes_and_releases_advisory_lock`,
/// `test_child_reacquires_after_parent_releases`.
#[tokio::test]
async fn a_lock_is_taken_released_and_taken_again() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let locks = manager(&schema).await;
    let lock = locks.locked(&room("r1")).await.unwrap();
    assert_eq!(advisory_locks(&schema).await, (1, 0));
    lock.release().await.unwrap();
    assert_eq!(advisory_locks(&schema).await, (0, 0));
    let again = locks.locked(&room("r1")).await.unwrap();
    assert_eq!(advisory_locks(&schema).await, (1, 0));
    again.release().await.unwrap();
}

/// Python: `test_advisory_lock_is_not_bypassed_by_another_manager`,
/// `test_advisory_lock_serializes_concurrent_assignment`: two managers, each
/// its own pool as two processes hold them, exclude each other on one room,
/// and not on another.
#[tokio::test]
async fn two_processes_exclude_each_other_on_one_room_only() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (first, second) = (manager(&schema).await, manager(&schema).await);
    let held = first.locked(&room("r1")).await.unwrap();
    let other_room = second.locked(&room("r2")).await.unwrap();
    other_room.release().await.unwrap();

    let (granted, was_granted) = oneshot::channel();
    let waiter = tokio::spawn({
        let second = second.clone();
        async move {
            let lock = second.locked(&room("r1")).await.unwrap();
            granted.send(()).unwrap();
            lock.release().await.unwrap();
        }
    });
    until_locks_are(&schema, (1, 1)).await;
    held.release().await.unwrap();
    was_granted.await.unwrap();
    waiter.await.unwrap();
}

/// The trait's rule: a wait dropped before it holds the lock leaves nothing
/// taken, here or on the server, and the pool's next lock does not wait behind
/// it.
#[tokio::test]
async fn a_wait_dropped_leaves_nothing_taken() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (first, second) = (manager(&schema).await, manager(&schema).await);
    let held = first.locked(&room("r1")).await.unwrap();
    let waited = room("r1");
    let cut = tokio::time::timeout(Duration::from_millis(100), second.locked(&waited)).await;
    assert!(cut.is_err(), "the wait was granted while the lock was held");
    until_locks_are(&schema, (1, 0)).await;
    let elsewhere = tokio::time::timeout(Duration::from_secs(2), second.locked(&room("r2")))
        .await
        .unwrap()
        .unwrap();
    elsewhere.release().await.unwrap();
    held.release().await.unwrap();
    until_locks_are(&schema, (0, 0)).await;
}

/// A lock dropped unreleased, its holder cut, is released on the server, with
/// no statement and no task of its own.
#[tokio::test]
async fn a_lock_dropped_unreleased_is_released_on_the_server() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let locks = manager(&schema).await;
    let lock = locks.locked(&room("r1")).await.unwrap();
    assert_eq!(advisory_locks(&schema).await, (1, 0));
    drop(lock);
    until_locks_are(&schema, (0, 0)).await;
    let again = tokio::time::timeout(Duration::from_secs(2), locks.locked(&room("r1")))
        .await
        .unwrap()
        .unwrap();
    again.release().await.unwrap();
}

/// Python: `PostgresAdvisoryLockManager._close_bounded`: a lock still held past
/// a second has its connection cut, which releases it on the server; its
/// holder's release then finds nothing to do. A second close does nothing, and
/// a lock after it fails.
#[tokio::test]
async fn a_close_cuts_the_lock_still_held_past_its_bound() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let locks = manager(&schema).await;
    let lock = locks.locked(&room("r1")).await.unwrap();
    locks.close().await.unwrap();
    until_locks_are(&schema, (0, 0)).await;
    lock.release().await.unwrap();
    locks.close().await.unwrap();
    assert!(matches!(
        locks.locked(&room("r1")).await,
        Err(RoomKitError::Runtime(_))
    ));
}

/// Python: `test_close_skips_external_pool`: a pool handed in stays its owner's.
#[tokio::test]
async fn a_pool_handed_in_stays_open_after_the_manager_s_close() {
    use deadpool_postgres::{Manager, Pool};
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let config: tokio_postgres::Config = schema.dsn().parse().unwrap();
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(2)
        .build()
        .unwrap();
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        pool: Some(pool.clone()),
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    locks.init().await.unwrap();
    let lock = locks.locked(&room("r1")).await.unwrap();
    lock.release().await.unwrap();
    locks.close().await.unwrap();
    assert!(!pool.is_closed());
    locks
        .locked(&room("r1"))
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
}

/// A kit on `schema`'s store and a manager of its own, with its transports.
async fn worker(schema: &TempSchema) -> RoomKit<PostgresStore> {
    let kit = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(manager(schema).await)),
        ..RoomKitOptions::with_store(schema.initialized_store().await)
    });
    for (id, kind) in [("src", ChannelType::SMS), ("out", ChannelType::WEBSOCKET)] {
        kit.register_channel(RecordingTransport::new(
            ChannelId::new(id).unwrap(),
            kind,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
    }
    kit
}

/// Python: `test_inbound_pipeline_commits_atomically_with_advisory_lock`,
/// `test_ai_reentry_commits_delivered_with_consistent_counters`: two kits, each
/// its store, its pool and its manager, as two processes hold them, process one
/// room's messages at once: no index given twice, the counters exact, and the
/// delivery cursor past every event. The kit's claim and verification keys go
/// through the same manager under their own leading NUL.
#[tokio::test]
async fn two_kits_process_one_room_with_no_index_given_twice() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (first, second) = (worker(&schema).await, worker(&schema).await);
    first
        .create_room(CreateRoomOptions {
            room_id: Some(room("r1")),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
    for id in ["src", "out"] {
        first
            .attach_channel(
                &room("r1"),
                &ChannelId::new(id).unwrap(),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
    }
    let send = async |kit: &RoomKit<PostgresStore>, number: u32| {
        kit.process_inbound(
            InboundMessage::new(
                ChannelId::new("src").unwrap(),
                "u",
                TextContent::new(format!("{number}")),
            ),
            ProcessInboundOptions::default(),
        )
        .await
        .unwrap();
    };
    let burst_first = async {
        for number in 0..5 {
            send(&first, number).await;
        }
    };
    let burst_second = async {
        for number in 5..10 {
            send(&second, number).await;
        }
    };
    tokio::join!(burst_first, burst_second);

    let store = schema.initialized_store().await;
    let events = store
        .list_events(&room("r1"), roomkit::store::ListEventsOptions::default())
        .await
        .unwrap();
    let mut indexes: Vec<u64> = events.iter().map(|event| event.index).collect();
    indexes.sort_unstable();
    let distinct = indexes.windows(2).filter(|pair| pair[0] != pair[1]).count() + 1;
    assert_eq!(distinct, indexes.len(), "{indexes:?}");
    let stored = store.get_room(&room("r1")).await.unwrap().unwrap();
    assert_eq!(stored.event_count, u64::try_from(indexes.len()).unwrap());
    assert_eq!(stored.latest_index, indexes.last().copied());
    first.close().await.unwrap();
    second.close().await.unwrap();
}

/// Beside the store, an in-process lock is told; the advisory lock is not.
#[tokio::test]
async fn the_advisory_lock_is_not_warned_of() {
    let (warnings, _capture) = warnings::Warnings::capture();
    let _in_memory = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(InMemoryLockManager::default())),
        ..RoomKitOptions::with_store(PostgresStore::new(Default::default()))
    });
    assert_eq!(warnings.containing("is paired with").len(), 1);
    let _advisory = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(PostgresAdvisoryLockManager::new(
            PostgresAdvisoryLockManagerOptions::default(),
        ))),
        ..RoomKitOptions::with_store(PostgresStore::new(Default::default()))
    });
    assert_eq!(warnings.containing("is paired with").len(), 1);
}
