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
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventContent, EventType, InboundMessage,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{EventTap, RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    HookExecution, HookOptions, HookTrigger, InMemoryLockManager, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions, RoomLockManager,
};
use roomkit_postgres::{
    PostgresAdvisoryLockManager, PostgresAdvisoryLockManagerOptions, PostgresStore,
    PostgresStoreOptions,
};
use tokio::sync::oneshot;

use crate::support::{SMALL_POOL, TempSchema, dsn};

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

/// A wait dropped on a Unix socket, in a list that also names a TCP host under
/// `sslmode=require`, is cancelled on the server as a TCP one is: the cancel
/// request of a socket's connection asks no TLS. Runs with
/// `POSTGRES_SOCKET_DIR`, the directory of the server's socket.
#[tokio::test]
async fn a_wait_dropped_on_a_socket_beside_a_tcp_host_is_cancelled() {
    let Some(server) = dsn() else {
        return;
    };
    let Some(directory) = std::env::var("POSTGRES_SOCKET_DIR").ok() else {
        return;
    };
    let config: tokio_postgres::Config = server.parse().unwrap();
    let port = *config.get_ports().first().unwrap_or(&5432);
    if !std::path::Path::new(&directory)
        .join(format!(".s.PGSQL.{port}"))
        .exists()
    {
        return;
    }
    let schema = TempSchema::new().await;
    let first = manager(&schema).await;
    // A password, or the store would read the real ~/.pgpass.
    let password = String::from_utf8(config.get_password().unwrap_or(b"unused").to_vec()).unwrap();
    let second = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        dsn: Some(format!(
            "postgresql://{}:{password}@/{}?host={directory},127.0.0.1&port={port}&sslmode=require",
            config.get_user().unwrap_or("postgres"),
            config.get_dbname().unwrap_or("postgres"),
        )),
        max_size: 2,
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    second.init().await.unwrap();
    let held = first.locked(&room("r1")).await.unwrap();
    let waited = room("r1");
    // Boxed, so that the drop below drops the wait itself.
    let mut waiting = Box::pin(second.locked(&waited));
    // Cut once the server shows the wait: one cut before it proves nothing.
    tokio::select! {
        _ = &mut waiting => panic!("the wait was granted while the lock was held"),
        () = until_locks_are(&schema, (1, 1)) => {}
    }
    drop(waiting);
    until_locks_are(&schema, (1, 0)).await;
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

/// Python: `_close_bounded`: a wait for a lock when the close gives up on it is
/// refused then, on the server too, never granted once the holder lets go.
#[tokio::test]
async fn a_close_ends_the_wait_for_a_lock() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (first, second) = (manager(&schema).await, manager(&schema).await);
    let held = first.locked(&room("r1")).await.unwrap();
    let waiter = tokio::spawn({
        let second = second.clone();
        async move { second.locked(&room("r1")).await.map(drop) }
    });
    until_locks_are(&schema, (1, 1)).await;
    second.close().await.unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(refused, Err(RoomKitError::Runtime(_))),
        "{refused:?}"
    );
    until_locks_are(&schema, (1, 0)).await;
    held.release().await.unwrap();
}

/// A pool closed while a lock is waited for grants nothing once the holder lets
/// go: its owner's close of a pool handed in, which no close of the manager tells.
#[tokio::test]
async fn a_lock_granted_on_a_pool_closed_meanwhile_is_refused() {
    use deadpool_postgres::{Manager, Pool};
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let first = manager(&schema).await;
    let config: tokio_postgres::Config = schema.dsn().parse().unwrap();
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(2)
        .build()
        .unwrap();
    let second = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        pool: Some(pool.clone()),
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    let held = first.locked(&room("r1")).await.unwrap();
    let waiter = tokio::spawn({
        let second = second.clone();
        async move { second.locked(&room("r1")).await.map(drop) }
    });
    until_locks_are(&schema, (1, 1)).await;
    pool.close();
    held.release().await.unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(refused, Err(RoomKitError::Runtime(_))),
        "{refused:?}"
    );
    until_locks_are(&schema, (0, 0)).await;
}

/// A wait dropped on a pool handed in, whose TLS the manager does not know, ends
/// on the server when it sees the client gone, should the cancel fail: its
/// session asks to look.
#[tokio::test]
async fn a_pool_handed_in_has_the_server_watch_for_a_client_gone() {
    use deadpool_postgres::{Manager, Pool};
    use tokio_postgres::NoTls;
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let config: tokio_postgres::Config = schema.dsn().parse().unwrap();
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(1)
        .build()
        .unwrap();
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        pool: Some(pool.clone()),
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    locks
        .locked(&room("r1"))
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
    let row = pool
        .get()
        .await
        .unwrap()
        .query_one("SHOW client_connection_check_interval", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "1s");
}

/// The manager's failure at `init` says why, not only that the store failed.
#[tokio::test]
async fn a_failed_init_says_why() {
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        dsn: Some("postgresql://user:hunter2@127.0.0.1:1/db?sslmode=disable".to_owned()),
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    let refused = locks.init().await.unwrap_err().to_string();
    assert!(refused.contains("connect"), "{refused}");
    assert!(!refused.contains("hunter2"), "{refused}");
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
        ..RoomKitOptions::with_store(PostgresStore::new(PostgresStoreOptions::default()))
    });
    assert_eq!(warnings.containing("is paired with").len(), 1);
    let _advisory = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(PostgresAdvisoryLockManager::new(
            PostgresAdvisoryLockManagerOptions::default(),
        ))),
        ..RoomKitOptions::with_store(PostgresStore::new(PostgresStoreOptions::default()))
    });
    assert_eq!(warnings.containing("is paired with").len(), 1);
}

// A process killed holding a room's lock or its delivery claim (RFC §13.5): the
// test binary run again as the holder, in a process of its own, killed by SIGKILL.
// It closes nothing, so only the server, ending the dead process's sessions, lets
// its locks go (RK-13.5-003, RK-13.5-005).

/// The holder's environment, set by the test that runs it: the connection string of
/// the test's schema, the room it sends its message to, and the file it writes
/// where it holds what it holds.
const HOLDER_DSN: &str = "ROOMKIT_TEST_HOLDER_DSN";
const HOLDER_ROOM: &str = "ROOMKIT_TEST_HOLDER_ROOM";
const HOLDER_OUT: &str = "ROOMKIT_TEST_HOLDER_OUT";

/// The application name of the holder's sessions, by which `pg_locks` tells its locks.
const HOLDER_APP: &str = "roomkit_killed_holder";

/// What the holder holds until it is killed.
#[derive(Clone, Copy, Debug)]
enum Hold {
    /// The room's lock, in a sync `BEFORE_BROADCAST` hook that never returns,
    /// before its commit: it writes `holding` from the hook.
    RoomLock,
    /// The room's delivery claim, in `out`'s delivery of its committed message,
    /// which never returns: it writes `delivering <index>` from the delivery.
    DeliveryClaim,
}

/// `id`'s advisory lock key, as the manager computes it: Python's `_advisory_key`.
fn advisory_key(id: &str) -> i64 {
    use blake2::digest::consts::U8;
    use blake2::{Blake2b, Digest};
    i64::from_be_bytes(Blake2b::<U8>::digest(id.as_bytes()).into())
}

/// `line`, written to `out` whole: the test reads it while it is written.
fn say(out: &std::path::Path, line: &str) {
    let partial = out.with_extension("partial");
    std::fs::write(&partial, format!("{line}\n")).unwrap();
    std::fs::rename(&partial, out).unwrap();
}

/// A transport whose delivery says which event it holds and never returns.
#[derive(Debug)]
struct Holding {
    out: std::path::PathBuf,
}

impl Channel for Holding {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("out").unwrap()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's method is a future"
    )]
    async fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        Err(ChannelError::from("the holder's out receives no message"))
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        say(&self.out, &format!("delivering {}", event.index));
        std::future::pending().await
    }
}

/// In the holder's process, run again by a test: opens a store and a manager of
/// its own on the test's schema, as another process of the deployment does,
/// sends one message to the room, whose `src` and `out` the test attached, and
/// holds `hold` until it is killed. `false` in the test's own process.
async fn holds_in_this_process(hold: Hold) -> bool {
    let (Ok(dsn), Ok(room_id), Some(out)) = (
        std::env::var(HOLDER_DSN),
        std::env::var(HOLDER_ROOM),
        std::env::var_os(HOLDER_OUT),
    ) else {
        return false;
    };
    let out = std::path::PathBuf::from(out);
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some(dsn.clone()),
        ..PostgresStoreOptions::default()
    });
    store.init(SMALL_POOL).await.unwrap();
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        dsn: Some(dsn),
        max_size: 4,
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    locks.init().await.unwrap();
    let kit = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(locks)),
        ..RoomKitOptions::with_store(store)
    });
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("src").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    match hold {
        Hold::RoomLock => {
            kit.register_channel(RecordingTransport::new(
                ChannelId::new("out").unwrap(),
                ChannelType::WEBSOCKET,
                RecordingTransportOptions::default(),
            ))
            .unwrap();
            let options = HookOptions {
                execution: HookExecution::Sync,
                timeout: Duration::from_secs(600),
                ..HookOptions::default()
            };
            let hold_the_room = move |_event: RoomEvent, _context: RoomContext| {
                let out = out.clone();
                async move {
                    say(&out, "holding");
                    std::future::pending::<()>().await;
                }
            };
            kit.hook(HookTrigger::BeforeBroadcast, hold_the_room, options)
                .unwrap();
        }
        Hold::DeliveryClaim => kit.register_channel(Holding { out }).unwrap(),
    }
    let message = InboundMessage::new(
        ChannelId::new("src").unwrap(),
        "u1",
        TextContent::new("from the killed process"),
    );
    let options = ProcessInboundOptions {
        room_id: Some(room(&room_id)),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    true
}

/// The test binary run again as `test`'s holder, killed when dropped.
struct Holder {
    child: std::process::Child,
    out: std::path::PathBuf,
}

impl Holder {
    /// Runs `test` again in a process of its own, as the holder of `room_id` on
    /// `schema`.
    fn start(test: &str, schema: &TempSchema, room_id: &RoomId) -> Self {
        let out = std::env::temp_dir().join(format!("{}-holder.out", schema.name()));
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--test-threads=1"])
            .env(
                HOLDER_DSN,
                format!("{}&application_name={HOLDER_APP}", schema.dsn()),
            )
            .env(HOLDER_ROOM, room_id.as_str())
            .env(HOLDER_OUT, &out)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        Self { child, out }
    }

    /// The line the holder wrote, once it starts with `prefix`; fails when the
    /// holder ends first, or after twenty seconds.
    async fn said(&mut self, prefix: &str) -> String {
        let mut said = String::new();
        for _ in 0..2000 {
            said = std::fs::read_to_string(&self.out).unwrap_or_default();
            if said.starts_with(prefix) {
                break;
            }
            let ended = self.child.try_wait().unwrap();
            assert!(
                ended.is_none(),
                "the holder ended ({ended:?}) before writing {prefix:?}: {}",
                self.stderr()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            said.starts_with(prefix),
            "the holder wrote {said:?}, not {prefix:?}"
        );
        said.trim_end().to_owned()
    }

    /// What the holder wrote to its standard error, once it ended.
    fn stderr(&mut self) -> String {
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            std::io::Read::read_to_string(&mut pipe, &mut stderr).unwrap();
        }
        stderr
    }

    /// Kills the holder with SIGKILL, and returns the signal that ended it.
    fn kill(&mut self) -> Option<i32> {
        use std::os::unix::process::ExitStatusExt;
        // A process already gone cannot be killed again.
        let _already_gone = self.child.kill();
        self.child.wait().unwrap().signal()
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let _already_gone = self.child.kill();
        let _waited = self.child.wait();
        let _removed = std::fs::remove_file(&self.out);
        let _removed = std::fs::remove_file(self.out.with_extension("partial"));
    }
}

/// The sessions `pg_locks` lists on the advisory lock `key` of this database, the
/// granted first: each one's pid, whether it holds the lock or waits on it, and
/// whether it is the holder's.
async fn sessions_on(schema: &TempSchema, key: i64) -> Vec<(i32, bool, bool)> {
    let unsigned = u64::from_be_bytes(key.to_be_bytes());
    let (classid, objid) = (
        u32::try_from(unsigned >> 32).unwrap(),
        u32::try_from(unsigned & 0xFFFF_FFFF).unwrap(),
    );
    let rows = schema
        .client()
        .await
        .query(
            "SELECT l.pid, l.granted, a.application_name = $3 FROM pg_locks l \
             JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE l.locktype = 'advisory' AND l.objsubid = 1 \
             AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
             AND l.classid = $1 AND l.objid = $2 ORDER BY NOT l.granted",
            &[&classid, &objid, &HOLDER_APP],
        )
        .await
        .unwrap();
    rows.iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

/// The pid of the holder's session granted the advisory lock `key`, the only
/// session listed on it.
async fn held_by_the_holder(schema: &TempSchema, key: i64) -> i32 {
    let sessions = sessions_on(schema, key).await;
    assert_eq!(
        sessions
            .iter()
            .map(|&(_, granted, holder)| (granted, holder))
            .collect::<Vec<_>>(),
        [(true, true)]
    );
    sessions.first().unwrap().0
}

/// The sessions on the advisory lock `key`, once one of them waits on it; fails
/// after ten seconds.
async fn until_waited_on(schema: &TempSchema, key: i64) -> Vec<(i32, bool, bool)> {
    let mut sessions = sessions_on(schema, key).await;
    for _ in 0..1000 {
        if sessions.iter().any(|&(_, granted, _)| !granted) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        sessions = sessions_on(schema, key).await;
    }
    sessions
}

/// Returns once the server lists no lock of the session `pid`; fails after ten
/// seconds.
async fn until_gone(schema: &TempSchema, pid: i32) {
    let watcher = schema.client().await;
    let mut held = 0_i64;
    for _ in 0..1000 {
        held = watcher
            .query_one("SELECT count(*) FROM pg_locks WHERE pid = $1", &[&pid])
            .await
            .unwrap()
            .get(0);
        if held == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(held, 0, "the killed session {pid} still holds locks");
}

/// A kit on `schema` with a store and a manager of its own, as another process of
/// the deployment opens them, with `src` and the recording `out` attached to a
/// room of the schema's own, which it returns.
async fn own_kit(
    schema: &TempSchema,
    delivery_gap_timeout: Duration,
) -> (RoomKit<PostgresStore>, RecordingTransport, RoomId) {
    let kit = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(manager(schema).await)),
        delivery_gap_timeout,
        ..RoomKitOptions::with_store(schema.initialized_store().await)
    });
    let out = RecordingTransport::new(
        ChannelId::new("out").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("src").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(out.clone()).unwrap();
    // Advisory locks are the database's, whatever the schema: a room of its own.
    let room_id = room(&format!("room-{}", schema.name()));
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for id in ["src", "out"] {
        kit.attach_channel(
            &room_id,
            &ChannelId::new(id).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    (kit, out, room_id)
}

/// This process's message to `room_id`.
fn after_the_kill(room_id: &RoomId) -> (InboundMessage, ProcessInboundOptions) {
    let message = InboundMessage::new(
        ChannelId::new("src").unwrap(),
        "u1",
        TextContent::new("after the kill"),
    );
    let options = ProcessInboundOptions {
        room_id: Some(room_id.clone()),
        ..ProcessInboundOptions::default()
    };
    (message, options)
}

// Python: tests/test_postgres_multiprocess.py::test_a_room_lock_a_killed_process_held_is_released_and_the_room_goes_on
/// RK-13.5-003: a process holds a room's lock in a hook before its commit, and
/// this process's kit waits on it on the server. The holder killed, the server
/// ends its sessions and drops its lock: the kit takes the lock and commits, and
/// the killed process's message was never stored.
#[tokio::test]
async fn a_room_lock_a_killed_process_held_is_released_and_the_room_goes_on() {
    if holds_in_this_process(Hold::RoomLock).await || dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let (kit, _out, room_id) = own_kit(&schema, Duration::from_secs(30)).await;
    let mut holder = Holder::start(
        "a_room_lock_a_killed_process_held_is_released_and_the_room_goes_on",
        &schema,
        &room_id,
    );
    holder.said("holding").await;
    let key = advisory_key(room_id.as_str());
    let holder_pid = held_by_the_holder(&schema, key).await;
    let sending = tokio::spawn({
        let (kit, (message, options)) = (kit.clone(), after_the_kill(&room_id));
        async move { kit.process_inbound(message, options).await }
    });
    let waiting = until_waited_on(&schema, key).await;
    // The holder's session holds it, one of this kit's waits on it.
    assert_eq!(
        waiting
            .iter()
            .map(|&(pid, granted, holder)| (pid == holder_pid, granted, holder))
            .collect::<Vec<_>>(),
        [(true, true, true), (false, false, false)]
    );
    assert!(!sending.is_finished());

    let signal = holder.kill();
    let result = tokio::time::timeout(Duration::from_secs(10), sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    until_gone(&schema, holder_pid).await;
    let store = schema.initialized_store().await;
    let events = store
        .list_events(&room_id, roomkit::store::ListEventsOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    store.close().await;

    assert_eq!(signal, Some(9));
    assert!(!result.blocked && result.event.is_some(), "{result:?}");
    let messages: Vec<&str> = events
        .iter()
        .filter(|event| event.r#type == EventType::Message)
        .filter_map(|event| {
            if let EventContent::Text(text) = &event.content {
                Some(text.body.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(messages, ["after the kill"]);
    let left = sessions_on(&schema, key).await;
    assert!(left.is_empty(), "{left:?}");
}

// Python: tests/test_postgres_multiprocess.py::test_a_delivery_claim_a_killed_process_held_leaves_its_index_skipped
/// RK-13.5-005: a process committed index N and holds the room's delivery claim,
/// delivering N, when it is killed. The server drops the claim with its sessions;
/// this process's next message, N+1, waits on the hole at N for
/// `delivery_gap_timeout`, skips it with `delivery_skipped`, and is delivered once,
/// the cursor at N+1.
#[tokio::test]
async fn a_delivery_claim_a_killed_process_held_leaves_its_index_skipped() {
    if holds_in_this_process(Hold::DeliveryClaim).await || dsn().is_none() {
        return;
    }
    let gap = Duration::from_millis(300);
    let schema = TempSchema::new().await;
    let (kit, out, room_id) = own_kit(&schema, gap).await;
    let tap = EventTap::on(&kit, &["delivery_skipped"]);
    let mut holder = Holder::start(
        "a_delivery_claim_a_killed_process_held_leaves_its_index_skipped",
        &schema,
        &room_id,
    );
    let said = holder.said("delivering ").await;
    let lost: u64 = said.trim_start_matches("delivering ").parse().unwrap();
    let claim = advisory_key(&format!("\0__delivery__:{}", room_id.as_str()));
    let holder_pid = held_by_the_holder(&schema, claim).await;

    let signal = holder.kill();
    until_gone(&schema, holder_pid).await;
    let store = schema.initialized_store().await;
    let before = store.get_room(&room_id).await.unwrap().unwrap();
    let started = tokio::time::Instant::now();
    let (message, options) = after_the_kill(&room_id);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        kit.process_inbound(message, options),
    )
    .await
    .unwrap()
    .unwrap();
    let waited = started.elapsed();
    let after = store.get_room(&room_id).await.unwrap().unwrap();
    kit.close().await.unwrap();
    store.close().await;

    assert_eq!(signal, Some(9));
    let lost_signed = i64::try_from(lost).unwrap();
    assert_eq!(
        (before.latest_index, before.delivered_index),
        (Some(lost), lost_signed - 1)
    );
    assert_eq!(result.event.map(|event| event.index), Some(lost + 1));
    assert!(waited >= gap, "{waited:?}");
    let skipped: Vec<(u64, u64)> = tap
        .events()
        .iter()
        .map(|skip| {
            (
                skip.data["from_index"].as_u64().unwrap(),
                skip.data["to_index"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(skipped, [(lost, lost)]);
    let delivered: Vec<u64> = out
        .deliveries()
        .iter()
        .map(|delivery| delivery.event.index)
        .collect();
    assert_eq!(delivered, [lost + 1]);
    assert_eq!(
        (after.latest_index, after.delivered_index),
        (Some(lost + 1), lost_signed + 1)
    );
    let left = sessions_on(&schema, claim).await;
    assert!(left.is_empty(), "{left:?}");
}
