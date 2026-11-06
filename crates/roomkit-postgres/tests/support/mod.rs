//! A Postgres schema of its own for each store a test opens, on the database
//! `POSTGRES_DSN` names: tests never see each other's rows, and need no order.

#![allow(
    dead_code,
    reason = "each test binary uses what it needs of this module"
)]

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_postgres::NoTls;

/// The schemas a test made so far in this process.
static MADE: AtomicU64 = AtomicU64::new(0);

/// One test at a time on the database: `init` reads catalogs a concurrent
/// `DROP` makes it fail on ("could not open relation"), and the advisory locks
/// are the database's, whatever the schema.
static IN_USE: Semaphore = Semaphore::const_new(1);

thread_local! {
    /// How many schemas this thread's test holds: a second one, of a test that
    /// opens two stores, takes its turn with the first.
    static HELD: Cell<usize> = const { Cell::new(0) };
}

/// The connection string the tests run against, if the environment names one.
pub(crate) fn dsn() -> Option<String> {
    std::env::var("POSTGRES_DSN")
        .ok()
        .filter(|dsn| !dsn.is_empty())
}

/// `error`'s message, each of its causes' after it.
pub(crate) fn full_text(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(current) = cause {
        text.push_str(": ");
        text.push_str(&current.to_string());
        cause = current.source();
    }
    text
}

/// `dsn` with the session writing to `schema`.
fn in_schema(dsn: &str, schema: &str) -> String {
    let separator = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{separator}options=-csearch_path%3D{schema}")
}

/// A new schema, dropped when the test is done with it.
#[derive(Debug)]
pub(crate) struct TempSchema {
    name: String,
    server: String,
    in_use: Option<SemaphorePermit<'static>>,
}

impl TempSchema {
    /// A schema no other test of any process uses, on `POSTGRES_DSN`'s
    /// database.
    pub(crate) async fn new() -> Self {
        let server = dsn().unwrap_or_default();
        let in_use = if HELD.get() == 0 {
            Some(IN_USE.acquire().await.unwrap())
        } else {
            None
        };
        HELD.set(HELD.get() + 1);
        let made = MADE.fetch_add(1, Ordering::Relaxed);
        let name = format!("rk_test_{}_{made}", std::process::id());
        let (client, connection) = tokio_postgres::connect(&server, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
            .batch_execute(&format!("CREATE SCHEMA {name}"))
            .await
            .unwrap();
        Self {
            name,
            server,
            in_use,
        }
    }

    /// The connection string of a session writing to this schema.
    pub(crate) fn dsn(&self) -> String {
        in_schema(&self.server, &self.name)
    }

    /// The connection string of a session writing to this schema, with
    /// `setting` (`lock_timeout=500`) beside it.
    pub(crate) fn dsn_with(&self, setting: &str) -> String {
        format!("{}%20-c{}", self.dsn(), setting.replace('=', "%3D"))
    }

    /// The name of this schema.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// A store on this schema, not initialized.
    pub(crate) fn store(&self) -> PostgresStore {
        PostgresStore::new(PostgresStoreOptions {
            dsn: Some(self.dsn()),
            ..PostgresStoreOptions::default()
        })
    }

    /// A store on this schema, initialized.
    pub(crate) async fn initialized_store(&self) -> PostgresStore {
        let store = self.store();
        store.init(SMALL_POOL).await.unwrap();
        store
    }

    /// A plain connection on this schema, for what a test reads or writes
    /// beside the store.
    pub(crate) async fn client(&self) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(&self.dsn(), NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    }
}

/// Returns once a session waits on a lock `holder`'s transaction holds.
pub(crate) async fn until_blocked_by(schema: &TempSchema, holder: &tokio_postgres::Client) {
    let pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let watcher = schema.client().await;
    let mut blocked = 0_i64;
    for _ in 0..400 {
        blocked = watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
                &[&pid],
            )
            .await
            .unwrap()
            .get(0);
        if blocked > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(blocked > 0, "no session ever waited on the holder");
}

/// Returns once the query `seen`, a count of sessions, finds one.
pub(crate) async fn until_a_session(schema: &TempSchema, seen: &str) {
    let watcher = schema.client().await;
    let mut found = 0_i64;
    for _ in 0..400 {
        found = watcher.query_one(seen, &[]).await.unwrap().get(0);
        if found > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(found > 0, "no session matched: {seen}");
}

/// A test's pool: its work takes few connections at once.
pub(crate) const SMALL_POOL: InitOptions = InitOptions {
    min_size: 1,
    max_size: 4,
};

impl Drop for TempSchema {
    /// Drops the schema from a thread of its own: this one may be the test's
    /// runtime, which the store's connections still need. The next test waits
    /// for it; a schema the process ended before dropping stays in a throwaway
    /// database.
    fn drop(&mut self) {
        HELD.set(HELD.get().saturating_sub(1));
        let (server, name, in_use) = (self.server.clone(), self.name.clone(), self.in_use.take());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                if let Ok((client, connection)) = tokio_postgres::connect(&server, NoTls).await {
                    tokio::spawn(connection);
                    client
                        .batch_execute(&format!("DROP SCHEMA IF EXISTS {name} CASCADE"))
                        .await
                        .ok();
                }
            });
            drop(in_use);
        });
    }
}
