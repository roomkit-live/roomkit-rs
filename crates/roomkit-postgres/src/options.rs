//! What the store's own methods take: Python's keyword arguments, with its
//! defaults.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use deadpool_postgres::Pool;

/// Cancels a query using the exact connector of an externally supplied pool.
///
/// `Ok(())` must mean the server closed the cancellation connection. RoomKit
/// then waits for Sync/ReadyForQuery on the retained main connection. The
/// callback must not fall back to another TLS policy after a failure.
/// Cloning this value shares the callback.
///
/// ```no_run
/// # fn example(config: tokio_postgres::Config) -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::Arc;
/// use roomkit::StoreError;
/// use roomkit_postgres::{PostgresStore, PostgresStoreOptions};
/// use tokio_postgres::NoTls;
///
/// let pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(config, NoTls))
///     .build()?;
/// let store = PostgresStore::new(PostgresStoreOptions {
///     pool: Some(pool),
///     cancel_query: Some(Arc::new(|token| Box::pin(async move {
///         token.cancel_query(NoTls).await.map_err(StoreError::unavailable)
///     }))),
///     ..PostgresStoreOptions::default()
/// });
/// # Ok(())
/// # }
/// ```
/// For TLS, clone the same connector into both the manager and the callback.
pub type PostgresCancelQuery = Arc<
    dyn Fn(
            tokio_postgres::CancelToken,
        )
            -> Pin<Box<dyn Future<Output = Result<(), roomkit::StoreError>> + Send + 'static>>
        + Send
        + Sync,
>;

/// What [`PostgresStore::new`](crate::PostgresStore::new) takes: Python's
/// `PostgresStore(dsn=None, pool=None)`.
///
/// Its `Debug` writes no DSN: one holds a password.
#[derive(Clone, Default)]
pub struct PostgresStoreOptions {
    /// The connection string, `postgresql://user:password@host:port/database`,
    /// read as Python's asyncpg reads it: `sslmode`, `sslrootcert`, `sslcrl` and
    /// `passfile`, a parameter the client does not read passed to the server as
    /// a setting (`?search_path=tenant`). What it leaves out is read from these
    /// `PG*` environment variables, the only ones this crate reads: `PGHOST`,
    /// `PGPORT`, `PGUSER` (else `LOGNAME`, `USER`, `LNAME` or `USERNAME`, else
    /// the account database's),
    /// `PGPASSWORD`, `PGPASSFILE`, `PGDATABASE`, `PGSSLMODE`, `PGSSLROOTCERT` and
    /// `PGSSLCRL`. `None` takes the whole connection from them (HITL 78cd4240).
    ///
    /// The hosts may be a list, in the netloc (`h1:5432,h2`), the `host` and
    /// `port` parameters, or `PGHOST` and `PGPORT`, tried in its order: a host
    /// that cannot be reached is passed for the next, a server that refuses the
    /// connection ends the list. With no host anywhere, the Unix sockets of
    /// `/run/postgresql`, `/var/run/postgresql`, `/tmp` and `/private/tmp`, then
    /// `localhost`. With no password, the first line of `~/.pgpass` (or of the
    /// file `passfile` or `PGPASSFILE` names) that matches the host, port,
    /// database and user, read at each new connection; a file the group or
    /// others can access is not read, and warned of (HITL fa9dd934). A
    /// connection is given 60 s, asyncpg's `timeout`, whichever hosts it tries.
    ///
    /// Not read: `sslcert` and `sslkey`, a client certificate asyncpg reads and
    /// this crate does not; and, as asyncpg reads none of them, `hostaddr`,
    /// `connect_timeout`, `keepalives` and its kin, `channel_binding` and
    /// `load_balance_hosts`. Each is sent to the server as a setting, which it
    /// refuses; `tcp_user_timeout` is sent too, the server's own setting, in
    /// milliseconds (HITL 22f38d15). As asyncpg's, the connection asks no TCP
    /// keepalive.
    /// A certificate that is its own root and a CA (the recipe of Postgres's
    /// documentation) is refused under a mode that checks the chain: rustls
    /// takes no CA as an end entity; give the server one issued by that CA.
    pub dsn: Option<String>,
    /// A pool of the integrator's, which stays theirs: the store never closes
    /// it. A connection block requires [`Self::cancel_query`] with its exact
    /// connector. Outside a block the crate cannot read its TLS: cancellation
    /// before closing asks for TLS that checks no certificate when the mode
    /// asks for TLS, then goes in clear if that fails, never under
    /// `sslmode=require` (HITL f281e902).
    pub pool: Option<Pool>,
    /// The cancellation capability of `pool`, required by
    /// [`ConversationStore::connection`](roomkit::ConversationStore::connection)
    /// for an externally supplied pool. Absence refuses the block before its
    /// body runs. A store-created pool retains its own connector instead.
    ///
    /// A standard pool can clone its TLS connector into this callback and await
    /// `token.cancel_query(tls)`. A plaintext pool must explicitly use `NoTls`.
    /// The driver must await server closure, not just the sending of the request.
    /// Custom pools whose token cannot reconnect to the same target are outside
    /// this capability. `None` is not a plaintext fallback.
    pub cancel_query: Option<PostgresCancelQuery>,
}

impl fmt::Debug for PostgresStoreOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStoreOptions")
            .field("dsn", &self.dsn.as_ref().map(|_| "<set>"))
            .field("pool", &self.pool.as_ref().map(|_| "<set>"))
            .field("cancel_query", &self.cancel_query.as_ref().map(|_| "<set>"))
            .finish()
    }
}

/// What [`PostgresKnowledgeSource::new`](crate::PostgresKnowledgeSource::new)
/// takes: Python's `PostgresKnowledgeSource(dsn=None, pool=None, *,
/// source_name='postgres', language='english')`.
///
/// Its `Debug` writes no DSN: one holds a password.
#[derive(Clone)]
pub struct PostgresKnowledgeSourceOptions {
    /// The connection string, read as [`PostgresStoreOptions::dsn`] is.
    pub dsn: Option<String>,
    /// A pool of the integrator's, a store's for one, which stays theirs: the
    /// source never closes it. The cancel request of a call cut on it checks
    /// no certificate, as [`PostgresStoreOptions::pool`]'s.
    pub pool: Option<Pool>,
    /// The name its results carry when a document was indexed with no source.
    pub source_name: String,
    /// The text search configuration it indexes and searches with: `french`
    /// finds "chevaux" when asked for "cheval". A document keeps the
    /// configuration it was indexed with until it is indexed again.
    pub language: String,
}

impl Default for PostgresKnowledgeSourceOptions {
    fn default() -> Self {
        Self {
            dsn: None,
            pool: None,
            source_name: "postgres".to_owned(),
            language: "english".to_owned(),
        }
    }
}

impl fmt::Debug for PostgresKnowledgeSourceOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresKnowledgeSourceOptions")
            .field("dsn", &self.dsn.as_ref().map(|_| "<set>"))
            .field("pool", &self.pool.as_ref().map(|_| "<set>"))
            .field("source_name", &self.source_name)
            .field("language", &self.language)
            .finish()
    }
}

/// What [`PostgresAdvisoryLockManager::new`](crate::PostgresAdvisoryLockManager::new)
/// takes: Python's `PostgresAdvisoryLockManager(dsn=None, *, pool=None,
/// min_size=1, max_size=10)`.
///
/// Its `Debug` writes no DSN: one holds a password.
#[derive(Clone)]
pub struct PostgresAdvisoryLockManagerOptions {
    /// The connection string, read as [`PostgresStoreOptions::dsn`] is.
    pub dsn: Option<String>,
    /// A pool of the integrator's, separate from the store's, which stays
    /// theirs: the manager never closes it. Its connections keep
    /// `client_connection_check_interval` at a second, from Postgres 14, set by
    /// the manager so that the server ends a wait whose client is gone. The
    /// cancel request of a wait or a release cut on it checks no certificate,
    /// as [`PostgresStoreOptions::pool`]'s.
    pub pool: Option<Pool>,
    /// How many connections the pool opens at once.
    pub min_size: usize,
    /// How many connections the pool holds at most: one per room locked at
    /// once.
    pub max_size: usize,
}

impl Default for PostgresAdvisoryLockManagerOptions {
    fn default() -> Self {
        Self {
            dsn: None,
            pool: None,
            min_size: 1,
            max_size: 10,
        }
    }
}

impl fmt::Debug for PostgresAdvisoryLockManagerOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAdvisoryLockManagerOptions")
            .field("dsn", &self.dsn.as_ref().map(|_| "<set>"))
            .field("pool", &self.pool.as_ref().map(|_| "<set>"))
            .field("min_size", &self.min_size)
            .field("max_size", &self.max_size)
            .finish()
    }
}

/// What [`PostgresStore::init`](crate::PostgresStore::init) takes: Python's
/// `init(min_size=2, max_size=10)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitOptions {
    /// How many connections the pool opens at once.
    pub min_size: usize,
    /// How many connections the pool holds at most.
    pub max_size: usize,
}

impl Default for InitOptions {
    fn default() -> Self {
        Self {
            min_size: 2,
            max_size: 10,
        }
    }
}

/// What [`PostgresStore::migrate`](crate::PostgresStore::migrate) takes:
/// Python's `migrate(*, dry_run=True, confirm=False)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrateOptions {
    /// Report what would happen, and run nothing.
    pub dry_run: bool,
    /// The acknowledgement that a backup exists, which the destructive drop
    /// requires.
    pub confirm: bool,
}

impl Default for MigrateOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            confirm: false,
        }
    }
}

/// What [`PostgresStore::dedupe_event_indices`](crate::PostgresStore::dedupe_event_indices)
/// takes: Python's `dedupe_event_indices(*, dry_run=True)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DedupeEventIndicesOptions {
    /// Count what would be repaired, and change nothing.
    pub dry_run: bool,
}

impl Default for DedupeEventIndicesOptions {
    fn default() -> Self {
        Self { dry_run: true }
    }
}

/// What [`PostgresStore::drop_legacy_parent_index`](crate::PostgresStore::drop_legacy_parent_index)
/// takes: Python's `drop_legacy_parent_index(*, dry_run=True)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DropLegacyParentIndexOptions {
    /// Say whether the legacy index is present, and change nothing.
    pub dry_run: bool,
}

impl Default for DropLegacyParentIndexOptions {
    fn default() -> Self {
        Self { dry_run: true }
    }
}
