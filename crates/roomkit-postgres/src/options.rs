//! What the store's own methods take: Python's keyword arguments, with its
//! defaults.

use std::fmt;

use deadpool_postgres::Pool;

/// What [`PostgresStore::new`](crate::PostgresStore::new) takes: Python's
/// `PostgresStore(dsn=None, pool=None)`.
///
/// Its `Debug` writes no DSN: one holds a password.
#[derive(Clone, Default)]
pub struct PostgresStoreOptions {
    /// The connection string, `postgresql://user:password@host:port/database`,
    /// read as Python's asyncpg reads it, `sslmode` and `sslrootcert`
    /// included. Without one, the `PG*` environment variables say where.
    pub dsn: Option<String>,
    /// A pool of the integrator's, which stays theirs: the store never closes
    /// it.
    pub pool: Option<Pool>,
}

impl fmt::Debug for PostgresStoreOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStoreOptions")
            .field("dsn", &self.dsn.as_ref().map(|_| "<set>"))
            .field("pool", &self.pool.as_ref().map(|_| "<set>"))
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
    /// theirs: the manager never closes it.
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
