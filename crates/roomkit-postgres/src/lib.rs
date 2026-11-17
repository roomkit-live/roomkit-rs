//! A Postgres conversation store for RoomKit (RFC §14), on the Python
//! implementation's schema.
//!
//! [`PostgresStore`] keeps rooms and their timelines in the tables the Python
//! implementation's `PostgresStore` keeps, with the same columns and the
//! same additive, idempotent schema batch, so that processes of both
//! implementations share one database:
//!
//! - a value of a JSONB column roomkit-rs does not model (a capability Python
//!   adds, a content's member) is kept when it rewrites the row, an integer past
//!   64 bits excepted, which `serde_json` reads as a float;
//! - every statement is parameterized, as Python's are; a statement's text
//!   holds only this crate's own names;
//! - a call its caller cuts (a task cancelled, a timeout, a future dropped) has
//!   the statement it left running cancelled on the server, as asyncpg cancels
//!   it. Outside a connection block, the connection is closed, never handed
//!   back to the pool. Inside a block, cancellation is acknowledged and
//!   Sync/ReadyForQuery is drained before the same connection serves another
//!   call or returns to the pool; failed cleanup closes it instead;
//! - a DSN holds a password: nothing here logs or prints one.
//!
//! [`ConversationStore::connection`](roomkit::ConversationStore::connection)
//! brackets sequential store calls, without starting a transaction or exposing
//! a SQL client. Nesting on the same store or one of its clones joins the current
//! connection. Another store temporarily masks that binding. A dropped body
//! leaves any pending cancellation owned by its cleanup task; its connection
//! cannot return to the pool before cleanup finishes. Cleanup uses the runtime
//! that entered the block even when the future is dropped on another thread;
//! that runtime must remain alive for the cleanup to complete.
//!
//! An externally supplied standard pool must provide
//! [`PostgresStoreOptions::cancel_query`], including when framework reads enter
//! the block. Keep a clone of the pool's TLS connector for that callback; a
//! plaintext pool must explicitly use `tokio_postgres::NoTls`. There is no TLS
//! fallback inside a block. A token from a custom raw connection may not know
//! its actual target; such a pool cannot use this capability as-is.
//!
//! Cancellation-safe reuse requires a tokio-postgres version whose
//! `cancel_query` waits for the cancellation connection to close. A root
//! workspace patch is a development check, not a transitive library dependency.
//!
//! One condition holds for Python to read a row this crate wrote: every
//! channel type in it is one Python's enumeration lists.
//!
//! [`PostgresAdvisoryLockManager`] is the room lock across processes, on the
//! same keys as Python's, so that processes of both implementations exclude
//! each other. It needs a pool of its own: a held lock keeps its connection.
//!
//! [`PostgresKnowledgeSource`] is a full-text keyword search for an AI channel's
//! memory (RFC §20.2), on the table of Python's source of that name.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use roomkit::{RoomKit, RoomKitOptions};
//! use roomkit_postgres::{
//!     InitOptions, PostgresAdvisoryLockManager, PostgresAdvisoryLockManagerOptions,
//!     PostgresStore, PostgresStoreOptions,
//! };
//! use std::sync::Arc;
//!
//! let dsn = "postgresql://roomkit:secret@localhost/roomkit".to_owned();
//! let store = PostgresStore::new(PostgresStoreOptions {
//!     dsn: Some(dsn.clone()),
//!     ..PostgresStoreOptions::default()
//! });
//! store.init(InitOptions::default()).await?;
//! let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
//!     dsn: Some(dsn),
//!     ..PostgresAdvisoryLockManagerOptions::default()
//! });
//! locks.init().await?;
//! let kit = RoomKit::new(RoomKitOptions {
//!     lock_manager: Some(Arc::new(locks)),
//!     ..RoomKitOptions::with_store(store)
//! });
//! # Ok(())
//! # }
//! ```

mod connect;
mod connection;
mod dsn;
mod error;
mod events;
mod hosts;
mod identities;
mod knowledge;
mod lent;
mod lock;
mod lookups;
mod markers;
mod members;
mod options;
mod passfile;
mod pool;
mod rooms;
mod rows;
mod schema;
mod side_effects;
mod spans;
mod sqlstates;
mod store;
mod timeline;
mod tls;
mod values;
mod verifications;

pub use self::error::PostgresSchemaError;
pub use self::knowledge::PostgresKnowledgeSource;
pub use self::lock::PostgresAdvisoryLockManager;
pub use self::options::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresAdvisoryLockManagerOptions, PostgresCancelQuery, PostgresKnowledgeSourceOptions,
    PostgresStoreOptions,
};
pub use self::schema::{SCHEMA as POSTGRES_SCHEMA, SCHEMA_VERSION as POSTGRES_SCHEMA_VERSION};
pub use self::store::PostgresStore;
