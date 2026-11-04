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
//! - a DSN holds a password: nothing here logs or prints one.
//!
//! One condition holds for Python to read a row this crate wrote: every
//! channel type in it is one Python's enumeration lists.
//!
//! [`PostgresAdvisoryLockManager`] is the room lock across processes, on the
//! same keys as Python's, so that processes of both implementations exclude
//! each other. It needs a pool of its own: a held lock keeps its connection.
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

mod error;
mod events;
mod identities;
mod lock;
mod lookups;
mod markers;
mod members;
mod options;
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
pub use self::lock::PostgresAdvisoryLockManager;
pub use self::options::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresAdvisoryLockManagerOptions, PostgresStoreOptions,
};
pub use self::store::PostgresStore;
