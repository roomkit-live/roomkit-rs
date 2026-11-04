//! A Postgres conversation store for RoomKit (RFC §14), on the Python
//! implementation's schema.
//!
//! [`PostgresStore`] keeps rooms and their timelines in the tables the Python
//! implementation's `PostgresStore` keeps, with the same columns and the
//! same additive, idempotent schema batch, so that processes of both
//! implementations share one database:
//!
//! - a value of a JSONB column roomkit-rs does not model (a capability Python
//!   adds, a content's member) is kept when it rewrites the row;
//! - every statement is parameterized, as Python's are; a statement's text
//!   holds only this crate's own names;
//! - a DSN holds a password: nothing here logs or prints one.
//!
//! One condition holds for Python to read a row this crate wrote: every
//! channel type in it is one Python's enumeration lists.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use roomkit::{RoomKit, RoomKitOptions};
//! use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
//!
//! let store = PostgresStore::new(PostgresStoreOptions {
//!     dsn: Some("postgresql://roomkit:secret@localhost/roomkit".to_owned()),
//!     ..PostgresStoreOptions::default()
//! });
//! store.init(InitOptions::default()).await?;
//! let kit = RoomKit::new(RoomKitOptions::with_store(store));
//! # Ok(())
//! # }
//! ```

mod error;
mod events;
mod identities;
mod lookups;
mod markers;
mod members;
mod options;
mod pool;
mod rooms;
mod rows;
mod schema;
mod side_effects;
mod store;
mod timeline;
mod values;
mod verifications;

pub use self::error::PostgresSchemaError;
pub use self::options::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresStoreOptions,
};
pub use self::store::PostgresStore;
