//! A SQLite conversation store for RoomKit (RFC §14), on the Python
//! implementation's schema.
//!
//! [`SqliteStore`] keeps rooms and their timelines in one SQLite file, the
//! file the Python implementation's `SQLiteStore` keeps (its schema version
//! 12): the same tables and indexed columns, each record's JSON in a `data`
//! column. Either implementation reads what the other wrote:
//!
//! - a record's members roomkit-rs does not model (an identity's `email`, a
//!   capability Python adds) are kept when it rewrites the record;
//! - the tables it does not use (verifications, read markers, voice
//!   deliveries) are left alone, and those it does not read but Python
//!   does are kept right: the idempotency keys, the full-text index, which
//!   a deleted message leaves (RFC §10.3), and the index sequences;
//! - a file with a newer schema is refused rather than written, and a new
//!   file is created at version 12, exactly as Python creates it.
//!
//! One condition holds for Python to read a file this crate wrote: every
//! channel type in it is one Python's enumeration lists (`sms`, `teams`,
//! `discord`, ...). A type of the integrator's own
//! (`ChannelType::new("slack")`), which roomkit-rs reads and writes as any
//! other, makes the bindings and events of its room unreadable to the
//! Python implementation until it opens its type (RoomKit's RMK-986).
//!
//! One process uses a file at a time: the store holds one connection,
//! called on tokio's blocking threads, in the write-ahead log's mode. Other
//! processes on the same file, and the distributed lock they would need
//! (RFC §13.5), are out of scope.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use roomkit::{RoomKit, RoomKitOptions};
//! use roomkit_sqlite::SqliteStore;
//!
//! let store = SqliteStore::open("rooms.db").await?;
//! let kit = RoomKit::new(RoomKitOptions::with_store(store));
//! # Ok(())
//! # }
//! ```

mod connection;
mod error;
mod event_rows;
mod events;
mod identities;
mod lookups;
mod members;
mod records;
mod rooms;
mod schema;
mod side_effects;
mod store;
mod timeline;

pub use self::error::OpenError;
pub use self::store::SqliteStore;
