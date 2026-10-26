//! Why a file could not be opened, and what a store operation meets.

use roomkit::{StoreError, StoreFailure};
use thiserror::Error;

/// Why [`SqliteStore::open`](crate::SqliteStore::open) refused or failed a
/// file.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    /// The file's schema is newer than the one this crate knows: it is not
    /// written, so as not to corrupt what a newer implementation keeps.
    #[error(
        "the file's schema version {found} is newer than version {supported}, which roomkit-sqlite knows"
    )]
    NewerSchema {
        /// The file's version.
        found: i64,
        /// The version this crate knows.
        supported: i64,
    },
    /// The file's schema is older than the one this crate knows; the Python
    /// implementation's `SQLiteStore` migrates it when it opens it.
    #[error(
        "the file's schema version {found} is older than version {supported}: open it once with \
         the Python implementation's SQLiteStore, which migrates it"
    )]
    OlderSchema {
        /// The file's version.
        found: i64,
        /// The version this crate knows.
        supported: i64,
    },
    /// The file's directory, missing, could not be created.
    #[error("the file's directory could not be created")]
    Directory(#[source] std::io::Error),
    /// The database failed: its cause is SQLite's error, which a caller
    /// reads through the source chain, so that the SQLite library's version
    /// is no part of this crate's API.
    #[error("the database failed")]
    Database(#[source] StoreFailure),
    /// The thread that opens the file stopped before it was done.
    #[error("the store's thread stopped")]
    Stopped(#[from] tokio::task::JoinError),
}

impl OpenError {
    /// The database failed with `error`.
    pub(crate) fn database(error: rusqlite::Error) -> Self {
        Self::Database(StoreFailure::new(error))
    }
}

/// What an operation meets: the store's refusal, or a failure, which the
/// store reports unavailable.
#[derive(Debug)]
pub(crate) enum Failure {
    /// A refusal the trait names.
    Refused(StoreError),
    /// The database failed.
    Database(rusqlite::Error),
    /// A row's JSON could not be read or written.
    Json(serde_json::Error),
    /// A value the store holds is out of the range the model takes.
    OutOfRange(&'static str),
}

impl From<StoreError> for Failure {
    fn from(error: StoreError) -> Self {
        Self::Refused(error)
    }
}

impl From<rusqlite::Error> for Failure {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl From<serde_json::Error> for Failure {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<Failure> for StoreError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Refused(error) => error,
            Failure::Database(error) => Self::unavailable(error),
            Failure::Json(error) => Self::unavailable(error),
            Failure::OutOfRange(what) => Self::unavailable(format!("{what} is out of range")),
        }
    }
}
