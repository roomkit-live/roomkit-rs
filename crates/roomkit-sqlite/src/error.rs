//! Why a file could not be opened, and what a store operation meets.

use roomkit::StoreError;
use thiserror::Error;

/// A file of a schema this crate does not open: Python's
/// `SQLiteSchemaError`. A newer version is not written, so as not to corrupt
/// what a newer implementation keeps; an older one is migrated by the
/// Python implementation's `SQLiteStore` when it opens it, which this crate
/// does not do in 0.x. The store's first operation fails with it, as
/// [`StoreError::Unavailable`]'s cause.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct SqliteSchemaError {
    message: String,
}

impl SqliteSchemaError {
    /// A file at `found`, newer than `supported`, in Python's words.
    pub(crate) fn newer(found: i64, supported: i64) -> Self {
        Self {
            message: format!(
                "SQLite schema version {found} is newer than supported version {supported}; \
                 upgrade RoomKit before opening this file"
            ),
        }
    }

    /// A file at `found`, older than `supported`.
    pub(crate) fn older(found: i64, supported: i64) -> Self {
        Self {
            message: format!(
                "SQLite schema version {found} is older than supported version {supported}: \
                 open it once with the Python implementation's SQLiteStore, which migrates it"
            ),
        }
    }
}

/// Why the file could not be opened, its schema aside: the cause of the
/// [`StoreError::Unavailable`] the first operation fails with.
#[derive(Debug, Error)]
pub(crate) enum OpenFailure {
    /// The file's directory, missing, could not be created.
    #[error("the file's directory could not be created")]
    Directory(#[source] std::io::Error),
    /// The database failed: its cause is SQLite's error, which a caller
    /// reads through the source chain, so that the SQLite library's version
    /// is no part of this crate's API.
    #[error("the database could not be opened")]
    Database(#[source] rusqlite::Error),
    /// The thread that opens the file stopped before it was done.
    #[error("the store's thread stopped")]
    Stopped(#[source] tokio::task::JoinError),
}

impl From<OpenFailure> for StoreError {
    fn from(failure: OpenFailure) -> Self {
        Self::unavailable(failure)
    }
}

impl From<SqliteSchemaError> for StoreError {
    fn from(error: SqliteSchemaError) -> Self {
        Self::unavailable(error)
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
