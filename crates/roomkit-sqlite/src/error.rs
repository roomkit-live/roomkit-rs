//! Why a file could not be opened, and what a store operation meets.

use roomkit::StoreError;
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
    /// The database failed.
    #[error("the database failed")]
    Database(#[from] rusqlite::Error),
    /// The thread that opens the file stopped before it was done.
    #[error("the store's thread stopped")]
    Stopped(#[from] tokio::task::JoinError),
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
