//! Why a database could not be opened, and what a store operation meets.

use roomkit::StoreError;
use thiserror::Error;

/// A database whose schema needs the explicit, opt-in migration: Python's
/// `PostgresSchemaError`. [`init`](crate::PostgresStore::init) never touches
/// a version 1 schema, so that a routine connect can never destroy data; it
/// fails with this error, as [`StoreError::Unavailable`]'s cause, and so does
/// a [`migrate`](crate::PostgresStore::migrate) without its confirmation.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct PostgresSchemaError {
    message: String,
}

impl PostgresSchemaError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<PostgresSchemaError> for StoreError {
    fn from(error: PostgresSchemaError) -> Self {
        Self::unavailable(error)
    }
}

/// Why an operation got no connection, in Python's words: an operation before
/// [`PostgresStore::init`](crate::PostgresStore::init), or after the close of a pool
/// the store owned; a pool none of whose connections came free in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub(crate) enum NoConnection {
    #[error("PostgresStore.init() must be called before use")]
    NotInitialized,
    #[error("no connection of the pool came free in time")]
    TimedOut,
}

/// What an operation meets: the store's refusal, or a failure, which the
/// store reports unavailable.
#[derive(Debug)]
pub(crate) enum Failure {
    /// A refusal the trait names.
    Refused(StoreError),
    /// The database failed.
    Database(tokio_postgres::Error),
    /// The database refused an event's key: the event's id may be held, which
    /// is a second write of the event rather than a key taken (Python's
    /// `_a_held_id_as_such`).
    KeyRefused {
        driver: tokio_postgres::Error,
        taken: StoreError,
    },
    /// A row's JSON could not be read or written.
    Json(serde_json::Error),
    /// A value is out of the range its column or the model takes.
    OutOfRange(&'static str),
}

impl From<StoreError> for Failure {
    fn from(error: StoreError) -> Self {
        Self::Refused(error)
    }
}

impl From<tokio_postgres::Error> for Failure {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Database(error)
    }
}

impl From<serde_json::Error> for Failure {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<PostgresSchemaError> for Failure {
    fn from(error: PostgresSchemaError) -> Self {
        Self::Refused(error.into())
    }
}

impl From<Failure> for StoreError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Refused(error) => error,
            Failure::Database(error) => Self::unavailable(error),
            Failure::KeyRefused { taken, .. } => taken,
            Failure::Json(error) => Self::unavailable(error),
            Failure::OutOfRange(what) => Self::unavailable(format!("{what} is out of range")),
        }
    }
}
