//! The store's one connection, called on tokio's blocking threads.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::StoreError;
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::error::{Failure, OpenError};
use crate::schema;

/// A file's connection, one call at a time: SQLite writes one transaction
/// at a time anyway, and a single connection never waits on another of the
/// same process.
#[derive(Debug, Clone)]
pub(crate) struct Database {
    connection: Arc<Mutex<Connection>>,
}

impl Database {
    /// Opens the file at `path`, creating it, and its directory, at the
    /// schema version this crate knows when it holds none, and refusing one
    /// of another version.
    pub(crate) async fn open(path: PathBuf) -> Result<Self, OpenError> {
        let connection = tokio::task::spawn_blocking(move || {
            if let Some(directory) = path.parent() {
                std::fs::create_dir_all(directory).map_err(OpenError::Directory)?;
            }
            let mut connection = Connection::open(path)?;
            schema::prepare(&mut connection)?;
            Ok::<_, OpenError>(connection)
        })
        .await??;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Runs `work` on the connection, on a blocking thread.
    pub(crate) async fn run<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&mut Connection) -> Result<T, Failure> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        let outcome = tokio::task::spawn_blocking(move || {
            // A panic while the connection was held rolled its transaction
            // back as it unwound, so the connection keeps serving.
            let mut connection = connection.lock().unwrap_or_else(PoisonError::into_inner);
            work(&mut connection)
        })
        .await
        .map_err(StoreError::unavailable)?;
        outcome.map_err(StoreError::from)
    }

    /// Runs `work` in one write transaction, which takes the file's write
    /// lock at once: all of it is written, or nothing.
    pub(crate) async fn write<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&Transaction<'_>) -> Result<T, Failure> + Send + 'static,
    {
        self.run(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let written = work(&transaction)?;
            transaction.commit()?;
            Ok(written)
        })
        .await
    }

    /// Runs `work` in one read transaction, so what it reads is one state of
    /// the file.
    pub(crate) async fn read<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&Transaction<'_>) -> Result<T, Failure> + Send + 'static,
    {
        self.run(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let read = work(&transaction)?;
            transaction.commit()?;
            Ok(read)
        })
        .await
    }
}
