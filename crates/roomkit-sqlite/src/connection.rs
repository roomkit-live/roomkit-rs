//! The store's connections, called on tokio's blocking threads: one writer,
//! and a few read-only connections beside it, as SQLite's write-ahead log
//! allows (RMK-1033).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::StoreError;
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use tokio::sync::Semaphore;

use crate::error::{Failure, OpenFailure};
use crate::schema;

/// What an operation on a closed store fails with, in Python's words.
pub(crate) const CLOSED: &str = "SQLiteStore is closed";

/// How many read-only connections serve reads at once: enough for the reads
/// of a few rooms' passes together, few enough that each stays warm.
const READERS: usize = 4;

/// A file's connections. Writes run one at a time on the writer, in the
/// order they come; reads run on read-only connections beside it, each in
/// one read transaction, which sees every write committed before it began.
/// A call waits for its connection without holding a thread: at most one
/// blocking thread per connection works at once. An in-memory database has
/// one connection, which reads and writes, since another would open a
/// database of its own. Closed, it holds none.
#[derive(Debug, Clone)]
pub(crate) struct Database {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    path: PathBuf,
    writer: Mutex<Option<Connection>>,
    /// One permit: the writer's turn, waited for asynchronously.
    write_turn: Semaphore,
    /// The read-only connections not in use.
    idle_readers: Mutex<Vec<Connection>>,
    /// [`READERS`] permits: a reader's turn.
    read_turns: Semaphore,
    /// Whether reads go to the writer: an in-memory database.
    reads_on_writer: bool,
    closed: AtomicBool,
}

impl Database {
    /// Opens the file at `path`, creating it, and its directory, at the
    /// schema version this crate knows when it holds none, migrating one of
    /// version 12 or 13, and refusing one of another version. The writer opens it;
    /// a reader opens it later, at its first read.
    pub(crate) async fn open(path: PathBuf) -> Result<Self, StoreError> {
        let opened = path.clone();
        let connection = tokio::task::spawn_blocking(move || {
            if let Some(directory) = opened.parent() {
                std::fs::create_dir_all(directory).map_err(OpenFailure::Directory)?;
            }
            let mut connection = Connection::open(&opened).map_err(OpenFailure::Database)?;
            schema::prepare(&mut connection)?;
            Ok::<_, StoreError>(connection)
        })
        .await
        .map_err(OpenFailure::Stopped)??;
        Ok(Self {
            inner: Arc::new(Inner {
                reads_on_writer: in_memory(&path),
                path,
                writer: Mutex::new(Some(connection)),
                write_turn: Semaphore::new(1),
                idle_readers: Mutex::new(Vec::new()),
                read_turns: Semaphore::new(READERS),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Closes the connections, once the operations holding them are done:
    /// every later one fails, and so does one still waiting for its turn.
    pub(crate) async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.write_turn.close();
        self.inner.read_turns.close();
        let inner = Arc::clone(&self.inner);
        // A connection closed on a thread that stopped is closed all the
        // same, when its last handle drops.
        let _closed = tokio::task::spawn_blocking(move || {
            let writer = inner
                .writer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            let readers = std::mem::take(
                &mut *inner
                    .idle_readers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
            );
            drop((writer, readers));
        })
        .await;
    }

    /// Runs `work` on the writer, on a blocking thread, once it is its turn.
    async fn on_writer<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&mut Connection) -> Result<T, Failure> + Send + 'static,
    {
        let Ok(turn) = self.inner.write_turn.acquire().await else {
            return Err(StoreError::unavailable(CLOSED));
        };
        let inner = Arc::clone(&self.inner);
        let outcome = tokio::task::spawn_blocking(move || {
            // A panic while the connection was held rolled its transaction
            // back as it unwound, so the connection keeps serving.
            let mut held = inner.writer.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(connection) = held.as_mut() else {
                return Err(Failure::Refused(StoreError::unavailable(CLOSED)));
            };
            work(connection)
        })
        .await
        .map_err(StoreError::unavailable)?;
        drop(turn);
        outcome.map_err(StoreError::from)
    }

    /// Runs `work` on a read-only connection, on a blocking thread, once a
    /// reader is free; on the writer, for an in-memory database.
    async fn on_reader<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&mut Connection) -> Result<T, Failure> + Send + 'static,
    {
        if self.inner.reads_on_writer {
            return self.on_writer(work).await;
        }
        let Ok(turn) = self.inner.read_turns.acquire().await else {
            return Err(StoreError::unavailable(CLOSED));
        };
        let inner = Arc::clone(&self.inner);
        let outcome = tokio::task::spawn_blocking(move || {
            let idle = inner
                .idle_readers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop();
            let mut connection = match idle {
                Some(connection) => connection,
                None => open_reader(&inner.path)?,
            };
            let read = work(&mut connection);
            // A reader goes back to the idle ones unless the store closed
            // meanwhile; one a panic unwound is dropped, and another opens.
            if !inner.closed.load(Ordering::Acquire) {
                inner
                    .idle_readers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(connection);
            }
            read
        })
        .await
        .map_err(StoreError::unavailable)?;
        drop(turn);
        outcome.map_err(StoreError::from)
    }

    /// Runs `work` in one write transaction, which takes the file's write
    /// lock at once: all of it is written, or nothing.
    pub(crate) async fn write<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&Transaction<'_>) -> Result<T, Failure> + Send + 'static,
    {
        self.on_writer(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let written = work(&transaction)?;
            transaction.commit()?;
            Ok(written)
        })
        .await
    }

    /// Runs `work` in one read transaction, so what it reads is one state of
    /// the file: every write committed before it began.
    pub(crate) async fn read<T, W>(&self, work: W) -> Result<T, StoreError>
    where
        T: Send + 'static,
        W: FnOnce(&Transaction<'_>) -> Result<T, Failure> + Send + 'static,
    {
        self.on_reader(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let read = work(&transaction)?;
            transaction.commit()?;
            Ok(read)
        })
        .await
    }
}

/// A read-only connection to the file the writer opened and readied.
fn open_reader(path: &Path) -> Result<Connection, Failure> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    connection.busy_timeout(schema::BUSY_TIMEOUT)?;
    Ok(connection)
}

/// Whether `path` names an in-memory database, which each connection would
/// open apart.
fn in_memory(path: &Path) -> bool {
    path.to_str().is_some_and(|path| {
        path.is_empty() || path == ":memory:" || path.starts_with("file::memory:")
    })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    /// RMK-1033: a read runs beside a write in progress, on a connection of
    /// its own. Behind one connection, a read waited for every queued write:
    /// under load a call waited 87 ms for 0.58 ms of work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_does_not_wait_for_a_write_in_progress() {
        let directory = std::env::temp_dir().join(format!("rmk1033-{}", std::process::id()));
        let database = Database::open(directory.join("readers.db")).await.unwrap();
        database
            .write(|transaction| {
                transaction.execute_batch("CREATE TABLE IF NOT EXISTS probe(x INTEGER)")?;
                transaction.execute("INSERT INTO probe VALUES (1)", [])?;
                Ok(())
            })
            .await
            .unwrap();
        let (writing, wrote) = mpsc::channel::<()>();
        let (release, released) = mpsc::channel::<()>();
        let held = tokio::spawn({
            let database = database.clone();
            async move {
                database
                    .write(move |_transaction| {
                        writing.send(()).unwrap();
                        released.recv_timeout(Duration::from_secs(5)).unwrap();
                        Ok(())
                    })
                    .await
            }
        });
        tokio::task::spawn_blocking(move || wrote.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let read = tokio::time::timeout(
            Duration::from_secs(2),
            database.read(|transaction| {
                Ok(transaction
                    .query_row("SELECT COUNT(*) FROM probe", [], |row| row.get::<_, i64>(0))?)
            }),
        )
        .await;
        release.send(()).unwrap();
        held.await.unwrap().unwrap();
        database.close().await;
        let _removed = std::fs::remove_dir_all(&directory);
        assert_eq!(read.ok().map(Result::unwrap), Some(1));
    }
}
