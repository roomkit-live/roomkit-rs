//! [`PostgresAdvisoryLockManager`]: the room lock across processes, on
//! Postgres's session advisory locks (RFC §8.1, §13.5).

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use blake2::digest::consts::U8;
use blake2::{Blake2b, Digest};
use deadpool_postgres::{GenericClient, Object, Pool};
use roomkit::model::{ErrorRecord, RoomId};
use roomkit::{RoomKitError, RoomLock, RoomLockManager, StoreError};
use tokio::sync::Notify;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::options::PostgresAdvisoryLockManagerOptions;
use crate::pool;
use crate::values::Param;

/// What a lock before [`PostgresAdvisoryLockManager::init`], or after the
/// close of a pool the manager owned, fails with, in Python's words.
const NOT_INITIALIZED: &str = "PostgresAdvisoryLockManager.init() must be called before use";

/// How long a close gives a held lock's connection: Python's `_close_timeout`.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

const TAKE: &str = "SELECT pg_advisory_lock($1)";
/// On a pool handed in, whose TLS the manager cannot cancel through, has the
/// server notice a client gone while it waits (Postgres 14 on; a server that
/// cannot is told nothing, and ends such a wait when its lock comes).
const CHECK_THE_CLIENT: &str = "SELECT set_config('client_connection_check_interval', '1000', \
     false) WHERE current_setting('server_version_num')::int >= 140000";
const GIVE_BACK: &str = "SELECT pg_advisory_unlock($1)";

/// A lock's connection, the manager's way to cut it at its close.
type Held = Mutex<Option<Object>>;

/// The signed 64-bit key of `room_id`'s lock: Python's `_advisory_key` to the
/// bit, `hashlib.blake2b(room_id, digest_size=8)` read as a big-endian integer,
/// so that processes of both implementations exclude each other. Distinct
/// rooms may fall on one key: they merely serialize together.
fn advisory_key(room_id: &str) -> i64 {
    i64::from_be_bytes(Blake2b::<U8>::digest(room_id.as_bytes()).into())
}

/// The lock's failure, as Python propagates the driver's exception: `error`'s
/// message, each of its causes' after it.
fn failure(error: impl Error) -> RoomKitError {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(current) = cause {
        message = format!("{message}: {current}");
        cause = current.source();
    }
    RoomKitError::Lock(ErrorRecord::new("PostgresError", message))
}

/// Takes `client` out of its pool and closes it: the server ends the session,
/// and releases whatever lock it holds. No statement or runtime is needed, so
/// a drop can do it.
fn close_connection(client: Object) {
    drop(Object::take(client));
}

/// A connection whose lock statement is on its way. Dropped before it was
/// granted (a section cancelled, the kit's `process_timeout`), the connection
/// never goes back to the pool, whose next user would wait behind the
/// statement: a cancel request ends the wait, and the connection closes, so
/// that a wait dropped leaves nothing taken.
struct Waiting {
    client: Option<Object>,
    tls: Option<MakeRustlsConnect>,
}

impl Waiting {
    /// The connection, once the statement is answered.
    fn answered(mut self) -> Option<Object> {
        self.client.take()
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            close_connection(client);
            return;
        };
        let cancel = client.cancel_token();
        let tls = self.tls.take();
        runtime.spawn(async move {
            // A cancel that fails leaves the session closing below: the server
            // ends it when its lock comes.
            let cancelled = match tls {
                Some(tls) => cancel.cancel_query(tls).await,
                None => cancel.cancel_query(NoTls).await,
            };
            drop(cancelled);
            close_connection(client);
        });
    }
}

/// A lock granted: its connection, held for the whole section, as a session's
/// advisory lock is bound to the connection that took it.
struct AdvisoryLock {
    held: Arc<Held>,
    key: i64,
}

impl RoomLock for AdvisoryLock {
    /// Gives the lock back, and the connection to its pool. A release that
    /// fails closes the connection, which releases the lock on the server: the
    /// failure is reported all the same (RFC §13.5). A manager's close that
    /// already cut the connection leaves nothing to do.
    fn release(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + 'static>> {
        Box::pin(async move {
            let taken = self
                .held
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            let Some(client) = taken else {
                return Ok(());
            };
            let releasing = Waiting {
                client: Some(client),
                tls: None,
            };
            let Some(client) = releasing.client.as_ref() else {
                return Ok(());
            };
            let statement = client.prepare_cached(GIVE_BACK).await.map_err(failure)?;
            client
                .execute(&statement, &[&self.key])
                .await
                .map_err(failure)?;
            drop(releasing.answered());
            Ok(())
        })
    }
}

impl Drop for AdvisoryLock {
    /// A lock dropped unreleased closes its connection, which releases it on
    /// the server: no statement to run, and no runtime needed for it. The kit
    /// releases every lock it took; this is for a holder that does not.
    fn drop(&mut self) {
        let taken = self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(client) = taken {
            close_connection(client);
        }
    }
}

/// Per-room locking across processes, on Postgres's session advisory locks:
/// Python's `PostgresAdvisoryLockManager`.
///
/// A session's advisory lock is bound to the connection that took it, held for
/// the whole section. The manager therefore MUST use a pool **separate** from
/// the store's: a held lock's connection would otherwise starve the query the
/// locked section itself needs, and deadlock. Size `max_size` for the rooms
/// processed at once. The kit grants a lock to the code that runs under it
/// (RFC §9.5.1), so the manager needs no reentrance of its own. Its clones
/// share the pool.
#[derive(Clone)]
pub struct PostgresAdvisoryLockManager {
    shared: Arc<Shared>,
}

/// A pool, and what a cancel request connects with, when the manager opened it.
type Connections = (Pool, Option<MakeRustlsConnect>);

struct Shared {
    options: PostgresAdvisoryLockManagerOptions,
    connections: RwLock<Option<Connections>>,
    owns_pool: bool,
    /// The connections of the locks granted, which a bounded close cuts.
    granted: Mutex<Vec<Weak<Held>>>,
    /// Told when a bounded close gives up: each wait ends, and is refused.
    closing: Notify,
}

impl fmt::Debug for PostgresAdvisoryLockManager {
    /// Writes no DSN: one holds a password.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAdvisoryLockManager")
            .field("owns_pool", &self.shared.owns_pool)
            .finish_non_exhaustive()
    }
}

impl PostgresAdvisoryLockManager {
    /// The manager of `options.dsn`, or of `options.pool`, which stays the
    /// integrator's: no I/O happens here. [`init`](Self::init) must run before
    /// a lock is taken.
    pub fn new(options: PostgresAdvisoryLockManagerOptions) -> Self {
        let owns_pool = options.pool.is_none();
        let connections = options.pool.clone().map(|pool| (pool, None));
        Self {
            shared: Arc::new(Shared {
                options,
                connections: RwLock::new(connections),
                owns_pool,
                granted: Mutex::new(Vec::new()),
                closing: Notify::new(),
            }),
        }
    }

    /// Creates the separate pool of the locks, if needed: Python's `init()`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for sizes or a connection string asyncpg
    /// refuses, [`RoomKitError::Lock`] when the server cannot be reached.
    pub async fn init(&self) -> Result<(), RoomKitError> {
        if self.connections().is_some() {
            return Ok(());
        }
        let options = &self.shared.options;
        let (pool, tls) = pool::open(options.dsn.as_deref(), options.min_size, options.max_size)
            .await
            .map_err(|error| {
                if let StoreError::Value(message) = &error {
                    RoomKitError::Value(message.clone())
                } else {
                    failure(error)
                }
            })?;
        *self
            .shared
            .connections
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some((pool, tls));
        Ok(())
    }

    fn connections(&self) -> Option<Connections> {
        self.shared
            .connections
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Takes `room_id`'s lock on a connection of the pool, waiting for its
    /// holder in any process.
    async fn take(&self, room_id: &RoomId) -> Result<Box<dyn RoomLock>, RoomKitError> {
        let Some((pool, tls)) = self.connections() else {
            return Err(RoomKitError::Runtime(NOT_INITIALIZED.to_owned()));
        };
        let key = advisory_key(room_id.as_str());
        let client = pool.get().await.map_err(failure)?;
        if !self.shared.owns_pool {
            // A failure is a server that cannot: the wait goes on without it.
            drop(client.execute(CHECK_THE_CLIENT, &[]).await);
        }
        let statement = client.prepare_cached(TAKE).await.map_err(failure)?;
        let waiting = Waiting {
            client: Some(client),
            tls,
        };
        let Some(client) = waiting.client.as_ref() else {
            return Err(RoomKitError::Runtime(NOT_INITIALIZED.to_owned()));
        };
        let params: [Param<'_>; 1] = [&key];
        tokio::select! {
            answered = client.execute(&statement, &params) => {
                answered.map_err(failure)?;
            }
            () = self.shared.closing.notified() => {
                return Err(RoomKitError::Runtime(NOT_INITIALIZED.to_owned()));
            }
        }
        // A close that came between the grant and here saw no wait to end.
        if pool.is_closed() {
            return Err(RoomKitError::Runtime(NOT_INITIALIZED.to_owned()));
        }
        let held = Arc::new(Mutex::new(waiting.answered()));
        let mut granted = self
            .shared
            .granted
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        granted.retain(|lock| lock.strong_count() > 0);
        granted.push(Arc::downgrade(&held));
        Ok(Box::new(AdvisoryLock { held, key }))
    }

    /// Closes the connections of the locks still held, which releases them on
    /// the server, and ends the waits for one.
    fn cut_what_is_held(&self) {
        self.shared.closing.notify_waiters();
        let granted = std::mem::take(
            &mut *self
                .shared
                .granted
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for lock in granted.iter().filter_map(Weak::upgrade) {
            let taken = lock.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(client) = taken {
                close_connection(client);
            }
        }
    }
}

impl RoomLockManager for PostgresAdvisoryLockManager {
    fn locked<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>> {
        Box::pin(self.take(room_id))
    }

    /// Closes the pool if the manager opened it, as Python's `close()` when the
    /// kit's close runs it (RFC §12.10.4): the connections still in use get a
    /// second to come back, then a lock still held has its connection closed,
    /// which releases it, and a wait for one is refused, so a close never waits
    /// on work that holds one past its cut. A pool handed in stays its owner's.
    /// A second close does nothing.
    fn close(&self) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + '_>> {
        Box::pin(async move {
            if !self.shared.owns_pool {
                return Ok(());
            }
            let connections = self
                .shared
                .connections
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some((pool, _)) = connections
                && !pool::close_own_pool(&pool, CLOSE_TIMEOUT, "PostgresAdvisoryLockManager").await
            {
                self.cut_what_is_held();
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python: `tests/test_postgres_lock.py::test_advisory_key_is_stable_signed_int64`:
    /// each key is the one `hashlib.blake2b(room_id, digest_size=8)` gives, read
    /// as a signed big-endian integer, so that both implementations take one lock.
    #[test]
    fn an_advisory_key_is_python_s_to_the_bit() {
        for (room_id, python) in [
            ("r1", -8_944_841_499_210_040_016),
            ("room-\u{e9}", 6_645_099_454_872_090_092),
            ("\0__delivery__:r1", -8_080_782_317_417_378_807),
            ("\0verification:org-a:alice", -6_607_731_101_731_361_839),
            ("", -1_970_711_489_451_281_740),
        ] {
            assert_eq!(advisory_key(room_id), python, "{room_id:?}");
        }
    }
}
