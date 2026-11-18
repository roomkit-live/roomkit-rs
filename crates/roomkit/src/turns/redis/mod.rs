//! Redis Streams transport for text turns between processes (RFC §10.6).
mod after_turn;
pub(crate) use after_turn::PeerOwner;
mod changes;
pub(crate) use changes::{CoverageNotice, ServingBackends};
mod control;
mod dispatch;
mod jobs;
mod liveness;
mod outbox;
mod owner;
mod placement;
#[cfg(test)]
mod placement_tests;
mod process;
mod registry;
mod routes;
#[cfg(test)]
mod shutdown_tests;
mod stream;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::{ExecutorInfo, TurnTimeouts};
use crate::RoomKitError;
use crate::turns::executor::Lifecycle;

type Worker = Arc<dyn super::ServedExecutor>;
pub(crate) type WorkerLookup = Box<dyn Fn() -> Option<Worker> + Send + Sync>;

/// Keyword arguments of Python's `RedisTurnBackend(url, *, client, key_prefix, timeouts)`.
#[derive(Clone)]
pub struct RedisTurnBackendOptions {
    /// Supply the data connection client; the heartbeat still uses the given URL.
    pub client: Option<redis::Client>,
    /// Namespace shared by the room owners and their workers.
    pub key_prefix: String,
    /// The time bounds of this backend's turns.
    pub timeouts: TurnTimeouts,
}

impl Default for RedisTurnBackendOptions {
    fn default() -> Self {
        Self {
            client: None,
            key_prefix: "roomkit:turns".into(),
            timeouts: TurnTimeouts::default(),
        }
    }
}

impl fmt::Debug for RedisTurnBackendOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisTurnBackendOptions")
            .field("key_prefix", &self.key_prefix)
            .field("timeouts", &self.timeouts)
            .finish_non_exhaustive()
    }
}

/// One process's Redis turn backend. Clones share its node identity.
#[derive(Clone)]
pub struct RedisTurnBackend(Arc<Node>);

struct Node {
    outbox: Mutex<outbox::Outbox>,
    id: String,
    prefix: String,
    owner: Mutex<Option<owner::OwnerPort>>,
    own_executor: OnceLock<Weak<Lifecycle>>,
    owner_worker: OnceLock<placement::Reserved>,
    routes: Mutex<routes::Routes>,
    serving: Mutex<process::Serving>,
    reader: OnceCell<tokio::task::AbortHandle>,
    heartbeat: OnceCell<tokio::sync::watch::Receiver<()>>,
    draining: tokio::sync::watch::Sender<bool>,
    jobs_changed: tokio::sync::Notify,
    watchers: Mutex<Vec<changes::Watcher>>,
    stop: tokio::sync::watch::Sender<bool>,
    closing: tokio::sync::Mutex<bool>,
    timeouts: TurnTimeouts,
    client: redis::Client,
    choice: Mutex<usize>,
    address: redis::Client,
    data: OnceCell<ConnectionManager>,
}

impl fmt::Debug for RedisTurnBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisTurnBackend")
            .field("node_id", &self.0.id)
            .field("key_prefix", &self.0.prefix)
            .finish_non_exhaustive()
    }
}

impl RedisTurnBackend {
    /// Configure this process. The first backend operation opens its Redis connection.
    ///
    /// # Errors
    /// A supplied client requires a URL for the independent heartbeat connection.
    /// Invalid Redis URLs return a value error.
    pub fn new(url: Option<&str>, options: RedisTurnBackendOptions) -> Result<Self, RoomKitError> {
        if options.client.is_some() && url.is_none() {
            return Err(RoomKitError::Value(
                "RedisTurnBackend(client=...) needs url= too: its beat connects with it".into(),
            ));
        }
        let client = redis::Client::open(url.unwrap_or("redis://localhost:6379"))
            .map_err(|_| RoomKitError::Value("invalid Redis turn backend URL".into()))?;
        Ok(Self(Arc::new(Node {
            outbox: Mutex::default(),
            id: Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect(),
            prefix: options.key_prefix,
            owner: Mutex::default(),
            own_executor: OnceLock::new(),
            owner_worker: OnceLock::new(),
            routes: Mutex::default(),
            serving: Mutex::default(),
            watchers: Mutex::default(),
            reader: OnceCell::new(),
            heartbeat: OnceCell::new(),
            draining: tokio::sync::watch::channel(false).0,
            jobs_changed: tokio::sync::Notify::new(),
            stop: tokio::sync::watch::channel(false).0,
            closing: tokio::sync::Mutex::new(false),
            timeouts: options.timeouts,
            client: options.client.unwrap_or_else(|| client.clone()),
            choice: Mutex::new(0),
            address: client,
            data: OnceCell::new(),
        })))
    }

    /// This process's node identifier.
    pub fn node_id(&self) -> &str {
        &self.0.id
    }

    /// The time bounds given to the backend.
    pub fn timeouts(&self) -> TurnTimeouts {
        self.0.timeouts
    }

    /// Locally served executors and other nodes' declarations. Only remote entries have a last heartbeat.
    ///
    /// # Errors
    /// Returns `backend_closed` once shutdown starts, or an error if the registry is unreachable.
    pub async fn executors(&self) -> Result<Vec<ExecutorInfo>, RoomKitError> {
        self.ensure_open()?;
        registry::executors(self).await
    }

    fn ensure_open(&self) -> Result<(), RoomKitError> {
        if *self.0.draining.borrow() {
            return Err(routes::closed());
        }
        Ok(())
    }

    async fn connection(&self) -> Result<ConnectionManager, RoomKitError> {
        let node = &*self.0;
        let bound = (node.timeouts.heartbeat_timeout / 2).max(Duration::from_millis(1));
        node.data
            .get_or_try_init(|| async {
                let config = ConnectionManagerConfig::new()
                    .set_connection_timeout(Some(bound))
                    .set_response_timeout(Some(bound))
                    .set_number_of_retries(0);
                ConnectionManager::new_with_config(node.client.clone(), config)
                    .await
                    .map_err(|error| redis_error(&error))
            })
            .await
            .cloned()
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.0.prefix)
    }
}

fn redis_error(error: &redis::RedisError) -> RoomKitError {
    RoomKitError::Runtime(error.to_string())
}
