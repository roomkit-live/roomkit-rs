//! Redis Streams transport for text turns between processes (RFC §10.6).
mod control;
mod dispatch;
mod owner;
mod process;
mod registry;
mod routes;
mod stream;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::{ExecutorInfo, TurnTimeouts};
use crate::RoomKitError;

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
    node_id: String,
    prefix: String,
    owner: std::sync::Mutex<Option<owner::OwnerPort>>,
    routes: std::sync::Mutex<routes::Routes>,
    serving: std::sync::Mutex<process::Serving>,
    reader: OnceCell<()>,
    heartbeat: OnceCell<()>,
    quitting: Arc<std::sync::atomic::AtomicBool>,
    stop: tokio::sync::watch::Sender<bool>,
    closing: tokio::sync::Mutex<()>,
    timeouts: TurnTimeouts,
    client: redis::Client,
    choice: std::sync::atomic::AtomicUsize,
    address: redis::Client,
    data: OnceCell<ConnectionManager>,
}

impl fmt::Debug for RedisTurnBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisTurnBackend")
            .field("node_id", &self.0.node_id)
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
            node_id: Uuid::new_v4().simple().to_string()[..12].into(),
            prefix: options.key_prefix,
            owner: Default::default(),
            routes: Default::default(),
            serving: Default::default(),
            reader: OnceCell::new(),
            heartbeat: OnceCell::new(),
            quitting: Arc::default(),
            stop: tokio::sync::watch::channel(false).0,
            closing: tokio::sync::Mutex::new(()),
            timeouts: options.timeouts,
            client: options.client.unwrap_or_else(|| client.clone()),
            choice: std::sync::atomic::AtomicUsize::new(0),
            address: client,
            data: OnceCell::new(),
        })))
    }

    /// This process's node identifier.
    pub fn node_id(&self) -> &str {
        &self.0.node_id
    }

    /// The time bounds given to the backend.
    pub fn timeouts(&self) -> TurnTimeouts {
        self.0.timeouts
    }

    /// The executors still declared in Redis, including the time of their last heartbeat.
    ///
    /// # Errors
    /// The registry must be reachable and answer within its configured timeout.
    pub async fn executors(&self) -> Result<Vec<ExecutorInfo>, RoomKitError> {
        registry::executors(self).await
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
                    .map_err(redis_error)
            })
            .await
            .cloned()
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.0.prefix)
    }
}

fn redis_error(error: redis::RedisError) -> RoomKitError {
    RoomKitError::Runtime(error.to_string())
}
