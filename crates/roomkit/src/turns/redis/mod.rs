//! Redis Streams transport for text turns between processes (RFC §10.6).
mod after_turn;
mod broker;
#[cfg(test)]
mod broker_tests;
mod changes;
mod control;
mod dispatch;
mod jobs;
mod liveness;
#[cfg(test)]
mod memory_broker;
mod outbox;
mod owner;
mod placement;
#[cfg(test)]
mod placement_tests;
mod process;
mod redis_broker;
mod registry;
mod routes;
#[cfg(test)]
mod shutdown_tests;
mod stream;
mod transport;
mod worker_link;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use redis::aio::ConnectionManager;
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::{ExecutorInfo, TurnTimeouts};
use crate::RoomKitError;

type Worker = Arc<dyn super::ServedExecutor>;
pub(crate) type WorkerLookup = Box<dyn Fn() -> Option<Worker> + Send + Sync>;

/// Keyword arguments of Python's `RedisTurnBackend(url, *, client, key_prefix, timeouts)`.
#[derive(Clone)]
pub struct RedisTurnBackendOptions {
    /// Reuse the application’s data connection manager, including its configured timeouts.
    /// The inbox and heartbeat open independent connections at the supplied URL.
    pub client: Option<ConnectionManager>,
    /// Test option matching Python: route this node's local turns through its Redis inbox.
    #[expect(
        clippy::pub_underscore_fields,
        reason = "the private Python test option has this exact name"
    )]
    pub _loop_back: bool,
    /// Namespace shared by the room owners and their workers.
    pub key_prefix: String,
    /// The time bounds of this backend's turns.
    pub timeouts: TurnTimeouts,
}

impl Default for RedisTurnBackendOptions {
    fn default() -> Self {
        Self {
            client: None,
            _loop_back: false,
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
    /// What carries the backend's messages between processes.
    broker: Arc<dyn broker::TurnBroker>,
    /// What the processes share: beats, holders, presence.
    registry: Arc<dyn broker::TurnRegistry>,
    /// The namespace of a node on Redis, which its tests read too.
    key_prefix: Option<String>,
    #[cfg(test)]
    redis: Option<Arc<redis_broker::RedisBroker>>,
    owner: Mutex<Option<Arc<dyn super::RoomOwner>>>,
    owner_worker: OnceLock<placement::Reserved>,
    routes: Mutex<routes::Routes>,
    serving: Mutex<process::Serving>,
    reader: OnceCell<tokio::task::AbortHandle>,
    heartbeat: OnceCell<tokio::sync::watch::Receiver<()>>,
    draining: tokio::sync::watch::Sender<bool>,
    jobs_changed: tokio::sync::Notify,
    watchers: crate::turns::ExecutorWatchers,
    /// What each executor served here runs once it closed.
    closed_listener: OnceLock<crate::turns::ExecutorsListener>,
    stop: tokio::sync::watch::Sender<bool>,
    closing: tokio::sync::Mutex<bool>,
    timeouts: TurnTimeouts,
    choice: Mutex<usize>,
    loop_back: bool,
}

impl fmt::Debug for RedisTurnBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut shown = f.debug_struct("RedisTurnBackend");
        shown.field("node_id", &self.0.id);
        if let Some(prefix) = &self.0.key_prefix {
            shown.field("key_prefix", prefix);
        }
        shown.finish_non_exhaustive()
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
        let address = url
            .filter(|value| !value.is_empty())
            .unwrap_or("redis://localhost:6379");
        let client = redis::Client::open(address)
            .map_err(|_| RoomKitError::Value("invalid Redis turn backend URL".into()))?;
        let RedisTurnBackendOptions {
            _loop_back: loop_back,
            ..
        } = options;
        let key_prefix = options.key_prefix.clone();
        let redis = Arc::new(redis_broker::RedisBroker::new(
            client,
            options.client,
            options.key_prefix,
            options.timeouts,
        ));
        let broker: Arc<dyn broker::TurnBroker> = Arc::<redis_broker::RedisBroker>::clone(&redis);
        let registry: Arc<dyn broker::TurnRegistry> =
            Arc::<redis_broker::RedisBroker>::clone(&redis);
        let mut node = Node::over(broker, registry, options.timeouts, loop_back);
        node.key_prefix = Some(key_prefix);
        #[cfg(test)]
        {
            node.redis = Some(redis);
        }
        Ok(Self(Arc::new(node)))
    }
}

impl Node {
    /// A node whose messages `broker` carries and whose registry
    /// `registry` keeps.
    fn over(
        broker: Arc<dyn broker::TurnBroker>,
        registry: Arc<dyn broker::TurnRegistry>,
        timeouts: TurnTimeouts,
        loop_back: bool,
    ) -> Self {
        Self {
            outbox: Mutex::default(),
            id: Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect(),
            broker,
            registry,
            key_prefix: None,
            #[cfg(test)]
            redis: None,
            owner: Mutex::default(),
            owner_worker: OnceLock::new(),
            routes: Mutex::default(),
            serving: Mutex::default(),
            watchers: crate::turns::ExecutorWatchers::new(),
            closed_listener: OnceLock::new(),
            reader: OnceCell::new(),
            heartbeat: OnceCell::new(),
            draining: tokio::sync::watch::channel(false).0,
            jobs_changed: tokio::sync::Notify::new(),
            stop: tokio::sync::watch::channel(false).0,
            closing: tokio::sync::Mutex::new(false),
            timeouts,
            choice: Mutex::new(0),
            loop_back,
        }
    }
}

impl RedisTurnBackend {
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

    /// What carries the backend's messages, reached by its port alone.
    fn broker(&self) -> &dyn broker::TurnBroker {
        &*self.0.broker
    }

    /// What the processes share, reached by its port alone.
    fn registry(&self) -> &dyn broker::TurnRegistry {
        &*self.0.registry
    }
}

/// What the tests of the Redis side read of it: its connections and keys.
#[cfg(test)]
impl RedisTurnBackend {
    /// A node of this process alone: its messages and its registry kept
    /// by `broker`, which the nodes of one test share.
    fn in_process(
        broker: &memory_broker::MemoryBroker,
        timeouts: TurnTimeouts,
        loop_back: bool,
    ) -> Self {
        Self(Arc::new(Node::over(
            Arc::new(broker.clone()),
            Arc::new(broker.clone()),
            timeouts,
            loop_back,
        )))
    }

    fn redis(&self) -> &redis_broker::RedisBroker {
        self.0.redis.as_deref().expect("a node on Redis")
    }

    async fn connection(&self) -> Result<ConnectionManager, RoomKitError> {
        self.redis().connection().await
    }

    fn key(&self, suffix: &str) -> String {
        self.redis().key(suffix)
    }

    fn ttl_ms(&self) -> u64 {
        self.redis().ttl_ms()
    }

    async fn inbox_connection(&self) -> Result<redis::aio::MultiplexedConnection, RoomKitError> {
        self.redis().inbox_connection(self.read_block()).await
    }

    async fn beat_connection(&self) -> Result<redis::aio::MultiplexedConnection, RoomKitError> {
        self.redis().beat_connection().await
    }
}

#[cfg(test)]
mod constructor_tests {
    use super::*;
    use redis::aio::ConnectionManagerConfig;

    #[tokio::test]
    async fn only_an_absent_url_is_refused_with_an_injected_client() {
        let default = redis::ConnectionAddr::Tcp("localhost".into(), 6379);
        let custom = redis::Client::open("redis://example.test:6380").unwrap();
        let injected = RedisTurnBackendOptions {
            client: Some(
                custom
                    .get_connection_manager_lazy(ConnectionManagerConfig::new())
                    .unwrap(),
            ),
            ..Default::default()
        };
        let refused = RedisTurnBackend::new(None, injected.clone()).unwrap_err();
        assert!(
            matches!(refused, RoomKitError::Value(message) if message == "RedisTurnBackend(client=...) needs url= too: its beat connects with it")
        );
        for url in [None, Some("")] {
            let backend = RedisTurnBackend::new(url, RedisTurnBackendOptions::default()).unwrap();
            assert_eq!(
                backend.redis().address.get_connection_info().addr(),
                &default
            );
            assert!(backend.redis().data.get().is_none());
        }
        let backend = RedisTurnBackend::new(Some(""), injected).unwrap();
        assert_eq!(
            backend.redis().address.get_connection_info().addr(),
            &default
        );
        assert!(backend.redis().data.get().is_some());
    }
}
