//! The backend on Redis: its messages and its registry kept by one Redis
//! server (Python's `RedisTurnBackend`).
use std::fmt;
#[cfg(test)]
use std::ops::Deref;
use std::sync::Arc;

use redis::aio::ConnectionManager;

use super::redis_broker::RedisBroker;
use super::{DistributedTurnBackend, DistributedTurnBackendOptions, TurnBroker, TurnRegistry};
use crate::RoomKitError;
use crate::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorsListener, RoomOwner, ServedExecutor,
    TurnBackend, TurnFuture, TurnLink, TurnRequest, TurnTimeouts,
};

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
pub struct RedisTurnBackend {
    node: DistributedTurnBackend,
    /// The Redis side, which its tests read too.
    redis: Arc<RedisBroker>,
}

impl fmt::Debug for RedisTurnBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisTurnBackend")
            .field("node_id", &self.node.node_id())
            .field("key_prefix", &self.redis.prefix)
            .finish_non_exhaustive()
    }
}

/// The node itself, for the crate's own tests of the backend on Redis.
#[cfg(test)]
impl Deref for RedisTurnBackend {
    type Target = DistributedTurnBackend;

    fn deref(&self) -> &DistributedTurnBackend {
        &self.node
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
            client: data,
            _loop_back: loop_back,
            key_prefix,
            timeouts,
        } = options;
        let redis = Arc::new(RedisBroker::new(client, data, key_prefix, timeouts));
        let node = DistributedTurnBackend::new(
            Arc::<RedisBroker>::clone(&redis),
            Arc::<RedisBroker>::clone(&redis),
            DistributedTurnBackendOptions {
                timeouts,
                _loop_back: loop_back,
            },
        );
        Ok(Self { node, redis })
    }

    /// The node of the distributed backend this one assembles on Redis.
    #[doc(hidden)]
    pub fn node(&self) -> &DistributedTurnBackend {
        &self.node
    }

    /// The broker and the registry this backend runs on, both Redis.
    #[doc(hidden)]
    pub fn services(&self) -> (Arc<dyn TurnBroker>, Arc<dyn TurnRegistry>) {
        (
            Arc::<RedisBroker>::clone(&self.redis),
            Arc::<RedisBroker>::clone(&self.redis),
        )
    }

    /// This process's node identifier.
    pub fn node_id(&self) -> &str {
        self.node.node_id()
    }

    /// The time bounds given to the backend.
    pub fn timeouts(&self) -> TurnTimeouts {
        self.node.timeouts()
    }

    /// Locally served executors and other nodes' declarations. Only remote entries have a last heartbeat.
    ///
    /// # Errors
    /// Returns `backend_closed` once shutdown starts, or an error if the registry is unreachable.
    pub async fn executors(&self) -> Result<Vec<ExecutorInfo>, RoomKitError> {
        self.node.executors().await
    }

    /// Advertise and serve this executor. A later registration can replace a closed executor.
    ///
    /// # Errors
    /// A registration on a closed backend fails, as does publishing its first Redis heartbeat.
    pub async fn serve<S: crate::ConversationStore>(
        &self,
        executor: crate::TurnExecutor<S>,
    ) -> Result<(), RoomKitError> {
        self.node.serve(executor).await
    }

    /// Record a room's agent presence (§10.6.7). Exactly one owner claims each change.
    /// Passing `None` forgets the state and returns `false`, so the next attach publishes again.
    ///
    /// # Errors
    /// The backend is closed, or the Redis registry cannot be reached.
    pub async fn claim_presence(
        &self,
        channel_id: &str,
        room_id: &str,
        online: Option<bool>,
    ) -> Result<bool, RoomKitError> {
        self.node.claim_presence(channel_id, room_id, online).await
    }

    /// Withdraw this node and close the links it owns.
    ///
    /// # Errors
    /// Redis may be unavailable when withdrawing. Expired heartbeats are forgotten by readers.
    pub async fn close(&self) -> Result<(), RoomKitError> {
        self.node.close().await
    }
}

impl TurnBackend for RedisTurnBackend {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        TurnBackend::submit(&self.node, request)
    }

    fn submit_to(
        &self,
        request: TurnRequest,
        executor: ExecutorInfo,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        TurnBackend::submit_to(&self.node, request, executor)
    }

    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::serve(&self.node, executor)
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        TurnBackend::control(&self.node, message)
    }

    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::attend(&self.node, owner)
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        TurnBackend::claim_presence(&self.node, channel_id, room_id, online)
    }

    fn watch_executors(&self, listener: ExecutorsListener) {
        TurnBackend::watch_executors(&self.node, listener);
    }

    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        TurnBackend::executors(&self.node)
    }

    fn executors_now(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        TurnBackend::executors_now(&self.node)
    }

    fn timeouts(&self) -> Option<TurnTimeouts> {
        TurnBackend::timeouts(&self.node)
    }

    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::close(&self.node)
    }
}

/// What the tests of the Redis side read of it: its connections and keys.
#[cfg(test)]
impl RedisTurnBackend {
    pub(super) fn redis(&self) -> &RedisBroker {
        &self.redis
    }

    pub(super) async fn connection(&self) -> Result<ConnectionManager, RoomKitError> {
        self.redis.connection().await
    }

    pub(super) fn key(&self, suffix: &str) -> String {
        self.redis.key(suffix)
    }

    pub(super) fn ttl_ms(&self) -> u64 {
        self.redis.ttl_ms()
    }

    pub(super) async fn inbox_connection(
        &self,
    ) -> Result<redis::aio::MultiplexedConnection, RoomKitError> {
        self.redis.inbox_connection(self.read_block()).await
    }

    pub(super) async fn beat_connection(
        &self,
    ) -> Result<redis::aio::MultiplexedConnection, RoomKitError> {
        self.redis.beat_connection().await
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
