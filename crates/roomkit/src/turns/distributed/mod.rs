//! A turn backend across processes (RFC §10.6): its messages carried by a
//! broker, what its processes share kept by a registry.
mod after_turn;
mod broker;
#[cfg(test)]
mod broker_tests;
mod changes;
mod control;
mod dispatch;
mod jobs;
mod liveness;
mod memory;
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
#[cfg(test)]
mod test_support;
mod transport;
mod worker_link;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::OnceCell;
use uuid::Uuid;

use super::{ExecutorInfo, TurnTimeouts};
use crate::RoomKitError;

pub use broker::{
    Beat, BeatWriter, Beats, BrokerMessage, InboxEntry, RegistryView, TurnBroker, TurnInbox,
    TurnRegistry,
};
pub use memory::{InMemoryTurnBroker, InMemoryTurnRegistry};

type Worker = Arc<dyn super::ServedExecutor>;
pub(crate) type WorkerLookup = Box<dyn Fn() -> Option<Worker> + Send + Sync>;

/// What [`DistributedTurnBackend::new`] takes beside its broker and its
/// registry.
#[derive(Clone, Debug, Default)]
pub struct DistributedTurnBackendOptions {
    /// The time bounds of this backend's turns.
    pub timeouts: TurnTimeouts,
    /// Test option: route this node's local turns through its inbox.
    #[expect(
        clippy::pub_underscore_fields,
        reason = "the private Python test option has this exact name"
    )]
    pub _loop_back: bool,
}

/// One process's node of a turn backend across processes. Clones share
/// its node identity.
#[derive(Clone)]
pub struct DistributedTurnBackend(Arc<Node>);

struct Node {
    outbox: Mutex<outbox::Outbox>,
    id: String,
    /// What carries the backend's messages between processes.
    broker: Arc<dyn TurnBroker>,
    /// What the processes share: beats, holders, presence.
    registry: Arc<dyn TurnRegistry>,
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

impl fmt::Debug for DistributedTurnBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedTurnBackend")
            .field("node_id", &self.0.id)
            .finish_non_exhaustive()
    }
}

impl DistributedTurnBackend {
    /// A node whose messages `broker` carries and whose registry
    /// `registry` keeps: the nodes given the same ones reach each other.
    /// Its first operation reaches them.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "its options are taken whole, as each constructor's are"
    )]
    pub fn new(
        broker: Arc<dyn TurnBroker>,
        registry: Arc<dyn TurnRegistry>,
        options: DistributedTurnBackendOptions,
    ) -> Self {
        let DistributedTurnBackendOptions {
            timeouts,
            _loop_back: loop_back,
        } = options;
        Self(Arc::new(Node {
            outbox: Mutex::default(),
            id: Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect(),
            broker,
            registry,
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
        }))
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

    /// What carries the backend's messages, reached by its port alone.
    fn broker(&self) -> &dyn TurnBroker {
        &*self.0.broker
    }

    /// What the processes share, reached by its port alone.
    fn registry(&self) -> &dyn TurnRegistry {
        &*self.0.registry
    }
}
