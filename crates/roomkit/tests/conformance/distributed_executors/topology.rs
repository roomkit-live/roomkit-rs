//! Placement cases use separate worker nodes. The ordered presence scenarios
//! follow Python's fixture: the same Redis backend serves the room owner's
//! workers and reports their start and close before the next scenario step.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use roomkit::turns::{
    DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker,
    InMemoryTurnRegistry, TurnBroker, TurnRegistry,
};
use roomkit::{RoomKitError, TurnExecutor, TurnTimeouts};
use serde_json::Value;
use uuid::Uuid;

use crate::scenarios::Node;

pub(super) fn is_presence(id: &str) -> bool {
    matches!(
        id,
        "executors/a-room-hook-no-executor-defines-takes-its-agent-offline-in-that-room"
            | "executors/an-agent-is-offline-in-a-room-of-an-organization-its-executors-do-not-serve"
            | "executors/an-agent-s-presence-is-published-in-its-rooms-on-attach-and-when-its-coverage-changes"
    )
}

/// What the scenario's nodes run on: the Redis server `REDIS_URL` names
/// when the runner has its Redis assembly, the broker and the registry of
/// this process otherwise.
enum Service {
    Redis {
        node: crate::scenarios::RedisNode,
        url: String,
        key_prefix: String,
    },
    InProcess {
        broker: Arc<InMemoryTurnBroker>,
        registry: Arc<InMemoryTurnRegistry>,
    },
}

impl Service {
    fn of_this_run() -> Self {
        match (crate::REDIS_NODE, crate::scenarios::redis_url()) {
            (Some(node), Some(url)) => Self::Redis {
                node,
                url,
                key_prefix: format!("roomkit:test:scenario:{}", Uuid::new_v4()),
            },
            _ => Self::InProcess {
                broker: Arc::new(InMemoryTurnBroker::new()),
                registry: Arc::new(InMemoryTurnRegistry::new()),
            },
        }
    }

    /// A new node of the scenario, with the services it runs on.
    fn node(&self, timeouts: TurnTimeouts) -> Result<Node, RoomKitError> {
        match self {
            Self::Redis {
                node,
                url,
                key_prefix,
            } => node(url, key_prefix, timeouts),
            Self::InProcess { broker, registry } => {
                let broker: Arc<dyn TurnBroker> = Arc::<InMemoryTurnBroker>::clone(broker);
                let registry: Arc<dyn TurnRegistry> = Arc::<InMemoryTurnRegistry>::clone(registry);
                let options = DistributedTurnBackendOptions {
                    timeouts,
                    ..DistributedTurnBackendOptions::default()
                };
                Ok(Node {
                    backend: DistributedTurnBackend::new(
                        Arc::clone(&broker),
                        Arc::clone(&registry),
                        options,
                    ),
                    broker,
                    registry,
                })
            }
        }
    }
}

pub(super) struct Topology {
    owner: DistributedTurnBackend,
    service: Service,
    pub(super) timeouts: TurnTimeouts,
    nodes: Mutex<BTreeMap<String, DistributedTurnBackend>>,
    shared: bool,
}

impl Topology {
    pub(super) fn new(scenario: &Value) -> Result<Self, RoomKitError> {
        let timeouts = timeouts(scenario)?;
        let service = Service::of_this_run();
        let mut nodes = BTreeMap::new();
        for process in scenario["setup"]["processes"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let name = process["id"]
                .as_str()
                .ok_or_else(|| RoomKitError::Value("process id is missing".into()))?;
            if name == "main" {
                return Err(RoomKitError::Value(
                    "main names the run's own process".into(),
                ));
            }
            nodes.insert(name.to_owned(), service.node(timeouts)?.backend);
        }
        Ok(Self {
            owner: service.node(timeouts)?.backend,
            nodes: Mutex::new(nodes),
            service,
            timeouts,
            shared: scenario["id"].as_str().is_some_and(is_presence),
        })
    }

    pub(super) fn owner(&self) -> &DistributedTurnBackend {
        &self.owner
    }

    /// A node of the scenario no process of its setup names, with the
    /// services it runs on: a worker's own, or a peer's the runner writes.
    pub(super) fn fresh(&self) -> Result<Node, RoomKitError> {
        self.service.node(self.timeouts)
    }

    pub(super) fn node(
        &self,
        name: &str,
        restart: bool,
    ) -> Result<DistributedTurnBackend, RoomKitError> {
        if name == "main" {
            return Ok(self.owner.clone());
        }
        let mut nodes = crate::run::lock(&self.nodes);
        let node = nodes
            .get_mut(name)
            .ok_or_else(|| RoomKitError::Value(format!("unknown executor process {name}")))?;
        if restart {
            *node = self.fresh()?.backend;
        }
        Ok(node.clone())
    }

    pub(super) fn worker(
        &self,
        name: Option<&str>,
    ) -> Result<DistributedTurnBackend, RoomKitError> {
        if let Some(name) = name {
            return self.node(name, false);
        }
        if self.shared {
            Ok(self.owner.clone())
        } else {
            Ok(self.fresh()?.backend)
        }
    }

    pub(super) async fn close_worker(
        &self,
        hosting: &DistributedTurnBackend,
        executor: &TurnExecutor,
    ) -> Result<(), RoomKitError> {
        executor.close().await;
        let owned = hosting.node_id() == self.owner.node_id()
            || crate::run::lock(&self.nodes)
                .values()
                .any(|node| node.node_id() == hosting.node_id());
        if owned { Ok(()) } else { hosting.close().await }
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_field_names,
    reason = "the scenario schema names the unit on every timeout"
)]
struct Bounds {
    heartbeat_interval_ms: Option<u64>,
    heartbeat_timeout_ms: Option<u64>,
    accept_ms: Option<u64>,
    cancel_ms: Option<u64>,
}

fn timeouts(scenario: &Value) -> Result<TurnTimeouts, RoomKitError> {
    let bounds: Option<Bounds> =
        serde_json::from_value(scenario["setup"]["kit"]["turn_timeouts"].clone())
            .map_err(|err| RoomKitError::Value(err.to_string()))?;
    let mut result = TurnTimeouts::default();
    if let Some(bounds) = bounds {
        for (duration, ms) in [
            (&mut result.heartbeat_interval, bounds.heartbeat_interval_ms),
            (&mut result.heartbeat_timeout, bounds.heartbeat_timeout_ms),
            (&mut result.accept, bounds.accept_ms),
            (&mut result.cancel, bounds.cancel_ms),
        ] {
            if let Some(ms) = ms {
                *duration = std::time::Duration::from_millis(ms);
            }
        }
    }
    Ok(result)
}
