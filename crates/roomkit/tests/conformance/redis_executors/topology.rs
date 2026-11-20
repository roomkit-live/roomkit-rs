//! Placement cases use separate worker nodes. The ordered presence scenarios
//! follow Python's fixture: the same Redis backend serves the room owner's
//! workers and reports their start and close before the next scenario step.

use std::collections::BTreeMap;
use std::sync::Mutex;

use roomkit::{RedisTurnBackend, RedisTurnBackendOptions, RoomKitError, TurnExecutor};
use serde_json::Value;
use uuid::Uuid;

pub(super) fn is_presence(id: &str) -> bool {
    matches!(
        id,
        "executors/a-room-hook-no-executor-defines-takes-its-agent-offline-in-that-room"
            | "executors/an-agent-is-offline-in-a-room-of-an-organization-its-executors-do-not-serve"
            | "executors/an-agent-s-presence-is-published-in-its-rooms-on-attach-and-when-its-coverage-changes"
    )
}

pub(super) struct Topology {
    owner: RedisTurnBackend,
    pub(super) url: String,
    pub(super) options: RedisTurnBackendOptions,
    nodes: Mutex<BTreeMap<String, RedisTurnBackend>>,
    shared: bool,
}

impl Topology {
    pub(super) fn new(url: &str, scenario: &Value) -> Result<Self, RoomKitError> {
        let options = RedisTurnBackendOptions {
            timeouts: timeouts(scenario)?,
            key_prefix: format!("roomkit:test:scenario:{}", Uuid::new_v4()),
            ..RedisTurnBackendOptions::default()
        };
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
            nodes.insert(
                name.to_owned(),
                RedisTurnBackend::new(Some(url), options.clone())?,
            );
        }
        Ok(Self {
            owner: RedisTurnBackend::new(Some(url), options.clone())?,
            nodes: Mutex::new(nodes),
            url: url.to_owned(),
            options,
            shared: scenario["id"].as_str().is_some_and(is_presence),
        })
    }

    pub(super) fn owner(&self) -> &RedisTurnBackend {
        &self.owner
    }

    pub(super) fn node(&self, name: &str, restart: bool) -> Result<RedisTurnBackend, RoomKitError> {
        if name == "main" {
            return Ok(self.owner.clone());
        }
        let mut nodes = crate::run::lock(&self.nodes);
        let node = nodes
            .get_mut(name)
            .ok_or_else(|| RoomKitError::Value(format!("unknown executor process {name}")))?;
        if restart {
            *node = RedisTurnBackend::new(Some(&self.url), self.options.clone())?;
        }
        Ok(node.clone())
    }

    pub(super) fn worker(&self, name: Option<&str>) -> Result<RedisTurnBackend, RoomKitError> {
        if let Some(name) = name {
            return self.node(name, false);
        }
        if self.shared {
            Ok(self.owner.clone())
        } else {
            RedisTurnBackend::new(Some(&self.url), self.options.clone())
        }
    }

    pub(super) async fn close_worker(
        &self,
        hosting: &RedisTurnBackend,
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

fn timeouts(scenario: &Value) -> Result<roomkit::TurnTimeouts, RoomKitError> {
    let bounds: Option<Bounds> =
        serde_json::from_value(scenario["setup"]["kit"]["turn_timeouts"].clone())
            .map_err(|err| RoomKitError::Value(err.to_string()))?;
    let mut result = roomkit::TurnTimeouts::default();
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
