//! Public executor declarations and agent presence (RFC §10.6.7).

use std::collections::BTreeSet;

use roomkit_core::model::Timestamp;
use serde::{Deserialize, Serialize};

use crate::RoomKitError;
use crate::error::repr;

/// The optional fields of Python's `ExecutorInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorInfoOptions {
    /// Python `node=None`: the process the backend names, when it names one.
    pub node: Option<String>,
    /// Python `hosts=None`: exact channel ids and trailing-star prefixes.
    pub hosts: Option<BTreeSet<String>>,
    /// Python `organizations=None`: only rooms without an organization;
    /// `"*"` serves every organization.
    pub organizations: Option<BTreeSet<String>>,
    /// Python `room_hooks=frozenset()`: the room hooks it defines.
    pub room_hooks: BTreeSet<String>,
    /// Python `last_beat=None`: its last heartbeat, when the backend reads one.
    pub last_beat: Option<Timestamp>,
    /// Python `online=True`: whether it takes turns now.
    pub online: bool,
}

impl Default for ExecutorInfoOptions {
    fn default() -> Self {
        Self {
            node: None,
            hosts: None,
            organizations: None,
            room_hooks: BTreeSet::new(),
            last_beat: None,
            online: true,
        }
    }
}

/// An executor as its backend sees it, Python's `ExecutorInfo`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorInfo {
    /// The id the executor answers with.
    pub executor_id: String,
    /// The process it runs in, when its backend names one.
    pub node: Option<String>,
    /// Declared channel ids and trailing-star prefixes. `None` declares no
    /// channels in this data value; a local predicate is not serialized.
    pub hosts: Option<BTreeSet<String>>,
    /// The organizations it serves; `None` serves rooms without one alone.
    pub organizations: Option<BTreeSet<String>>,
    /// The named room hooks it defines.
    #[serde(default)]
    pub room_hooks: BTreeSet<String>,
    /// Its last heartbeat, when its backend reads one.
    pub last_beat: Option<Timestamp>,
    /// Whether it currently takes turns.
    #[serde(default = "online")]
    pub online: bool,
}

fn online() -> bool {
    true
}

impl ExecutorInfo {
    /// Builds the information with Python's fields and optional defaults.
    pub fn new(executor_id: impl Into<String>, options: ExecutorInfoOptions) -> Self {
        Self {
            executor_id: executor_id.into(),
            node: options.node,
            hosts: options.hosts,
            organizations: options.organizations,
            room_hooks: options.room_hooks,
            last_beat: options.last_beat,
            online: options.online,
        }
    }

    /// Whether this online executor covers the channel and the room's
    /// required hooks and organization. An empty hook slice and `None`
    /// organization are Python's defaults.
    ///
    /// # Errors
    /// [`RoomKitError::Value`] for a `*` anywhere but the end of a host
    /// entry, only after online, hooks and organization permit coverage.
    pub fn covers(
        &self,
        channel_id: &str,
        room_hooks: &[String],
        organization_id: Option<&str>,
    ) -> Result<bool, RoomKitError> {
        if !self.online || !room_hooks.iter().all(|name| self.room_hooks.contains(name)) {
            return Ok(false);
        }
        let serves = self
            .organizations
            .as_ref()
            .map_or(organization_id.is_none(), |names| {
                names.contains("*") || organization_id.is_some_and(|name| names.contains(name))
            });
        if !serves {
            return Ok(false);
        }
        let Some(hosts) = &self.hosts else {
            return Ok(false);
        };
        for entry in hosts {
            if entry.strip_suffix('*').unwrap_or(entry).contains('*') {
                return Err(RoomKitError::Value(format!(
                    "hosts entry {}: a '*' ends an entry to declare a prefix ('org-*'), and stands nowhere else",
                    repr(entry)
                )));
            }
        }
        Ok(hosts.iter().any(|entry| {
            entry == channel_id
                || entry
                    .strip_suffix('*')
                    .is_some_and(|prefix| channel_id.starts_with(prefix))
        }))
    }
}

/// Whether one of a room's agents can answer there now, Python's
/// `AgentPresence`. Each value owns its list of covering executor ids.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPresence {
    /// The agent's channel id.
    pub channel_id: String,
    /// Whether an online executor covers it in this room.
    pub online: bool,
    /// The ids of the executors that cover it.
    pub executors: Vec<String>,
}

impl AgentPresence {
    /// Builds the presence from Python's three required fields.
    pub fn new(channel_id: impl Into<String>, online: bool, executors: Vec<String>) -> Self {
        Self {
            channel_id: channel_id.into(),
            online,
            executors,
        }
    }
}
