//! A discussion's configuration, stored with the room (RFC §19.7.5 rule 16):
//! Python's `_config.py`. Every process that serves the room routes and
//! queues by it, whether or not its host installed the discussion there;
//! `done` is the host's and stays in the processes that installed it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::CONFIG_KEY;
use crate::model::{ChannelId, Metadata};

/// What a process needs to follow a room's discussion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscussionConfig {
    /// The agents, in the order the discussion gave them.
    pub agents: Vec<ChannelId>,
    /// The name each agent is addressed by, by channel id. `None` (a
    /// configuration stored without handles): each agent's channel id.
    #[serde(default)]
    pub handles: Option<BTreeMap<ChannelId, String>>,
    /// The people agents may address, when the host lists them.
    #[serde(default)]
    pub people: Option<Vec<String>>,
    /// A message that addresses nobody asks nobody.
    #[serde(default)]
    pub addressed_only: bool,
    /// Who an unaddressed message asks, and in which order.
    #[serde(default)]
    pub everyone: Option<Vec<ChannelId>>,
    /// The turns it gives before it is over.
    #[serde(default)]
    pub max_turns: Option<u32>,
    /// Its own chain-depth limit.
    pub max_depth: u32,
    /// What an agent answers when it has nothing to add.
    #[serde(default = "default_silent_token")]
    pub silent_token: String,
    /// How the room works, as each turn's notes give it (rule 14). `None`:
    /// the default, as for a configuration stored before rules existed.
    #[serde(default)]
    pub rules: Option<String>,
    /// A dispatch policy decides who takes a person's unaddressed message: it
    /// waits for the process holding the lease (rule 18).
    #[serde(default)]
    pub dispatch: bool,
    /// The agents installs of the discussion attached to the room, which any
    /// process uninstalling it detaches (RFC §19.7).
    #[serde(default)]
    pub attached: Vec<ChannelId>,
}

fn default_silent_token() -> String {
    "(silent)".to_owned()
}

impl DiscussionConfig {
    /// The configuration stored in a room's `metadata`, if one is: one that
    /// does not read as a configuration is none.
    #[must_use]
    pub fn stored(metadata: &Metadata) -> Option<Self> {
        serde_json::from_value(metadata.get(CONFIG_KEY)?.clone()).ok()
    }

    /// Whether `other` configures the same discussion, whatever agents its
    /// installs attached.
    #[must_use]
    pub fn same_as(&self, other: Option<&Self>) -> bool {
        other.is_some_and(|other| {
            Self {
                attached: Vec::new(),
                ..self.clone()
            } == Self {
                attached: Vec::new(),
                ..other.clone()
            }
        })
    }

    /// The name `agent` is addressed by: its handle, else its channel id (a
    /// stored turn of an agent the discussion no longer holds).
    #[must_use]
    pub fn handle<'a>(&'a self, agent: &'a ChannelId) -> &'a str {
        self.handles
            .as_ref()
            .filter(|_| self.agents.contains(agent))
            .and_then(|handles| handles.get(agent))
            .map_or_else(|| agent.as_str(), String::as_str)
    }

    /// An agent as a model reads it named: `@` and its handle.
    #[must_use]
    pub fn label(&self, agent: &ChannelId) -> String {
        format!("@{}", self.handle(agent))
    }

    /// Each agent's handle and channel id, in the agents' order, as
    /// `read_names` takes them (rule 4): one agent under a handle, the last
    /// a configuration names.
    #[must_use]
    pub fn by_handle(&self) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = Vec::new();
        for agent in &self.agents {
            let handle = self.handle(agent);
            if let Some((_, held)) = pairs.iter_mut().find(|(known, _)| known == handle) {
                *held = agent.to_string();
            } else {
                pairs.push((handle.to_owned(), agent.to_string()));
            }
        }
        pairs
    }
}
