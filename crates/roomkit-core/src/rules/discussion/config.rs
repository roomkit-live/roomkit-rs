//! A discussion's configuration, stored with the room (RFC §19.7.5 rule 16):
//! Python's `_config.py`. Every process that serves the room routes and
//! queues by it, whether or not its host installed the discussion there;
//! `done` is the host's and stays in the processes that installed it.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::CONFIG_KEY;
use super::names::{is_blank, is_handle};
use crate::model::{ChannelId, Metadata};
use crate::rules::unicode::casefold;

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
    /// The configuration stored in a room's `metadata`, if one is. One that
    /// does not read as a configuration, or that the constructor would
    /// refuse, reads as none, with a warning, never an error: this runs for
    /// every context the kit builds, and an error would break every message
    /// of the room.
    #[must_use]
    pub fn stored(metadata: &Metadata) -> Option<Self> {
        let (config, refusal) = Self::read(metadata);
        if let Some(refusal) = refusal {
            tracing::warn!(
                target: "roomkit::orchestration::discussion",
                "a stored discussion reads as none: {refusal}"
            );
        }
        config
    }

    /// [`stored`](Self::stored) with no warning: what a reader of its handles
    /// asks.
    #[must_use]
    pub(crate) fn valid(metadata: &Metadata) -> Option<Self> {
        Self::read(metadata).0
    }

    /// The configuration `metadata` stores, or what keeps it from being one.
    fn read(metadata: &Metadata) -> (Option<Self>, Option<&'static str>) {
        let Some(raw) = metadata.get(CONFIG_KEY).filter(|raw| !raw.is_null()) else {
            return (None, None);
        };
        match serde_json::from_value::<Self>(raw.clone()) {
            Ok(config) => match config.refusal() {
                None => (Some(config), None),
                refusal => (None, refusal),
            },
            // Its error would quote what the store holds.
            Err(_) => (None, Some("its fields do not read")),
        }
    }

    /// What `Discussion::new` would refuse in this configuration, if anything.
    fn refusal(&self) -> Option<&'static str> {
        let distinct: BTreeSet<&ChannelId> = self.agents.iter().collect();
        if self.agents.is_empty() {
            return Some("it holds no agent");
        }
        if distinct.len() != self.agents.len() {
            return Some("its agents are not distinct");
        }
        if self.max_turns.is_some_and(|turns| turns < 1) {
            return Some("max_turns is below 1");
        }
        // 1 is not refused: the kit writes its max_chain_depth when the
        // strategy gives none, and a kit may hold 1.
        if self.max_depth < 1 {
            return Some("max_depth is below 1");
        }
        if is_blank(&self.silent_token) {
            return Some("silent_token is blank");
        }
        if self.rules.as_deref().is_some_and(is_blank) {
            return Some("rules is blank");
        }
        if self
            .everyone
            .iter()
            .flatten()
            .any(|agent| !self.agents.contains(agent))
        {
            return Some("everyone names a channel that is not an agent");
        }
        self.handles_refusal()
    }

    fn handles_refusal(&self) -> Option<&'static str> {
        let given = self.handles.as_ref();
        if given
            .into_iter()
            .flatten()
            .any(|(agent, _)| !self.agents.contains(agent))
        {
            return Some("handles names a channel that is not an agent");
        }
        if !given
            .into_iter()
            .flatten()
            .all(|(_, handle)| is_handle(handle))
        {
            return Some("a handle cannot name an agent");
        }
        // An agent with no handle stored is addressed by its channel id.
        let addressed: BTreeSet<String> = self
            .agents
            .iter()
            .map(|agent| casefold(self.handle(agent)))
            .collect();
        (addressed.len() != self.agents.len()).then_some("two agents are addressed alike")
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
