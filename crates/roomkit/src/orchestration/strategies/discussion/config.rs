//! A discussion's configuration, stored with the room (RFC §19.7.5 rule 16):
//! Python's `_config.py`. Every process that serves the room routes and
//! queues by it, whether or not its host installed the discussion there;
//! `done` is the host's and stays in the processes that installed it.

use std::collections::BTreeMap;

use roomkit_core::model::{ChannelId, Metadata};
use roomkit_core::rules::discussion::CONFIG_KEY;
use serde::{Deserialize, Serialize};

use super::strategy::Discussion;

/// What a process needs to follow a room's discussion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiscussionConfig {
    pub(crate) agents: Vec<ChannelId>,
    /// The name each agent is addressed by, by channel id. `None` (a
    /// configuration stored without handles): each agent's channel id.
    #[serde(default)]
    pub(crate) handles: Option<BTreeMap<ChannelId, String>>,
    #[serde(default)]
    pub(crate) people: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) addressed_only: bool,
    #[serde(default)]
    pub(crate) everyone: Option<Vec<ChannelId>>,
    #[serde(default)]
    pub(crate) max_turns: Option<u32>,
    pub(crate) max_depth: u32,
    #[serde(default = "default_silent_token")]
    pub(crate) silent_token: String,
    /// How the room works, as each turn's notes give it (rule 14). `None`:
    /// the default, as for a configuration stored before rules existed.
    #[serde(default)]
    pub(crate) rules: Option<String>,
    /// A dispatch policy decides who takes a person's unaddressed message: it
    /// waits for the process holding the lease (rule 18).
    #[serde(default)]
    pub(crate) dispatch: bool,
    /// The agents installs of the discussion attached to the room, which any
    /// process uninstalling it detaches (RFC §19.7).
    #[serde(default)]
    pub(crate) attached: Vec<ChannelId>,
}

fn default_silent_token() -> String {
    "(silent)".to_owned()
}

impl DiscussionConfig {
    /// What `strategy` configures, `default_depth` for its depth limit unless
    /// it gives one (the kit's `max_chain_depth`).
    pub(crate) fn of(strategy: &Discussion, default_depth: u32) -> Self {
        Self {
            agents: strategy
                .agents()
                .iter()
                .map(|agent| agent.channel_id().clone())
                .collect(),
            handles: Some(strategy.handles()),
            people: strategy.people.clone(),
            addressed_only: strategy.addressed_only,
            everyone: strategy.everyone.clone(),
            max_turns: strategy.max_turns,
            max_depth: strategy.max_depth.unwrap_or(default_depth),
            silent_token: strategy.silent_token.clone(),
            rules: strategy.rules.clone(),
            dispatch: false,
            attached: Vec::new(),
        }
    }

    /// The configuration stored in a room's `metadata`, if one is: one that
    /// does not read as a configuration is none.
    pub(crate) fn stored(metadata: &Metadata) -> Option<Self> {
        serde_json::from_value(metadata.get(CONFIG_KEY)?.clone()).ok()
    }

    /// Whether `other` configures the same discussion, whatever agents its
    /// installs attached.
    pub(crate) fn same_as(&self, other: Option<&Self>) -> bool {
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
    pub(crate) fn handle<'a>(&'a self, agent: &'a ChannelId) -> &'a str {
        self.handles
            .as_ref()
            .filter(|_| self.agents.contains(agent))
            .and_then(|handles| handles.get(agent))
            .map_or_else(|| agent.as_str(), String::as_str)
    }

    /// An agent as a model reads it named: `@` and its handle.
    pub(crate) fn label(&self, agent: &ChannelId) -> String {
        format!("@{}", self.handle(agent))
    }

    /// Each agent's handle and channel id, in the agents' order, as
    /// `read_names` takes them (rule 4): one agent under a handle, the last
    /// a configuration names.
    pub(crate) fn by_handle(&self) -> Vec<(String, String)> {
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::orchestration::DiscussionOptions;
    use crate::orchestration::StrategyAgent;

    fn id(agent: &str) -> ChannelId {
        ChannelId::new(agent).unwrap()
    }

    fn pair(rules: Option<&str>) -> Discussion {
        Discussion::new(
            vec![
                StrategyAgent::id(id("jev")),
                StrategyAgent::id(id("counsel")),
            ],
            DiscussionOptions {
                everyone: Some(vec![id("jev")]),
                rules: rules.map(ToOwned::to_owned),
                ..DiscussionOptions::default()
            },
        )
        .unwrap()
    }

    fn metadata(value: &serde_json::Value) -> Metadata {
        serde_json::from_value(json!({ CONFIG_KEY: value })).unwrap()
    }

    // Python: tests/orchestration/test_discussion_rules.py::test_the_rules_are_stored_with_the_discussion
    #[test]
    fn the_rules_are_stored_with_the_discussion() {
        let stored = DiscussionConfig::of(&pair(Some("Hand over with {handle}.")), 6);

        let read =
            DiscussionConfig::stored(&metadata(&serde_json::to_value(&stored).unwrap())).unwrap();

        assert_eq!(read.rules.as_deref(), Some("Hand over with {handle}."));
    }

    // Python: tests/orchestration/test_discussion_rules.py::test_a_discussion_stored_before_rules_reads_with_the_default
    #[test]
    fn a_discussion_stored_before_rules_reads_with_the_default() {
        let mut stored = serde_json::to_value(DiscussionConfig::of(&pair(None), 6)).unwrap();
        stored.as_object_mut().unwrap().shift_remove("rules");

        let read = DiscussionConfig::stored(&metadata(&stored)).unwrap();

        assert_eq!(read.rules, None);
    }

    // Python: tests/orchestration/test_discussion_handles.py::test_a_configuration_stored_without_handles_addresses_agents_by_channel_id
    #[test]
    fn a_configuration_stored_without_handles_addresses_agents_by_channel_id() {
        let config =
            DiscussionConfig::stored(&metadata(&json!({"agents": ["a", "b"], "max_depth": 5})))
                .unwrap();
        assert_eq!(config.handles, None);
        assert_eq!(
            config.by_handle(),
            [
                ("a".to_owned(), "a".to_owned()),
                ("b".to_owned(), "b".to_owned())
            ]
        );
        assert_eq!(config.label(&id("b")), "@b");
    }

    /// What Python's `DiscussionConfig.of(...).model_dump(mode="json")` writes
    /// for the same discussion: both implementations read what either stored.
    #[test]
    fn the_configuration_is_stored_as_python_stores_it() {
        let strategy = Discussion::new(
            vec![StrategyAgent::id(id("a")), StrategyAgent::id(id("b"))],
            DiscussionOptions {
                handles: Some(BTreeMap::from([(id("b"), "ops".to_owned())])),
                people: Some(vec!["Ana".to_owned()]),
                addressed_only: true,
                everyone: Some(vec![id("b")]),
                max_turns: Some(3),
                max_depth: Some(9),
                silent_token: "PASS".to_owned(),
                ..DiscussionOptions::default()
            },
        )
        .unwrap();
        let python = json!({
            "agents": ["a", "b"],
            "handles": {"a": "a", "b": "ops"},
            "people": ["Ana"],
            "addressed_only": true,
            "everyone": ["b"],
            "max_turns": 3,
            "max_depth": 9,
            "silent_token": "PASS",
            "rules": null,
            "dispatch": false,
            "attached": []
        });
        let config = DiscussionConfig::of(&strategy, 6);
        assert_eq!(serde_json::to_value(&config).unwrap(), python);
        assert_eq!(
            DiscussionConfig::stored(&metadata(&python)).as_ref(),
            Some(&config)
        );
    }

    // Python: _room.py _configure, the handles of the agents held, by_handle a
    // dict keeping one agent under a handle.
    #[test]
    fn a_stored_handle_names_only_an_agent_the_discussion_holds() {
        let config = DiscussionConfig::stored(&metadata(
            &json!({"agents": ["a"], "handles": {"b": "B"}, "max_depth": 5}),
        ))
        .unwrap();
        assert_eq!(config.handle(&id("b")), "b");
        assert_eq!(config.by_handle(), [("a".to_owned(), "a".to_owned())]);

        let shared = DiscussionConfig::stored(&metadata(
            &json!({"agents": ["a", "b"], "handles": {"a": "x", "b": "x"}, "max_depth": 5}),
        ))
        .unwrap();
        assert_eq!(shared.by_handle(), [("x".to_owned(), "b".to_owned())]);
    }

    // Python: _config.py of, `strategy.max_depth or default_depth`.
    #[test]
    fn a_discussion_s_depth_limit_is_its_own_else_the_kit_s() {
        assert_eq!(DiscussionConfig::of(&pair(None), 6).max_depth, 6);
        let own = Discussion::new(
            vec![StrategyAgent::id(id("jev"))],
            DiscussionOptions {
                max_depth: Some(2),
                ..DiscussionOptions::default()
            },
        )
        .unwrap();
        assert_eq!(DiscussionConfig::of(&own, 6).max_depth, 2);
    }

    #[test]
    fn what_does_not_read_as_a_configuration_is_none() {
        assert_eq!(DiscussionConfig::stored(&Metadata::new()), None);
        for stored in [json!(null), json!({}), json!("x"), json!({"agents": ["a"]})] {
            assert_eq!(DiscussionConfig::stored(&metadata(&stored)), None);
        }
    }

    #[test]
    fn the_same_discussion_is_the_same_whatever_its_installs_attached() {
        let config = DiscussionConfig::of(&pair(None), 6);
        let mut attached = config.clone();
        attached.attached.push(id("jev"));
        assert!(config.same_as(Some(&attached)));
        assert!(!config.same_as(None));
        let mut other = config.clone();
        other.max_depth = 7;
        assert!(!config.same_as(Some(&other)));
    }
}
