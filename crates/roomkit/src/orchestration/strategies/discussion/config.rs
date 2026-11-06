//! What a discussion stores of itself (RFC §19.7.5 rule 16): the
//! configuration a [`Discussion`] gives, in the shape
//! `roomkit_core::rules::discussion::config` reads.

pub(crate) use roomkit_core::rules::discussion::config::DiscussionConfig;

use super::strategy::Discussion;

/// What `strategy` configures, `default_depth` for its depth limit unless
/// it gives one (the kit's `max_chain_depth`).
pub(crate) fn config_of(strategy: &Discussion, default_depth: u32) -> DiscussionConfig {
    DiscussionConfig {
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
        dispatch: strategy.dispatch.is_some(),
        attached: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use std::collections::BTreeMap;

    use roomkit_core::model::{ChannelId, Metadata};
    use roomkit_core::rules::discussion::CONFIG_KEY;

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
        let stored = config_of(&pair(Some("Hand over with {handle}.")), 6);

        let read =
            DiscussionConfig::stored(&metadata(&serde_json::to_value(&stored).unwrap())).unwrap();

        assert_eq!(read.rules.as_deref(), Some("Hand over with {handle}."));
    }

    // Python: tests/orchestration/test_discussion_rules.py::test_a_discussion_stored_before_rules_reads_with_the_default
    #[test]
    fn a_discussion_stored_before_rules_reads_with_the_default() {
        let mut stored = serde_json::to_value(config_of(&pair(None), 6)).unwrap();
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
        let config = config_of(&strategy, 6);
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
        let config: DiscussionConfig = serde_json::from_value(json!({
            "agents": ["a"], "handles": {"b": "B"}, "max_depth": 5
        }))
        .unwrap();
        assert_eq!(config.handle(&id("b")), "b");
        assert_eq!(config.by_handle(), [("a".to_owned(), "a".to_owned())]);

        let shared: DiscussionConfig = serde_json::from_value(json!({
            "agents": ["a", "b"], "handles": {"a": "x", "b": "x"}, "max_depth": 5
        }))
        .unwrap();
        assert_eq!(shared.by_handle(), [("x".to_owned(), "b".to_owned())]);
    }

    // Python: _config.py of, `strategy.max_depth or default_depth`.
    #[test]
    fn a_discussion_s_depth_limit_is_its_own_else_the_kit_s() {
        assert_eq!(config_of(&pair(None), 6).max_depth, 6);
        let own = Discussion::new(
            vec![StrategyAgent::id(id("jev"))],
            DiscussionOptions {
                max_depth: Some(2),
                ..DiscussionOptions::default()
            },
        )
        .unwrap();
        assert_eq!(config_of(&own, 6).max_depth, 2);
    }

    #[test]
    fn what_does_not_read_as_a_configuration_is_none() {
        assert_eq!(DiscussionConfig::stored(&Metadata::new()), None);
        for stored in [json!(null), json!({}), json!("x"), json!({"agents": ["a"]})] {
            assert_eq!(DiscussionConfig::stored(&metadata(&stored)), None);
        }
    }

    // Python: tests/orchestration/test_discussion_handles.py::test_what_the_constructor_refuses_stored_reads_as_no_discussion
    #[test]
    fn test_what_the_constructor_refuses_stored_reads_as_no_discussion() {
        for stored in [
            json!({"agents": [], "max_depth": 5}),
            json!({"agents": ["a", "a"], "max_depth": 5}),
            json!({"agents": ["a"], "max_depth": 0}),
            json!({"agents": ["a"], "max_depth": 5, "max_turns": 0}),
            json!({"agents": ["a"], "max_depth": 5, "silent_token": "  "}),
            json!({"agents": ["a"], "max_depth": 5, "rules": "  "}),
            json!({"agents": ["a"], "max_depth": 5, "silent_token": "\u{1c}"}),
            json!({"agents": ["a"], "max_depth": 5, "rules": "\u{1f}"}),
            json!({"agents": ["a"], "max_depth": 5, "everyone": ["ghost"]}),
            json!({"agents": ["a"], "max_depth": 5, "handles": {"ghost": "ops"}}),
            json!({"agents": ["a", "b"], "max_depth": 5, "handles": {"a": "ops", "b": "OPS"}}),
            // An agent stored with no handle is addressed by its channel id.
            json!({"agents": ["a", "b"], "max_depth": 5, "handles": {"a": "b"}}),
            json!({"agents": ["a"], "max_depth": 5, "handles": {"a": "two\nlines"}}),
            json!({"agents": ["a"], "max_depth": 5, "handles": {"a": "all"}}),
        ] {
            assert_eq!(
                DiscussionConfig::stored(&metadata(&stored)),
                None,
                "{stored}"
            );
        }
    }

    // Python: tests/orchestration/test_discussion_handles.py::test_a_depth_of_1_the_kit_writes_for_a_discussion_is_read_back
    #[test]
    fn test_a_depth_of_1_the_kit_writes_for_a_discussion_is_read_back() {
        let written = config_of(&pair(None), 1);
        let stored = serde_json::to_value(&written).unwrap();
        assert_eq!(DiscussionConfig::stored(&metadata(&stored)), Some(written));
    }

    #[test]
    fn the_same_discussion_is_the_same_whatever_its_installs_attached() {
        let config = config_of(&pair(None), 6);
        let mut attached = config.clone();
        attached.attached.push(id("jev"));
        assert!(config.same_as(Some(&attached)));
        assert!(!config.same_as(None));
        let mut other = config.clone();
        other.max_depth = 7;
        assert!(!config.same_as(Some(&other)));
    }
}
