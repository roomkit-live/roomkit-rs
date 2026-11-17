//! Python's public host-tool accessor and its approved shared ownership form.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use roomkit::model::ChannelId;
use roomkit_ai::{
    Agent, AgentOptions, AiChannel, AiChannelOptions, AiTool, MockAiProvider, ToolEntry,
};
use serde_json::json;

mod extra_tools {
    mod human;
    mod snapshots;
    pub(crate) mod support;
    mod turns;
}
use extra_tools::support::{Door, tool};

// Python: tests/test_channels/test_ai_extra_tools.py::test_no_host_tools_gives_an_empty_list
#[test]
fn no_host_tools_gives_an_empty_list() {
    let channel = AiChannel::new(
        ChannelId::new("ai").unwrap(),
        MockAiProvider::new(roomkit_ai::MockAiProviderOptions::default()),
        AiChannelOptions::default(),
    )
    .unwrap();
    assert!(channel.extra_tools().is_empty());
}

// Python: tests/test_channels/test_ai_extra_tools.py::test_no_host_tools_gives_an_empty_list
#[test]
fn an_agent_exposes_the_same_host_tools() {
    let agent = Agent::new(
        ChannelId::new("ai").unwrap(),
        MockAiProvider::new(roomkit_ai::MockAiProviderOptions::default()),
        AgentOptions::default(),
    )
    .unwrap();
    assert!(agent.extra_tools().is_empty());
}

// Python: tests/test_channels/test_ai_extra_tools.py::test_host_definitions_keep_constructor_order_and_fields
#[test]
fn host_definitions_keep_constructor_order_and_fields() {
    for agent in [false, true] {
        let typed = AiTool {
            tags: vec!["one".into()],
            changes_state: true,
            defer_loading: true,
            ..AiTool::new("typed", "typed tool").unwrap()
        };
        let options = AiChannelOptions {
            tools: vec![
                tool("local"),
                typed.clone().into(),
                ToolEntry::Schema(
                    serde_json::from_value(
                        json!({"name":"raw","description":"raw tool","tags":["two"]}),
                    )
                    .unwrap(),
                ),
            ],
            ..Default::default()
        };
        let door = Door::new(
            agent,
            MockAiProvider::new(roomkit_ai::MockAiProviderOptions::default()),
            options,
        );
        let definitions: Vec<_> = door
            .tools()
            .iter()
            .map(roomkit_ai::AiToolHandle::get)
            .collect();
        assert_eq!(
            definitions,
            vec![
                AiTool {
                    parameters: json!({"type":"object","properties":{}}),
                    ..AiTool::new("local", "Fake local").unwrap()
                },
                typed,
                AiTool {
                    tags: vec!["two".into()],
                    ..AiTool::new("raw", "raw tool").unwrap()
                }
            ]
        );
    }
}

// Python: tests/test_channels/test_ai_extra_tools.py::test_the_input_and_returned_list_containers_do_not_change_host_tools
#[test]
fn returned_containers_and_clones_share_only_the_definition() {
    let original = AiTool::new("read", "original").unwrap();
    let mut given = vec![original.clone().into()];
    let channel = AiChannel::new(
        ChannelId::new("ai").unwrap(),
        MockAiProvider::new(roomkit_ai::MockAiProviderOptions::default()),
        AiChannelOptions {
            tools: given.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    given.clear();
    let clone = channel.clone();
    let mut first = channel.extra_tools();
    let retained = first[0].clone();
    first.clear();
    let mut second = clone.extra_tools();
    second.push(retained.clone());
    assert_eq!(channel.extra_tools().len(), 1);
    let mut snapshot = retained.get();
    snapshot.description = "updated".into();
    assert_eq!(clone.extra_tools()[0].get(), original);
    retained.replace(snapshot.clone());
    assert_eq!(clone.extra_tools()[0].get(), snapshot);
    assert_eq!(channel.extra_tools()[0].get(), snapshot);
}
