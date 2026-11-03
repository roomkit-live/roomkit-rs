//! Tool Search's names (RFC §21.1): what a scenario cannot say of a channel
//! that is never built. Its behaviour is in the conformance suite's
//! `tool-search/` scenarios.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use roomkit::model::ChannelId;
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiError, AiTool, HumanInputToolHandler,
    HumanInputToolHandlerOptions, TOOL_SEARCH_INFRA_TOOL_NAMES, ToolEntry,
};

fn build(options: AiChannelOptions) -> Result<AiChannel<ScriptedProvider>, AiError> {
    AiChannel::new(
        ChannelId::new("ai1").unwrap(),
        ScriptedProvider::new([]),
        options,
    )
}

fn host_tool(name: &str) -> Vec<ToolEntry> {
    vec![ToolEntry::Definition(
        AiTool::new(name, "host tool").unwrap(),
    )]
}

// Python: tests/test_policy_served_exemption.py::TestOneDeclarationPerName::test_turning_tool_search_off_frees_its_names
#[test]
fn turning_tool_search_off_frees_its_names() {
    for name in TOOL_SEARCH_INFRA_TOOL_NAMES {
        let off = AiChannelOptions {
            tools: host_tool(name),
            tool_search: Some(false),
            ..AiChannelOptions::default()
        };
        assert!(build(off).is_ok(), "{name}");
    }
}

// Python: tests/test_policy_served_exemption.py::TestOneDeclarationPerName, refuse_served_names
#[test]
fn a_host_tool_under_a_discovery_name_is_refused_unless_tool_search_is_off() {
    for (name, tool_search) in [("find_tools", None), ("list_tools", Some(true))] {
        let options = AiChannelOptions {
            tools: host_tool(name),
            tool_search,
            ..AiChannelOptions::default()
        };
        let Err(refused) = build(options) else {
            panic!("{name} was taken");
        };
        assert_eq!(
            refused.to_string(),
            format!(
                "tool \"{name}\" is a tool the channel serves itself: rename it or set \
                 tool_search to Some(false) (RFC §21.1)"
            )
        );
    }
}

// Python: tools/_human_input_channel.py's refuse_collisions()
#[test]
fn a_person_s_tool_under_a_discovery_name_is_refused() {
    let human = HumanInputToolHandler::new(
        ["list_tools"],
        HumanInputToolHandlerOptions {
            tool_definitions: vec![AiTool::new("list_tools", "Ask").unwrap()],
            ..HumanInputToolHandlerOptions::default()
        },
    )
    .unwrap();
    let options = AiChannelOptions {
        human_input_handler: Some(human),
        ..AiChannelOptions::default()
    };
    assert!(matches!(
        build(options),
        Err(AiError::ToolSearchName { name }) if name == "list_tools"
    ));
}
