//! Tool Search's names (RFC §21.1): what a scenario cannot say of a channel
//! that is never built. Its behaviour is in the conformance suite's
//! `tool-search/` scenarios.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource, EventType,
    Room as RoomRecord, RoomContext, RoomEvent, RoomId, TextContent, ToolCallContent,
};
use roomkit::tool_intent::INTENT_PROPERTY;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiError, AiTool, HumanInputToolHandler,
    HumanInputToolHandlerOptions, TOOL_SEARCH_INFRA_TOOL_NAMES, ToolEntry,
};
use serde_json::json;

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
                "Tool '{name}' is a tool channel 'ai1' serves itself: rename it or set \
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
        Err(AiError::ToolSearchName { name, .. }) if name == "list_tools"
    ));
}

// Python: providers/ai/tool_intent.py's presented_tools(), which reads every tool
/// RFC §6.4: the discovery tools take a call's intent as any tool does, required
/// where the agent requires one, and the search reads its own arguments.
#[tokio::test]
async fn a_discovery_tool_takes_a_call_s_intent_as_any_tool() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling(
            "",
            [(
                "c1",
                "find_tools",
                r#"{"query": "send sms", "_roomkit_intent": "Looking for the SMS tool"}"#,
            )],
        ),
        ScriptedRound::answering("Done."),
    ]);
    let options = AiChannelOptions {
        tools: vec![ToolEntry::Definition(
            AiTool::new("send_sms", "Send an SMS.").unwrap(),
        )],
        tool_search: Some(true),
        tool_call_intents: true,
        tool_call_intents_required: true,
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model.clone(), options).unwrap();
    let room = RoomId::new("r1").unwrap();
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("member").unwrap(), ChannelType::WEBSOCKET),
        EventContent::Text(TextContent::new("Send it.")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(
            ChannelId::new("ai1").unwrap(),
            room.clone(),
            ChannelType::AI,
        )
    };
    let context = RoomContext::new(RoomRecord::new(room));
    agent.on_event(event, binding, context).await.unwrap();

    let requests = model.requests();
    let find = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == "find_tools")
        .unwrap();
    assert!(find.parameters["properties"].get(INTENT_PROPERTY).is_some());
    assert!(
        find.parameters["required"]
            .as_array()
            .unwrap()
            .contains(&json!(INTENT_PROPERTY))
    );
    assert!(requests[1].tools.iter().any(|tool| tool.name == "send_sms"));
}

// Python: channels/_ai_context.py's _hydrate_room_memories(), which returns with no
// _tool_usage_loader: only a kit gives one
/// A channel driven outside a kit reads no stored row, whatever its context
/// holds: a tool it called in an earlier process stays hidden.
#[tokio::test]
async fn a_channel_driven_outside_a_kit_reads_no_stored_row() {
    let model = ScriptedProvider::new([ScriptedRound::answering("Done.")]);
    let options = AiChannelOptions {
        tools: host_tool("send_sms"),
        tool_search: Some(true),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model.clone(), options).unwrap();
    let room = RoomId::new("r1").unwrap();
    let mut end = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("ai1").unwrap(), ChannelType::AI),
        EventContent::ToolCall(ToolCallContent::new("send_sms", "t1")),
    );
    end.r#type = EventType::ToolCallEnd;
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("member").unwrap(), ChannelType::WEBSOCKET),
        EventContent::Text(TextContent::new("Send it.")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(
            ChannelId::new("ai1").unwrap(),
            room.clone(),
            ChannelType::AI,
        )
    };
    let context = RoomContext {
        recent_events: vec![end],
        ..RoomContext::new(RoomRecord::new(room))
    };
    agent.on_event(event, binding, context).await.unwrap();

    let names: Vec<String> = model.requests()[0]
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    assert_eq!(names, TOOL_SEARCH_INFRA_TOOL_NAMES);
}
