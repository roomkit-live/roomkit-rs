//! The name of the re-read of stored results, which the channel serves
//! itself (RFC §21.1, §21.5): what a scenario cannot say of a channel that
//! is never built. Its behaviour is in the conformance suite's `eviction/`
//! scenarios.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource,
    Room as RoomRecord, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiError, AiTool, HumanInputToolHandler,
    HumanInputToolHandlerOptions, ToolEntry,
};
use serde_json::json;

const REREAD: &str = "read_stored_result";

fn build(options: AiChannelOptions) -> Result<AiChannel<ScriptedProvider>, AiError> {
    AiChannel::new(
        ChannelId::new("ai1").unwrap(),
        ScriptedProvider::new([]),
        options,
    )
}

// Python: channels/_served_tools.py refuse_served_names (a host's tool) and
// tools/_human_input_channel.py refuse_collisions (a person's), each raising
// `ValueError` for a tool under a name the channel serves itself.
#[test]
fn a_tool_under_the_re_read_s_name_is_refused() {
    let host = AiChannelOptions {
        tools: vec![ToolEntry::Definition(AiTool::new(REREAD, "host").unwrap())],
        ..AiChannelOptions::default()
    };
    let human = HumanInputToolHandler::new(
        [REREAD],
        HumanInputToolHandlerOptions {
            tool_definitions: vec![AiTool::new(REREAD, "Ask the person.").unwrap()],
            ..HumanInputToolHandlerOptions::default()
        },
    )
    .unwrap();
    let person = AiChannelOptions {
        human_input_handler: Some(human),
        ..AiChannelOptions::default()
    };
    for options in [host, person] {
        assert!(matches!(
            build(options),
            Err(AiError::DuplicateTool { name }) if name == REREAD
        ));
    }
}

// Python: tests/test_policy_served_exemption.py::TestOneDeclarationPerName::test_a_dynamic_host_tool_under_a_served_name_is_not_declared
#[tokio::test]
async fn a_dynamic_host_tool_under_a_served_name_is_not_declared() {
    let model = ScriptedProvider::new([ScriptedRound::answering("Done.")]);
    let options = AiChannelOptions {
        tools: vec![ToolEntry::Definition(
            AiTool::new("search_docs", "Search").unwrap(),
        )],
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("ai1").unwrap(), model.clone(), options).unwrap();
    let room = RoomId::new("r1").unwrap();
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("member").unwrap(), ChannelType::WEBSOCKET),
        EventContent::Text(TextContent::new("Find it.")),
    );
    // A tool the binding brings with the turn, under the channel's own name.
    let mut binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(
            ChannelId::new("ai1").unwrap(),
            room.clone(),
            ChannelType::AI,
        )
    };
    binding.metadata.insert(
        "tools".to_owned(),
        json!([{"name": REREAD, "description": "host", "parameters": {}}]),
    );
    let context = RoomContext::new(RoomRecord::new(room));
    agent.on_event(event, binding, context).await.unwrap();

    // The channel declares its own from the first round (RFC §6.4).
    let declared: Vec<(String, String)> = model.requests()[0]
        .tools
        .iter()
        .map(|tool| (tool.name.clone(), tool.description.clone()))
        .collect();
    let reread = declared.iter().find(|(name, _)| name == REREAD).unwrap();
    assert_ne!(reread.1, "host");
    assert_eq!(declared.len(), 2);
}
