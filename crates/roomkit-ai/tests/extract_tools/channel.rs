use std::sync::{Arc, Mutex};

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource, Room,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, ToolHandler, ToolResult};
use serde_json::json;

use crate::{schema, tool};

// Python: tests/tools/test_extract_tools_channel.py::test_raw_schemas_and_tool_objects_reach_their_own_handlers
#[tokio::test]
async fn raw_schemas_and_tool_objects_reach_their_own_handlers() {
    let asked: Arc<Mutex<Vec<String>>> = Arc::default();
    let saw = Arc::clone(&asked);
    let handler = ToolHandler::new(move |name, arguments| {
        saw.lock().unwrap().push(name.clone());
        async move {
            let result = if name == "lookup" {
                arguments["q"].as_str().unwrap().to_owned()
            } else {
                "{\"error\": \"Unknown tool: echo\"}".to_owned()
            };
            Ok(ToolResult::Text(result))
        }
    });
    let provider = ScriptedProvider::new([
        ScriptedRound::calling(
            "",
            [
                ("c1", "lookup", "{\"q\":\"raw result\"}"),
                ("c2", "echo", "{}"),
            ],
        ),
        ScriptedRound::answering("done"),
    ]);
    let channel = AiChannel::new(ChannelId::new("ai").unwrap(), provider.clone(), AiChannelOptions {
        tool_handler: Some(handler),
        tools: vec![schema(json!({"name":"lookup", "description":"Raw schema", "tags":["raw"], "changes_state":true, "parameters":{"type":"object"}})), tool("echo", "object result")],
        ..AiChannelOptions::default()
    }).unwrap();
    let room = RoomId::new("r1").unwrap();
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("sms1").unwrap(), ChannelType::SMS),
        EventContent::Text(TextContent::new("go")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(ChannelId::new("ai").unwrap(), room.clone(), ChannelType::AI)
    };
    channel
        .on_event(event, binding, RoomContext::new(Room::new(room)))
        .await
        .unwrap();
    channel.close().await;

    assert_eq!(*asked.lock().unwrap(), ["lookup", "echo"]);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]
            .tools
            .iter()
            .map(|d| (
                d.name.as_str(),
                d.tags.iter().map(String::as_str).collect::<Vec<_>>(),
                d.changes_state
            ))
            .collect::<Vec<_>>(),
        [
            ("lookup", vec!["raw"], true),
            ("echo", vec![], false),
            ("read_stored_result", vec![], false)
        ]
    );
    let results = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.tool_results())
        .map(|part| (part.tool_call_id.as_str(), part.as_text(), part.is_error))
        .collect::<Vec<_>>();
    assert_eq!(
        results,
        [
            ("c1", "raw result".into(), false),
            ("c2", "object result".into(), false)
        ]
    );
}
