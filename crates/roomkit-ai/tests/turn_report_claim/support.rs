use std::sync::Arc;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelType, EventSource, Room, RoomContext, RoomEvent, TextContent,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiTool, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ToolHandler,
};
use serde_json::json;

pub(super) type Claim = Arc<dyn Fn() -> bool + Send + Sync>;

pub(super) fn call(query: &str) -> AiResponse {
    let mut response = AiResponse::new("");
    let mut call = AiToolCall::new("c1", "lookup");
    call.arguments.insert("q".into(), json!(query));
    response.tool_calls.push(call);
    response
}

pub(super) fn done() -> AiResponse {
    AiResponse::new("done")
}

pub(super) fn channel(
    streaming: bool,
    handler: ToolHandler,
    responses: Vec<AiResponse>,
) -> AiChannel<MockAiProvider> {
    let provider = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(responses),
        ..MockAiProviderOptions::default()
    });
    AiChannel::new(
        "ai".parse().unwrap(),
        provider,
        AiChannelOptions {
            tools: vec![AiTool::new("lookup", "Look up").unwrap().into()],
            tool_handler: Some(handler),
            tool_search: Some(false),
            ..AiChannelOptions::default()
        },
    )
    .unwrap()
}

pub(super) async fn respond(channel: &AiChannel<MockAiProvider>, room: &str) {
    let room = Room::new(room.parse().unwrap());
    let event = RoomEvent::new(
        room.id.clone(),
        EventSource::new("sms".parse().unwrap(), ChannelType::SMS),
        TextContent::new("go"),
    );
    let binding = ChannelBinding::new("ai".parse().unwrap(), room.id.clone(), ChannelType::AI);
    let output = channel
        .on_event(event, binding, RoomContext::new(room))
        .await
        .unwrap();
    assert!(output.responded);
    assert!(output.error.is_none());
}
