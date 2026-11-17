//! Public planning boundaries: Python's `tests/test_planning_*.py`.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "planning/boundaries.rs"]
mod boundaries;
#[path = "planning/compaction.rs"]
mod compaction;
#[path = "planning/concurrent.rs"]
mod concurrent;
#[path = "planning/handles.rs"]
mod handles;
#[path = "planning/observers.rs"]
mod observers;
#[path = "planning/rooms.rs"]
mod rooms;
#[path = "planning/skills.rs"]
mod skills;

use std::time::Duration;

use roomkit::model::{ChannelType, InboundMessage, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContentPart, AiContext, AiMessageContent, AiProvider,
    AiResponse, AiToolCall, AiToolResultPart, MockAiProvider, MockAiProviderOptions,
};
use serde_json::{Value, json};

const BOUND: Duration = Duration::from_secs(5);

fn task_values(tasks: &[roomkit_ai::PlanTaskHandle]) -> Value {
    json!(
        tasks
            .iter()
            .map(roomkit_ai::PlanTaskHandle::get)
            .collect::<Vec<_>>()
    )
}

fn plan(tasks: Value) -> AiResponse {
    let mut response = AiResponse::new("");
    let mut call = AiToolCall::new("p1", "plan_tasks");
    call.arguments.insert("tasks".to_owned(), tasks);
    response.tool_calls.push(call);
    response
}

fn tasks(title: &str) -> Value {
    json!([{"title": title, "status": "pending"}])
}

fn model(rounds: Vec<AiResponse>, streaming: bool) -> MockAiProvider {
    MockAiProvider::new(MockAiProviderOptions {
        ai_responses: Some(rounds),
        streaming,
        ..Default::default()
    })
}

async fn setup<P: AiProvider + 'static>(
    provider: P,
    rooms: &[&str],
    options: RoomKitOptions,
) -> RoomKit {
    let channel = AiChannel::new(
        "agent".parse().unwrap(),
        provider,
        AiChannelOptions {
            enable_planning: true,
            system_prompt: Some("Fixed.".into()),
            ..Default::default()
        },
    )
    .unwrap();
    setup_channel(channel, rooms, options).await
}

async fn setup_channel<P: AiProvider + 'static>(
    channel: AiChannel<P>,
    rooms: &[&str],
    options: RoomKitOptions,
) -> RoomKit {
    let kit = RoomKit::new(options);
    kit.register_channel(RecordingTransport::new(
        "ws".parse().unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(channel).unwrap();
    for room in rooms {
        let id = room.parse().unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(id),
            ..Default::default()
        })
        .await
        .unwrap();
        for name in ["ws", "agent"] {
            attach(&kit, room, name).await;
        }
    }
    kit
}

async fn attach(kit: &RoomKit, room: &str, channel: &str) {
    kit.attach_channel(
        &room.parse().unwrap(),
        &channel.parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}

async fn say(kit: &RoomKit, room: &str, text: &str) {
    let output = tokio::time::timeout(
        BOUND,
        kit.process_inbound(
            InboundMessage::new("ws".parse().unwrap(), "alice", TextContent::new(text)),
            ProcessInboundOptions {
                room_id: Some(room.parse().unwrap()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(output.error.is_none(), "{output:?}");
}

fn results(context: &AiContext) -> Vec<&AiToolResultPart> {
    context
        .messages
        .iter()
        .flat_map(|message| match &message.content {
            AiMessageContent::Parts(parts) => parts.as_slice(),
            AiMessageContent::Text(_) => &[],
        })
        .filter_map(|part| match part {
            AiContentPart::ToolResult(result) => Some(result),
            AiContentPart::Text(_)
            | AiContentPart::Image(_)
            | AiContentPart::ToolCall(_)
            | AiContentPart::Thinking(_) => None,
        })
        .collect()
}

fn input(context: &AiContext) -> String {
    context.messages.last().unwrap().content.text()
}
