//! The probe's room (step 4, `turns_probe`): an SMS transport and an AI
//! agent with a tool `lookup`, its model writing "Let me check." and
//! calling it, then answering "It is 42.".

use std::future::Future;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, InboundMessage, Metadata, RoomId, Timestamp, ToolCall,
};
use roomkit::testing::RecordingTransport;
use roomkit::{InboundResult, RoomKit, SequentialEnvironment};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, ToolContext, ToolDefinition, ToolError, ToolOutput, Tools};
use serde_json::json;

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// `lookup`, which takes a query.
pub(crate) fn lookup() -> ToolDefinition {
    ToolDefinition::new(
        "lookup",
        "Look it up.",
        json!({ "type": "object", "properties": { "q": { "type": "string" } } }),
    )
}

fn usage(tokens: u64) -> Metadata {
    let mut usage = Metadata::new();
    usage.insert("input_tokens".to_owned(), json!(tokens));
    usage
}

/// The probe's model: a text and a call to `lookup`, then the answer.
pub(crate) fn model() -> ScriptedProvider {
    ScriptedProvider::new([
        ScriptedRound::calling("Let me check.", [("call-1", "lookup", r#"{"q":"x"}"#)])
            .with_usage(usage(10)),
        ScriptedRound::answering("It is 42.").with_usage(usage(20)),
    ])
}

/// A kit on a deterministic environment, whose room binds the SMS
/// transport and the agent serving `lookup` with `handler`.
pub(crate) async fn kit<F, Fut>(handler: F) -> RoomKit
where
    F: Fn(ToolCall, ToolContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ToolOutput, ToolError>> + Send + 'static,
{
    let kit = RoomKit::new();
    let at: Timestamp = "2026-10-26T12:00:00Z".parse().unwrap();
    kit.set_environment(SequentialEnvironment::new(at));
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    let agent = AiAgent::new(channel("ai"), model())
        .with_tools(Tools::new().tool(lookup(), handler))
        .unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(&room()).await.unwrap();
    for id in ["sms", "ai"] {
        kit.attach(&room(), &channel(id)).await.unwrap();
    }
    kit
}

/// Alice asks by SMS.
pub(crate) async fn asks(kit: &RoomKit, text: &str) -> InboundResult {
    let message = InboundMessage::new(channel("sms"), "+15550000001", EventContent::text(text));
    kit.process_inbound_in(&room(), message).await.unwrap()
}
