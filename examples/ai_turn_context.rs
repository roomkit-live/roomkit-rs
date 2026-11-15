//! Read a real AI turn's room and author in configuration and tools (RFC §21.4).
//! Run `cargo run --example ai_turn_context`; no network or key is needed.

use std::error::Error;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelType, EventSource, Room, RoomContext, RoomEvent, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, ToolHandler, ToolResult, current_tool_actor_id,
    current_tool_chain_depth, current_tool_room,
};
use serde_json::json;

fn main() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    let readings = Arc::new(Mutex::new(Vec::new()));
    let (configured, handled) = (Arc::clone(&readings), Arc::clone(&readings));
    let agent = AiChannel::new(
        "assistant".parse()?,
        ScriptedProvider::new([
            ScriptedRound::calling("", [("lookup-1", "lookup", "{}")]),
            ScriptedRound::answering("Your order has shipped."),
        ]),
        AiChannelOptions {
            config_provider: Some(Arc::new(move |_, _| {
                configured
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(current_tool_room());
                Box::pin(async { Ok(None) })
            })),
            tool_handler: Some(ToolHandler::new(move |_, _| {
                handled
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(current_tool_room());
                async {
                    Ok(ToolResult::Text(format!(
                        "{}:{}",
                        current_tool_actor_id().unwrap_or_default(),
                        current_tool_chain_depth(),
                    )))
                }
            })),
            ..AiChannelOptions::default()
        },
    )?;
    let mut room = Room::new("support".parse()?);
    room.organization_id = Some("acme".into());
    let mut source = EventSource::new("sms".parse()?, ChannelType::SMS);
    source.participant_id = Some("alice".parse()?);
    let event = RoomEvent::new(
        room.id.clone(),
        source,
        TextContent::new("Where is my order?"),
    );
    let mut binding = ChannelBinding::new("assistant".parse()?, room.id.clone(), ChannelType::AI);
    binding.metadata.insert(
        "tools".into(),
        json!([{"name":"lookup", "description":"Find an order"}]),
    );
    let output = agent
        .on_event(event, binding, RoomContext::new(room.clone()))
        .await?;

    let mut checks = shared::Checks::default();
    let readings = readings.lock().unwrap_or_else(PoisonError::into_inner);
    checks.expect(
        "both callbacks see the room",
        readings.len() == 2 && readings.iter().all(|seen| seen.as_deref() == Some(&room)),
    );
    checks.expect(
        "one shared room snapshot",
        matches!(readings.as_slice(), [Some(first), Some(second)] if Arc::ptr_eq(first, second)),
    );
    let result = output.response_events.iter().find_map(|event| {
        let roomkit::model::EventContent::ToolCall(call) = &event.content else {
            return None;
        };
        (!call.result.is_null()).then_some(&call.result)
    });
    checks.expect(
        "handler sees the author and response depth",
        result == Some(&json!("alice:1")),
    );
    checks.expect("outside the turn", current_tool_room().is_none());
    Ok(checks.finish())
}
