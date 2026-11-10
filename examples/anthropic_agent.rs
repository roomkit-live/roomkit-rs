//! Native Anthropic Messages in a room, with deferred tool discovery.
//! `cargo run --example anthropic_agent` checks construction without a key.
//! `ANTHROPIC_API_KEY=... cargo run --example anthropic_agent -- --live`
//! runs two paid turns. `ANTHROPIC_MODEL` can select the model.
#![expect(clippy::print_stdout, reason = "an example prints its outcome")]

use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, ProcessInboundOptions, TransportChannel,
    TransportChannelOptions,
};
use roomkit_ai::{AiChannel, AiChannelOptions, AiProvider, AiTool, ToolHandler, ToolResult};
use roomkit_anthropic::{AnthropicAiProvider, AnthropicConfig};
use serde_json::json;
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let live = std::env::args().any(|arg| arg == "--live");
    let key = if live {
        std::env::var("ANTHROPIC_API_KEY")?
    } else {
        String::new()
    };
    let model = std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| "claude-sonnet-5-5".into());
    let provider = AnthropicAiProvider::new(AnthropicConfig::new(key, model))?;
    assert!(provider.supports_streaming() && provider.supports_response_schema());
    assert!(!AnthropicAiProvider::available_models().is_empty());
    if !live {
        println!(
            "Anthropic provider constructed. Pass --live with ANTHROPIC_API_KEY to run paid turns."
        );
        return Ok(());
    }
    run_room(provider).await
}

async fn run_room(provider: AnthropicAiProvider) -> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut lookup = AiTool::new(
        "lookup_code",
        "Look up the private delivery code of a parcel.",
    )?;
    lookup.parameters =
        json!({"type":"object", "properties":{"parcel":{"type":"string"}}, "required":["parcel"]});
    let served = Arc::clone(&calls);
    let agent = AiChannel::new(ChannelId::new("assistant")?, provider, AiChannelOptions {
        system_prompt: Some("Find the lookup_code tool and call it to retrieve each requested parcel's private delivery code. Never invent a code. Answer with the returned code.".into()),
        enable_thinking: Some(false),
        tool_search: Some(true),
        tools: vec![lookup.into()],
        tool_handler: Some(ToolHandler::new(move |_, _| {
            served.fetch_add(1, Ordering::Relaxed);
            async { Ok(ToolResult::Text("CODE-7319".into())) }
        })),
        ..AiChannelOptions::default()
    })?;
    let kit = shared::kit();
    let web = TransportChannel::new(
        ChannelId::new("web")?,
        ChannelType::WEBSOCKET,
        TransportChannelOptions::default(),
    );
    let agent_id = agent.channel_id();
    let web_id = web.channel_id();
    kit.register_channel(web)?;
    kit.register_channel(agent)?;
    let room = RoomId::new("anthropic-example")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in [&web_id, &agent_id] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await?;
    }
    for parcel in ["P7", "P8"] {
        let message = InboundMessage::new(
            web_id.clone(),
            "alice",
            TextContent::new(format!(
                "Retrieve the private delivery code for parcel {parcel}."
            )),
        );
        let result = kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room.clone()),
                    ..Default::default()
                },
            )
            .await?;
        assert!(result.error.is_none(), "turn failed: {:?}", result.error);
    }
    let timeline = shared::timeline(&kit, &room).await?;
    let answers: Vec<_> = timeline
        .iter()
        .filter(|event| event.source.channel_id == agent_id)
        .filter_map(|event| match &event.content {
            roomkit::model::EventContent::Text(text) => Some(&text.body),
            _ => None,
        })
        .collect();
    assert!(
        answers
            .iter()
            .filter(|answer| answer.contains("CODE-7319"))
            .count()
            >= 2
    );
    assert!(calls.load(Ordering::Relaxed) >= 2);
    println!(
        "Two Anthropic turns answered using a discovered tool, including reopening it on the later turn."
    );
    kit.close().await?;
    Ok(())
}
