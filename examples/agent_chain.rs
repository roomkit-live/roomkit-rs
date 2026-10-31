//! RFC Appendix B.4: an analyst and a writer answer each other until the
//! chain-depth limit stops the next turn, which is never asked.
//!
//! ```sh
//! cargo run --example agent_chain
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, EventStatus, InboundMessage, RoomId, TextContent};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{AttachChannelOptions, CreateRoomOptions, FrameworkEvent, ProcessInboundOptions};

use shared::Checks;

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let mut events = kit.subscribe();

    let human = RecordingTransport::new(
        ChannelId::new("ws")?,
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let analyst = EchoAgent::new(
        ChannelId::new("analyst")?,
        "Key findings: revenue is up.",
        EchoAgentOptions::default(),
    );
    let writer = EchoAgent::new(
        ChannelId::new("writer")?,
        "Report drafted from the findings.",
        EchoAgentOptions::default(),
    );
    kit.register_channel(human.clone())?;
    kit.register_channel(analyst.clone())?;
    kit.register_channel(writer.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in [
        human.channel_id(),
        analyst.channel_id(),
        writer.channel_id(),
    ] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await?;
    }

    println!("The human asks the analyst; each agent then answers the other (AGENT_CHAIN).");
    let mut message = InboundMessage::new(
        human.channel_id().clone(),
        "alice",
        TextContent::new("Analyse the quarter"),
    );
    message.addressed_to = Some(vec![analyst.channel_id().clone()]);
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    )
    .await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    let exceeded = shared::framework_events(&mut events)
        .iter()
        .filter(|event| matches!(event, FrameworkEvent::ChainDepthExceeded { .. }))
        .count();

    let mut checks = Checks::default();
    checks.expect(
        "four answers are delivered, at depths 1 to 4",
        timeline
            .iter()
            .filter(|event| event.status == EventStatus::Delivered && event.chain_depth > 0)
            .map(|event| event.chain_depth)
            .eq(1..=4),
    );
    checks.expect(
        "the turn that would reach depth 5 is stored blocked instead, never asked",
        timeline.last().is_some_and(|record| {
            record.status == EventStatus::Blocked
                && record.blocked_by.as_deref() == Some("event_chain_depth_limit")
                && record.chain_depth == 5
        }),
    );
    checks.expect(
        "a chain_depth_exceeded framework event is emitted",
        exceeded == 1,
    );
    checks.expect(
        "the human receives every delivered answer",
        human.texts().len() == 4,
    );
    Ok(checks.finish())
}
