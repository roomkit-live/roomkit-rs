//! RFC Appendix B.1, simplified: a customer writes on SMS, an agent answers,
//! and the answer reaches the customer.
//!
//! ```sh
//! cargo run --example echo_room
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions};

use shared::Checks;

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();

    let sms = RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let agent = EchoAgent::new(
        ChannelId::new("ai")?,
        "Bonjour Jean! You said: ",
        EchoAgentOptions { quote: true },
    );
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(&room, sms.channel_id(), AttachChannelOptions::default())
        .await?;
    kit.attach_channel(&room, agent.channel_id(), AttachChannelOptions::default())
        .await?;

    println!("The customer writes \"Bonjour\" on SMS.");
    let message = InboundMessage::new(
        sms.channel_id().clone(),
        "+15551234567",
        TextContent::new("Bonjour"),
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

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    println!("Delivered on SMS: {:?}", sms.texts());

    // The two attaches' lifecycle records take indexes 0 and 1 (RFC §5.1,
    // gap G-046); the conversation follows them.
    let mut checks = Checks::default();
    checks.expect(
        "the customer's message is committed first, at index 2, after the attaches' records",
        result.event.as_ref().map(|event| event.index) == Some(2),
    );
    checks.expect(
        "the agent's answer is committed next, one hop deeper",
        timeline.get(3).is_some_and(|reply| {
            reply.source.channel_id == *agent.channel_id() && reply.chain_depth == 1
        }),
    );
    checks.expect(
        "the answer reaches the customer on SMS",
        sms.texts() == ["Bonjour Jean! You said: Bonjour"],
    );
    checks.expect(
        "the agent does not receive its own answer",
        timeline.len() == 4,
    );
    Ok(checks.finish())
}
