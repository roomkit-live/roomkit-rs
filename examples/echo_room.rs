//! RFC Appendix B.1, simplified: a customer writes on SMS, an agent answers,
//! and the answer reaches the customer.
//!
//! ```sh
//! cargo run --example echo_room
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, EventContent, InboundMessage, RoomId};
use roomkit::testing::{EchoAgent, RecordingTransport};

use shared::Checks;

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();

    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "Bonjour Jean! You said: ").quoting();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    kit.attach(&room, sms.id()).await?;
    kit.attach(&room, agent.id()).await?;

    println!("The customer writes \"Bonjour\" on SMS.");
    let message = InboundMessage::new(
        sms.id().clone(),
        "+15551234567",
        EventContent::text("Bonjour"),
    );
    let result = kit.process_inbound_in(&room, message).await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    println!("Delivered on SMS: {:?}", sms.texts());

    let mut checks = Checks::default();
    checks.expect(
        "the customer's message is committed first, at index 0",
        result.event.as_ref().map(|event| event.index) == Some(0),
    );
    checks.expect(
        "the agent's answer is committed next, one hop deeper",
        timeline
            .get(1)
            .is_some_and(|reply| reply.source.channel_id == *agent.id() && reply.chain_depth == 1),
    );
    checks.expect(
        "the answer reaches the customer on SMS",
        sms.texts() == ["Bonjour Jean! You said: Bonjour"],
    );
    checks.expect(
        "the agent does not receive its own answer",
        timeline.len() == 2,
    );
    Ok(checks.finish())
}
