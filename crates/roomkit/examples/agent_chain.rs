//! RFC Appendix B.4: an analyst and a writer answer each other until the
//! chain-depth limit stops the next turn, which is never asked.
//!
//! ```sh
//! cargo run -p roomkit --example agent_chain
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod support;

use std::error::Error;
use std::process::ExitCode;

use roomkit::RoomKit;
use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventStatus, InboundMessage, RoomId, Timestamp,
};
use roomkit::ops::{FrameworkEvent, SequentialEnvironment};
use roomkit::testing::{EchoAgent, RecordingTransport};

use self::support::Checks;

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = RoomKit::new();
    kit.set_environment(SequentialEnvironment::new(Timestamp::from_unix_millis(
        1_792_677_780_000,
    )?));
    let mut events = kit.subscribe();

    let human = RecordingTransport::new(ChannelId::new("ws")?, ChannelType::Websocket);
    let analyst = EchoAgent::new(ChannelId::new("analyst")?, "Key findings: revenue is up.");
    let writer = EchoAgent::new(
        ChannelId::new("writer")?,
        "Report drafted from the findings.",
    );
    kit.register_channel(human.clone())?;
    kit.register_channel(analyst.clone())?;
    kit.register_channel(writer.clone())?;

    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    for channel in [human.id(), analyst.id(), writer.id()] {
        kit.attach(&room, channel).await?;
    }

    println!("The human asks the analyst; each agent then answers the other (AGENT_CHAIN).");
    let mut message = InboundMessage::new(
        human.id().clone(),
        "alice",
        EventContent::text("Analyse the quarter"),
    );
    message.addressed_to = Some(vec![analyst.id().clone()]);
    kit.process_inbound(&room, message).await?;

    let timeline = support::timeline(&kit, &room).await?;
    support::print_timeline(&timeline);
    let mut exceeded = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, FrameworkEvent::ChainDepthExceeded { .. }) {
            exceeded += 1;
        }
    }

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
