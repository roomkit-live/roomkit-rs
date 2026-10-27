//! Directing an agent without putting words in anyone's mouth (RFC
//! §10.1.1). The application needs the advisor agent to speak because of
//! something nobody in the room said: a handoff asking it to introduce
//! itself, then a summary. Sent as an `INSTRUCTION` addressed to the
//! advisor, the direction reaches that agent only, never a transport nor
//! the room's other agent, and is never stored: the room keeps the answer,
//! with the instruction's fingerprint, never its text. A `standalone`
//! instruction opens a turn that reads nothing of the room, for a pass that
//! must start from a blank page: the pipeline hands the agent the room as
//! for any event, and the turn leaves it unread (`turn_context`).
//!
//! ```sh
//! cargo run --example instruction_event
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, InboundResult, ProcessInboundOptions, text_of,
};

use shared::{Checks, Script, ScriptedAgent};

const INTRODUCE: &str = "You just took over the call. Introduce yourself.";
const SUMMARIZE: &str = "Summarize the call so far in one line.";

/// What the advisor answers.
const ADVISOR: Script = &[
    (INTRODUCE, "Hello, I'm Marie, your advisor. How can I help?"),
    (
        "I'd like to check my balance.",
        "Of course: your balance is $1,250.",
    ),
    (SUMMARIZE, "Summary: the customer checked their balance."),
];

/// The application's instruction `text` to the advisor, standing alone or
/// not.
fn instruction(
    app: &ChannelId,
    advisor: &ChannelId,
    text: &str,
    standalone: bool,
) -> InboundMessage {
    InboundMessage {
        event_type: EventType::Instruction,
        addressed_to: Some(vec![advisor.clone()]),
        standalone,
        ..InboundMessage::new(app.clone(), "app", EventContent::text(text))
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let app = RecordingTransport::new(ChannelId::new("app")?, ChannelType::WEBSOCKET);
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::SMS);
    let advisor = ScriptedAgent::new(ChannelId::new("advisor")?, ADVISOR);
    let notetaker = ScriptedAgent::new(ChannelId::new("notetaker")?, &[]);
    kit.register_channel(app.clone())?;
    kit.register_channel(sms.clone())?;
    kit.register_channel(advisor.clone())?;
    kit.register_channel(notetaker.clone())?;
    let room = RoomId::new("support")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in [app.id(), sms.id(), advisor.id(), notetaker.id()] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await?;
    }
    let into_room = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..ProcessInboundOptions::default()
    };
    let run = async |message: InboundMessage| -> Result<InboundResult, Box<dyn Error>> {
        let (channel, text) = (&message.channel_id, text_of(&message.content));
        println!("{channel} sends {text:?} ({:?}).", message.event_type);
        Ok(kit.process_inbound(message, into_room.clone()).await?)
    };

    let introduced = run(instruction(app.id(), advisor.id(), INTRODUCE, false)).await?;
    let customer = InboundMessage::new(
        sms.id().clone(),
        "+15551234567",
        EventContent::text("I'd like to check my balance."),
    );
    run(customer).await?;
    run(instruction(app.id(), advisor.id(), SUMMARIZE, false)).await?;
    run(instruction(app.id(), advisor.id(), SUMMARIZE, true)).await?;
    let unaddressed = run(InboundMessage {
        addressed_to: None,
        ..instruction(app.id(), advisor.id(), SUMMARIZE, false)
    })
    .await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    println!("Delivered on SMS: {:?}", sms.texts());
    let turns = advisor.turns();
    let summaries: Vec<usize> = turns
        .iter()
        .filter(|turn| text_of(&turn.event.content) == SUMMARIZE)
        .map(|turn| turn.history.len())
        .collect();
    println!("The advisor's summary turns read {summaries:?} events of history.");
    let greeting = timeline
        .iter()
        .find(|event| text_of(&event.content).starts_with("Hello, I'm Marie"));

    let is_instruction = |text: &String| text == INTRODUCE || text == SUMMARIZE;

    let mut checks = Checks::default();
    checks.expect(
        "the instruction is directed at the advisor, which answers it",
        introduced
            .event
            .is_some_and(|event| event.event_type == EventType::Instruction)
            && turns
                .first()
                .is_some_and(|turn| text_of(&turn.event.content) == INTRODUCE)
            && sms.texts().first().map(String::as_str)
                == Some("Hello, I'm Marie, your advisor. How can I help?"),
    );
    checks.expect(
        "no transport receives an instruction, nor does the room's other agent",
        !sms.texts().iter().any(is_instruction)
            && !app.texts().iter().any(is_instruction)
            && !notetaker.read().iter().any(is_instruction),
    );
    checks.expect(
        "the room never stores an instruction",
        timeline
            .iter()
            .all(|event| event.event_type != EventType::Instruction),
    );
    checks.expect(
        "the answer records the instruction's fingerprint, never its text",
        greeting
            .and_then(|event| event.metadata.get("instruction"))
            .is_some_and(|fingerprint| {
                fingerprint.get("sha256").is_some()
                    && fingerprint.get("length") == Some(&INTRODUCE.chars().count().into())
                    && !fingerprint.to_string().contains(INTRODUCE)
            }),
    );
    checks.expect(
        "a standalone instruction's turn reads nothing of the room; the other reads its history",
        summaries.len() == 2 && summaries.first() > Some(&0) && summaries.last() == Some(&0),
    );
    checks.expect(
        "an instruction addressed to no agent is refused instruction_unaddressed",
        unaddressed.blocked && unaddressed.reason.as_deref() == Some("instruction_unaddressed"),
    );
    Ok(checks.finish())
}
