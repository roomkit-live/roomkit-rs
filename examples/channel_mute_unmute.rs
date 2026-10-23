//! Muting and unmuting a channel (RFC §7.2, §7.5 rule 2). An assistant
//! answers a customer until a human agent takes over: the integrator mutes
//! it. Muted, the assistant still reads every message and still answers,
//! but its answers are stored `BLOCKED` by `source_muted` and reach no one.
//! Unmuted, it answers the customer again. `ON_CHANNEL_MUTED` and
//! `ON_CHANNEL_UNMUTED` fire when the mute changes, and only then.
//!
//! ```sh
//! cargo run --example channel_mute_unmute
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventStatus, InboundMessage, RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{HookTrigger, RoomKit, text_of};

use shared::{Checks, Log, Script, ScriptedAgent};

/// What the assistant answers, muted or not.
const SCRIPT: Script = &[
    (
        "I need help with my order",
        "I'm the assistant. What is your order number?",
    ),
    ("It's order #12345", "Let me check order #12345."),
    (
        "Order #12345 shipped yesterday.",
        "Noted: order #12345 has shipped.",
    ),
    ("Thanks! One more question.", "Of course, go ahead."),
];

/// Registers observers of `ON_CHANNEL_MUTED` and `ON_CHANNEL_UNMUTED`,
/// which note the change in `changes`.
fn observe_mutes(kit: &RoomKit, changes: &Log<String>) -> Result<(), Box<dyn Error>> {
    for (name, trigger) in [
        ("on_muted", HookTrigger::OnChannelMuted),
        ("on_unmuted", HookTrigger::OnChannelUnmuted),
    ] {
        let changes = changes.clone();
        kit.observe(name, trigger, move |event, _| {
            let changes = changes.clone();
            async move { changes.push(text_of(&event.content)) }
        })?;
    }
    Ok(())
}

/// `from` writes `text` into `room`.
async fn says(
    kit: &RoomKit,
    room: &RoomId,
    from: &RecordingTransport,
    text: &str,
) -> Result<(), Box<dyn Error>> {
    println!("  {} writes {text:?}.", from.id());
    let message = InboundMessage::new(
        from.id().clone(),
        from.id().as_str(),
        EventContent::text(text),
    );
    kit.process_inbound(message, Some(room)).await?;
    Ok(())
}

/// The texts of the assistant's answers stored `BLOCKED` by `source_muted`.
fn muted_answers(timeline: &[RoomEvent], assistant: &ChannelId) -> Vec<String> {
    timeline
        .iter()
        .filter(|event| {
            event.source.channel_id == *assistant
                && event.status == EventStatus::Blocked
                && event.blocked_by.as_deref() == Some("source_muted")
        })
        .map(|event| text_of(&event.content))
        .collect()
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let customer = RecordingTransport::new(ChannelId::new("ws_customer")?, ChannelType::Websocket);
    let agent = RecordingTransport::new(ChannelId::new("ws_agent")?, ChannelType::Websocket);
    let assistant = ScriptedAgent::new(ChannelId::new("assistant")?, SCRIPT);
    kit.register_channel(customer.clone())?;
    kit.register_channel(agent.clone())?;
    kit.register_channel(assistant.clone())?;
    let changes = Log::new();
    observe_mutes(&kit, &changes)?;

    let room = RoomId::new("support")?;
    kit.create_room(&room).await?;
    for channel in [customer.id(), agent.id(), assistant.id()] {
        kit.attach(&room, channel).await?;
    }

    println!("The assistant answers the customer.");
    says(&kit, &room, &customer, "I need help with my order").await?;

    println!("A human agent takes over: the integrator mutes the assistant.");
    kit.mute(&room, assistant.id()).await?;
    let delivered_before = customer.texts().len();
    says(&kit, &room, &customer, "It's order #12345").await?;
    says(&kit, &room, &agent, "Order #12345 shipped yesterday.").await?;
    let while_muted = customer.texts().split_off(delivered_before);
    println!("  The customer received: {while_muted:?}");
    kit.mute(&room, assistant.id()).await?;

    println!("The human agent is done: the integrator unmutes the assistant.");
    kit.unmute(&room, assistant.id()).await?;
    says(&kit, &room, &customer, "Thanks! One more question.").await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    // The hooks run in the background, after the calls that fired them.
    kit.idle().await;
    let mut fired = changes.entries();
    fired.sort();

    let mut checks = Checks::default();
    checks.expect(
        "muted, the assistant still reads every message",
        assistant.read()
            == [
                "I need help with my order",
                "It's order #12345",
                "Order #12345 shipped yesterday.",
                "Thanks! One more question.",
            ],
    );
    checks.expect(
        "its answers while muted are stored BLOCKED by source_muted",
        muted_answers(&timeline, assistant.id())
            == [
                "Let me check order #12345.",
                "Noted: order #12345 has shipped.",
            ],
    );
    checks.expect(
        "while it is muted, the customer receives the human agent only",
        while_muted == ["Order #12345 shipped yesterday."],
    );
    checks.expect(
        "unmuted, the assistant answers the customer again",
        customer.texts().last().map(String::as_str) == Some("Of course, go ahead."),
    );
    checks.expect(
        "ON_CHANNEL_MUTED and ON_CHANNEL_UNMUTED fire once each: muting a muted channel fires nothing",
        fired == ["channel muted", "channel unmuted"],
    );
    Ok(checks.finish())
}
