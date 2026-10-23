//! A conversation its timer paused (RFC §10.4 steps 1 and 3, §5.1). Alice
//! texts the business's number, and the one room bound to it takes her as
//! its correspondent. An hour without activity pauses the room. When Alice
//! writes again, she is routed back to her paused conversation, and her
//! message, once delivered, resumes it. Bob, whom the room never recorded,
//! is not let into it: he gets a room of his own, and Alice's stays paused.
//! A group's room is the channel's conversation: paused, it takes any
//! member's next text, which resumes it.
//!
//! ```sh
//! cargo run --example paused_conversation
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, InboundMessage, Room, RoomId, RoomStatus, RoomTimers,
};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{ConversationStore, HookTrigger, RoomKit};

use shared::{Checks, Clock, SmsNumber};

const ALICE: &str = "+15550000001";
const BOB: &str = "+15550000002";

/// An hour without activity pauses a room; a day paused closes it.
fn hourly() -> RoomTimers {
    RoomTimers {
        inactive_after_seconds: Some(3600),
        closed_after_seconds: Some(86_400),
        last_activity_at: None,
    }
}

/// A text from `from` on `channel`, naming no room; returns the room it was
/// routed to.
async fn text(
    kit: &RoomKit,
    channel: &ChannelId,
    from: &str,
    body: &str,
) -> Result<RoomId, Box<dyn Error>> {
    let message = InboundMessage::new(channel.clone(), from, EventContent::text(body));
    let result = kit.process_inbound(message).await?;
    let room = result.event.ok_or("the message was refused")?.room_id;
    println!("{from} texts {body:?}: routed to {room}.");
    Ok(room)
}

/// Two hours pass, and the host checks the rooms' timers; returns the rooms
/// the check changed.
async fn two_hours_later(kit: &RoomKit, clock: &Clock) -> Result<Vec<Room>, Box<dyn Error>> {
    clock.advance(Duration::from_hours(2));
    let changed = kit.check_timers().await?;
    for room in &changed {
        println!("Two hours later, {} is {:?}.", room.id, room.status);
    }
    Ok(changed)
}

/// The room `room_id`'s status.
async fn status(kit: &RoomKit, room_id: &RoomId) -> Result<Option<RoomStatus>, Box<dyn Error>> {
    Ok(kit.store().get_room(room_id).await?.map(|room| room.status))
}

/// Registers an observer of `ON_ROOM_CREATED` that attaches the agent to
/// every room created, by the host or for a first message.
fn agent_joins_every_room(kit: &RoomKit, agent: &ChannelId) -> Result<(), Box<dyn Error>> {
    let (attaching, agent) = (kit.clone(), agent.clone());
    kit.observe(
        "agent_joins",
        HookTrigger::OnRoomCreated,
        move |event, _| {
            let (kit, agent) = (attaching.clone(), agent.clone());
            async move {
                // A failure would be logged; the example's checks see it.
                let _attached = kit.attach(&event.room_id, &agent).await;
            }
        },
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let clock = Clock::new();
    let kit = clock.kit();
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    let team = RecordingTransport::new(ChannelId::new("team_sms")?, ChannelType::Sms);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "You said: ").quoting();
    kit.register_channel(sms.clone())?;
    kit.register_channel(team.clone())?;
    kit.register_channel(agent.clone())?;
    agent_joins_every_room(&kit, agent.id())?;
    let mut checks = Checks::default();

    let support = RoomId::new("support")?;
    kit.create_room_with(&support, |room| room.timers = hourly())
        .await?;
    kit.attach(&support, sms.id()).await?;
    text(&kit, sms.id(), ALICE, "Hello").await?;
    let paused = two_hours_later(&kit, &clock).await?;
    checks.expect(
        "an hour without activity pauses Alice's conversation",
        paused
            .iter()
            .any(|room| room.id == support && room.status == RoomStatus::Paused),
    );

    let landed = text(&kit, sms.id(), ALICE, "Are you still there?").await?;
    checks.expect(
        "writing again, Alice is routed back to her paused room, and her message resumes it",
        landed == support && status(&kit, &support).await? == Some(RoomStatus::Active),
    );
    checks.expect(
        "the agent answers her there, at her number",
        sms.sent_to(ALICE) == ["You said: Hello", "You said: Are you still there?"],
    );

    two_hours_later(&kit, &clock).await?;
    let bobs = text(&kit, sms.id(), BOB, "Hello").await?;
    checks.expect(
        "Bob, whom the paused room never recorded, gets a room of his own; Alice's stays paused",
        bobs != support && status(&kit, &support).await? == Some(RoomStatus::Paused),
    );

    println!("The team's number is dedicated to its room, bound as a group.");
    let team_room = RoomId::new("team")?;
    kit.create_room_with(&team_room, |room| room.timers = hourly())
        .await?;
    kit.attach_with(&team_room, team.id(), |binding| binding.group = true)
        .await?;
    text(&kit, team.id(), ALICE, "Meeting at 3?").await?;
    two_hours_later(&kit, &clock).await?;
    let paused_group = status(&kit, &team_room).await?;
    let landed = text(&kit, team.id(), BOB, "On my way.").await?;
    checks.expect(
        "a paused group room takes another member's text, which resumes it",
        paused_group == Some(RoomStatus::Paused)
            && landed == team_room
            && status(&kit, &team_room).await? == Some(RoomStatus::Active),
    );
    Ok(checks.finish())
}
