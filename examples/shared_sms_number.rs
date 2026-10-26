//! One SMS number, many correspondents (RFC §10.4, §5.7). A business with a
//! single number receives texts from many people, and the default router
//! keeps each conversation its own. A room the host opened for Alice names
//! her on its binding: Bob, writing first, is not let into it and gets a
//! room of his own, and Alice's text lands in hers. A group is the one case
//! where several senders share a room: its binding is declared `group`, on
//! a channel dedicated to it, and everyone's text lands there.
//! `new_conversation` shows a first message opening its sender's room.
//!
//! ```sh
//! cargo run --example shared_sms_number
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    BindingOptions, ChannelId, ChannelType, EventContent, EventType, InboundMessage, Metadata,
    ParticipantId, RoomId,
};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{HookTrigger, RoomKit};

use shared::{Checks, SmsNumber};

const ALICE: &str = "+15550000001";
const BOB: &str = "+15550000002";
const CAROL: &str = "+15550000003";

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
    let kit = shared::kit();
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "Thanks, we are on it.");
    let team = RecordingTransport::new(ChannelId::new("team_sms")?, ChannelType::SMS);
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;
    kit.register_channel(team.clone())?;
    agent_joins_every_room(&kit, agent.id())?;
    let mut checks = Checks::default();

    println!("The host opens a room for Alice's appointment, naming her on its binding.");
    let appointment = RoomId::new("alice_appointment")?;
    kit.create_room(&appointment).await?;
    let alice = ParticipantId::new(ALICE)?;
    // The binding names its correspondent, and the number it delivers to.
    let alice_s_binding = BindingOptions {
        participant_id: Some(alice),
        metadata: Metadata::from_iter([("phone_number".to_owned(), ALICE.into())]),
        ..BindingOptions::default()
    };
    kit.attach_with(&appointment, sms.id(), alice_s_binding)
        .await?;

    let bobs = text(&kit, sms.id(), BOB, "I would like an appointment.").await?;
    checks.expect(
        "Bob, writing first, is not let into Alice's room: he gets a room of his own",
        // Alice's room holds its attach's lifecycle record alone (RFC §5.1,
        // gap G-046).
        bobs != appointment
            && shared::timeline(&kit, &appointment)
                .await?
                .iter()
                .all(|event| event.event_type == EventType::ChannelAttached),
    );
    let alices = text(&kit, sms.id(), ALICE, "Confirming tomorrow.").await?;
    checks.expect(
        "Alice's text lands in the room opened for her",
        alices == appointment,
    );
    checks.expect(
        "each is answered at their own number, and only there",
        sms.sent_to(BOB) == ["Thanks, we are on it."]
            && sms.sent_to(ALICE) == ["Thanks, we are on it."],
    );

    println!("The team's number is dedicated to the team's room, bound as a group.");
    let team_room = RoomId::new("team")?;
    kit.create_room(&team_room).await?;
    kit.attach_with(
        &team_room,
        team.id(),
        BindingOptions {
            group: true,
            ..BindingOptions::default()
        },
    )
    .await?;
    let mut landed = Vec::new();
    for sender in [ALICE, BOB, CAROL] {
        landed.push(text(&kit, team.id(), sender, "Hi team").await?);
    }
    let senders: Vec<String> = shared::timeline(&kit, &team_room)
        .await?
        .iter()
        .filter(|event| event.source.channel_id == *team.id())
        .filter_map(|event| {
            event
                .source
                .participant_id
                .as_ref()
                .map(ToString::to_string)
        })
        .collect();
    checks.expect(
        "a group binding takes everyone's text into its one room",
        landed.iter().all(|room| *room == team_room) && senders == [ALICE, BOB, CAROL],
    );
    Ok(checks.finish())
}
