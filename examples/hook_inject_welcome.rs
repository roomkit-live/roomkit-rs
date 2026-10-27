//! A hook that injects a welcome (RFC §9.2, §9.4). An observer of
//! `ON_CHANNEL_ATTACHED` notes each channel attached to the room, which it
//! reads from its payload; an observer's result is discarded, so it cannot
//! inject anything itself.
//! When a newcomer's first message goes through, a SYNC `BEFORE_BROADCAST`
//! hook lets it pass and injects a system welcome addressed to that newcomer
//! only. The welcome is stored in the timeline, and the newcomer's message
//! reaches the room as usual.
//!
//! ```sh
//! cargo run --example hook_inject_welcome
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType, HookResult,
    InboundMessage, InjectedEvent, RoomEvent, RoomId, SystemContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookTrigger, LifecyclePayload, ProcessInboundOptions,
    RoomKit,
};

use shared::{Checks, Log};

/// A system welcome to `newcomer`, delivered to it only, for the hook to
/// inject beside `event`, the newcomer's first message.
fn welcome(event: &RoomEvent, newcomer: &ChannelId) -> InjectedEvent {
    let source = EventSource::new(
        ChannelId::system(),
        ChannelType::SYSTEM,
        EventDirection::Outbound,
    );
    let mut welcome = SystemContent::new(format!("Welcome to {}, {newcomer}!", event.room_id));
    welcome.code = Some("welcome".to_owned());
    let content = EventContent::System(welcome);
    // The pipeline gives every injected event a fresh id and its index.
    let welcome = RoomEvent::new(
        event.id.clone(),
        event.room_id.clone(),
        EventType::System,
        source,
        content,
        event.created_at,
    );
    InjectedEvent::to_channels(welcome, vec![newcomer.clone()])
}

/// Registers the observer that notes each channel attached in `attached`,
/// and the hook that welcomes each of them once, noting it in `welcomed`.
fn welcome_newcomers(
    kit: &RoomKit,
    attached: &Log<ChannelId>,
    welcomed: &Log<ChannelId>,
) -> Result<(), Box<dyn Error>> {
    let noted = attached.clone();
    kit.observe(
        "note_newcomer",
        HookTrigger::OnChannelAttached,
        move |event, _| {
            let noted = noted.clone();
            async move {
                if let Some(channel_id) =
                    LifecyclePayload::of(&event).and_then(|attached| attached.channel_id)
                {
                    noted.push(channel_id);
                }
            }
        },
    )?;
    let (attached, welcomed) = (attached.clone(), welcomed.clone());
    kit.before_broadcast("inject_welcome", move |event, _| {
        let newcomer = event.source.channel_id.clone();
        let first_message =
            attached.entries().contains(&newcomer) && !welcomed.entries().contains(&newcomer);
        if first_message {
            welcomed.push(newcomer.clone());
        }
        async move {
            if !first_message {
                return Ok(HookResult::allow());
            }
            // An allow lets the message through, with the events it injects.
            Ok(HookResult::allow().inject(welcome(&event, &newcomer)))
        }
    })?;
    Ok(())
}

/// The welcomes `transport` received.
fn welcomes(transport: &RecordingTransport) -> Vec<String> {
    transport
        .texts()
        .into_iter()
        .filter(|text| text.starts_with("Welcome"))
        .collect()
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let alice = RecordingTransport::new(ChannelId::new("ws_alice")?, ChannelType::WEBSOCKET);
    let bob = RecordingTransport::new(ChannelId::new("ws_bob")?, ChannelType::WEBSOCKET);
    kit.register_channel(alice.clone())?;
    kit.register_channel(bob.clone())?;
    let (attached, welcomed) = (Log::new(), Log::new());
    welcome_newcomers(&kit, &attached, &welcomed)?;

    let room = RoomId::new("welcome_room")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    println!("Alice and Bob join the room.");
    kit.attach_channel(&room, alice.id(), AttachChannelOptions::default())
        .await?;
    kit.attach_channel(&room, bob.id(), AttachChannelOptions::default())
        .await?;
    // The observer runs in the background, once each attach returned: the
    // room goes on once it noted both.
    attached.wait_for(2).await;

    let says = async |on: &RecordingTransport, text: &str| -> Result<(), Box<dyn Error>> {
        println!("{} writes {text:?}.", on.id());
        let message =
            InboundMessage::new(on.id().clone(), on.id().as_str(), EventContent::text(text));
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    };
    says(&alice, "Hey everyone, I just joined!").await?;
    says(&bob, "Hi Alice!").await?;
    says(&alice, "How are you, Bob?").await?;

    kit.close().await;
    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    println!("Alice received: {:?}", alice.texts());
    println!("Bob received: {:?}", bob.texts());

    let mut checks = Checks::default();
    checks.expect(
        "each newcomer's first message brings a welcome to that newcomer only",
        welcomes(&alice) == ["Welcome to welcome_room, ws_alice!"]
            && welcomes(&bob) == ["Welcome to welcome_room, ws_bob!"],
    );
    checks.expect(
        "each newcomer is welcomed once",
        welcomed.entries() == [alice.id().clone(), bob.id().clone()],
    );
    checks.expect(
        "the messages reach the room as usual",
        alice.texts().contains(&"Hi Alice!".to_owned())
            && bob
                .texts()
                .contains(&"Hey everyone, I just joined!".to_owned())
            && bob.texts().contains(&"How are you, Bob?".to_owned()),
    );
    checks.expect(
        "the welcomes are stored as system events from the framework",
        timeline
            .iter()
            .filter(|event| event.event_type == EventType::System)
            .all(|event| event.source.channel_id == ChannelId::system())
            && timeline
                .iter()
                .filter(|event| event.event_type == EventType::System)
                .count()
                == 2,
    );
    Ok(checks.finish())
}
