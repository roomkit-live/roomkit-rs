//! The room lifecycle (RFC §5.1, §8.2). The integrator pauses a room, which
//! still takes messages and is resumed by the first one delivered; closes
//! it, so that it refuses every message with nothing written; reopens it;
//! then archives it for good: a reopen or a close of the archived room is
//! refused, since a room never leaves ARCHIVED, and archiving it again
//! does nothing. A second room has timers: a minute without activity
//! pauses it, two minutes paused close it, and `check_timers` applies both,
//! each at the instant it fell due.
//! `ON_ROOM_PAUSED` and `ON_ROOM_CLOSED` fire with every transition.
//!
//! ```sh
//! cargo run --example room_lifecycle
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, InboundMessage, Room, RoomId, RoomStatus, RoomTimers,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    ConversationStore, FrameworkEvent, HookTrigger, InboundResult, KitError, LifecycleError,
    RoomKit, TransitionReason, text_of,
};

use shared::{Checks, Clock, Log};

/// The helpdesk's kit, with Alice's SMS and the advisor's WebSocket, which
/// every room of the example binds.
struct Helpdesk {
    kit: RoomKit,
    clock: Clock,
    sms: RecordingTransport,
    advisor: RecordingTransport,
}

impl Helpdesk {
    fn new() -> Result<Self, Box<dyn Error>> {
        let clock = Clock::new();
        let kit = clock.kit();
        let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
        let advisor =
            RecordingTransport::new(ChannelId::new("ws_advisor")?, ChannelType::Websocket);
        kit.register_channel(sms.clone())?;
        kit.register_channel(advisor.clone())?;
        Ok(Self {
            kit,
            clock,
            sms,
            advisor,
        })
    }

    /// Creates `room` with `timers`, Alice's SMS and the advisor attached.
    async fn open(&self, room: &RoomId, timers: RoomTimers) -> Result<(), Box<dyn Error>> {
        self.kit
            .create_room_with(room, |room| room.timers = timers)
            .await?;
        self.kit.attach(room, self.sms.id()).await?;
        self.kit.attach(room, self.advisor.id()).await?;
        Ok(())
    }

    async fn alice_says(&self, room: &RoomId, text: &str) -> Result<InboundResult, Box<dyn Error>> {
        println!("  Alice writes {text:?}.");
        let message = InboundMessage::new(
            self.sms.id().clone(),
            "+15551234567",
            EventContent::text(text),
        );
        Ok(self.kit.process_inbound_in(room, message).await?)
    }
}

/// Registers observers of `ON_ROOM_PAUSED` and `ON_ROOM_CLOSED`, which note
/// the room and the change in `transitions`.
fn observe_transitions(kit: &RoomKit, transitions: &Log<String>) -> Result<(), Box<dyn Error>> {
    for (name, trigger) in [
        ("on_paused", HookTrigger::OnRoomPaused),
        ("on_closed", HookTrigger::OnRoomClosed),
    ] {
        let transitions = transitions.clone();
        kit.observe(name, trigger, move |event, context| {
            let transitions = transitions.clone();
            async move {
                transitions.push(format!("{}: {}", context.room.id, text_of(&event.content)));
            }
        })?;
    }
    Ok(())
}

/// The integrator pauses, closes, reopens and archives the room `support`,
/// while Alice writes.
async fn by_hand(desk: &Helpdesk, checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    let (kit, room) = (&desk.kit, RoomId::new("support")?);
    println!("The integrator runs the room `support` through its lifecycle.");
    desk.open(&room, RoomTimers::default()).await?;
    desk.alice_says(&room, "Hello").await?;

    let paused = kit.pause_room(&room).await?;
    println!("  Paused.");
    let while_paused = desk.alice_says(&room, "Anyone there?").await?;
    let after_message = kit.store().get_room(&room).await?;
    checks.expect(
        "a paused room takes a message, and the message delivered resumes it",
        paused.status == RoomStatus::Paused
            && while_paused.event.is_some()
            && after_message.is_some_and(|room| room.status == RoomStatus::Active),
    );

    kit.pause_room(&room).await?;
    let resumed = kit.resume_room(&room).await?;
    println!("  Paused, then resumed by the integrator.");
    checks.expect(
        "the integrator resumes a paused room",
        resumed.status == RoomStatus::Active && resumed.paused_at.is_none(),
    );

    kit.close_room(&room).await?;
    println!("  Closed.");
    let while_closed = desk.alice_says(&room, "Hello?").await?;
    checks.expect(
        "a closed room refuses a message room_closed, writing nothing",
        while_closed.blocked
            && while_closed.reason.as_deref() == Some("room_closed")
            && while_closed.event.is_none(),
    );

    kit.reopen_room(&room).await?;
    println!("  Reopened.");
    let after_reopen = desk.alice_says(&room, "Back again").await?;
    checks.expect(
        "a reopened room takes messages again",
        after_reopen.event.is_some(),
    );

    kit.archive_room(&room).await?;
    println!("  Archived.");
    let while_archived = desk.alice_says(&room, "Still here?").await?;
    checks.expect(
        "an archived room refuses messages as a closed one does",
        while_archived.reason.as_deref() == Some("room_closed"),
    );
    let refused = [kit.reopen_room(&room).await, kit.close_room(&room).await];
    for error in refused.iter().filter_map(|result| result.as_ref().err()) {
        println!("  {error}.");
    }
    let archived_again = kit.archive_room(&room).await?;
    checks.expect(
        "an archived room is never left: a reopen or a close is refused, a new archive does nothing",
        refused.iter().all(|result| {
            matches!(
                result,
                Err(KitError::Lifecycle(
                    LifecycleError::TransitionRefused { .. }
                ))
            )
        }) && archived_again.status == RoomStatus::Archived,
    );
    checks.expect(
        "the advisor received the three messages delivered, and the room holds nothing it refused",
        desk.advisor.texts() == ["Hello", "Anyone there?", "Back again"]
            && shared::timeline(kit, &room).await?.len() == 3,
    );
    Ok(())
}

/// The room `timed` pauses after a minute without activity and closes two
/// minutes after the pause; the host checks its timers as the clock moves.
async fn by_timers(desk: &Helpdesk, checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    let (kit, room) = (&desk.kit, RoomId::new("timed")?);
    println!("The room `timed` pauses after 60 s without activity, and closes 120 s later.");
    let timers = RoomTimers {
        inactive_after_seconds: Some(60),
        closed_after_seconds: Some(120),
        last_activity_at: None,
    };
    desk.open(&room, timers).await?;
    desk.alice_says(&room, "Hello").await?;
    let mut events = kit.subscribe();

    desk.clock.advance(Duration::from_secs(90));
    let paused = kit.check_timers().await?;
    println!("  90 s later, check_timers changes {:?}", statuses(&paused));
    desk.clock.advance(Duration::from_secs(150));
    let closed = kit.check_timers().await?;
    println!(
        "  240 s later, check_timers changes {:?}",
        statuses(&closed)
    );
    let reported = shared::framework_events(&mut events);

    checks.expect(
        "the first check pauses the room, the second closes it",
        statuses(&paused) == [("timed".to_owned(), RoomStatus::Paused)]
            && statuses(&closed) == [("timed".to_owned(), RoomStatus::Closed)],
    );
    let paused_at = paused.first().and_then(|room| room.paused_at);
    let closed_at = closed.first().and_then(|room| room.closed_at);
    checks.expect(
        "the close takes the instant it fell due, 120 s after the pause, not the check's",
        paused_at
            .and_then(|paused_at| paused_at.checked_add(Duration::from_secs(120)))
            .is_some_and(|due| closed_at == Some(due)),
    );
    checks.expect(
        "each transition is reported with the reason timer",
        reported.contains(&FrameworkEvent::RoomPaused {
            room_id: room.clone(),
            reason: TransitionReason::Timer,
        }) && reported.contains(&FrameworkEvent::RoomClosed {
            room_id: room.clone(),
            reason: Some(TransitionReason::Timer),
        }),
    );
    Ok(())
}

/// The rooms a timer check changed, with their status.
fn statuses(rooms: &[Room]) -> Vec<(String, RoomStatus)> {
    rooms
        .iter()
        .map(|room| (room.id.to_string(), room.status))
        .collect()
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let desk = Helpdesk::new()?;
    let transitions = Log::new();
    observe_transitions(&desk.kit, &transitions)?;
    let mut checks = Checks::default();

    by_hand(&desk, &mut checks).await?;
    by_timers(&desk, &mut checks).await?;

    // The hooks run in the background, after the calls that fired them.
    desk.kit.idle().await;
    let mut fired = transitions.entries();
    fired.sort();
    println!("Lifecycle hooks: {fired:?}");
    checks.expect(
        "ON_ROOM_PAUSED and ON_ROOM_CLOSED fire for each room, by hand or by timer",
        fired
            == [
                "support: room closed",
                "support: room paused",
                "support: room paused",
                "timed: room closed",
                "timed: room paused",
            ],
    );
    Ok(checks.finish())
}
