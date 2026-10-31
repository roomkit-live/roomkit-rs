//! Direct event injection (RFC §10.5, §10.1 step 18, §13.5). The host puts
//! an event into a room without a channel's message, from a back office or
//! a scheduled job, with `send_event`: the event takes the inbound pipeline
//! from the room lock on, hooks and permissions included, and the call
//! returns it committed once its delivery is over. Its idempotency key
//! makes a retry harmless, and a channel the room does not bind is refused
//! with nothing written. An agent's turn calls `send_event` into its own
//! room too: the call is a new arrival, and returns at its decision, the
//! event committed while its delivery follows the turn's own in the room's
//! lane.
//!
//! ```sh
//! cargo run --example send_event
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::Future;
use std::process::ExitCode;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventId, EventSource, InboundMessage, RoomContext, RoomEvent, RoomId,
    TextContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    FrameworkEvent, ProcessInboundOptions, RoomKit, RoomKitError, SendEventOptions, WeakRoomKit,
};

use shared::text_of;
use shared::{Checks, Log, recording};

/// The note an agent's turn sent, as `send_event` returned it, and whether
/// the customer had received it by then.
#[derive(Clone, Debug)]
struct Note {
    event: RoomEvent,
    delivered_yet: bool,
}

/// An agent whose turn, on each of the customer's messages, first posts a
/// note into the room with `send_event`, then answers. The kit holds the
/// agent, so the agent holds the kit weakly.
struct Agent {
    declaration: ChannelDeclaration,
    kit: WeakRoomKit,
    customer: RecordingTransport,
    notes: Log<Note>,
}

impl Agent {
    fn new(
        channel_id: ChannelId,
        kit: &RoomKit,
        customer: &RecordingTransport,
        notes: &Log<Note>,
    ) -> Self {
        let declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Self {
            declaration,
            kit: kit.downgrade(),
            customer: customer.clone(),
            notes: notes.clone(),
        }
    }

    /// The agent's answer to `event`.
    fn answer(&self, event: &RoomEvent) -> RoomEvent {
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.declaration.channel_id.clone(), ChannelType::AI)
        };
        // The pipeline gives the answer its own id, room and depth.
        RoomEvent {
            id: event.id.clone(),
            created_at: event.created_at,
            ..RoomEvent::new(
                event.room_id.clone(),
                source,
                TextContent::new("Your refund was issued yesterday."),
            )
        }
    }
}

impl Channel for Agent {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let from_customer = event.source.channel_id == *self.customer.channel_id();
        let answer = self.answer(&event);
        let (kit, customer, notes, agent) = (
            self.kit.upgrade(),
            self.customer.clone(),
            self.notes.clone(),
            self.declaration.channel_id.clone(),
        );
        async move {
            if !from_customer {
                return Ok(ChannelOutput::default());
            }
            // A call from a turn into its own room returns at its decision.
            let kit = kit.ok_or("the kit was dropped")?;
            let note = TextContent::new("Let me check your refund...");
            let sent = kit
                .send_event(&event.room_id, &agent, note, SendEventOptions::default())
                .await?;
            let delivered_yet = customer.texts().contains(&text_of(&sent.content));
            notes.push(Note {
                event: sent,
                delivered_yet,
            });
            Ok(ChannelOutput {
                responded: true,
                response_events: vec![answer],
                ..ChannelOutput::empty()
            })
        }
    }
}

/// Whether `events` holds the `event_processed` of `event_id` (RFC §8.2).
fn reported_processed(events: &Log<FrameworkEvent>, event_id: &EventId) -> bool {
    let reported = Some(event_id);
    events
        .entries()
        .iter()
        .any(|event| event.event_id.as_ref() == reported)
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let customer = recording("sms", ChannelType::SMS)?;
    let console = recording("console", ChannelType::WEBSOCKET)?;
    let crm = recording("crm", ChannelType::WEBSOCKET)?;
    let notes = Log::new();
    let agent = Agent::new(ChannelId::new("agent")?, &kit, &customer, &notes);
    let agent_id = agent.declaration.channel_id.clone();
    kit.register_channel(customer.clone())?;
    kit.register_channel(console.clone())?;
    kit.register_channel(crm.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("support")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in [customer.channel_id(), console.channel_id(), &agent_id] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await?;
    }
    let mut checks = Checks::default();

    println!("The back office tells the customer their order shipped, and retries.");
    let shipped = || TextContent::new("Your order #42 has shipped.");
    let keyed = || SendEventOptions {
        idempotency_key: Some("order_42_shipped".to_owned()),
        ..Default::default()
    };
    let sent = kit
        .send_event(&room, console.channel_id(), shipped(), keyed())
        .await?;
    let delivered_on_return = customer.texts();
    let retried = kit
        .send_event(&room, console.channel_id(), shipped(), keyed())
        .await?;
    // The three attaches' lifecycle records take indexes 0 to 2 (RFC §5.1,
    // gap G-046).
    checks.expect(
        "the injected event is committed and delivered when send_event returns",
        sent.index == 3 && delivered_on_return == ["Your order #42 has shipped."],
    );
    checks.expect(
        "a retry with the same idempotency key returns the first event, and delivers nothing",
        retried.id == sent.id && customer.texts().len() == 1,
    );

    println!("The CRM, registered but not bound to the room, injects an event.");
    let refused = kit
        .send_event(
            &room,
            crm.channel_id(),
            TextContent::new("Account updated."),
            SendEventOptions::default(),
        )
        .await;
    checks.expect(
        "a channel the room does not bind is refused as not found in the room, nothing written",
        matches!(
            &refused,
            Err(RoomKitError::ChannelNotFound { channel_id, .. }) if channel_id == crm.channel_id()
        ) && shared::timeline(&kit, &room).await?.len() == 4,
    );

    println!("The customer asks; the agent's turn posts a note, then answers.");
    let events = shared::framework_events(&kit, &["event_processed"]);
    let question = InboundMessage::new(
        customer.channel_id().clone(),
        "+15551234567",
        TextContent::new("Where is my refund?"),
    );
    kit.process_inbound(
        question,
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    )
    .await?;
    let note = notes
        .entries()
        .first()
        .cloned()
        .ok_or("the agent sent no note")?;
    // Nothing awaits the note's delivery: it runs in the background, which
    // the kit's close lets finish, and the framework reports its end.
    kit.close().await?;
    let processed = reported_processed(&events, &note.event.id);
    shared::print_timeline(&shared::timeline(&kit, &room).await?);
    println!("Delivered on SMS: {:?}", customer.texts());
    checks.expect(
        "the turn's send_event returns the note committed, before its delivery",
        note.event.index == 5 && !note.delivered_yet,
    );
    checks.expect(
        "the note is delivered once the turn is over, ahead of the answer committed after it",
        processed
            && customer.texts()
                == [
                    "Your order #42 has shipped.",
                    "Let me check your refund...",
                    "Your refund was issued yesterday.",
                ],
    );
    Ok(checks.finish())
}
