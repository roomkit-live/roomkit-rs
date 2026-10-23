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
    DirectEvent, EventContent, EventDirection, EventSource, EventType, InboundMessage, RoomContext,
    RoomEvent, RoomId,
};
use roomkit::ops::{ChannelDeclaration, FrameworkEvent, InboundError};
use roomkit::testing::RecordingTransport;
use roomkit::{Channel, ChannelError, KitError, RoomKit, text_of};

use shared::{Checks, Log};

/// The note an agent's turn sent, as `send_event` returned it, and whether
/// the customer had received it by then.
#[derive(Clone, Debug)]
struct Note {
    event: RoomEvent,
    delivered_yet: bool,
}

/// An agent whose turn, on each of the customer's messages, first posts a
/// note into the room with `send_event`, then answers.
struct Agent {
    declaration: ChannelDeclaration,
    kit: RoomKit,
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
            ChannelType::Ai,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Self {
            declaration,
            kit: kit.clone(),
            customer: customer.clone(),
            notes: notes.clone(),
        }
    }

    /// The agent's answer to `event`.
    fn answer(&self, event: &RoomEvent) -> RoomEvent {
        let source = EventSource::new(
            self.declaration.channel_id.clone(),
            ChannelType::Ai,
            EventDirection::Outbound,
        );
        // The pipeline gives the answer its own id, room and depth.
        RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text("Your refund was issued yesterday."),
            event.created_at,
        )
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
        let from_customer = event.source.channel_id == *self.customer.id();
        let answer = self.answer(&event);
        let note = DirectEvent::new(
            self.declaration.channel_id.clone(),
            EventContent::text("Let me check your refund..."),
        );
        let (kit, customer, notes) = (self.kit.clone(), self.customer.clone(), self.notes.clone());
        async move {
            if !from_customer {
                return Ok(ChannelOutput::default());
            }
            // A call from a turn into its own room returns at its decision.
            let sent = kit.send_event(&event.room_id, note).await?;
            let delivered_yet = customer.texts().contains(&text_of(&sent.content));
            notes.push(Note {
                event: sent,
                delivered_yet,
            });
            Ok(ChannelOutput::respond(vec![answer]))
        }
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let customer = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
    let console = RecordingTransport::new(ChannelId::new("console")?, ChannelType::Websocket);
    let crm = RecordingTransport::new(ChannelId::new("crm")?, ChannelType::Websocket);
    let notes = Log::new();
    let agent = Agent::new(ChannelId::new("agent")?, &kit, &customer, &notes);
    let agent_id = agent.declaration.channel_id.clone();
    kit.register_channel(customer.clone())?;
    kit.register_channel(console.clone())?;
    kit.register_channel(crm.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("support")?;
    kit.create_room(&room).await?;
    for channel in [customer.id(), console.id(), &agent_id] {
        kit.attach(&room, channel).await?;
    }
    let mut checks = Checks::default();

    println!("The back office tells the customer their order shipped, and retries.");
    let shipped = DirectEvent {
        idempotency_key: Some("order_42_shipped".to_owned()),
        ..DirectEvent::new(
            console.id().clone(),
            EventContent::text("Your order #42 has shipped."),
        )
    };
    let sent = kit.send_event(&room, shipped.clone()).await?;
    let delivered_on_return = customer.texts();
    let retried = kit.send_event(&room, shipped).await?;
    checks.expect(
        "the injected event is committed and delivered when send_event returns",
        sent.index == 0 && delivered_on_return == ["Your order #42 has shipped."],
    );
    checks.expect(
        "a retry with the same idempotency key returns the first event, and delivers nothing",
        retried.id == sent.id && customer.texts().len() == 1,
    );

    println!("The CRM, registered but not bound to the room, injects an event.");
    let unbound = DirectEvent::new(crm.id().clone(), EventContent::text("Account updated."));
    let refused = kit.send_event(&room, unbound).await;
    checks.expect(
        "a channel the room does not bind is refused source_unbound, nothing written",
        matches!(
            &refused,
            Err(KitError::Inbound(InboundError::Refused { reason })) if reason == "source_unbound"
        ) && shared::timeline(&kit, &room).await?.len() == 1,
    );

    println!("The customer asks; the agent's turn posts a note, then answers.");
    let mut events = kit.subscribe();
    let question = InboundMessage::new(
        customer.id().clone(),
        "+15551234567",
        EventContent::text("Where is my refund?"),
    );
    kit.process_inbound(question, Some(&room)).await?;
    let note = notes
        .entries()
        .first()
        .cloned()
        .ok_or("the agent sent no note")?;
    // Nothing awaits the note's delivery: the framework reports its end.
    let processed = shared::next_event(&mut events, |event| {
        matches!(event, FrameworkEvent::EventProcessed { event_id, .. } if *event_id == note.event.id)
    })
    .await;
    shared::print_timeline(&shared::timeline(&kit, &room).await?);
    println!("Delivered on SMS: {:?}", customer.texts());
    checks.expect(
        "the turn's send_event returns the note committed, before its delivery",
        note.event.index == 2 && !note.delivered_yet,
    );
    checks.expect(
        "the note is delivered once the turn is over, ahead of the answer committed after it",
        processed.is_some()
            && customer.texts()
                == [
                    "Your order #42 has shipped.",
                    "Let me check your refund...",
                    "Your refund was issued yesterday.",
                ],
    );
    Ok(checks.finish())
}
