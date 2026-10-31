//! RFC Appendix B.6 and B.7: a WhatsApp customer edits a message, then
//! deletes another. Each change reaches the advisor's WebSocket natively
//! and the SMS copy as text, and the agent reads it with the target
//! corrected in its history. The target is updated in the change's commit,
//! and `ON_EVENT_UPDATED` and `ON_EVENT_DELETED` fire with it once the room
//! lock is released, with the room as the commit left it: the change the
//! newest event of its history (RFC §10.3). Then a moderation hook blocks an
//! edit that would write a card number: its target stays as it was, and no
//! trigger fires.
//!
//! ```sh
//! cargo run --example edit_delete_messages
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]
use std::error::Error;
use std::process::ExitCode;

use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelCapabilities, ChannelId, ChannelType, DeleteContent, EditContent, EditSource,
    EventContent, EventId, EventType, HookResult, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookTrigger, ProcessInboundOptions,
    RoomKit,
};

use shared::text_of;
use shared::{Checks, Log, ScriptedAgent, is_deleted, is_edited};

/// The customer's number.
const CUSTOMER: &str = "+15551234567";

/// What the customer writes before the changes, `I need 5000$` the fourth
/// (index 3 in B.6) and `Sorry, wrong chat` the sixth (index 5 in B.7). The
/// four attaches' lifecycle records come first (RFC §5.1, gap G-046), which
/// B.6 and B.7 do not write: the messages take indexes 4 to 9 here.
const MESSAGES: [&str; 6] = [
    "Hello",
    "I have a question about a transfer",
    "Is it possible today?",
    "I need 5000$",
    "To my savings account",
    "Sorry, wrong chat",
];

/// Text, with edits and deletes delivered natively (RFC §5.8, §6.6).
fn edits_natively() -> ChannelCapabilities {
    ChannelCapabilities {
        supports_edit: true,
        supports_delete: true,
        ..ChannelCapabilities::default()
    }
}

/// A mutation trigger that fired: the trigger, its payload, and the id of
/// the newest event of the room's history it was handed.
type Change = (HookTrigger, RoomEvent, Option<EventId>);

/// The room of the flow, with its channels and what the example observes.
struct Conversation {
    kit: RoomKit,
    room: RoomId,
    customer: RecordingTransport,
    advisor: RecordingTransport,
    sms: RecordingTransport,
    /// An agent that reads what it is asked and answers nothing.
    agent: ScriptedAgent,
    /// The ids of [`MESSAGES`], in order.
    sent: Vec<EventId>,
    /// The mutation triggers that fired, with their targets and the newest
    /// event of the room they were handed, as they fire.
    changed: Log<Change>,
}

impl Conversation {
    async fn open() -> Result<Self, Box<dyn Error>> {
        let kit = shared::kit();
        let customer = RecordingTransport::new(
            ChannelId::new("wa_customer")?,
            ChannelType::WHATSAPP,
            RecordingTransportOptions::default(),
        );
        let advisor = RecordingTransport::new(
            ChannelId::new("ws_advisor")?,
            ChannelType::WEBSOCKET,
            RecordingTransportOptions {
                capabilities: Some(edits_natively()),
            },
        );
        let sms = RecordingTransport::new(
            ChannelId::new("sms_copy")?,
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        let agent =
            ScriptedAgent::new(ChannelId::new("ai")?, &[]).with_capabilities(edits_natively());
        kit.register_channel(customer.clone())?;
        kit.register_channel(advisor.clone())?;
        kit.register_channel(sms.clone())?;
        kit.register_channel(agent.clone())?;
        let changed = Log::new();
        observe_changes(&kit, &changed)?;
        moderate_edits(&kit)?;
        let room = RoomId::new("room_1")?;
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await?;
        for channel_id in [
            customer.channel_id(),
            advisor.channel_id(),
            sms.channel_id(),
        ] {
            kit.attach_channel(&room, channel_id, AttachChannelOptions::default())
                .await?;
        }
        kit.attach_channel(&room, agent.id(), AttachChannelOptions::default())
            .await?;
        Ok(Self {
            kit,
            room,
            customer,
            advisor,
            sms,
            agent,
            sent: Vec::new(),
            changed,
        })
    }

    /// The mutation triggers that fired, once `count` did. They run as
    /// observers, in the background, after the call that fired them.
    async fn changes(&self, count: usize) -> Vec<Change> {
        self.changed.wait_for(count).await
    }

    /// The mutation triggers that fired, once the kit is closed, which lets
    /// the observers still running finish.
    async fn every_change(&self) -> Result<Vec<Change>, Box<dyn Error>> {
        self.kit.close().await?;
        Ok(self.changed.entries())
    }

    async fn send(
        &self,
        message: InboundMessage,
    ) -> Result<roomkit::InboundResult, Box<dyn Error>> {
        Ok(self
            .kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(self.room.clone()),
                    ..Default::default()
                },
            )
            .await?)
    }

    /// The customer writes [`MESSAGES`].
    async fn chat(&mut self) -> Result<(), Box<dyn Error>> {
        for text in MESSAGES {
            let message = InboundMessage::new(
                self.customer.channel_id().clone(),
                CUSTOMER,
                TextContent::new(text),
            );
            let committed = self
                .send(message)
                .await?
                .event
                .ok_or("a message was refused")?;
            self.sent.push(committed.id);
        }
        Ok(())
    }

    /// The customer's edit of the message at `index`, to `text` (B.6).
    fn edit(&self, index: usize, text: &str) -> Result<InboundMessage, Box<dyn Error>> {
        let target_event_id = self.sent.get(index).ok_or("no such message")?.clone();
        let mut message = InboundMessage::new(
            self.customer.channel_id().clone(),
            CUSTOMER,
            EventContent::Edit(EditContent {
                edit_source: Some(EditSource::Sender),
                ..EditContent::new(target_event_id, TextContent::new(text))
            }),
        );
        message.event_type = EventType::Edit;
        Ok(message)
    }

    /// The customer's delete of the message at `index` (B.7).
    fn delete(&self, index: usize) -> Result<InboundMessage, Box<dyn Error>> {
        let target_event_id = self.sent.get(index).ok_or("no such message")?.clone();
        let mut message = InboundMessage::new(
            self.customer.channel_id().clone(),
            CUSTOMER,
            EventContent::Delete(DeleteContent::new(target_event_id)),
        );
        message.event_type = EventType::Delete;
        Ok(message)
    }

    /// The stored message at `index`.
    async fn stored(&self, index: usize) -> Result<Option<RoomEvent>, Box<dyn Error>> {
        let id = self.sent.get(index).ok_or("no such message")?;
        Ok(self.kit.store().get_event(id).await?)
    }
}

/// Registers observers of `ON_EVENT_UPDATED` and `ON_EVENT_DELETED`, which
/// note each one's trigger, payload and the newest event of its room's
/// history in `changed`.
fn observe_changes(kit: &RoomKit, changed: &Log<Change>) -> Result<(), Box<dyn Error>> {
    for (name, trigger) in [
        ("projection_updated", HookTrigger::OnEventUpdated),
        ("projection_deleted", HookTrigger::OnEventDeleted),
    ] {
        let changed = changed.clone();
        kit.observe(name, trigger, move |event, context| {
            let changed = changed.clone();
            let newest = context.recent_events.last().map(|newest| newest.id.clone());
            async move { changed.push((trigger, event, newest)) }
        })?;
    }
    Ok(())
}

/// A card number: four groups of four digits.
fn has_card_number(text: &str) -> bool {
    let groups: Vec<&str> = text.split_whitespace().collect();
    groups.windows(4).any(|window| {
        window
            .iter()
            .all(|group| group.len() == 4 && group.chars().all(|c| c.is_ascii_digit()))
    })
}

/// Blocks an edit whose new text holds a card number (RFC §10.1 step 9).
fn moderate_edits(kit: &RoomKit) -> Result<(), Box<dyn Error>> {
    kit.before_broadcast("card_scanner", |event, _| async move {
        let corrects_to_a_card = if let EventContent::Edit(edit) = &event.content {
            has_card_number(&text_of(&edit.new_content))
        } else {
            false
        };
        Ok(if corrects_to_a_card {
            HookResult::block("card number in an edit", BlockOptions::default())
        } else {
            HookResult::allow(AllowOptions::default())
        })
    })?;
    Ok(())
}

/// The last event `transport` was delivered.
fn last_delivered(transport: &RecordingTransport) -> Option<RoomEvent> {
    transport
        .deliveries()
        .last()
        .map(|delivery| delivery.event.clone())
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut conversation = Conversation::open().await?;
    conversation.chat().await?;
    let mut checks = Checks::default();

    println!("B.6: the customer edits \"I need 5000$\" to \"I need 50000$\".");
    let edit = conversation.edit(3, "I need 50000$")?;
    let edited = conversation.send(edit).await?;
    let target = conversation.stored(3).await?;
    let to_advisor = last_delivered(&conversation.advisor);
    let read = conversation.agent.turns().last().cloned();
    let updated = conversation.changes(1).await.last().cloned();
    checks.expect(
        "the edit is stored after its target, which now reads \"I need 50000$\" and is marked edited",
        edited.event.as_ref().is_some_and(|event| event.index == 10)
            && target.as_ref().is_some_and(|target| target.content == TextContent::new("I need 50000$").into())
            && target.as_ref().is_some_and(is_edited),
    );
    checks.expect(
        "the WebSocket receives the edit natively",
        to_advisor.is_some_and(|event| matches!(event.content, EventContent::Edit(_))),
    );
    checks.expect(
        "SMS receives it as \"Correction: I need 50000$\"",
        conversation.sms.texts().last().map(String::as_str) == Some("Correction: I need 50000$"),
    );
    checks.expect(
        "the agent reads the edit, its history corrected",
        read.is_some_and(|turn| {
            turn.event.r#type == EventType::Edit
                && turn.history.contains(&"I need 50000$".to_owned())
        }),
    );
    let edit_id = edited.event.as_ref().map(|event| event.id.clone());
    checks.expect(
        "ON_EVENT_UPDATED fires with the edited target, the edit the newest event of its room",
        updated.is_some_and(|(trigger, event, newest)| {
            trigger == HookTrigger::OnEventUpdated && Some(event) == target && newest == edit_id
        }),
    );

    println!("B.7: the customer deletes \"Sorry, wrong chat\".");
    let delete = conversation.delete(5)?;
    let deleted = conversation.send(delete).await?;
    let target = conversation.stored(5).await?;
    let to_advisor = last_delivered(&conversation.advisor);
    let removed = conversation.changes(2).await.last().cloned();
    checks.expect(
        "the delete is stored after its target, which is marked deleted",
        deleted
            .event
            .as_ref()
            .is_some_and(|event| event.index == 11)
            && target.as_ref().is_some_and(is_deleted),
    );
    checks.expect(
        "the WebSocket receives the delete natively, SMS as \"[Message deleted]\"",
        to_advisor.is_some_and(|event| matches!(event.content, EventContent::Delete(_)))
            && conversation.sms.texts().last().map(String::as_str) == Some("[Message deleted]"),
    );
    let delete_id = deleted.event.as_ref().map(|event| event.id.clone());
    checks.expect(
        "ON_EVENT_DELETED fires with the deleted target, the delete the newest event of its room",
        removed.is_some_and(|(trigger, event, newest)| {
            trigger == HookTrigger::OnEventDeleted && Some(event) == target && newest == delete_id
        }),
    );

    println!("The customer tries to correct \"To my savings account\" with a card number.");
    let blocked = conversation.edit(4, "To card 4242 4242 4242 4242")?;
    let refused = conversation.send(blocked).await?;
    let target = conversation.stored(4).await?;
    let fired = conversation.every_change().await?;
    checks.expect(
        "the edit is blocked by the scanner, and its target still reads \"To my savings account\"",
        refused.blocked
            && refused.reason.as_deref() == Some("card number in an edit")
            && target.is_some_and(|target| {
                target.content == TextContent::new("To my savings account").into()
                    && target.metadata.is_empty()
            }),
    );
    checks.expect(
        "no mutation trigger fires for the blocked edit",
        fired.len() == 2,
    );
    checks.expect(
        "the card number reaches no channel",
        !conversation
            .advisor
            .texts()
            .iter()
            .chain(conversation.sms.texts().iter())
            .any(|text| has_card_number(text)),
    );

    let timeline = shared::timeline(&conversation.kit, &conversation.room).await?;
    shared::print_timeline(&timeline);
    println!("Delivered on SMS: {:?}", conversation.sms.texts());
    Ok(checks.finish())
}
