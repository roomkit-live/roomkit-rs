//! RFC Appendix B.5: an advisor joins a conversation between a customer on
//! SMS and an AI agent. The integrator mutes the agent, makes it visible to
//! the advisor only and unmutes it: the agent now whispers its suggestions
//! to the advisor, who answers the customer. Then the agent is made visible
//! to all again, and answers the customer directly.
//!
//! B.5's timeline also shows `CHANNEL_ATTACHED`, `CHANNEL_MUTED` and
//! `CHANNEL_UPDATED` rows: the RFC makes such lifecycle records optional,
//! and roomkit-rs reports these changes through framework events and hooks
//! instead (gap G-046), so its timeline holds the messages only.
//!
//! ```sh
//! cargo run -p roomkit --example advisor_joins
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod support;

use std::error::Error;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::{
    Access, BindingUpdate, ChannelBinding, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, EventContent, EventDirection, EventSource, EventType,
    InboundMessage, RoomContext, RoomEvent, RoomId, Timestamp, Visibility,
};
use roomkit::ops::{ChannelDeclaration, FrameworkEvent, SequentialEnvironment};
use roomkit::testing::RecordingTransport;
use roomkit::{Channel, ChannelError, HookTrigger, RoomKit, text_of};
use tokio::sync::{broadcast, mpsc};

use self::support::Checks;

/// What the agent answers, by what it reads. It stays silent on anything
/// else, the advisor's own messages among them.
const SCRIPT: [(&str, &str); 4] = [
    ("Bonjour", "Bonjour! How can I help?"),
    (
        "I need help with my mortgage",
        "I can help with mortgage info...",
    ),
    (
        "What rate can I get?",
        "Suggest offering 4.5% based on the customer's profile.",
    ),
    (
        "What documents do I need?",
        "You'll need: 1. ID 2. Income statements.",
    ),
];

/// An agent answering from [`SCRIPT`].
struct Assistant {
    declaration: ChannelDeclaration,
}

impl Assistant {
    fn new(channel_id: ChannelId) -> Self {
        Self {
            declaration: ChannelDeclaration {
                provider_name: Some("Assistant".to_owned()),
                ..ChannelDeclaration::new(
                    channel_id,
                    ChannelType::Ai,
                    ChannelCategory::Intelligence,
                    ChannelDirection::Bidirectional,
                )
            },
        }
    }

    fn answer(&self, event: &RoomEvent) -> Option<RoomEvent> {
        let read = text_of(&event.content);
        let (_, reply) = SCRIPT.iter().find(|(prompt, _)| *prompt == read)?;
        let source = EventSource::new(
            self.declaration.channel_id.clone(),
            ChannelType::Ai,
            EventDirection::Outbound,
        );
        // The pipeline gives the response its own id, room and depth.
        Some(RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text(*reply),
            event.created_at,
        ))
    }
}

impl Channel for Assistant {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let output = match self.answer(&event) {
            Some(answer) => ChannelOutput::respond(vec![answer]),
            None => ChannelOutput::default(),
        };
        ready(Ok(output))
    }
}

/// The room of the flow, with its channels and what the example observes.
struct Conversation {
    kit: RoomKit,
    room: RoomId,
    customer: RecordingTransport,
    advisor: RecordingTransport,
    agent: ChannelId,
    /// The payloads of the mute and unmute hooks, as they fire.
    mute_hooks: mpsc::UnboundedReceiver<String>,
    events: broadcast::Receiver<FrameworkEvent>,
}

impl Conversation {
    /// A room with the customer on SMS and the agent; the advisor's
    /// WebSocket is registered, not attached yet.
    async fn open() -> Result<Self, Box<dyn Error>> {
        let kit = RoomKit::new();
        kit.set_environment(SequentialEnvironment::new(Timestamp::from_unix_millis(
            1_792_677_780_000,
        )?));
        let customer = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::Sms);
        let advisor =
            RecordingTransport::new(ChannelId::new("ws_advisor")?, ChannelType::Websocket);
        let agent = ChannelId::new("ai_support")?;
        kit.register_channel(customer.clone())?;
        kit.register_channel(advisor.clone())?;
        kit.register_channel(Assistant::new(agent.clone()))?;
        let mute_hooks = observe_mutes(&kit)?;
        let events = kit.subscribe();
        let room = RoomId::new("room_1")?;
        kit.create_room(&room).await?;
        kit.attach(&room, customer.id()).await?;
        kit.attach(&room, &agent).await?;
        Ok(Self {
            kit,
            room,
            customer,
            advisor,
            agent,
            mute_hooks,
            events,
        })
    }

    async fn customer_says(&self, text: &str) -> Result<(), Box<dyn Error>> {
        self.say(&self.customer, "+15551234567", text).await
    }

    async fn advisor_says(&self, text: &str) -> Result<(), Box<dyn Error>> {
        self.say(&self.advisor, "advisor_marie", text).await
    }

    async fn say(
        &self,
        on: &RecordingTransport,
        sender: &str,
        text: &str,
    ) -> Result<(), Box<dyn Error>> {
        println!("{sender} writes {text:?}.");
        let message = InboundMessage::new(on.id().clone(), sender, EventContent::text(text));
        self.kit.process_inbound(&self.room, message).await?;
        Ok(())
    }

    /// Sets who sees what the agent writes (RFC §7.3).
    async fn agent_visible_to(&self, visibility: Visibility) -> Result<(), Box<dyn Error>> {
        let update = BindingUpdate::visibility(visibility);
        self.kit
            .update_binding(&self.room, &self.agent, update)
            .await?;
        Ok(())
    }

    /// The flow of B.5.
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        self.customer_says("Bonjour").await?;
        self.customer_says("I need help with my mortgage").await?;

        println!("The advisor joins.");
        self.kit.attach(&self.room, self.advisor.id()).await?;
        println!(
            "The integrator mutes the agent, makes it visible to the advisor only, unmutes it."
        );
        self.kit.mute(&self.room, &self.agent).await?;
        self.agent_visible_to(Visibility::channels([self.advisor.id().clone()])?)
            .await?;
        self.kit.unmute(&self.room, &self.agent).await?;

        self.customer_says("What rate can I get?").await?;
        self.advisor_says("We can offer you 4.5% fixed.").await?;

        println!("The advisor lets the agent answer the customer directly again.");
        self.agent_visible_to(Visibility::All).await?;
        self.customer_says("What documents do I need?").await?;
        Ok(())
    }

    /// The mute and unmute hooks that fired. They run as observers, in the
    /// background: two are waited for, then a moment for any other.
    async fn mute_hooks_fired(&mut self) -> Vec<String> {
        let mut fired = Vec::new();
        for wait in [1000, 1000, 100] {
            let next = tokio::time::timeout(Duration::from_millis(wait), self.mute_hooks.recv());
            match next.await {
                Ok(Some(hook)) => fired.push(hook),
                Ok(None) | Err(_) => break,
            }
        }
        fired.sort();
        fired
    }

    /// The advisor's `room_channel_attached` event, if it was emitted.
    fn advisor_attached(&mut self) -> Option<FrameworkEvent> {
        let advisor = self.advisor.id().clone();
        std::iter::from_fn(|| self.events.try_recv().ok()).find(|event| {
            matches!(
                event,
                FrameworkEvent::RoomChannelAttached { channel_id, .. } if *channel_id == advisor
            )
        })
    }
}

/// Registers observers of `ON_CHANNEL_MUTED` and `ON_CHANNEL_UNMUTED`; the
/// receiver gets each one's payload text as it fires.
fn observe_mutes(kit: &RoomKit) -> Result<mpsc::UnboundedReceiver<String>, Box<dyn Error>> {
    let (fired, receiver) = mpsc::unbounded_channel();
    for (name, trigger) in [
        ("on_muted", HookTrigger::OnChannelMuted),
        ("on_unmuted", HookTrigger::OnChannelUnmuted),
    ] {
        let fired = fired.clone();
        kit.observe(name, trigger, move |event, _| {
            let fired = fired.clone();
            async move {
                // Once the example stops listening, there is no one to tell.
                let _told = fired.send(text_of(&event.content));
            }
        })?;
    }
    Ok(receiver)
}

/// Whether one of `texts` is the agent's suggestion.
fn has_suggestion(texts: &[String]) -> bool {
    texts
        .iter()
        .any(|text| text.starts_with("Suggest offering"))
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut conversation = Conversation::open().await?;
    conversation.run().await?;

    let timeline = support::timeline(&conversation.kit, &conversation.room).await?;
    support::print_timeline(&timeline);
    let (customer, advisor) = (conversation.customer.texts(), conversation.advisor.texts());
    println!("Delivered on SMS: {customer:?}");
    println!("Delivered to the advisor: {advisor:?}");
    let attached = conversation.advisor_attached();
    let hooks = conversation.mute_hooks_fired().await;
    let whisper = timeline
        .iter()
        .find(|event| has_suggestion(&[text_of(&event.content)]));

    let mut checks = Checks::default();
    checks.expect(
        "the advisor joins mid-conversation, read-write and visible to all",
        matches!(
            attached,
            Some(FrameworkEvent::RoomChannelAttached {
                access: Access::ReadWrite,
                visibility: Visibility::All,
                ..
            })
        ),
    );
    checks.expect(
        "muting and unmuting the agent fire ON_CHANNEL_MUTED and ON_CHANNEL_UNMUTED once each, its visibility changes neither",
        hooks == ["channel muted", "channel unmuted"],
    );
    checks.expect(
        "the agent's suggestion is committed visible to the advisor only",
        whisper.is_some_and(|event| event.visibility.to_string() == "ws_advisor"),
    );
    checks.expect(
        "the suggestion reaches the advisor, not the customer",
        has_suggestion(&advisor) && !has_suggestion(&customer),
    );
    checks.expect(
        "the advisor's offer reaches the customer",
        customer.contains(&"We can offer you 4.5% fixed.".to_owned()),
    );
    checks.expect(
        "visible to all again, the agent answers the customer directly",
        customer.last().map(String::as_str) == Some("You'll need: 1. ID 2. Income statements."),
    );
    checks.expect(
        "the timeline holds the nine messages in index order, and no lifecycle record",
        timeline.len() == 9
            && timeline
                .iter()
                .zip(0..)
                .all(|(event, index)| event.index == index),
    );
    Ok(checks.finish())
}
