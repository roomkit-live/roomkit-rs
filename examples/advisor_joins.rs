//! RFC Appendix B.5: an advisor joins a conversation between a customer on
//! SMS and an AI agent. The integrator mutes the agent, makes it visible to
//! the advisor only and unmutes it: the agent now whispers its suggestions
//! to the advisor, who answers the customer. Then the agent is made visible
//! to all again, and answers the customer directly.
//!
//! B.5's timeline numbers the messages alone. roomkit-rs writes a
//! lifecycle record of each change, as the Python implementation does (RFC
//! §5.1, gap G-046): a `CHANNEL_ATTACHED`, `CHANNEL_MUTED`, `CHANNEL_UPDATED`
//! or `CHANNEL_UNMUTED` event at the room's next index, `internal` and
//! never delivered, so its timeline holds them between the messages.
//!
//! ```sh
//! cargo run --example advisor_joins
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    Access, ChannelId, ChannelType, EventContent, InboundMessage, RoomId, Visibility,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, FrameworkEvent, HookTrigger, MuteOptions,
    ProcessInboundOptions, RoomKit, SetVisibilityOptions, UnmuteOptions, text_of,
};
use tokio::sync::broadcast;

use shared::{Checks, Log, Script, ScriptedAgent};

/// What the agent answers, by what it reads. It stays silent on anything
/// else, the advisor's own messages among them.
const SCRIPT: Script = &[
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

/// The room of the flow, with its channels and what the example observes.
struct Conversation {
    kit: RoomKit,
    room: RoomId,
    customer: RecordingTransport,
    advisor: RecordingTransport,
    agent: ChannelId,
    /// The payloads of the mute and unmute hooks, as they fire.
    mute_hooks: Log<String>,
    events: broadcast::Receiver<FrameworkEvent>,
}

impl Conversation {
    /// A room with the customer on SMS and the agent; the advisor's
    /// WebSocket is registered, not attached yet.
    async fn open() -> Result<Self, Box<dyn Error>> {
        let kit = shared::kit();
        let customer = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::SMS);
        let advisor =
            RecordingTransport::new(ChannelId::new("ws_advisor")?, ChannelType::WEBSOCKET);
        let agent = ChannelId::new("ai_support")?;
        kit.register_channel(customer.clone())?;
        kit.register_channel(advisor.clone())?;
        kit.register_channel(ScriptedAgent::new(agent.clone(), SCRIPT))?;
        let mute_hooks = Log::new();
        observe_mutes(&kit, &mute_hooks)?;
        let events = kit.subscribe();
        let room = RoomId::new("room_1")?;
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await?;
        kit.attach_channel(&room, customer.id(), AttachChannelOptions::default())
            .await?;
        kit.attach_channel(&room, &agent, AttachChannelOptions::default())
            .await?;
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
        self.kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(self.room.clone()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Sets who sees what the agent writes (RFC §7.3).
    async fn agent_visible_to(&self, visibility: Visibility) -> Result<(), Box<dyn Error>> {
        self.kit
            .set_visibility(
                &self.room,
                &self.agent,
                visibility,
                SetVisibilityOptions::default(),
            )
            .await?;
        Ok(())
    }

    /// The flow of B.5.
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        self.customer_says("Bonjour").await?;
        self.customer_says("I need help with my mortgage").await?;

        println!("The advisor joins.");
        self.kit
            .attach_channel(
                &self.room,
                self.advisor.id(),
                AttachChannelOptions::default(),
            )
            .await?;
        println!(
            "The integrator mutes the agent, makes it visible to the advisor only, unmutes it."
        );
        self.kit
            .mute(&self.room, &self.agent, MuteOptions::default())
            .await?;
        self.agent_visible_to(Visibility::channels([self.advisor.id().clone()])?)
            .await?;
        self.kit
            .unmute(&self.room, &self.agent, UnmuteOptions::default())
            .await?;

        self.customer_says("What rate can I get?").await?;
        self.advisor_says("We can offer you 4.5% fixed.").await?;

        println!("The advisor lets the agent answer the customer directly again.");
        self.agent_visible_to(Visibility::All).await?;
        self.customer_says("What documents do I need?").await?;
        Ok(())
    }

    /// The mute and unmute hooks that fired. They run as observers, in the
    /// background: the kit is closed first, which lets them finish.
    async fn mute_hooks_fired(&self) -> Vec<String> {
        self.kit.close().await;
        let mut fired = self.mute_hooks.entries();
        fired.sort();
        fired
    }

    /// The advisor's `room_channel_attached` event, if it was emitted.
    fn advisor_attached(&mut self) -> Option<FrameworkEvent> {
        let advisor = self.advisor.id().clone();
        shared::framework_events(&mut self.events)
            .into_iter()
            .find(|event| {
                matches!(
                    event,
                    FrameworkEvent::RoomChannelAttached { channel_id, .. } if *channel_id == advisor
                )
            })
    }
}

/// Registers observers of `ON_CHANNEL_MUTED` and `ON_CHANNEL_UNMUTED`,
/// which note each one's payload text in `fired`.
fn observe_mutes(kit: &RoomKit, fired: &Log<String>) -> Result<(), Box<dyn Error>> {
    for (name, trigger) in [
        ("on_muted", HookTrigger::OnChannelMuted),
        ("on_unmuted", HookTrigger::OnChannelUnmuted),
    ] {
        let fired = fired.clone();
        kit.observe(name, trigger, move |event, _| {
            let fired = fired.clone();
            async move { fired.push(text_of(&event.content)) }
        })?;
    }
    Ok(())
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

    let timeline = shared::timeline(&conversation.kit, &conversation.room).await?;
    shared::print_timeline(&timeline);
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
    let (records, messages): (Vec<_>, Vec<_>) = timeline
        .iter()
        .partition(|event| event.visibility == Visibility::Internal);
    checks.expect(
        "the timeline holds the nine messages and a lifecycle record of each of the seven changes, in index order",
        messages.len() == 9
            && records.len() == 7
            && timeline
                .iter()
                .zip(0..)
                .all(|(event, index)| event.index == index),
    );
    Ok(checks.finish())
}
