//! The host writes to a room's people in its own name: `RoomKit::post`
//! (RFC §10.5, Appendix A.14.1).
//!
//! ```text
//! shop backend ──post()──▶ Room ──┬──▶ SMS "sms"            (the customer)
//!                                 └──▶ agent "assistant"     (answers her)
//! ```
//!
//! A shop's backend texts a customer that her order shipped. The message
//! is the host's own words, not an agent's: it reaches the customer's
//! phone, and no agent answers it. When the customer replies, the agent
//! answers as usual. Later the backend asks the agent, in front of the
//! customer, to confirm the address: it names the agent, and speaks as
//! "desk", a member added on the host channel. A post naming the customer,
//! reached through SMS only, as its speaker is refused with nothing sent,
//! and a post replayed with its key texts nothing again.
//!
//! What `post` saves: the host channel, a transport of type `HOST`, is
//! registered and attached on first use, and the post solicits no agent
//! unless it names one.
//!
//! ```sh
//! cargo run --example host_post
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    BindingOptions, ChannelId, EventContent, InboundMessage, Participant, ParticipantId, Post,
    RoomId,
};
use roomkit::{ConversationStore, InboundError, KitError};

use shared::{Checks, ScriptedAgent, SmsNumber, print_timeline, timeline};

const CUSTOMER: &str = "+15550100001";

/// The agent's script: what it answers, by what it reads.
const ASSISTANT: shared::Script = &[
    (
        "Where is my order?",
        "It left our warehouse this morning; you will get it tomorrow before noon.",
    ),
    (
        "Confirm the delivery address with the customer.",
        "Marie, can you confirm the delivery address is 12 rue des Lilas?",
    ),
];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let room = RoomId::new("order-1042")?;
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    let assistant = ScriptedAgent::new(ChannelId::new("assistant")?, ASSISTANT);
    kit.register_channel(sms.clone())?;
    kit.register_channel(assistant.clone())?;
    kit.create_room(&room).await?;
    let mut customer = BindingOptions::default();
    customer
        .metadata
        .insert("phone_number".to_owned(), CUSTOMER.into());
    kit.attach_with(&room, sms.id(), customer).await?;
    kit.attach(&room, assistant.id()).await?;
    // The backend's desk speaks through the host channel, whose id is
    // "host"; the customer is reached through SMS.
    let now = kit
        .store()
        .get_room(&room)
        .await?
        .map(|room| room.created_at);
    let joined = now.unwrap_or(roomkit::model::Timestamp::UNIX_EPOCH);
    let member = |id: &str, channel: &str| -> Result<Participant, Box<dyn Error>> {
        Ok(Participant::new(
            ParticipantId::new(id)?,
            room.clone(),
            ChannelId::new(channel)?,
            joined,
        ))
    };
    kit.store()
        .create_participant(member("desk", "host")?)
        .await?;
    kit.store()
        .create_participant(member(CUSTOMER, "sms")?)
        .await?;

    // 1. The backend's notice: texted to the customer, no agent answers it.
    let mut notice = Post::new(EventContent::text("Your order shipped."));
    notice.idempotency_key = Some("shipped-1042".to_owned());
    let posted = kit.post(&room, notice.clone()).await?;
    // 2. The customer replies: the agent answers her, as usual.
    let question = InboundMessage::new(
        sms.id().clone(),
        CUSTOMER,
        EventContent::text("Where is my order?"),
    );
    kit.process_inbound_in(&room, question).await?;
    // 3. The backend asks the agent, in front of the customer, as "desk".
    let mut ask = Post::new(EventContent::text(
        "Confirm the delivery address with the customer.",
    ));
    ask.participant_id = Some(ParticipantId::new("desk")?);
    ask.addressed_to = vec![assistant.id().clone()];
    kit.post(&room, ask).await?;
    // 4. A retry of the notice is a replay: nothing is texted again.
    let replayed = kit.post(&room, notice).await?;
    // 5. The host never speaks in the customer's name.
    let mut impersonating = Post::new(EventContent::text("I cancel my order."));
    impersonating.participant_id = Some(ParticipantId::new(CUSTOMER)?);
    let refused = kit.post(&room, impersonating).await;
    kit.idle().await;

    let texted = sms.sent_to(CUSTOMER);
    print_timeline(&timeline(&kit, &room).await?);
    println!("Texted to {CUSTOMER}: {texted:?}");

    let mut checks = Checks::default();
    checks.expect(
        "the notice comes from the host channel, a person's message",
        posted.source.channel_id.as_str() == "host",
    );
    checks.expect(
        "the customer received the notice, the agent's answer and its question",
        texted
            == [
                "Your order shipped.",
                "It left our warehouse this morning; you will get it tomorrow before noon.",
                "Confirm the delivery address with the customer.",
                "Marie, can you confirm the delivery address is 12 rue des Lilas?",
            ],
    );
    checks.expect(
        "no agent answered the notice; it answered the customer and the desk",
        assistant.read()
            == [
                "Where is my order?",
                "Confirm the delivery address with the customer.",
            ],
    );
    checks.expect(
        "a replay of the notice returns it",
        replayed.id == posted.id,
    );
    checks.expect(
        "a post as the customer is refused, nothing sent",
        matches!(
            refused,
            Err(KitError::Inbound(InboundError::ParticipantNotFound { .. }))
        ),
    );
    Ok(checks.finish())
}
