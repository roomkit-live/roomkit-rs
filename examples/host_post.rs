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
    ChannelId, InboundMessage, Metadata, Participant, ParticipantId, RoomId, TextContent,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, PostOptions, ProcessInboundOptions,
    RoomKit, RoomKitError,
};

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

/// Opens the order's room: the customer's SMS, its binding naming their
/// number, and the assistant; the backend's desk a member through the host
/// channel, whose id is "host", and the customer one through SMS.
async fn open_order(
    kit: &RoomKit,
    room: &RoomId,
    sms: &SmsNumber,
    assistant: &ScriptedAgent,
) -> Result<(), Box<dyn Error>> {
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    let customer = AttachChannelOptions {
        metadata: Metadata::from_iter([("phone_number".to_owned(), CUSTOMER.into())]),
        ..AttachChannelOptions::default()
    };
    kit.attach_channel(room, sms.id(), customer).await?;
    kit.attach_channel(room, assistant.id(), AttachChannelOptions::default())
        .await?;
    let member = |id: &str, channel: &str| -> Result<Participant, Box<dyn Error>> {
        Ok(Participant::new(
            ParticipantId::new(id)?,
            room.clone(),
            ChannelId::new(channel)?,
        ))
    };
    kit.store()
        .create_participant(member("desk", "host")?)
        .await?;
    kit.store()
        .create_participant(member(CUSTOMER, "sms")?)
        .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let room = RoomId::new("order-1042")?;
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    let assistant = ScriptedAgent::new(ChannelId::new("assistant")?, ASSISTANT);
    kit.register_channel(sms.clone())?;
    kit.register_channel(assistant.clone())?;
    open_order(&kit, &room, &sms, &assistant).await?;

    // 1. The backend's notice: texted to the customer, no agent answers it.
    let notice = || TextContent::new("Your order shipped.");
    let shipped = || PostOptions {
        idempotency_key: Some("shipped-1042".to_owned()),
        ..Default::default()
    };
    let posted = kit.post(&room, notice(), shipped()).await?;
    // 2. The customer replies: the agent answers her, as usual.
    let question = InboundMessage::new(
        sms.id().clone(),
        CUSTOMER,
        TextContent::new("Where is my order?"),
    );
    let into_room = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(question, into_room).await?;
    // 3. The backend asks the agent, in front of the customer, as "desk".
    let ask = PostOptions {
        participant_id: Some(ParticipantId::new("desk")?),
        addressed_to: vec![assistant.id().clone()],
        ..Default::default()
    };
    kit.post(
        &room,
        TextContent::new("Confirm the delivery address with the customer."),
        ask,
    )
    .await?;
    // 4. A retry of the notice is a replay: nothing is texted again.
    let replayed = kit.post(&room, notice(), shipped()).await?;
    // 5. The host never speaks in the customer's name.
    let impersonating = PostOptions {
        participant_id: Some(ParticipantId::new(CUSTOMER)?),
        ..Default::default()
    };
    let refused = kit
        .post(&room, TextContent::new("I cancel my order."), impersonating)
        .await;
    // What the posts started runs in the background: the kit's close lets
    // it finish.
    kit.close().await?;

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
        matches!(refused, Err(RoomKitError::ParticipantNotFound { .. })),
    );
    Ok(checks.finish())
}
