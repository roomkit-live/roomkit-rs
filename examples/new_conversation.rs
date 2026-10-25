//! RFC Appendix B.1 with two customers: each writes to one SMS number with no
//! room of theirs yet. The router finds none, so each first message opens a
//! room of its own, bound to its sender (RFC §10.4, §10.1 step 2). An
//! `ON_ROOM_CREATED` hook attaches the agent, which answers each customer at
//! their own number. The first customer's next message is routed back to
//! their room.
//!
//! ```sh
//! cargo run --example new_conversation
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, EventContent, InboundMessage, RoomId};
use roomkit::testing::EchoAgent;
use roomkit::{ConversationStore, FrameworkEvent, HookTrigger, RoomKit, text_of};

use shared::{Checks, SmsNumber};

const JEAN: &str = "+15551234567";
const MARIE: &str = "+15557654321";

/// A customer's message, routed: it names no room.
async fn customer_says(
    kit: &RoomKit,
    sms: &SmsNumber,
    from: &str,
    text: &str,
) -> Result<RoomId, Box<dyn Error>> {
    println!("{from} writes {text:?}.");
    let message = InboundMessage::new(sms.id().clone(), from, EventContent::text(text));
    let result = kit.process_inbound(message).await?;
    let room = result
        .event
        .map(|event| event.room_id)
        .ok_or("the message was not committed")?;
    println!("  routed to {room}");
    Ok(room)
}

/// The correspondent and the recipient the room's SMS binding names.
async fn bound_to(
    kit: &RoomKit,
    room: &RoomId,
    sms: &ChannelId,
) -> Result<(Option<String>, Option<String>), Box<dyn Error>> {
    let bindings = kit.store().list_bindings(room).await?;
    let binding = bindings.iter().find(|binding| &binding.channel_id == sms);
    Ok(binding.map_or((None, None), |binding| {
        (
            binding.participant_id.as_ref().map(ToString::to_string),
            binding
                .metadata
                .get("phone_number")
                .and_then(|number| number.as_str())
                .map(str::to_owned),
        )
    }))
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let sms = SmsNumber::new(ChannelId::new("sms_main")?);
    let agent = EchoAgent::new(ChannelId::new("ai")?, "Bonjour! You said: ").quoting();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent.clone())?;
    let attaching = kit.clone();
    let agent_id = agent.id().clone();
    kit.observe(
        "attach_agent",
        HookTrigger::OnRoomCreated,
        move |event, _| {
            let (kit, agent_id) = (attaching.clone(), agent_id.clone());
            async move {
                // The hook's failure would be logged; the example's checks see it.
                let _attached = kit.attach(&event.room_id, &agent_id).await;
            }
        },
    )?;
    let mut events = kit.subscribe();

    let jeans = customer_says(&kit, &sms, JEAN, "Bonjour").await?;
    let maries = customer_says(&kit, &sms, MARIE, "Hello").await?;
    let jeans_again = customer_says(&kit, &sms, JEAN, "Merci").await?;

    for (who, room) in [("Jean", &jeans), ("Marie", &maries)] {
        println!("{who}'s room {room}:");
        shared::print_timeline(&shared::timeline(&kit, room).await?);
    }
    let created = shared::framework_events(&mut events)
        .iter()
        .filter(|event| matches!(event, FrameworkEvent::RoomCreated { .. }))
        .count();
    let jeans_timeline = shared::timeline(&kit, &jeans).await?;

    let mut checks = Checks::default();
    checks.expect(
        "each customer's first message opens a room of its own",
        jeans != maries && created == 2,
    );
    checks.expect(
        "each room's SMS binding names its customer, as correspondent and as recipient",
        bound_to(&kit, &jeans, sms.id()).await? == (Some(JEAN.to_owned()), Some(JEAN.to_owned()))
            && bound_to(&kit, &maries, sms.id()).await?
                == (Some(MARIE.to_owned()), Some(MARIE.to_owned())),
    );
    checks.expect(
        "the agent the ON_ROOM_CREATED hook attached answers each first message at its sender's number",
        sms.sent_to(JEAN).first().map(String::as_str) == Some("Bonjour! You said: Bonjour")
            && sms.sent_to(MARIE) == ["Bonjour! You said: Hello"],
    );
    checks.expect(
        "Jean's next message is routed back to Jean's room, after the first exchange",
        // The room's first events are the lifecycle records of the agent the
        // creation hook attached and of the SMS step 2 attached (RFC §5.1,
        // gap G-046).
        jeans_again == jeans
            && jeans_timeline
                .get(4)
                .is_some_and(|event| text_of(&event.content) == "Merci" && event.index == 4),
    );
    checks.expect(
        "no answer reaches the other customer",
        sms.sent_to(MARIE).len() == 1 && sms.sent_to(JEAN).len() == 2,
    );
    Ok(checks.finish())
}
