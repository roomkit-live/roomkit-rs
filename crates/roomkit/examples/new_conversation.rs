//! RFC Appendix B.1 with two customers: each writes to one SMS number with no
//! room of theirs yet. The router finds none, so each first message opens a
//! room of its own, bound to its sender (RFC §10.4, §10.1 step 2). An
//! `ON_ROOM_CREATED` hook attaches the agent, which answers each customer at
//! their own number. The first customer's next message is routed back to
//! their room.
//!
//! ```sh
//! cargo run -p roomkit --example new_conversation
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod support;

use std::error::Error;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, InboundMessage, RoomContext, RoomEvent, RoomId, Timestamp,
};
use roomkit::ops::{ChannelDeclaration, FrameworkEvent, SequentialEnvironment};
use roomkit::testing::EchoAgent;
use roomkit::{Channel, ChannelError, ConversationStore, HookTrigger, RoomKit, text_of};

use self::support::Checks;

const JEAN: &str = "+15551234567";
const MARIE: &str = "+15557654321";

/// The kit's SMS number, as a provider sends from it: to the number the
/// binding names under `phone_number` (RFC Appendix A.1). It records what
/// it sent where.
#[derive(Clone)]
struct SmsNumber {
    declaration: ChannelDeclaration,
    sent: Arc<Mutex<Vec<(String, String)>>>,
}

impl SmsNumber {
    fn new(channel_id: ChannelId) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel_id,
                ChannelType::Sms,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            sent: Arc::default(),
        }
    }

    fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// What was sent to `number`, in order.
    fn sent_to(&self, number: &str) -> Vec<String> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(to, _)| to == number)
            .map(|(_, text)| text.clone())
            .collect()
    }
}

impl Channel for SmsNumber {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let to = binding
            .metadata
            .get("phone_number")
            .and_then(|number| number.as_str())
            .unwrap_or_default()
            .to_owned();
        println!("  SMS to {to}: {:?}", text_of(&event.content));
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((to, text_of(&event.content)));
        ready(Ok(ChannelOutput::default()))
    }
}

/// A customer's message, routed: it names no room.
async fn customer_says(
    kit: &RoomKit,
    sms: &SmsNumber,
    from: &str,
    text: &str,
) -> Result<RoomId, Box<dyn Error>> {
    println!("{from} writes {text:?}.");
    let message = InboundMessage::new(sms.id().clone(), from, EventContent::text(text));
    let result = kit.process_inbound(message, None).await?;
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
    let kit = RoomKit::new();
    kit.set_environment(SequentialEnvironment::new(Timestamp::from_unix_millis(
        1_792_677_780_000,
    )?));
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
        support::print_timeline(&support::timeline(&kit, room).await?);
    }
    let created = std::iter::from_fn(|| events.try_recv().ok())
        .filter(|event| matches!(event, FrameworkEvent::RoomCreated { .. }))
        .count();
    let jeans_timeline = support::timeline(&kit, &jeans).await?;

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
        jeans_again == jeans
            && jeans_timeline
                .get(2)
                .is_some_and(|event| text_of(&event.content) == "Merci" && event.index == 2),
    );
    checks.expect(
        "no answer reaches the other customer",
        sms.sent_to(MARIE).len() == 1 && sms.sent_to(JEAN).len() == 2,
    );
    Ok(checks.finish())
}
