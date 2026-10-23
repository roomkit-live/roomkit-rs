//! RFC Appendix B.2: a `BEFORE_BROADCAST` hook finds a social insurance
//! number in an SMS, blocks the message, tells the customer and the advisor,
//! and records a compliance observation.
//!
//! ```sh
//! cargo run -p roomkit --example block_and_inject
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod support;

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventStatus, EventType,
    HookResult, InboundMessage, InjectedEvent, Observation, RoomEvent, RoomId, Timestamp,
};
use roomkit::ops::SequentialEnvironment;
use roomkit::testing::RecordingTransport;
use roomkit::{ConversationStore, RoomKit, text_of};

use self::support::Checks;

/// A Canadian social insurance number, as `123-456-789`.
fn has_sin(text: &str) -> bool {
    text.split_whitespace().any(|word| {
        let digits: Vec<&str> = word.split('-').collect();
        digits.len() == 3
            && digits
                .iter()
                .all(|group| group.len() == 3 && group.chars().all(|c| c.is_ascii_digit()))
    })
}

/// An event from the framework itself, for the hook to inject.
fn notice(text: &str, now: Timestamp) -> Result<RoomEvent, Box<dyn Error>> {
    Ok(RoomEvent::new(
        // The pipeline gives every injected event a fresh id.
        "placeholder".parse()?,
        RoomId::new("room_1")?,
        EventType::Message,
        EventSource::new(
            ChannelId::new("system")?,
            ChannelType::System,
            EventDirection::Outbound,
        ),
        EventContent::text(text),
        now,
    ))
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = RoomKit::new();
    let now = Timestamp::from_unix_millis(1_792_677_780_000)?;
    kit.set_environment(SequentialEnvironment::new(now));

    let customer = RecordingTransport::new(ChannelId::new("sms_customer")?, ChannelType::Sms);
    let advisor = RecordingTransport::new(ChannelId::new("ws_advisor")?, ChannelType::Websocket);
    kit.register_channel(customer.clone())?;
    kit.register_channel(advisor.clone())?;

    let to_customer = notice("Message blocked. Do not send your SIN by SMS.", now)?;
    let to_advisor = notice("The client attempted to send a SIN. Blocked.", now)?;
    let (customer_id, advisor_id) = (customer.id().clone(), advisor.id().clone());
    kit.before_broadcast("sensitivity_scanner", move |event, context| {
        let found = has_sin(&text_of(&event.content));
        let (to_customer, to_advisor) = (to_customer.clone(), to_advisor.clone());
        let (customer_id, advisor_id) = (customer_id.clone(), advisor_id.clone());
        async move {
            if !found {
                return Ok(HookResult::allow());
            }
            let observation = Observation::new(
                "obs_sin".parse()?,
                context.room.id.clone(),
                customer_id.clone(),
                "SIN sent by SMS",
                event.created_at,
            );
            Ok(HookResult::block("SIN detected")
                .inject(InjectedEvent::to_channels(to_customer, vec![customer_id]))
                .inject(InjectedEvent::to_channels(to_advisor, vec![advisor_id]))
                .with_observation(observation))
        }
    })?;

    let room = RoomId::new("room_1")?;
    kit.create_room(&room).await?;
    kit.attach(&room, customer.id()).await?;
    kit.attach(&room, advisor.id()).await?;

    println!("The customer writes their SIN on SMS.");
    let message = InboundMessage::new(
        customer.id().clone(),
        "+15551234567",
        EventContent::text("Mon NAS est 123-456-789"),
    );
    let result = kit.process_inbound(message, Some(&room)).await?;

    let timeline = support::timeline(&kit, &room).await?;
    support::print_timeline(&timeline);
    println!("Delivered to the customer: {:?}", customer.texts());
    println!("Delivered to the advisor: {:?}", advisor.texts());
    let observations = kit.store().list_observations(&room).await?;

    let mut checks = Checks::default();
    checks.expect(
        "the message is refused, with the hook's reason",
        result.blocked && result.reason.as_deref() == Some("SIN detected"),
    );
    checks.expect(
        "the message is stored blocked by the scanner, for audit",
        timeline.first().is_some_and(|event| {
            event.status == EventStatus::Blocked
                && event.blocked_by.as_deref() == Some("sensitivity_scanner")
        }),
    );
    checks.expect(
        "the advisor never sees the number",
        !advisor.texts().iter().any(|text| has_sin(text)),
    );
    checks.expect(
        "each injected notice reaches its own target only",
        customer.texts() == ["Message blocked. Do not send your SIN by SMS."]
            && advisor.texts() == ["The client attempted to send a SIN. Blocked."],
    );
    checks.expect(
        "the compliance observation is recorded",
        observations
            .iter()
            .any(|observation| observation.content == "SIN sent by SMS"),
    );
    Ok(checks.finish())
}
