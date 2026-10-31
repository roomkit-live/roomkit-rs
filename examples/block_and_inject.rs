//! RFC Appendix B.2: a `BEFORE_BROADCAST` hook finds a social insurance
//! number in an SMS, blocks the message, tells the customer and the advisor,
//! and records a compliance observation.
//!
//! ```sh
//! cargo run --example block_and_inject
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]
use std::error::Error;
use std::process::ExitCode;

use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelId, ChannelType, EventDirection, EventSource, EventStatus, HookResult, InboundMessage,
    InjectedEvent, Observation, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, text_of,
};

use shared::{Checks, Clock};

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

/// An event from the framework itself, for the hook to inject: the
/// pipeline gives every injected event a fresh id.
fn notice(text: &str, now: Timestamp) -> Result<RoomEvent, Box<dyn Error>> {
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(ChannelId::new("system")?, ChannelType::SYSTEM)
    };
    Ok(RoomEvent {
        created_at: now,
        ..RoomEvent::new(RoomId::new("room_1")?, source, TextContent::new(text))
    })
}

/// The scanner's answer to a message holding a SIN, as Python writes it,
/// `HookResult.block("SIN detected", injected=[...], observations=[...])`:
/// each notice injected to its own target only.
fn refusal(notices: [(RoomEvent, ChannelId); 2], observation: Observation) -> HookResult {
    let injected = notices
        .into_iter()
        .map(|(event, target)| InjectedEvent {
            target_channel_ids: Some(vec![target]),
            ..InjectedEvent::new(event)
        })
        .collect();
    HookResult::block(
        "SIN detected",
        BlockOptions {
            injected: Some(injected),
            observations: Some(vec![observation]),
            ..BlockOptions::default()
        },
    )
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let clock = Clock::new();
    let kit = clock.kit();
    let now = clock.now();

    let customer = RecordingTransport::new(
        ChannelId::new("sms_customer")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let advisor = RecordingTransport::new(
        ChannelId::new("ws_advisor")?,
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(customer.clone())?;
    kit.register_channel(advisor.clone())?;

    let to_customer = notice("Message blocked. Do not send your SIN by SMS.", now)?;
    let to_advisor = notice("The client attempted to send a SIN. Blocked.", now)?;
    let (customer_id, advisor_id) = (customer.channel_id().clone(), advisor.channel_id().clone());
    kit.before_broadcast("sensitivity_scanner", move |event, context| {
        let found = has_sin(&text_of(&event.content));
        let (to_customer, to_advisor) = (to_customer.clone(), to_advisor.clone());
        let (customer_id, advisor_id) = (customer_id.clone(), advisor_id.clone());
        async move {
            if !found {
                return Ok(HookResult::allow(AllowOptions::default()));
            }
            let observation = Observation {
                created_at: event.created_at,
                ..Observation::new(
                    "obs_sin".parse()?,
                    context.room.id.clone(),
                    customer_id.clone(),
                    "SIN sent by SMS",
                )
            };
            Ok(refusal(
                [(to_customer, customer_id), (to_advisor, advisor_id)],
                observation,
            ))
        }
    })?;

    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(
        &room,
        customer.channel_id(),
        AttachChannelOptions::default(),
    )
    .await?;
    kit.attach_channel(&room, advisor.channel_id(), AttachChannelOptions::default())
        .await?;

    println!("The customer writes their SIN on SMS.");
    let message = InboundMessage::new(
        customer.channel_id().clone(),
        "+15551234567",
        TextContent::new("Mon NAS est 123-456-789"),
    );
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    println!("Delivered to the customer: {:?}", customer.texts());
    println!("Delivered to the advisor: {:?}", advisor.texts());
    let observations = kit.store().list_observations(&room).await?;

    let mut checks = Checks::default();
    checks.expect(
        "the message is refused, with the hook's reason",
        result.blocked && result.reason.as_deref() == Some("SIN detected"),
    );
    // The room's first events are its attaches' lifecycle records (RFC
    // §5.1, gap G-046); the message is the first event of the SMS.
    checks.expect(
        "the message is stored blocked by the scanner, for audit",
        timeline
            .iter()
            .find(|event| event.source.channel_type == ChannelType::SMS)
            .is_some_and(|event| {
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
