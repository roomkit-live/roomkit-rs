//! Instructions on the tokio runtime (RFC §10.1.1): the application directs
//! an agent, the agent answers in the room, and the room never stores the
//! instruction, only the answer and the fingerprint it records.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventType, InboundMessage, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
    SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A room with the application's WebSocket `app`, the customer's SMS and an
/// echo agent, all attached.
async fn room() -> (RoomKit, RecordingTransport, RecordingTransport) {
    let kit = kit_with_environment(
        RoomKitOptions::default(),
        SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
    );
    let app = RecordingTransport::new(
        channel("app"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let agent = EchoAgent::new(
        channel("ai"),
        "Hello, I am the assistant.",
        EchoAgentOptions::default(),
    );
    kit.register_channel(app.clone()).unwrap();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in [app.channel_id(), sms.channel_id(), agent.channel_id()] {
        kit.attach_channel(&room_id(), id, AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, app, sms)
}

/// The application's instruction `text`, addressed to the agent.
fn instruction(text: &str) -> InboundMessage {
    let mut message = InboundMessage::new(channel("app"), "app", TextContent::new(text));
    message.event_type = EventType::Instruction;
    message.addressed_to = Some(vec![channel("ai")]);
    message
}

/// The room's timeline but the attaches' lifecycle records (gap G-046).
async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    let mut timeline = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    timeline.retain(|event| event.source.channel_type != ChannelType::SYSTEM);
    timeline
}

/// RFC §10.1.1 rules 5 and 6.
#[tokio::test]
async fn an_echo_agent_answers_an_instruction_the_room_never_stores() {
    let (kit, _app, sms) = room().await;
    let result = kit
        .process_inbound(
            instruction("Introduce yourself."),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let instruction = result.event.unwrap();
    let stored = timeline(&kit).await;
    assert_eq!(
        (
            instruction.r#type,
            sms.texts(),
            stored.len(),
            stored[0].responds_to.as_ref(),
            stored[0]
                .metadata
                .get("instruction")
                .map(ToString::to_string),
        ),
        (
            EventType::Instruction,
            vec!["Hello, I am the assistant.".to_owned()],
            1,
            Some(&instruction.id),
            Some(
                "{\"length\":19,\"sha256\":\
                 \"3554dc2dd06b3ebfe63a746bc71c12b591d2a0173185087859a8c2d7e912ca6e\"}"
                    .to_owned()
            ),
        )
    );
}

/// RFC §10.1.1 rule 3: the application's own channel and the customer's
/// never receive the instruction, only the answer.
#[tokio::test]
async fn no_transport_receives_the_instruction() {
    let (kit, app, sms) = room().await;
    kit.process_inbound(
        instruction("Introduce yourself."),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        (app.texts(), sms.texts()),
        (
            vec!["Hello, I am the assistant.".to_owned()],
            vec!["Hello, I am the assistant.".to_owned()]
        )
    );
}

/// RFC §10.1.1 rule 1: refused before anything is read or written.
#[tokio::test]
async fn an_unaddressed_instruction_is_refused_and_reaches_nobody() {
    let (kit, _app, sms) = room().await;
    let mut message = instruction("Introduce yourself.");
    message.addressed_to = None;
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (
            result.blocked,
            result.reason.as_deref(),
            sms.texts(),
            timeline(&kit).await.len()
        ),
        (
            true,
            Some("instruction_unaddressed"),
            Vec::<String>::new(),
            0
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}
