//! Instructions on the tokio runtime (RFC §10.1.1): the application directs
//! an agent, the agent answers in the room, and the room never stores the
//! instruction, only the answer and the fingerprint it records.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "strategies/faulty_store.rs"]
mod faulty_store;

use std::sync::atomic::Ordering;

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
                "{\"sha256\":\
                 \"3554dc2dd06b3ebfe63a746bc71c12b591d2a0173185087859a8c2d7e912ca6e\",\
                 \"length\":19}"
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

/// A room with the application's WebSocket `app`, the customer's SMS and an
/// AI agent `ai` over a scripted model answering `answer`, on `store`.
async fn ai_room<S: ConversationStore>(store: S, answer: &str) -> (RoomKit<S>, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
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
    let model = roomkit_ai::testing::ScriptedProvider::new([
        roomkit_ai::testing::ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        roomkit_ai::testing::ScriptedRound::answering(answer),
    ]);
    let tools = roomkit_ai::Tools::new().tool(
        roomkit_ai::AiTool::new("lookup", "Looks up").unwrap(),
        |_call, _context| async { Ok(roomkit_ai::ToolResult::Text("found".to_owned())) },
    );
    let options = roomkit_ai::AiChannelOptions {
        tools: tools.into(),
        ..roomkit_ai::AiChannelOptions::default()
    };
    let agent = roomkit_ai::AiChannel::new(channel("ai"), model, options).unwrap();
    kit.register_channel(app).unwrap();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["app", "sms", "ai"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, sms)
}

/// The agent's rows in the room, every one.
async fn agent_rows<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<RoomEvent> {
    let rows = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    rows.into_iter()
        .filter(|row| row.source.channel_id == channel("ai"))
        .collect()
}

// Python: tests/test_instruction_fingerprint.py::test_every_row_of_a_streamed_answer_records_the_instruction
/// RFC §10.1.1 rule 6, §8.5: the agent's copy of the instruction its turn
/// read carries what every row of the turn does, the instruction's
/// fingerprint, the turn's correlation id and the instruction it answers,
/// never its text but as its body.
#[tokio::test]
async fn the_agent_s_copy_records_the_instruction_as_the_turn_s_rows_do() {
    let (kit, _sms) = ai_room(roomkit::InMemoryStore::new(), "Hello.").await;
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
    roomkit::testing::idle(&kit).await;
    let instruction = result.event.unwrap();
    let rows = agent_rows(&kit).await;
    kit.close().await.unwrap();
    let kinds: Vec<EventType> = rows.iter().map(|row| row.r#type).collect();
    assert_eq!(
        kinds,
        [
            EventType::InstructionRead,
            EventType::ToolCallStart,
            EventType::ToolCallEnd,
            EventType::Message
        ]
    );
    let fingerprint = rows[0].metadata.get("instruction").cloned();
    assert_eq!(
        fingerprint.as_ref().map(ToString::to_string),
        Some(
            "{\"sha256\":\
             \"3554dc2dd06b3ebfe63a746bc71c12b591d2a0173185087859a8c2d7e912ca6e\",\
             \"length\":19}"
                .to_owned()
        )
    );
    for row in &rows {
        assert_eq!(
            (
                row.metadata.get("instruction"),
                row.correlation_id.as_ref(),
                row.responds_to.as_ref(),
            ),
            (
                fingerprint.as_ref(),
                rows[0].correlation_id.as_ref(),
                Some(&instruction.id),
            ),
            "{:?}",
            row.r#type
        );
    }
    assert!(rows[0].correlation_id.is_some());
}

// Answers Python's tests/test_instruction_events.py::test_a_copy_the_kit_cannot_write_costs_the_turn_nothing,
// by roomkit-rs's own rule: any failure of the copy's write is logged and
// the turn goes on, where Python catches a closing kit and a room gone or
// closed only, and lets a store's failure end the turn's read (RMK-1506).
/// RFC §10.1.1: the agent's copy of an instruction the kit cannot write is
/// not kept, and the turn goes on as its other rows do: its answer stored
/// and delivered, no error.
#[tokio::test]
async fn a_copy_the_kit_cannot_write_costs_the_turn_nothing() {
    let store = faulty_store::AbstractOnly::default();
    store.fail_kept_instructions.store(true, Ordering::SeqCst);
    let (kit, sms) = ai_room(store, "Hello.").await;
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
    roomkit::testing::idle(&kit).await;
    let kinds: Vec<EventType> = agent_rows(&kit)
        .await
        .iter()
        .map(|row| row.r#type)
        .collect();
    kit.close().await.unwrap();
    // The answer the customer reads; the call's rows reach the SMS as no text.
    let said: Vec<String> = sms
        .texts()
        .into_iter()
        .filter(|text| !text.is_empty())
        .collect();
    assert_eq!(
        (result.error, kinds, said),
        (
            None,
            vec![
                EventType::ToolCallStart,
                EventType::ToolCallEnd,
                EventType::Message
            ],
            vec!["Hello.".to_owned()]
        )
    );
}
