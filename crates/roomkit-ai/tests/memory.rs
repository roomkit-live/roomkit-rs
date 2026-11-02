//! The AI channel's memory (RFC §20): where a turn reads its history, run
//! in a kit on a scripted model, as the Python implementation's
//! `TestMemoryIntegration` runs it.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventSource, EventType, InboundMessage, ParticipantId,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, SendEventOptions,
};
use roomkit_ai::memory::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiMessage, AiRole};

/// The person who writes by SMS.
const SENDER: &str = "+15550001";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// What a scripted memory was asked.
#[derive(Debug, Default)]
struct Asked {
    retrieved: Vec<(RoomId, Option<ChannelId>)>,
    ingested: Vec<String>,
    closed: bool,
}

/// Python's `MockMemoryProvider`: it answers its messages and events, and
/// records what it was asked.
#[derive(Clone, Default)]
struct Scripted {
    messages: Vec<AiMessage>,
    events: Vec<RoomEvent>,
    asked: Arc<Mutex<Asked>>,
}

impl MemoryProvider for Scripted {
    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        _current_event: &'a RoomEvent,
        _context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        let mut asked = self.asked.lock().unwrap();
        asked.retrieved.push((room_id.clone(), options.channel_id));
        Box::pin(future::ready(Ok(MemoryResult {
            messages: self.messages.clone(),
            events: self.events.clone(),
            ..MemoryResult::default()
        })))
    }

    fn ingest<'a>(
        &'a self,
        _room_id: &'a RoomId,
        event: &'a RoomEvent,
        _options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        let text = if let EventContent::Text(text) = &event.content {
            text.body.clone()
        } else {
            String::new()
        };
        self.asked.lock().unwrap().ingested.push(text);
        Box::pin(future::ready(Ok(())))
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.asked.lock().unwrap().closed = true;
        Box::pin(future::ready(()))
    }
}

/// A room where a person writes by SMS to the agent `ai1`, whose memory is
/// `memory` when given.
struct Room {
    kit: RoomKit,
    model: ScriptedProvider,
}

impl Room {
    async fn new(rounds: usize, memory: Option<Scripted>) -> Self {
        let model = ScriptedProvider::new(
            (0..rounds).map(|n| ScriptedRound::answering(format!("Answer {n}."))),
        );
        let memory: Option<Arc<dyn MemoryProvider>> = match memory {
            Some(memory) => Some(Arc::new(memory)),
            None => None,
        };
        let options = AiChannelOptions {
            memory,
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("ai1"), model.clone(), options).unwrap();
        let kit = RoomKit::new(RoomKitOptions::default());
        let sms = RecordingTransport::new(
            channel("sms1"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(sms).unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["sms1", "ai1"] {
            kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        Self { kit, model }
    }

    async fn says(&self, text: &str) {
        let message = InboundMessage::new(channel("sms1"), SENDER, TextContent::new(text));
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        self.kit.process_inbound(message, options).await.unwrap();
        roomkit::testing::idle(&self.kit).await;
    }

    /// Each message of the agent's last request: its role and its text.
    fn last_request(&self) -> Vec<(AiRole, String)> {
        let requests = self.model.requests();
        let last = requests.last().unwrap();
        last.messages
            .iter()
            .map(|message| (message.role, message.content.text()))
            .collect()
    }
}

/// A message of the room's past, from the person who writes by SMS.
fn past(text: &str) -> RoomEvent {
    let source = EventSource {
        participant_id: Some(ParticipantId::new(SENDER).unwrap()),
        ..EventSource::new(channel("sms1"), ChannelType::SMS)
    };
    RoomEvent::new(
        room_id(),
        source,
        EventContent::Text(TextContent::new(text)),
    )
}

fn user(text: &str) -> (AiRole, String) {
    (AiRole::User, text.to_owned())
}

// Python: tests/test_channels/test_ai.py::TestMemoryIntegration::test_default_memory_is_sliding_window
// and test_max_context_events_configures_default_memory
#[test]
fn the_default_memory_is_a_window_of_max_context_events() {
    let window = |options: AiChannelOptions| {
        AiChannel::new(channel("ai1"), ScriptedProvider::new([]), options)
            .unwrap()
            .recent_events_window()
    };
    assert_eq!(
        [
            window(AiChannelOptions::default()),
            window(AiChannelOptions {
                max_context_events: 25,
                ..AiChannelOptions::default()
            })
        ],
        [50, 25]
    );
}

// Python: tests/test_channels/test_ai.py::TestMemoryIntegration::test_custom_memory_provider_is_used
#[tokio::test]
async fn a_custom_memory_is_asked_in_the_turn_s_room_for_its_channel() {
    let memory = Scripted::default();
    let room = Room::new(1, Some(memory.clone())).await;
    room.says("hello").await;
    let asked = memory.asked.lock().unwrap();
    assert_eq!(asked.retrieved, [(room_id(), Some(channel("ai1")))]);
    assert_eq!(asked.ingested, ["hello"]);
}

// Python: tests/test_channels/test_ai.py::TestMemoryIntegration::test_memory_messages_appear_in_context
#[tokio::test]
async fn the_memory_s_messages_come_before_the_input() {
    let memory = Scripted {
        messages: vec![AiMessage::new(
            AiRole::System,
            "Previous conversation summary",
        )],
        ..Scripted::default()
    };
    let room = Room::new(1, Some(memory)).await;
    room.says("hello").await;
    assert_eq!(
        room.last_request(),
        [
            (AiRole::System, "Previous conversation summary".to_owned()),
            user("hello")
        ]
    );
}

// Python: tests/test_channels/test_ai.py::TestMemoryIntegration::test_memory_events_converted_by_channel
#[tokio::test]
async fn the_memory_s_events_are_converted_as_the_history() {
    let memory = Scripted {
        events: vec![past("past message")],
        ..Scripted::default()
    };
    let room = Room::new(1, Some(memory)).await;
    room.says("current").await;
    assert_eq!(room.last_request(), [user("past message"), user("current")]);
}

// Python: tests/test_channels/test_ai.py::TestMemoryIntegration::test_memory_both_messages_and_events
#[tokio::test]
async fn the_memory_s_messages_then_its_events_then_the_input() {
    let memory = Scripted {
        messages: vec![AiMessage::new(AiRole::Assistant, "Summary")],
        events: vec![past("recent msg")],
        ..Scripted::default()
    };
    let room = Room::new(1, Some(memory)).await;
    room.says("current").await;
    assert_eq!(
        room.last_request(),
        [
            (AiRole::Assistant, "Summary".to_owned()),
            user("recent msg"),
            user("current")
        ]
    );
}

// Python: tests/test_channels/test_ai.py::TestMemoryIntegration::test_close_closes_memory_provider
#[tokio::test]
async fn the_channel_s_close_closes_its_memory() {
    let memory = Scripted::default();
    let agent = AiChannel::new(
        channel("ai1"),
        ScriptedProvider::new([]),
        AiChannelOptions {
            memory: Some(Arc::new(memory.clone())),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    agent.close().await;
    assert!(memory.asked.lock().unwrap().closed);
}

// Python: tests/test_instruction_events.py::test_a_standalone_instruction_reads_nothing_of_the_room
// (its memory: not asked, and an instruction never ingested)
#[tokio::test]
async fn an_instruction_standing_alone_asks_no_memory_and_none_learns_it() {
    let memory = Scripted::default();
    let room = Room::new(2, Some(memory.clone())).await;
    room.says("hello").await;
    let alone = SendEventOptions {
        event_type: EventType::Instruction,
        standalone: true,
        addressed_to: Some(vec![channel("ai1")]),
        ..Default::default()
    };
    room.kit
        .send_event(
            &room_id(),
            &channel("sms1"),
            TextContent::new("Summarize."),
            alone,
        )
        .await
        .unwrap();
    roomkit::testing::idle(&room.kit).await;
    let asked = memory.asked.lock().unwrap();
    assert_eq!(
        (asked.retrieved.len(), asked.ingested.clone()),
        (1, vec!["hello".to_owned()])
    );
    assert_eq!(room.model.requests().len(), 2);
}

/// RFC §20.3: the default window of 50 reads the room's latest 50 events,
/// the turn's own among them: past 60 messages and their answers, a turn
/// reads 49 messages before its input (step 5's
/// `context_holds_max_context_events`, RMK-1103).
#[tokio::test]
async fn context_holds_max_context_events() {
    let room = Room::new(31, None).await;
    for n in 0..31 {
        room.says(&format!("Question {n}.")).await;
    }
    let read = room.last_request();
    assert_eq!(read.len(), 50);
    assert_eq!(
        (read.first().unwrap(), read.last().unwrap()),
        (
            &(AiRole::Assistant, "Answer 5.".to_owned()),
            &user("Question 30.")
        )
    );
}
