//! An AI channel measures the turn's footprint before reading its memory
//! (RFC §20), as the Python implementation's `tests/test_turn_footprint.py`
//! checks it: what the history may take is what the window leaves once the
//! rest of the turn is in it.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventContent, EventSource,
    InboundMessage, Participant, ParticipantId, Room as RoomRecord, RoomContext, RoomEvent, RoomId,
    TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};
use roomkit_ai::memory::{
    MemoryProvider, MemoryResult, RetrieveOptions, TurnFootprint, current_turn_footprint,
    estimate_message_tokens, estimate_tokens,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiContentPart, AiTool, ToolHandler, ToolResult};

/// The notes' room every turn's footprint keeps (Python's
/// `turn_notes([SPEAKER_ATTRIBUTION_NOTE, CUT_NOTE])`): 1,048 characters.
const NOTES_ROOM: usize = 263;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A memory that records the footprint the channel measured when it reads
/// the room, and reads nothing.
#[derive(Clone, Default)]
struct Measured(Arc<Mutex<Vec<Option<TurnFootprint>>>>);

impl MemoryProvider for Measured {
    fn retrieve<'a>(
        &'a self,
        _room_id: &'a RoomId,
        _current_event: &'a RoomEvent,
        _context: &'a RoomContext,
        _options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        self.0.lock().unwrap().push(current_turn_footprint());
        Box::pin(std::future::ready(Ok(MemoryResult::default())))
    }
}

/// The same, reading the context's events as a window does.
#[derive(Clone, Default)]
struct MeasuredWindow(Arc<Mutex<Vec<Option<TurnFootprint>>>>);

impl MemoryProvider for MeasuredWindow {
    fn retrieve<'a>(
        &'a self,
        _room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        _options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        self.0.lock().unwrap().push(current_turn_footprint());
        let events = context
            .recent_events
            .iter()
            .filter(|event| event.id != current_event.id)
            .cloned()
            .collect();
        Box::pin(std::future::ready(Ok(MemoryResult {
            events,
            ..MemoryResult::default()
        })))
    }
}

fn tool(name: &str) -> AiTool {
    AiTool::new(name, "looks things up ".repeat(30)).unwrap()
}

/// What `tool` costs as Python's `estimate_tool_tokens` measures it.
fn tool_tokens(tool: &AiTool) -> usize {
    // A tool taking no parameters declares none.
    let parameters = match tool.parameters.as_object() {
        Some(members) if members.is_empty() => 0,
        _ => estimate_tokens(&roomkit_ai::dumps(&tool.parameters)),
    };
    estimate_tokens(&tool.name) + estimate_tokens(&tool.description) + parameters
}

/// The footprint `options`' agent measured on its turn answering `hello`
/// outside a kit, recorded by its memory, and what its model was asked.
async fn measured(options: AiChannelOptions) -> (Vec<Option<TurnFootprint>>, ScriptedProvider) {
    let model = ScriptedProvider::new([ScriptedRound::answering("ok")]);
    let memory = Measured::default();
    let options = AiChannelOptions {
        memory: Some(Arc::new(memory.clone())),
        ..options
    };
    let agent = AiChannel::new(channel("agent"), model.clone(), options).unwrap();
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(channel("member"), ChannelType::WEBSOCKET),
        EventContent::Text(TextContent::new("hello")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(channel("agent"), room_id(), ChannelType::AI)
    };
    agent
        .on_event(event, binding, context_of(Vec::new()))
        .await
        .unwrap();
    let footprints = memory.0.lock().unwrap().clone();
    (footprints, model)
}

/// A context of room `r1`, where the agent and the member's channel are
/// bound and `people` take part on the member's channel.
fn context_of(people: Vec<Participant>) -> RoomContext {
    let mut context = RoomContext::new(RoomRecord::new(room_id()));
    context.bindings = vec![
        ChannelBinding::new(channel("member"), room_id(), ChannelType::WEBSOCKET),
        ChannelBinding {
            category: ChannelCategory::Intelligence,
            ..ChannelBinding::new(channel("agent"), room_id(), ChannelType::AI)
        },
    ];
    context.participants = people;
    context
}

// Python: tests/test_turn_footprint.py::test_the_footprint_is_the_turn_as_round_0_sends_it
// (without tool search and skills, which this implementation has not yet)
#[tokio::test]
async fn the_footprint_is_the_turn_as_its_first_round_sends_it() {
    let model = ScriptedProvider::new([ScriptedRound::answering("ok")]);
    let memory = Measured::default();
    let handler = ToolHandler::new(|_name, _arguments| async { Ok(ToolResult::Text("ok".into())) });
    let options = AiChannelOptions {
        system_prompt: Some("Answer billing questions.".to_owned()),
        max_tokens: Some(400),
        tools: (0..5).map(|n| tool(&format!("tool_{n}")).into()).collect(),
        tool_handler: Some(handler),
        memory: Some(Arc::new(memory.clone())),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("agent"), model.clone(), options).unwrap();
    let event = RoomEvent::new(
        room_id(),
        EventSource::new(channel("member"), ChannelType::WEBSOCKET),
        EventContent::Text(TextContent::new("hello")),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(channel("agent"), room_id(), ChannelType::AI)
    };
    let context = RoomContext::new(RoomRecord::new(room_id()));
    agent.on_event(event, binding, context).await.unwrap();

    let sent = model.requests().remove(0);
    let tools: usize = sent.tools.iter().map(tool_tokens).sum();
    let prompt = estimate_tokens(sent.system_prompt.as_deref().unwrap_or(""));
    assert_eq!(
        memory.0.lock().unwrap().clone(),
        vec![Some(TurnFootprint::new(prompt + tools + NOTES_ROOM, 400))]
    );
}

// Python: tests/test_turn_footprint.py::test_a_later_turn_is_measured_again_with_the_rounds_it_replays
#[tokio::test]
async fn a_later_turn_is_measured_again_with_the_rounds_it_replays() {
    let seen_in_handler: Arc<Mutex<Vec<Option<TurnFootprint>>>> = Arc::default();
    let seen = Arc::clone(&seen_in_handler);
    let handler = ToolHandler::new(move |_name, _arguments| {
        seen.lock().unwrap().push(current_turn_footprint());
        async { Ok(ToolResult::Text("result ".repeat(1000))) }
    });
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", r#"{"q": "a"}"#)]),
        ScriptedRound::answering("done"),
        ScriptedRound::answering("again"),
    ]);
    let memory = MeasuredWindow::default();
    let options = AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Looks up").unwrap().into()],
        tool_handler: Some(handler),
        memory: Some(Arc::new(memory.clone())),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("agent"), model.clone(), options).unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    let member = RecordingTransport::new(
        channel("member"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(member).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["member", "agent"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    for body in ["first", "second"] {
        let message = InboundMessage::new(channel("member"), "u1", TextContent::new(body));
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        kit.process_inbound(message, options).await.unwrap();
        roomkit::testing::idle(&kit).await;
    }
    kit.close().await.unwrap();

    let last = model.requests().pop().unwrap();
    let replayed: Vec<_> = last
        .messages
        .iter()
        .filter(|message| matches!(message.content, roomkit_ai::AiMessageContent::Parts(_)))
        .collect();
    let footprints = memory.0.lock().unwrap().clone();
    let (first, second) = (footprints[0].unwrap(), footprints[1].unwrap());
    // The rest of the turn is the same as the first's: the difference is
    // the round, its results as one message.
    let calls = estimate_message_tokens(replayed[0]);
    let results: usize = replayed[1..]
        .iter()
        .map(|message| estimate_message_tokens(message) - 4)
        .sum();
    assert!(matches!(
        replayed[1].content,
        roomkit_ai::AiMessageContent::Parts(ref parts) if matches!(parts[0], AiContentPart::ToolResult(_))
    ));
    assert_eq!(
        second.input_tokens - first.input_tokens,
        calls + 4 + results
    );
    assert_eq!(seen_in_handler.lock().unwrap().clone(), vec![None]);
}

// Python: tests/test_turn_footprint.py::test_the_footprint_is_the_turn_as_round_0_sends_it
// (with tool_call_intents on: Python's request alone carries the intent
// property, `_shown_to_model`, and the footprint measures the tools' own
// schemas)
#[tokio::test]
async fn the_footprint_measures_the_tools_own_schemas_when_intents_are_on() {
    let declared: Vec<AiTool> = (0..3).map(|n| tool(&format!("tool_{n}"))).collect();
    let handler = ToolHandler::new(|_name, _arguments| async { Ok(ToolResult::Text("ok".into())) });
    let options = AiChannelOptions {
        tools: declared.iter().cloned().map(Into::into).collect(),
        tool_handler: Some(handler),
        tool_call_intents: true,
        ..AiChannelOptions::default()
    };
    let (footprints, model) = measured(options).await;

    let own: usize = declared.iter().map(tool_tokens).sum();
    let shown: usize = model.requests()[0].tools.iter().map(tool_tokens).sum();
    assert!(shown > own, "the request shows the intent property");
    // No system prompt: an empty text's estimate.
    let prompt = estimate_tokens("");
    assert_eq!(
        footprints,
        vec![Some(TurnFootprint::new(prompt + own + NOTES_ROOM, 0))]
    );
}

// Python: tests/test_people_taking_part.py::TestTheAgentKnowsWhoIsThere::test_the_turn_s_measure_counts_the_note
#[tokio::test]
async fn the_turn_s_measure_counts_the_people_note() {
    let person = |id: &str, name: &str| Participant {
        display_name: Some(name.to_owned()),
        ..Participant::at(
            ParticipantId::new(id).unwrap(),
            room_id(),
            channel("member"),
            roomkit::model::Timestamp::UNIX_EPOCH,
        )
    };
    let mut sizes = Vec::new();
    for people in [
        vec![person("u-alice", "Alice")],
        vec![person("u-alice", "Alice"), person("u-bob", "Bob")],
    ] {
        let model = ScriptedProvider::new([ScriptedRound::answering("ok")]);
        let memory = Measured::default();
        let options = AiChannelOptions {
            memory: Some(Arc::new(memory.clone())),
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("agent"), model, options).unwrap();
        let mut event = RoomEvent::new(
            room_id(),
            EventSource::new(channel("member"), ChannelType::WEBSOCKET),
            EventContent::Text(TextContent::new("hello")),
        );
        event.source.participant_id = Some(ParticipantId::new("u-alice").unwrap());
        let binding = ChannelBinding {
            category: ChannelCategory::Intelligence,
            ..ChannelBinding::new(channel("agent"), room_id(), ChannelType::AI)
        };
        agent
            .on_event(event, binding, context_of(people))
            .await
            .unwrap();
        sizes.push(memory.0.lock().unwrap()[0].unwrap().input_tokens);
    }
    assert_eq!(sizes[0], estimate_tokens("") + NOTES_ROOM);
    assert!(sizes[1] > sizes[0], "{sizes:?}");
}
