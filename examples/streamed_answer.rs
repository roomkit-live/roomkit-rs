//! A streamed answer (RFC §6.1, §6.4, §12.2 steps 13s and 14s): a bakery's
//! agent answers on a screen that shows its text as the model writes it,
//! and on an SMS number that receives each message once it is stored.
//!
//! - The screen is a streaming channel (`supports_streaming_delivery`): its
//!   `deliver_stream` reads the answer's text piece by piece, the words
//!   the model writes together reaching it as one piece (RFC §10.6.4), then
//!   each row as the store holds it, with its id and index. The SMS number
//!   buffers:
//!   it receives the rows through `deliver()` while the turn goes on, and
//!   never a tool call's row.
//! - Mia's question makes the model say a sentence, call `check_shelves`,
//!   then answer: two text segments with the call between them, each
//!   segment a message of its own. The call runs only once the screen has
//!   read past its start.
//! - Noah's question is answered slowly, and the screen stops reading after
//!   a few words, as a person who turns away: the answer stops there, the
//!   text the screen was handed stored marked `cancelled`, and the model is
//!   asked nothing more.
//!
//! The screen shows text before `BEFORE_BROADCAST` judges the message it
//! ends in (RFC §6.4: a streamed segment, once shown, is not taken back): a
//! room whose hooks rewrite answers delivers them buffered.
//!
//! It runs against a local fake of the OpenAI wire, which streams, so it
//! needs no network and no key.
//!
//! ```sh
//! cargo run --example streamed_answer
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventType, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, DeliveryStream,
    ProcessInboundOptions, RoomKit, StreamItem,
};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, ToolHandler, ToolResult};
use serde_json::{Value, json};

use shared::text_of;
use shared::{Checks, FakeOpenAi, Log, OpenAiEndpoint, Reply};

const SYSTEM_PROMPT: &str = "You answer a bakery's customers, in a few words.";

/// What Mia asks, what the model says before and after its call.
const MIA_ASKS: &str = "Any sourdough left?";
const CHECKING: &str = "Let me check the shelves.";
const THREE_LEFT: &str = "We have three sourdough loaves left.";

/// What Noah asks, the answer the model writes slowly, and the words the
/// screen reads of it before it stops.
const NOAH_ASKS: &str = "What time do you open on Sunday?";
const SUNDAY: &str =
    "On Sunday we open at eight and close at one, with fresh croissants all morning.";
const WORDS_READ: usize = 3;
const WORD_PAUSE: Duration = Duration::from_millis(20);

/// The model's script: Mia's two rounds, then Noah's answer.
fn script() -> Vec<Reply> {
    vec![
        Reply::calling(
            CHECKING,
            [("call_shelves", "check_shelves", r#"{"bread": "sourdough"}"#)],
        ),
        Reply::from(THREE_LEFT),
        Reply::from(SUNDAY).pausing(WORD_PAUSE),
    ]
}

/// What the screen read, in order.
#[derive(Clone, Debug, PartialEq)]
enum Read {
    /// A piece of text.
    Text(String),
    /// A row the store holds: its type, its index, its text.
    Row(EventType, u64, String),
}

/// A screen that shows the agent's answers as they are written, and stops
/// reading an answer once it showed `stops_after` pieces of it, when told.
#[derive(Clone)]
struct Screen {
    channel_id: ChannelId,
    read: Log<Read>,
    stops_after: Arc<AtomicUsize>,
}

impl Screen {
    fn new(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            read: Log::new(),
            stops_after: Arc::new(AtomicUsize::new(usize::MAX)),
        }
    }
}

impl Channel for Screen {
    fn channel_id(&self) -> ChannelId {
        self.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn supports_streaming_delivery(&self) -> bool {
        true
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let mut pieces = 0;
        while let Some(item) = stream.next().await {
            match item? {
                StreamItem::Text(text) => {
                    println!("  screen: {text:?}");
                    self.read.push(Read::Text(text));
                    pieces += 1;
                    if pieces >= self.stops_after.load(Ordering::SeqCst) {
                        println!("  the screen stops reading");
                        break;
                    }
                }
                StreamItem::Row(row) => {
                    let text = text_of(&row.content);
                    println!("  screen: #{} {:?} {text:?}", row.index, row.r#type);
                    self.read.push(Read::Row(row.r#type, row.index, text));
                }
                StreamItem::Thinking(reasoning) => println!("  screen: thinking {reasoning:?}"),
                StreamItem::Usage(told) => println!("  screen: this step used {:?}", told.usage),
                _ => {}
            }
        }
        Ok(ChannelOutput::default())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let shelves = Arc::new(AtomicUsize::new(0));
    let (kit, screen, sms, room) = bakery(&endpoint, &shelves).await?;
    let mut checks = Checks::default();

    println!("Mia asks by SMS: {MIA_ASKS:?}");
    ask(&kit, &sms, &room, "+15550001", MIA_ASKS).await?;
    let mia = shared::timeline(&kit, &room).await?;
    check_mia(&mut checks, &screen.read.entries(), &sms, &mia, &shelves);

    println!("Noah asks by SMS: {NOAH_ASKS:?}; the screen reads {WORDS_READ} words.");
    screen.stops_after.store(WORDS_READ, Ordering::SeqCst);
    let before = screen.read.entries().len();
    ask(&kit, &sms, &room, "+15550002", NOAH_ASKS).await?;
    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    let read_by_noah = screen.read.entries().split_off(before);
    check_noah(&mut checks, &read_by_noah, &sms, &timeline, &endpoint);
    kit.close().await?;
    Ok(checks.finish())
}

/// The bakery's room: the screen, the SMS number and the agent, which
/// serves `check_shelves`, counting its runs in `shelves`.
async fn bakery(
    endpoint: &OpenAiEndpoint,
    shelves: &Arc<AtomicUsize>,
) -> Result<(RoomKit, Screen, RecordingTransport, RoomId), Box<dyn Error>> {
    let counted = Arc::clone(shelves);
    let check = AiTool {
        parameters: json!({ "type": "object", "properties": { "bread": { "type": "string" } } }),
        ..AiTool::new("check_shelves", "How many loaves of a bread are left")?
    };
    let handler = ToolHandler::new(move |_name, _arguments| {
        counted.fetch_add(1, Ordering::SeqCst);
        async { Ok(ToolResult::Json(json!({ "sourdough": 3 }))) }
    });
    let options = AiChannelOptions {
        system_prompt: Some(SYSTEM_PROMPT.to_owned()),
        tools: vec![check.into()],
        tool_handler: Some(handler),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("assistant")?, endpoint.provider()?, options)?;
    let kit = shared::kit();
    let screen = Screen::new(ChannelId::new("screen")?);
    let sms = RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(screen.clone())?;
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("bakery")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in ["screen", "sms", "assistant"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(channel)?,
            AttachChannelOptions::default(),
        )
        .await?;
    }
    Ok((kit, screen, sms, room))
}

/// `text` from `number` by SMS, once its turn answered.
async fn ask(
    kit: &RoomKit,
    sms: &RecordingTransport,
    room: &RoomId,
    number: &str,
    text: &str,
) -> Result<(), Box<dyn Error>> {
    let message = InboundMessage::new(sms.channel_id().clone(), number, TextContent::new(text));
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;
    if let Some(error) = result.error {
        println!("  the turn failed: {}", error.message);
    }
    Ok(())
}

/// The agent's rows in `timeline`: type and text.
fn rows(timeline: &[RoomEvent]) -> Vec<(EventType, String)> {
    timeline
        .iter()
        .filter(|event| event.source.channel_id.as_str() == "assistant")
        .map(|event| (event.r#type, text_of(&event.content)))
        .collect()
}

/// Mia's turn: the screen reads the pieces of each segment, the words the
/// model wrote together as one, then its row as stored, the call's two
/// rows between them; the SMS receives the two messages only; the call ran
/// once, between the segments.
fn check_mia(
    checks: &mut Checks,
    read: &[Read],
    sms: &RecordingTransport,
    timeline: &[RoomEvent],
    shelves: &AtomicUsize,
) {
    let stored = rows(timeline);
    checks.expect(
        "the room stores a segment, the call's start and end, then the answer, the call \
         run once between the segments",
        stored.iter().map(|(kind, _)| *kind).eq([
            EventType::Message,
            EventType::ToolCallStart,
            EventType::ToolCallEnd,
            EventType::Message,
        ]) && stored.first().map(|(_, text)| text.as_str()) == Some(CHECKING)
            && stored.last().map(|(_, text)| text.as_str()) == Some(THREE_LEFT)
            && shelves.load(Ordering::SeqCst) == 1,
    );
    // Each run of pieces joins to the row that follows it.
    let mut joined = String::new();
    let mut segments_join = true;
    let mut kinds = Vec::new();
    for item in read {
        match item {
            Read::Text(text) => joined.push_str(text),
            Read::Row(kind, index, text) => {
                if *kind == EventType::Message {
                    segments_join &= &joined == text;
                    joined.clear();
                }
                kinds.push(*kind);
                let held = timeline
                    .iter()
                    .any(|event| event.index == *index && event.r#type == *kind);
                segments_join &= held;
            }
        }
    }
    checks.expect(
        "the screen reads each segment's pieces, then its row as the store holds it, the \
         pieces joining to the row's text, and the call's rows between them",
        segments_join
            && kinds
                == [
                    EventType::Message,
                    EventType::ToolCallStart,
                    EventType::ToolCallEnd,
                    EventType::Message,
                ]
            && read
                .iter()
                .filter(|item| matches!(item, Read::Text(_)))
                .count()
                >= 2,
    );
    checks.expect(
        "the SMS number receives the two messages once each is stored, and the call's rows, \
         which carry no text",
        sms.texts() == [CHECKING, "", "", THREE_LEFT],
    );
}

/// Noah's turn: the screen read a few words and stopped; the room keeps
/// what it was handed, marked cancelled, and the model wrote no more.
fn check_noah(
    checks: &mut Checks,
    read: &[Read],
    sms: &RecordingTransport,
    timeline: &[RoomEvent],
    endpoint: &OpenAiEndpoint,
) {
    let shown: String = read
        .iter()
        .filter_map(|item| match item {
            Read::Text(text) => Some(text.as_str()),
            Read::Row(..) => None,
        })
        .collect();
    let answer = timeline.last();
    println!("  the screen showed {shown:?}");
    checks.expect(
        "a screen that stops reading leaves the text it was handed stored, marked cancelled, \
         the turn recording cancelled",
        answer.is_some_and(|answer| {
            text_of(&answer.content) == shown
                && shown.len() < SUNDAY.len()
                && answer.metadata.get("cancelled") == Some(&json!(true))
                && answer.metadata.get("loop_end_reason") == Some(&json!("cancelled"))
        }),
    );
    checks.expect(
        "the SMS number receives that text, the screen no row of it",
        sms.texts().last().map(String::as_str) == Some(shown.as_str())
            && !read.iter().any(|item| matches!(item, Read::Row(..))),
    );
    let requests: Vec<Value> = endpoint
        .fake()
        .map(FakeOpenAi::requests)
        .unwrap_or_default();
    checks.expect(
        "the model is asked once for Noah's answer, and each request asks for a stream",
        requests.len() == 3
            && requests
                .iter()
                .all(|request| request.get("stream") == Some(&json!(true))),
    );
}
