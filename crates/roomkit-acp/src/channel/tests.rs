//! The channel's turn driven against an agent written as raw JSON lines
//! over an in-memory pipe: what a turn holds while it runs, and how it
//! ends when it is cut.

mod cancellation;
mod configuration;
mod context;
mod detachment;
mod directories;
mod handler_lifecycle;
mod mcp;
mod opening;
mod record;
pub(in crate::channel) mod recovery;
mod session_lifecycle;
mod standalone;
mod steering;
mod steering_drain;

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_core::Stream;
use roomkit::Channel;
use roomkit::model::{ChannelId, ResponseMetadata, RoomId};
use serde_json::{Value, json};
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
};
use tokio::sync::mpsc;

use super::turn::{Ask, TurnStream};
use super::{AcpChannel, AcpChannelOptions};
use crate::client::AcpClient;
use crate::client_side::ClientSideConnection;
use crate::error::AcpError;
use crate::transport::AcpTransport;

const PATIENCE: Duration = Duration::from_secs(10);

/// A transport handing each connection's other end to the test.
struct Piped {
    agents: mpsc::UnboundedSender<DuplexStream>,
    closed: Arc<AtomicUsize>,
    /// The opens and closes, in order; a close takes a few yields.
    log: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl AcpTransport for Piped {
    type Connection = ClientSideConnection;

    fn name(&self) -> &'static str {
        "piped"
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's open is a future"
    )]
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<ClientSideConnection, AcpError> {
        self.log.lock().unwrap().push("open");
        let (ours, theirs) = tokio::io::duplex(1 << 16);
        self.agents.send(theirs).unwrap();
        let (reader, writer) = tokio::io::split(ours);
        Ok(ClientSideConnection::new(client, reader, writer))
    }

    async fn close(&self) {
        self.log.lock().unwrap().push("close-start");
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        self.closed.fetch_add(1, Ordering::SeqCst);
        self.log.lock().unwrap().push("close-end");
    }
}

/// The agent's end of the pipe.
struct Agent {
    lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
    writer: WriteHalf<DuplexStream>,
}

impl Agent {
    fn new(stream: DuplexStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            lines: BufReader::new(reader).lines(),
            writer,
        }
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(PATIENCE, self.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }

    async fn answer(&mut self, request: &Value, result: Value) {
        let line = json!({ "jsonrpc": "2.0", "id": request["id"], "result": result });
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .unwrap();
    }

    /// Answers `initialize` and `session/new`, and returns the prompt.
    async fn prompted(&mut self, initialize: bool) -> Value {
        if initialize {
            let request = self.read().await;
            assert_eq!(request["method"], "initialize");
            let announced = json!({
                "protocolVersion": 1,
                "agentCapabilities": { "sessionCapabilities": { "close": {} } },
            });
            self.answer(&request, announced).await;
        }
        let request = self.read().await;
        assert_eq!(request["method"], "session/new");
        let session = format!(
            "session-{}",
            request["params"]["_meta"]["roomkit.live/roomId"]
                .as_str()
                .unwrap()
        );
        self.answer(&request, json!({ "sessionId": session })).await;
        let prompt = self.read().await;
        assert_eq!(prompt["method"], "session/prompt");
        prompt
    }
}

fn channel() -> (
    AcpChannel<Piped>,
    mpsc::UnboundedReceiver<DuplexStream>,
    Arc<AtomicUsize>,
) {
    let (agents, opened) = mpsc::unbounded_channel();
    let closed = Arc::new(AtomicUsize::new(0));
    let transport = Piped {
        agents,
        closed: Arc::clone(&closed),
        log: Arc::default(),
    };
    let options = AcpChannelOptions {
        transport: Some(transport),
        ..AcpChannelOptions::default()
    };
    let channel = AcpChannel::new(ChannelId::new("claude").unwrap(), "/srv", options).unwrap();
    (channel, opened, closed)
}

fn test_input(room: &str) -> super::context::Input {
    use roomkit::model::{
        ChannelType, EventSource, Room, RoomContext, RoomEvent, TextContent, Timestamp,
    };
    let room_id = RoomId::new(room).unwrap();
    let trigger = RoomEvent::new(
        room_id.clone(),
        EventSource::new(ChannelId::new("ws").unwrap(), ChannelType::WEBSOCKET),
        TextContent::new("hello"),
    );
    super::context::Input {
        notes: None,
        context: RoomContext::new(Room::at(room_id, Timestamp::UNIX_EPOCH)),
        trigger,
        text: "hello".to_owned(),
        blocks: Vec::new(),
    }
}

fn turn<T: AcpTransport>(channel: &AcpChannel<T>, room: &str) -> (TurnStream<T>, ResponseMetadata) {
    let record = ResponseMetadata::new();
    record.insert("acp", json!({ "protocol_version": 1 }));
    let ask = Ask {
        standalone: false,
        room_id: RoomId::new(room).unwrap(),
        event_id: format!("{room}-event"),
        input: test_input(room),
        record: record.clone(),
        host: None,
        answers_room: true,
    };
    (TurnStream::new(Arc::clone(&channel.shared), ask), record)
}

/// Polls `stream` once.
async fn poll_once<T: AcpTransport>(stream: &mut TurnStream<T>) {
    let mut polled = false;
    poll_fn(|cx| {
        if !polled {
            polled = true;
            let _item = Pin::new(&mut *stream).poll_next(cx);
        }
        std::task::Poll::Ready(())
    })
    .await;
}

/// Reads `stream` to its end: its texts, and whether it ended failing.
async fn drained<T: AcpTransport>(stream: &mut TurnStream<T>) -> (Vec<String>, bool) {
    let mut texts = Vec::new();
    loop {
        let item = poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await;
        match item {
            Some(Ok(roomkit::ResponseItem::Text(text))) => texts.push(text),
            Some(Ok(_)) => {}
            Some(Err(_)) => return (texts, true),
            None => return (texts, false),
        }
    }
}

/// Drives `stream` until `agent` returns, polling the stream meanwhile.
async fn while_agent<T: AcpTransport, F: Future>(
    stream: &mut TurnStream<T>,
    agent: F,
) -> F::Output {
    tokio::pin!(agent);
    loop {
        tokio::select! {
            output = &mut agent => return output,
            () = poll_once(stream) => tokio::task::yield_now().await,
        }
    }
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_active_turns_counts_a_turn_from_prompt_to_stream_end
#[tokio::test]
async fn a_turn_is_counted_from_its_room_to_the_end_of_its_stream() {
    let (channel, mut agents, _closed) = channel();
    let (mut stream, _record) = turn(&channel, "room-1");
    // Nothing runs before the stream is read.
    assert_eq!(channel.active_turns(), 0);
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let prompt = while_agent(&mut stream, agent.prompted(true)).await;
    assert_eq!(channel.active_turns(), 1);
    agent
        .answer(&prompt, json!({ "stopReason": "end_turn" }))
        .await;
    let (_texts, failed) = drained(&mut stream).await;
    assert!(!failed);
    // Counted until the stream is over, as Python's: none once its end
    // was read.
    assert_eq!(channel.active_turns(), 0);
    drop(stream);
    assert_eq!(channel.active_turns(), 0);
}

#[tokio::test]
async fn a_turn_cut_mid_prompt_is_marked_interrupted_and_cancelled() {
    let (channel, mut agents, _closed) = channel();
    let (mut stream, record) = turn(&channel, "room-1");
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    while_agent(&mut stream, agent.prompted(true)).await;
    drop(stream);
    assert_eq!(
        record.get("acp"),
        Some(json!({ "protocol_version": 1, "interrupted": true }))
    );
    let cancel = agent.read().await;
    assert_eq!(cancel["method"], "session/cancel");
    assert_eq!(channel.active_turns(), 0);
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_sessions_are_isolated_by_room_and_can_be_cancelled
#[tokio::test]
async fn rooms_run_their_turns_at_once_on_one_connection() {
    let (channel, mut agents, _closed) = channel();
    let (mut first, _) = turn(&channel, "room-1");
    poll_once(&mut first).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let held = while_agent(&mut first, agent.prompted(true)).await;
    // Room 2's turn runs to its end while room 1's prompt is held.
    let (mut second, record) = turn(&channel, "room-2");
    let prompt = while_agent(&mut second, agent.prompted(false)).await;
    agent
        .answer(&prompt, json!({ "stopReason": "end_turn" }))
        .await;
    let (_texts, failed) = drained(&mut second).await;
    assert!(!failed);
    assert_eq!(
        record.get("acp"),
        Some(json!({ "protocol_version": 1, "prompt_returned": true }))
    );
    agent
        .answer(&held, json!({ "stopReason": "end_turn" }))
        .await;
    let (_texts, failed) = drained(&mut first).await;
    assert!(!failed);
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_close_survives_an_agent_that_stopped_answering
#[tokio::test]
async fn close_is_bounded_when_the_agent_stops_answering() {
    let (channel, mut agents, closed) = channel();
    let (mut stream, _) = turn(&channel, "room-1");
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let prompt = while_agent(&mut stream, agent.prompted(true)).await;
    agent
        .answer(&prompt, json!({ "stopReason": "end_turn" }))
        .await;
    drained(&mut stream).await;
    drop(stream);
    // The agent reads `session/close` and never answers it; the clock runs
    // on as the close waits.
    tokio::time::pause();
    let closing = channel.close();
    let ignoring = async {
        assert_eq!(agent.read().await["method"], "session/close");
        std::future::pending::<()>().await;
    };
    tokio::select! {
        () = closing => {}
        () = ignoring => {}
    }
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failure_before_the_prompt_carries_its_python_class() {
    let (channel, mut agents, _closed) = channel();
    let (mut stream, _) = turn(&channel, "room-1");
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let request = agent.read().await;
    agent
        .answer(&request, json!({ "protocolVersion": 2 }))
        .await;
    let failure = loop {
        match poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
            Some(Err(failure)) => break failure,
            Some(Ok(_)) => {}
            None => panic!("the turn did not fail"),
        }
    };
    let classed = failure.downcast_ref::<roomkit::ClassedError>().unwrap();
    assert_eq!(classed.class(), "RuntimeError");
}

// Python: ACPChannel.on_event outside a kit returns its stream; an AI
// channel's turn is read whole there, and so is this one (HITL c2e5eb96).
#[tokio::test]
async fn a_turn_outside_a_kit_is_read_whole_as_one_message() {
    let (channel, mut agents, _closed) = channel();
    let (stream, record) = turn(&channel, "room-1");
    let trigger = roomkit::model::RoomEvent::new(
        RoomId::new("room-1").unwrap(),
        roomkit::model::EventSource::new(
            ChannelId::new("ws").unwrap(),
            roomkit::model::ChannelType::WEBSOCKET,
        ),
        roomkit::model::TextContent::new("hello"),
    );
    let reading = super::buffered(stream, &trigger, channel.source(), record);
    let agent_side = async {
        let mut agent = Agent::new(agents.recv().await.unwrap());
        let prompt = agent.prompted(true).await;
        let update = json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": prompt["params"]["sessionId"],
                "update": { "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "Hi." } },
            },
        });
        agent
            .writer
            .write_all(format!("{update}\n").as_bytes())
            .await
            .unwrap();
        agent
            .answer(&prompt, json!({ "stopReason": "end_turn" }))
            .await;
        agent
    };
    let (output, _agent) = tokio::join!(reading, agent_side);
    let output = output.unwrap();
    assert!(output.responded);
    assert_eq!(output.response_events.len(), 1);
    let message = &output.response_events[0];
    assert_eq!(
        message.content,
        roomkit::model::TextContent::new("Hi.").into()
    );
    assert_eq!(
        message.metadata.get("acp"),
        Some(&json!({ "protocol_version": 1, "prompt_returned": true }))
    );
}

/// Whether `agent` writes nothing more within a short wait.
async fn quiet(agent: &mut Agent) -> bool {
    tokio::time::timeout(Duration::from_millis(100), agent.lines.next_line())
        .await
        .is_err()
}

#[tokio::test]
async fn a_cut_turns_cancel_reaches_the_agent_before_the_rooms_next_prompt() {
    let (channel, mut agents, _closed) = channel();
    let (mut first, _) = turn(&channel, "room-1");
    poll_once(&mut first).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    while_agent(&mut first, agent.prompted(true)).await;
    let (mut second, _) = turn(&channel, "room-1");
    drop(first);
    poll_once(&mut second).await;
    let next = while_agent(&mut second, agent.read()).await;
    assert_eq!(next["method"], "session/cancel");
    let next = while_agent(&mut second, agent.read()).await;
    assert_eq!(next["method"], "session/prompt");
}

#[tokio::test]
async fn a_turn_cut_once_the_agent_answered_is_left_as_it_ended() {
    let (channel, mut agents, _closed) = channel();
    let (mut stream, record) = turn(&channel, "room-1");
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let prompt = while_agent(&mut stream, agent.prompted(true)).await;
    agent
        .answer(&prompt, json!({ "stopReason": "end_turn" }))
        .await;
    // The answer is read by the prompt's own task, the stream never read
    // again.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while record
        .get("acp")
        .and_then(|acp| acp.get("prompt_returned").cloned())
        .is_none()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the answer was not read"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(stream);
    assert_eq!(
        record.get("acp"),
        Some(json!({ "protocol_version": 1, "prompt_returned": true }))
    );
    assert!(
        quiet(&mut agent).await,
        "the agent was told to cancel an answered prompt"
    );
}

#[tokio::test]
async fn a_handshake_cut_part_way_closes_the_transport_and_the_next_turn_reconnects() {
    let (channel, mut agents, closed) = channel();
    let (mut first, _) = turn(&channel, "room-1");
    poll_once(&mut first).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let initialize = while_agent(&mut first, agent.read()).await;
    assert_eq!(initialize["method"], "initialize");
    drop(first);
    let (mut second, _) = turn(&channel, "room-1");
    // The second turn waits for the cut handshake's close, then opens.
    let opened = while_agent(&mut second, agents.recv()).await;
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    // The next open comes once the cut handshake's close ended.
    let log = channel
        .shared
        .connector
        .transport
        .log
        .lock()
        .unwrap()
        .clone();
    assert_eq!(log, ["open", "close-start", "close-end", "open"]);
    let mut agent = Agent::new(opened.unwrap());
    let prompt = while_agent(&mut second, agent.prompted(true)).await;
    agent
        .answer(&prompt, json!({ "stopReason": "end_turn" }))
        .await;
    let (_texts, failed) = drained(&mut second).await;
    assert!(!failed);
}

#[tokio::test]
async fn close_cancels_a_prompt_in_flight_and_a_cut_close_still_closes_the_transport() {
    let (channel, mut agents, closed) = channel();
    let (mut stream, _) = turn(&channel, "room-1");
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    while_agent(&mut stream, agent.prompted(true)).await;
    // The close is cut while the agent leaves its session/close unanswered.
    let cut = tokio::time::timeout(Duration::from_millis(50), channel.close()).await;
    assert!(cut.is_err());
    assert_eq!(agent.read().await["method"], "session/cancel");
    assert_eq!(agent.read().await["method"], "session/close");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while closed.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the transport stayed open"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A second close does nothing.
    channel.close().await;
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_turn_stopped_by_the_close_ends_quietly_marked_interrupted() {
    let (channel, mut agents, _closed) = channel();
    let (mut stream, record) = turn(&channel, "room-1");
    poll_once(&mut stream).await;
    let mut agent = Agent::new(agents.recv().await.unwrap());
    while_agent(&mut stream, agent.prompted(true)).await;
    // The agent leaves its session/close unanswered: the close's bound
    // passes on a paused clock.
    tokio::time::pause();
    let closing = channel.close();
    let reading = drained(&mut stream);
    let ((), (texts, failed)) = tokio::join!(closing, reading);
    assert_eq!(texts.len(), 0);
    assert!(!failed, "a turn the close stopped ends with no error");
    assert_eq!(
        record.get("acp"),
        Some(json!({ "protocol_version": 1, "interrupted": true }))
    );
}

#[tokio::test]
async fn a_program_that_is_missing_fails_the_turn_as_file_not_found() {
    let options = AcpChannelOptions {
        command: Some(vec!["/nonexistent/roomkit-acp-agent".to_owned()]),
        ..AcpChannelOptions::default()
    };
    let channel: AcpChannel =
        AcpChannel::new(ChannelId::new("claude").unwrap(), "/srv", options).unwrap();
    let record = ResponseMetadata::new();
    let ask = Ask {
        standalone: false,
        room_id: RoomId::new("room-1").unwrap(),
        event_id: "e".to_owned(),
        input: test_input("room-1"),
        record,
        host: None,
        answers_room: true,
    };
    let mut stream = TurnStream::new(Arc::clone(&channel.shared), ask);
    let failure = loop {
        match poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
            Some(Err(failure)) => break failure,
            Some(Ok(_)) => {}
            None => panic!("the turn did not fail"),
        }
    };
    let classed = failure.downcast_ref::<roomkit::ClassedError>().unwrap();
    assert_eq!(classed.class(), "FileNotFoundError");
}

mod received_instructions;
