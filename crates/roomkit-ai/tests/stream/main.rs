//! An AI agent's streamed response, read by a streaming channel (RFC §6.1
//! `deliver_stream`, §6.4, §12.2 steps 13s and 14s; step 4, milestone 6).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod access;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    DirectEvent, EventContent, EventType, HookResult, InboundMessage, RoomContext, RoomEvent,
    RoomId, ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, HookExecution, HookSpec, HookTrigger, InboundResult,
    ProcessInboundOptions, RoomKit, RoomKitOptions, StreamItem,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiAgent, AiProvider, AiRequest, ProviderEvent, ProviderStream, ToolDefinition, ToolOutput,
    Tools,
};
use serde_json::json;
use tokio::sync::Notify;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// What a streaming channel read, in order.
#[derive(Clone, Debug, PartialEq)]
enum Read {
    Text(String),
    Row(EventType, String, u64),
    Failed,
}

/// When a streaming channel stops reading.
#[derive(Clone, Debug, Default)]
enum Stop {
    /// It reads to the end.
    #[default]
    Never,
    /// Once handed this text.
    AfterText(&'static str),
    /// Once handed a call's start row.
    AtACallsStart,
    /// When the signal comes, wherever it reads.
    OnSignal(Arc<Notify>),
    /// It fails once handed this text.
    FailsAfter(&'static str),
    /// It fails when the signal comes, wherever it reads.
    FailsOnSignal(Arc<Notify>),
}

/// A WebSocket-like channel that streams: it records what it reads and
/// what it is delivered.
#[derive(Clone)]
struct Streaming {
    declaration: ChannelDeclaration,
    read: Arc<Mutex<Vec<Read>>>,
    delivered: Arc<Mutex<Vec<RoomEvent>>>,
    stop: Stop,
}

impl Streaming {
    fn new(stop: Stop) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        Self {
            declaration,
            read: Arc::default(),
            delivered: Arc::default(),
            stop,
        }
    }

    fn taking_no_tool_row(mut self) -> Self {
        self.declaration.receives_tool_calls = false;
        self
    }

    fn read(&self) -> Vec<Read> {
        self.read.lock().unwrap().clone()
    }

    fn delivered(&self) -> Vec<u64> {
        self.delivered
            .lock()
            .unwrap()
            .iter()
            .map(|event| event.index)
            .collect()
    }

    fn note(&self, read: Read) {
        self.read.lock().unwrap().push(read);
    }
}

impl Channel for Streaming {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.delivered.lock().unwrap().push(event);
        std::future::ready(Ok(ChannelOutput::default()))
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        loop {
            let item = match &self.stop {
                Stop::OnSignal(signal) => tokio::select! {
                    item = stream.next() => item,
                    () = signal.notified() => return Ok(ChannelOutput::default()),
                },
                Stop::FailsOnSignal(signal) => tokio::select! {
                    item = stream.next() => item,
                    () = signal.notified() => return Err("the socket broke".into()),
                },
                Stop::Never | Stop::AfterText(_) | Stop::AtACallsStart | Stop::FailsAfter(_) => {
                    stream.next().await
                }
            };
            let Some(item) = item else {
                return Ok(ChannelOutput::default());
            };
            match item {
                Ok(StreamItem::Text(text)) => {
                    self.note(Read::Text(text.clone()));
                    match self.stop {
                        Stop::AfterText(stop) if text == stop => {
                            return Ok(ChannelOutput::default());
                        }
                        Stop::FailsAfter(stop) if text == stop => {
                            return Err("the socket broke".into());
                        }
                        Stop::Never
                        | Stop::AfterText(_)
                        | Stop::AtACallsStart
                        | Stop::OnSignal(_)
                        | Stop::FailsAfter(_)
                        | Stop::FailsOnSignal(_) => {}
                    }
                }
                Ok(StreamItem::Row(row)) => {
                    let text = roomkit::text_of(&row.content);
                    self.note(Read::Row(row.event_type, text, row.index));
                    if matches!(self.stop, Stop::AtACallsStart)
                        && row.event_type == EventType::ToolCallStart
                    {
                        return Ok(ChannelOutput::default());
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    self.note(Read::Failed);
                    return Err(error);
                }
            }
        }
    }
}

/// A provider answering each generation with the next round of its
/// script, one event per read, its stream saying when it is dropped.
#[derive(Clone, Default)]
struct Pieces {
    rounds: Arc<Mutex<VecDeque<ScriptedRound>>>,
    requests: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Pieces {
    fn new(rounds: Vec<ScriptedRound>) -> Self {
        Self {
            rounds: Arc::new(Mutex::new(rounds.into())),
            ..Self::default()
        }
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

/// A generation's events, read one at a time.
struct Events {
    events: VecDeque<Result<ProviderEvent, String>>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for Events {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Stream for Events {
    type Item = Result<ProviderEvent, roomkit_ai::ProviderError>;

    fn poll_next(
        self: Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let next = self.get_mut().events.pop_front();
        std::task::Poll::Ready(next.map(|event| event.map_err(Into::into)))
    }
}

impl AiProvider for Pieces {
    fn generate<'a>(&'a self, _request: &'a AiRequest) -> ProviderStream<'a> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let round = self.rounds.lock().unwrap().pop_front();
        let mut events = VecDeque::new();
        match round {
            None => events.push_back(Err("no round left".to_owned())),
            Some(round) => {
                let pieces = if round.deltas.is_empty() && !round.text.is_empty() {
                    vec![round.text.clone()]
                } else {
                    round.deltas.clone()
                };
                events.extend(
                    pieces
                        .into_iter()
                        .map(|piece| Ok(ProviderEvent::Text(piece))),
                );
                if let Some(failure) = round.fails {
                    events.push_back(Err(failure));
                } else {
                    let calls = round.calls.into_iter();
                    events.extend(calls.map(|call| Ok(ProviderEvent::ToolCall(call))));
                    events.push_back(Ok(ProviderEvent::Done(round.done)));
                }
            }
        }
        Box::pin(Events {
            events,
            dropped: Arc::clone(&self.dropped),
        })
    }
}

/// `lookup_order`, counting its runs, signalling `running` while it runs
/// when given one.
fn lookup(runs: Arc<AtomicUsize>, running: Option<Arc<Notify>>) -> Tools {
    let definition = ToolDefinition::new(
        "lookup_order",
        "Find an order",
        json!({ "type": "object", "properties": { "order": { "type": "string" } } }),
    );
    Tools::new().tool(definition, move |_call, _context| {
        let (runs, running) = (Arc::clone(&runs), running.clone());
        async move {
            if let Some(running) = running {
                running.notify_one();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            runs.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutput::Text("shipped".to_owned()))
        }
    })
}

/// A room with a webhook (a target that does not stream), the streaming
/// channel `ws` and an agent over `model`.
struct Room {
    kit: RoomKit,
    web: RecordingTransport,
    ws: Streaming,
    dropped: Arc<AtomicUsize>,
}

impl Room {
    async fn new(rounds: Vec<ScriptedRound>, tools: Tools, ws: Streaming) -> (Self, Pieces) {
        Self::with(rounds, |_kit| tools, ws).await
    }

    /// The room, its agent's tools made with the kit they call into.
    async fn with(
        rounds: Vec<ScriptedRound>,
        tools: impl FnOnce(&RoomKit) -> Tools,
        ws: Streaming,
    ) -> (Self, Pieces) {
        let model = Pieces::new(rounds);
        let dropped = Arc::clone(&model.dropped);
        let kit = RoomKit::new(RoomKitOptions::default());
        let agent = AiAgent::new(channel("agent"), model.clone())
            .with_tools(tools(&kit))
            .unwrap();
        let web = RecordingTransport::new(channel("web"), ChannelType::WEBHOOK);
        kit.register_channel(web.clone()).unwrap();
        kit.register_channel(ws.clone()).unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["web", "ws", "agent"] {
            kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        let room = Self {
            kit,
            web,
            ws,
            dropped,
        };
        (room, model)
    }

    async fn says(&self, text: &str) -> InboundResult {
        let message = InboundMessage::new(channel("web"), "alice", EventContent::text(text));
        let result = self
            .kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        self.kit.idle().await;
        result
    }

    /// The agent's rows: type, text, `cancelled`, `loop_end_reason`.
    async fn rows(&self) -> Vec<(EventType, String, Option<bool>, Option<String>)> {
        self.kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.source.channel_id == channel("agent"))
            .map(|event| {
                let cancelled = event
                    .metadata
                    .get("cancelled")
                    .and_then(serde_json::Value::as_bool);
                let reason = event
                    .metadata
                    .get("loop_end_reason")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                (
                    event.event_type,
                    roomkit::text_of(&event.content),
                    cancelled,
                    reason,
                )
            })
            .collect()
    }

    async fn timeline(&self) -> Vec<RoomEvent> {
        self.kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap()
    }

    fn web_rows(&self) -> Vec<(EventType, u64)> {
        self.web
            .deliveries()
            .into_iter()
            .map(|delivery| (delivery.event.event_type, delivery.event.index))
            .collect()
    }
}

fn end_of(timeline: &[RoomEvent]) -> ToolCallContent {
    timeline
        .iter()
        .find_map(|event| match (&event.content, event.event_type) {
            (EventContent::ToolCall(call), EventType::ToolCallEnd) => Some(call.clone()),
            _ => None,
        })
        .unwrap()
}

/// RFC §6.1, §12.2 steps 13s and 14s: the streaming channel reads every
/// delta, then each row once the store holds it, with its index, the
/// deltas joining to the row's text; it is never delivered a row through
/// `deliver()`. A target that does not stream receives each row through
/// `deliver()`, in index order. `BEFORE_BROADCAST` judges each row once
/// (RFC §9.5).
#[tokio::test]
async fn a_streaming_target_reads_the_deltas_then_each_row_the_store_holds() {
    let rounds = vec![
        ScriptedRound::calling("", [("call_1", "lookup_order", r#"{"order": "42"}"#)])
            .in_pieces(["Let me ", "check."]),
        ScriptedRound::writing(["Order 42 ", "shipped."]),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let (room, _model) = Room::new(
        rounds,
        lookup(Arc::clone(&runs), None),
        Streaming::new(Stop::Never),
    )
    .await;
    let judged = Arc::new(Mutex::new(Vec::new()));
    let noting = Arc::clone(&judged);
    let spec = HookSpec::new("judge", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    room.kit
        .hook(spec, move |event, _context| {
            noting.lock().unwrap().push(event.event_type);
            async { Ok(HookResult::allow()) }
        })
        .unwrap();
    let result = room.says("Where is order 42?").await;
    assert_eq!(result.error, None);
    assert_eq!(
        room.ws.read(),
        [
            Read::Text("Let me ".to_owned()),
            Read::Text("check.".to_owned()),
            Read::Row(EventType::Message, "Let me check.".to_owned(), 4),
            Read::Row(
                EventType::ToolCallStart,
                "[Tool call: lookup_order]".to_owned(),
                5
            ),
            Read::Row(
                EventType::ToolCallEnd,
                "[Tool call: lookup_order]".to_owned(),
                6
            ),
            Read::Text("Order 42 ".to_owned()),
            Read::Text("shipped.".to_owned()),
            Read::Row(EventType::Message, "Order 42 shipped.".to_owned(), 7),
        ]
    );
    assert_eq!(room.ws.delivered(), [3]);
    assert_eq!(
        room.web_rows(),
        [
            (EventType::Message, 4),
            (EventType::ToolCallStart, 5),
            (EventType::ToolCallEnd, 6),
            (EventType::Message, 7)
        ]
    );
    assert_eq!(
        *judged.lock().unwrap(),
        [
            EventType::Message,
            EventType::Message,
            EventType::ToolCallStart,
            EventType::ToolCallEnd,
            EventType::Message
        ]
    );
    let rows = room.rows().await;
    assert_eq!(rows.last().unwrap().3.as_deref(), Some("completed"));
    room.kit.shutdown().await;
}

/// RFC §12.2 step 13s: a reader that stops mid-answer leaves the text it
/// was handed stored, marked cancelled, the turn recording `cancelled`; the
/// model's next delta is never read, its stream closed, and nothing is left
/// running.
#[tokio::test]
async fn a_reader_that_stops_leaves_its_text_stored_cancelled_and_its_stream_closed() {
    let rounds = vec![ScriptedRound::writing(["The ", "answer ", "is 42."])];
    let ws = Streaming::new(Stop::AfterText("answer "));
    let (room, model) = Room::new(rounds, Tools::new(), ws).await;
    let result = room.says("What is the answer?").await;
    assert_eq!(result.error, None);
    assert_eq!(
        room.rows().await,
        [(
            EventType::Message,
            "The answer ".to_owned(),
            Some(true),
            Some("cancelled".to_owned())
        )]
    );
    assert_eq!(
        room.ws.read(),
        [
            Read::Text("The ".to_owned()),
            Read::Text("answer ".to_owned())
        ]
    );
    // Delivered to the others, never to the reader that stopped.
    assert_eq!(room.ws.delivered(), [3]);
    assert_eq!(room.web_rows(), [(EventType::Message, 4)]);
    assert_eq!(
        (model.requests(), room.dropped.load(Ordering::SeqCst)),
        (1, 1)
    );
    room.kit.shutdown().await;
}

/// RFC §12.2 step 13s, §10.6.4: a reader that stops at a call's start
/// leaves the call unrun, its end stored `failed`, and the model is asked
/// nothing more; the turn's record, `cancelled`, is written on its last
/// message, `ON_EVENT_UPDATED` firing (RFC §6.4).
#[tokio::test]
async fn a_reader_that_stops_at_a_call_s_start_runs_no_call() {
    let rounds = vec![
        ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", "{}")]),
        ScriptedRound::answering("Shipped."),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let ws = Streaming::new(Stop::AtACallsStart);
    let (room, model) = Room::new(rounds, lookup(Arc::clone(&runs), None), ws).await;
    let updated = Arc::new(Mutex::new(Vec::new()));
    let noting = Arc::clone(&updated);
    room.kit
        .observe(
            "updated",
            HookTrigger::OnEventUpdated,
            move |event, _context| {
                noting
                    .lock()
                    .unwrap()
                    .push(roomkit::text_of(&event.content));
                async {}
            },
        )
        .unwrap();
    room.says("Where is my order?").await;
    let timeline = room.timeline().await;
    let end = end_of(&timeline);
    assert_eq!(
        (
            runs.load(Ordering::SeqCst),
            model.requests(),
            end.status,
            end.outcome,
            end.error.as_deref(),
        ),
        (
            0,
            1,
            ToolCallStatus::Failed,
            Some(ToolCallOutcome::Cancelled),
            Some("cancelled")
        )
    );
    assert_eq!(
        room.rows().await[0],
        (
            EventType::Message,
            "Let me check.".to_owned(),
            None,
            Some("cancelled".to_owned())
        )
    );
    assert_eq!(*updated.lock().unwrap(), ["Let me check."]);
    room.kit.shutdown().await;
}

/// RFC §12.2 step 13s: a call already running when its reader stops is let
/// finish, its end stored with its real result and delivered to the
/// streaming channel too, which no longer reads it; the stream is closed
/// before the model's next round.
#[tokio::test]
async fn a_reader_that_stops_while_a_call_runs_lets_it_finish_and_asks_no_more() {
    let rounds = vec![
        ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", "{}")]),
        ScriptedRound::answering("Shipped."),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(Notify::new());
    let ws = Streaming::new(Stop::OnSignal(Arc::clone(&running)));
    let (room, model) = Room::new(rounds, lookup(Arc::clone(&runs), Some(running)), ws).await;
    room.says("Where is my order?").await;
    let timeline = room.timeline().await;
    let end = end_of(&timeline);
    assert_eq!(
        (
            runs.load(Ordering::SeqCst),
            model.requests(),
            end.status,
            end.result,
        ),
        (1, 1, ToolCallStatus::Completed, json!("shipped"))
    );
    let end_index = timeline
        .iter()
        .find(|event| event.event_type == EventType::ToolCallEnd)
        .map(|event| event.index)
        .unwrap();
    assert_eq!(room.ws.delivered(), [3, end_index]);
    assert_eq!(room.dropped.load(Ordering::SeqCst), 1);
    room.kit.shutdown().await;
}

/// RFC §12.2 step 13s: a response that fails mid-answer stores the text it
/// produced, not marked cancelled; the streaming channel is handed that
/// row, then the failure, and is not delivered it again; the others are;
/// `ON_ERROR` fires and the call carries the error.
#[tokio::test]
async fn a_response_that_fails_stores_its_text_and_hands_it_to_the_target_first() {
    let rounds =
        vec![ScriptedRound::writing(["The ", "answer "]).failing("the provider went away")];
    let (room, _model) = Room::new(rounds, Tools::new(), Streaming::new(Stop::Never)).await;
    let errors = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&errors);
    room.kit
        .observe("errors", HookTrigger::OnError, move |_event, _context| {
            counting.fetch_add(1, Ordering::SeqCst);
            async {}
        })
        .unwrap();
    let result = room.says("What is the answer?").await;
    assert!(
        result
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("the provider went away")),
        "{result:?}"
    );
    assert_eq!(
        room.rows().await,
        [(EventType::Message, "The answer ".to_owned(), None, None)]
    );
    assert_eq!(
        room.ws.read(),
        [
            Read::Text("The ".to_owned()),
            Read::Text("answer ".to_owned()),
            Read::Row(EventType::Message, "The answer ".to_owned(), 4),
            Read::Failed,
        ]
    );
    assert_eq!(
        (
            room.ws.delivered(),
            room.web_rows(),
            errors.load(Ordering::SeqCst)
        ),
        (vec![3], vec![(EventType::Message, 4)], 1)
    );
    room.kit.shutdown().await;
}

/// RFC §12.2 step 13s: a streaming channel that fails, not the response,
/// may not have shown all it was handed: the text left goes to every
/// target, the streaming one included, and the failure is the call's.
#[tokio::test]
async fn a_streaming_channel_that_fails_leaves_the_text_to_every_target() {
    let rounds = vec![ScriptedRound::writing(["The ", "answer ", "is 42."])];
    let ws = Streaming::new(Stop::FailsAfter("The "));
    let (room, _model) = Room::new(rounds, Tools::new(), ws).await;
    let result = room.says("What is the answer?").await;
    assert!(
        result
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("the socket broke")),
        "{result:?}"
    );
    assert_eq!(
        room.rows().await,
        [(EventType::Message, "The ".to_owned(), None, None)]
    );
    assert_eq!(
        (room.ws.delivered(), room.web_rows()),
        (vec![3, 4], vec![(EventType::Message, 4)])
    );
    room.kit.shutdown().await;
}

/// RFC §12.2 step 13s: a streaming channel that fails while a call runs
/// lets the call finish, its end stored with its real result and delivered
/// to every target, the failed channel included; the stream is closed
/// before the model's next round, and the failure is the call's.
#[tokio::test]
async fn a_streaming_channel_that_fails_while_a_call_runs_lets_it_finish() {
    let rounds = vec![
        ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", "{}")]),
        ScriptedRound::answering("Shipped."),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(Notify::new());
    let ws = Streaming::new(Stop::FailsOnSignal(Arc::clone(&running)));
    let (room, model) = Room::new(rounds, lookup(Arc::clone(&runs), Some(running)), ws).await;
    let result = room.says("Where is my order?").await;
    assert!(
        result
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("the socket broke")),
        "{result:?}"
    );
    let timeline = room.timeline().await;
    let end = end_of(&timeline);
    assert_eq!(
        (
            runs.load(Ordering::SeqCst),
            model.requests(),
            end.status,
            end.result,
        ),
        (1, 1, ToolCallStatus::Completed, json!("shipped"))
    );
    let end_index = timeline
        .iter()
        .find(|event| event.event_type == EventType::ToolCallEnd)
        .map(|event| event.index)
        .unwrap();
    assert_eq!(room.ws.delivered(), [3, end_index]);
    room.kit.shutdown().await;
}

/// RFC §6.4 "The turn's record": a turn that ends on no text, an empty
/// answer after a round, writes its record on the last message it wrote
/// once it ends, `ON_EVENT_UPDATED` firing; the message is not delivered
/// again. (Milestone 5 wrote it into the rows the turn returned, and
/// fired nothing.)
#[tokio::test]
async fn a_turn_ending_on_no_text_writes_its_record_on_its_last_message() {
    let rounds = vec![
        ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", "{}")]),
        ScriptedRound::answering(""),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let (room, _model) = Room::new(rounds, lookup(runs, None), Streaming::new(Stop::Never)).await;
    let updated = Arc::new(Mutex::new(Vec::new()));
    let noting = Arc::clone(&updated);
    room.kit
        .observe(
            "updated",
            HookTrigger::OnEventUpdated,
            move |event, _context| {
                let reason = event.metadata.get("loop_end_reason").cloned();
                noting
                    .lock()
                    .unwrap()
                    .push((roomkit::text_of(&event.content), reason));
                async {}
            },
        )
        .unwrap();
    room.says("Where is my order?").await;
    assert_eq!(
        *updated.lock().unwrap(),
        [("Let me check.".to_owned(), Some(json!("empty_response")))]
    );
    assert_eq!(
        room.web_rows(),
        [
            (EventType::Message, 4),
            (EventType::ToolCallStart, 5),
            (EventType::ToolCallEnd, 6)
        ]
    );
    room.kit.shutdown().await;
}

/// RFC §6.4, §12.2 step 13s: the rows a round ends are stored as its reader
/// reads past them, before its calls run: an event a tool sends through its
/// turn is stored after the round's text and its call's start. (Milestone 5
/// stored the rows when the turn returned, the tool's event first.)
#[tokio::test]
async fn an_event_a_tool_sends_is_stored_after_the_round_s_text_and_start() {
    let definition = ToolDefinition::new("notify", "Notify", json!({ "type": "object" }));
    let tools = Tools::new().tool(definition, |_call, context| {
        let turn = context.turn().cloned();
        async move {
            if let Some(turn) = turn {
                let note = DirectEvent::new(channel("web"), EventContent::text("Noted."));
                turn.send_event(note)
                    .await
                    .map_err(|error| roomkit_ai::ToolError::raised(&error))?;
            }
            Ok(ToolOutput::Text("done".to_owned()))
        }
    });
    let rounds = vec![
        ScriptedRound::calling("Notifying.", [("call_1", "notify", "{}")]),
        ScriptedRound::answering("Done."),
    ];
    let (room, _model) = Room::new(rounds, tools, Streaming::new(Stop::Never)).await;
    room.says("Notify them.").await;
    let order: Vec<String> = room
        .timeline()
        .await
        .into_iter()
        .filter(|event| event.index > 2)
        .map(|event| {
            format!(
                "{:?} {}",
                event.event_type,
                roomkit::text_of(&event.content)
            )
        })
        .collect();
    assert_eq!(
        order,
        [
            "Message Notify them.",
            "Message Notifying.",
            "ToolCallStart [Tool call: notify]",
            "Message Noted.",
            "ToolCallEnd [Tool call: notify]",
            "Message Done."
        ]
    );
    room.kit.shutdown().await;
}

/// The streaming path keeps milestone 5's rule (F2): a streaming channel
/// that takes no tool-call row is handed none in the stream, nor delivered
/// one; the rows are stored all the same.
#[tokio::test]
async fn a_streaming_target_that_takes_no_tool_row_is_handed_none() {
    let rounds = vec![
        ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", "{}")]),
        ScriptedRound::answering("Shipped."),
    ];
    let runs = Arc::new(AtomicUsize::new(0));
    let ws = Streaming::new(Stop::Never).taking_no_tool_row();
    let (room, _model) = Room::new(rounds, lookup(runs, None), ws).await;
    room.says("Where is my order?").await;
    assert_eq!(
        room.ws.read(),
        [
            Read::Text("Let me check.".to_owned()),
            Read::Row(EventType::Message, "Let me check.".to_owned(), 4),
            Read::Text("Shipped.".to_owned()),
            Read::Row(EventType::Message, "Shipped.".to_owned(), 7),
        ]
    );
    assert_eq!(room.ws.delivered(), [3]);
    assert_eq!(room.rows().await.len(), 4);
    room.kit.shutdown().await;
}

/// A provider whose one generation hands the events a test sends it, as it
/// sends them.
#[derive(Clone, Default)]
struct Controlled {
    events: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<ProviderEvent>>>>,
    dropped: Arc<AtomicUsize>,
}

/// The events a test hands a [`Controlled`] generation.
struct Fed {
    events: tokio::sync::mpsc::UnboundedReceiver<ProviderEvent>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for Fed {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Stream for Fed {
    type Item = Result<ProviderEvent, roomkit_ai::ProviderError>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut()
            .events
            .poll_recv(context)
            .map(|event| event.map(Ok))
    }
}

impl AiProvider for Controlled {
    fn generate<'a>(&'a self, _request: &'a AiRequest) -> ProviderStream<'a> {
        let events = self.events.lock().unwrap().take();
        let (_spare, empty) = tokio::sync::mpsc::unbounded_channel();
        Box::pin(Fed {
            events: events.unwrap_or(empty),
            dropped: Arc::clone(&self.dropped),
        })
    }
}

/// RFC §10.1 "Explicit delivery cancellation", §12.2 step 13s: a caller
/// that stops waiting once its message is decided cuts its turns; the
/// stream being read is stopped as a reader that stopped, the text its
/// reader was handed stored marked cancelled, the turn recording
/// `cancelled`, and the model's stream is dropped. A server whose client
/// leaves mid-answer drops its call so.
#[tokio::test]
async fn a_caller_that_leaves_mid_answer_leaves_the_text_read_stored_cancelled() {
    let (feed, events) = tokio::sync::mpsc::unbounded_channel();
    let model = Controlled::default();
    *model.events.lock().unwrap() = Some(events);
    let dropped = Arc::clone(&model.dropped);
    let agent = AiAgent::new(channel("agent"), model);
    let kit = RoomKit::new(RoomKitOptions::default());
    let ws = Streaming::new(Stop::Never);
    kit.register_channel(RecordingTransport::new(
        channel("web"),
        ChannelType::WEBHOOK,
    ))
    .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["web", "ws", "agent"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("web"), "alice", EventContent::text("Hi"));
    let calling = kit.clone();
    let caller = tokio::spawn(async move {
        calling
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
    });
    for piece in ["The ", "answer "] {
        feed.send(ProviderEvent::Text(piece.to_owned())).unwrap();
    }
    let read_both = async {
        while ws.read().len() < 2 {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), read_both)
        .await
        .unwrap();
    // The caller leaves: its call's turns are cut.
    caller.abort();
    let _gone = caller.await;
    tokio::time::timeout(Duration::from_secs(5), kit.idle())
        .await
        .unwrap();
    let answer = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.source.channel_id == channel("agent"))
        .unwrap();
    assert_eq!(
        (
            roomkit::text_of(&answer.content),
            answer.metadata.get("cancelled").cloned(),
            answer.metadata.get("loop_end_reason").cloned(),
            dropped.load(Ordering::SeqCst),
        ),
        (
            "The answer ".to_owned(),
            Some(json!(true)),
            Some(json!("cancelled")),
            1
        )
    );
    drop(feed);
    kit.shutdown().await;
}

/// A channel that says it streams and keeps the default `deliver_stream`
/// is delivered each row once the stream is over, through `deliver()`
/// (RFC §6.1, step 4 decision 9).
#[tokio::test]
async fn the_default_deliver_stream_delivers_each_row_once_the_stream_is_over() {
    let model = ScriptedProvider::new([ScriptedRound::writing(["Hello ", "there."])]);
    let agent = AiAgent::new(channel("agent"), model);
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    let buffered = Buffered {
        inner: RecordingTransport::new(channel("ws"), ChannelType::WEBSOCKET),
    };
    kit.register_channel(sms).unwrap();
    kit.register_channel(buffered.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "agent"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text("Hi"));
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    kit.idle().await;
    let delivered: Vec<String> = buffered
        .inner
        .deliveries()
        .into_iter()
        .map(|delivery| roomkit::text_of(&delivery.event.content))
        .collect();
    assert_eq!(delivered, ["Hi", "Hello there."]);
    kit.shutdown().await;
}

/// A recording WebSocket that says it streams, with the default
/// `deliver_stream`.
#[derive(Clone)]
struct Buffered {
    inner: RecordingTransport,
}

impl Channel for Buffered {
    fn declaration(&self) -> ChannelDeclaration {
        let mut declaration = self.inner.declaration();
        declaration.supports_streaming_delivery = true;
        declaration
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.inner.deliver(event, binding, context).await
    }
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
