//! The one log line a failed turn gets (RFC §15.2, §15.3), as the Python
//! implementation's `log_failure` writes it: a provider's error at the
//! level its status calls for, ERROR for a missing model (404) or a server
//! fault (5xx), WARNING otherwise, with the provider and the status in the
//! line; DEBUG when the caller receives the error itself. Run in a kit on a
//! model that streams and on one that answers whole, as
//! `tests/test_inbound_stream_error_propagation.py` and
//! `tests/test_inbound_error_surfacing.py` run it.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::cell::RefCell;
use std::fmt::{Debug, Write as _};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once, PoisonError};
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, DelegateOptions,
    DeliveryStream, HookExecution, HookOptions, HookTrigger, InboundResult, ProcessInboundOptions,
    ResponseItem, RoomKit, RoomKitOptions, SendEventOptions, current_turn_host,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, ProviderError,
    ToolResult, Tools,
};
use roomkit_core::ops::ChannelConfig;
use serde_json::json;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata as Callsite, Subscriber};

/// The person who writes by SMS.
const SENDER: &str = "+15550001";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

// ── The lines `tracing` emits ─────────────────────────────────────────────

thread_local! {
    /// Where the lines of this thread go while it captures.
    static CAPTURING: RefCell<Option<Lines>> = const { RefCell::new(None) };
}

/// The lines `tracing` emits on this thread while the guard lives, DEBUG
/// and above, each as its level, its target, then its message and fields:
/// `roomkit`'s `tests/support/warnings.rs`, down to DEBUG, the binary's
/// global subscriber.
#[derive(Clone, Default)]
struct Lines(Arc<Mutex<Vec<(Level, String)>>>);

/// Captures this thread's lines while it lives.
struct Capturing(());

impl Lines {
    fn capture() -> (Self, Capturing) {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| tracing::subscriber::set_global_default(Sink).unwrap());
        let lines = Self::default();
        CAPTURING.with(|capturing| *capturing.borrow_mut() = Some(lines.clone()));
        (lines, Capturing(()))
    }

    /// The lines of a `roomkit` target at `level` or above, Python's
    /// `_roomkit_records(caplog, level)`.
    fn roomkit(&self, level: Level) -> Vec<(Level, String)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(at, line)| *at <= level && line.starts_with("roomkit"))
            .cloned()
            .collect()
    }

    /// The levels of the `roomkit` lines at WARN or above.
    fn warnings(&self) -> Vec<Level> {
        self.roomkit(Level::WARN)
            .into_iter()
            .map(|(level, _)| level)
            .collect()
    }

    /// The lines that contain `text`, at any level.
    fn containing(&self, text: &str) -> Vec<(Level, String)> {
        self.roomkit(Level::DEBUG)
            .into_iter()
            .filter(|(_, line)| line.contains(text))
            .collect()
    }
}

impl Drop for Capturing {
    fn drop(&mut self) {
        CAPTURING.with(|capturing| capturing.borrow_mut().take());
    }
}

struct Line<'line>(&'line mut String);

impl Visit for Line<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        write!(self.0, " {}={value:?}", field.name()).unwrap();
    }
}

/// The binary's subscriber: every line at DEBUG or above, kept by the
/// thread that emits it when it captures.
struct Sink;

impl Subscriber for Sink {
    fn register_callsite(&self, callsite: &'static Callsite<'static>) -> Interest {
        if *callsite.level() <= Level::DEBUG {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::DEBUG)
    }

    fn enabled(&self, callsite: &Callsite<'_>) -> bool {
        *callsite.level() <= Level::DEBUG
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        CAPTURING.with(|capturing| {
            let Some(lines) = &*capturing.borrow() else {
                return;
            };
            let mut line = event.metadata().target().to_owned();
            event.record(&mut Line(&mut line));
            lines
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((*event.metadata().level(), line));
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

// ── The models ────────────────────────────────────────────────────────────

/// The provider's error `round` ends on: a refusal with the HTTP `status`
/// by the provider `mock`, Python's `ProviderError(message, provider="mock",
/// status_code=status)`; without a status, a connection that broke, which
/// names no provider.
fn refusal(round: ScriptedRound, status: Option<u16>, message: &str) -> ScriptedRound {
    match status {
        Some(status) => round.refusing(status, message).refused_by("mock"),
        None => round.failing(message),
    }
}

/// A provider that does not stream: it answers each generation whole, the
/// scripted model's rounds read by `generate()`, as Python's
/// `MockAIProvider(streaming=False)` with its `generate` raising.
#[derive(Clone)]
struct Whole(ScriptedProvider);

impl AiProvider for Whole {
    fn model_name(&self) -> &'static str {
        "mock"
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        self.0.generate(context)
    }
}

/// The agent `ai1` over `model`, with the tool `look` when `tools`.
fn agent<P: AiProvider>(model: P, tools: bool) -> AiChannel<P> {
    let tools = if tools { look() } else { Tools::new() };
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    AiChannel::new(channel("ai1"), model, options).unwrap()
}

/// The tool `look`, answering `seen`.
fn look() -> Tools {
    let definition = AiTool {
        parameters: json!({ "type": "object" }),
        ..AiTool::new("look", "Look.").unwrap()
    };
    Tools::new().tool(definition, |_call, _context| async {
        Ok(ToolResult::Text("seen".to_owned()))
    })
}

/// A model that calls `look`, then meets `failure` on the next round:
/// Python's `_FailsAfterARound`.
fn fails_after_a_round(failure: ScriptedRound) -> Vec<ScriptedRound> {
    vec![ScriptedRound::calling("", [("c1", "look", "{}")]), failure]
}

/// An agent whose response fails before any text with an error of its own,
/// no provider's: what Python's AI channel over a provider raising
/// `RuntimeError("bug")` in its stream answers with.
#[derive(Clone)]
struct Buggy;

/// [`Buggy`]'s response: its failure, then nothing.
struct Raising(Option<ChannelError>);

impl Stream for Raising {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.take().map(Err))
    }
}

impl Channel for Buggy {
    fn channel_id(&self) -> ChannelId {
        channel("ai1")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let output = if event.source.channel_id == self.channel_id() {
            ChannelOutput::empty()
        } else {
            let response = Raising(Some("bug".into()));
            current_turn_host()
                .unwrap()
                .respond_with_stream(Box::pin(response))
        };
        std::future::ready(Ok(output))
    }
}

// ── The rooms ─────────────────────────────────────────────────────────────

/// A transport that takes the live text of a streamed answer and reads it
/// to its end, raising what the stream raises (Python's
/// `_StreamingTransport`), or an error of its own when it `breaks` (Python's
/// `_BreakingTransport`).
#[derive(Clone)]
struct Streaming {
    declaration: ChannelConfig,
    breaks: bool,
}

impl Streaming {
    fn new(id: &str, breaks: bool) -> Self {
        let mut declaration = ChannelConfig::new(
            channel(id),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        Self {
            declaration,
            breaks,
        }
    }
}

impl Channel for Streaming {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn supports_streaming_delivery(&self) -> bool {
        true
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::default()))
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        while let Some(item) = stream.next().await {
            if item.is_err() && self.breaks {
                return Err("the socket broke".into());
            }
            item?;
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
}

/// A kit whose room `r1` binds the SMS transport `sms1`, the streaming
/// transport `ws1` when `streaming`, and the agent `ai1` over `rounds`, on
/// a model that streams them when `streams`, else answers each whole, with
/// the tool `look` when `tools`; `ON_ERROR` counted.
async fn kit(
    (streams, rounds, tools): (bool, Vec<ScriptedRound>, bool),
    streaming: bool,
) -> (RoomKit, Arc<AtomicUsize>) {
    let model = ScriptedProvider::new(rounds);
    if streams {
        kit_of(agent(model, tools), streaming.then_some(false)).await
    } else {
        kit_of(agent(Whole(model), tools), streaming.then_some(false)).await
    }
}

/// A kit whose room `r1` binds the SMS transport `sms1`, the streaming
/// transport `ws1` when `streaming` says whether it breaks, and `agent`;
/// `ON_ERROR` counted.
async fn kit_of(agent: impl Channel, streaming: Option<bool>) -> (RoomKit, Arc<AtomicUsize>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    if let Some(breaks) = streaming {
        kit.register_channel(Streaming::new("ws1", breaks)).unwrap();
    }
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    let ids: &[&str] = if streaming.is_some() {
        &["sms1", "ws1", "ai1"]
    } else {
        &["sms1", "ai1"]
    };
    for id in ids {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let errors = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&errors);
    kit.hook(
        HookTrigger::OnError,
        move |_event: RoomEvent, _context| {
            counting.fetch_add(1, Ordering::SeqCst);
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "test_capture_error".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    (kit, errors)
}

/// One turn of the agent `kit` describes on a message by SMS, in a room
/// whose only transport does not stream when not `streaming` (Python's
/// `_run_headless_turn`), else beside a transport that takes the live
/// stream (`_run_targeted_turn`): the call's result and how many times
/// `ON_ERROR` fired.
async fn turn(agent: (bool, Vec<ScriptedRound>, bool), streaming: bool) -> (InboundResult, usize) {
    let (kit, errors) = kit(agent, streaming).await;
    let message = InboundMessage::new(channel("sms1"), SENDER, TextContent::new("go"));
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
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    (result, errors.load(Ordering::SeqCst))
}

/// The error the call carries: its type and its message.
fn error_of(result: &InboundResult) -> (String, String) {
    let error = result.error.as_ref().unwrap();
    (error.error_type.clone(), error.message.clone())
}

/// Whether the call carries the provider's error saying `text`.
fn carries(result: &InboundResult, text: &str) -> bool {
    let (error_type, message) = error_of(result);
    error_type == "ProviderError" && message.contains(text)
}

// ── The tests ─────────────────────────────────────────────────────────────

// Python: tests/test_inbound_stream_error_propagation.py::test_headless_provider_error_logged_at_debug_no_traceback
#[tokio::test]
async fn a_headless_provider_error_is_logged_at_debug() {
    let (lines, _capturing) = Lines::capture();
    let rounds = vec![refusal(
        ScriptedRound::answering(""),
        None,
        "connection error",
    )];
    turn((true, rounds, false), false).await;
    let logged = lines.containing("stream consumption (no targets)");
    assert_eq!(
        logged.iter().map(|(level, _)| *level).collect::<Vec<_>>(),
        [Level::DEBUG],
        "{logged:?}"
    );
}

// Python: tests/test_inbound_stream_error_propagation.py::test_unexpected_error_keeps_traceback
// Its traceback is Python's: the line keeps the error's type and text.
#[tokio::test]
async fn an_unexpected_error_is_logged_at_error() {
    let (kit, _errors) = kit_of(Buggy, None).await;
    let (lines, _capturing) = Lines::capture();
    let message = InboundMessage::new(channel("sms1"), SENDER, TextContent::new("go"));
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
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    assert_eq!(
        result.error.as_ref().map(|error| error.message.as_str()),
        Some("bug")
    );
    let logged = lines.containing("stream consumption (no targets)");
    assert_eq!(
        logged.iter().map(|(level, _)| *level).collect::<Vec<_>>(),
        [Level::ERROR],
        "{logged:?}"
    );
    assert!(logged[0].1.contains(r#"error="bug""#), "{logged:?}");
}

// Python: tests/test_inbound_stream_error_propagation.py::test_non_streaming_provider_error_propagates_with_cause
// Python: tests/test_inbound_error_surfacing.py::test_non_streaming_provider_error_fires_on_error
#[tokio::test]
async fn a_provider_error_answering_whole_reaches_the_caller_and_on_error() {
    let rounds = vec![refusal(
        ScriptedRound::answering(""),
        None,
        "provider request failed",
    )];
    let (result, errors) = turn((false, rounds, false), false).await;
    assert_eq!(
        (error_of(&result), errors),
        (
            (
                "ProviderError".to_owned(),
                "provider request failed".to_owned()
            ),
            1
        )
    );
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_turn_failing_before_any_round_is_one_warning_for_a_streaming_target
#[tokio::test]
async fn a_turn_failing_before_any_round_is_one_warning_for_a_streaming_target() {
    for streams in [true, false] {
        let (lines, _capturing) = Lines::capture();
        let rounds = vec![refusal(
            ScriptedRound::answering(""),
            None,
            "connection refused",
        )];
        let (result, _) = turn((streams, rounds, false), true).await;
        assert!(
            carries(&result, "connection refused"),
            "streams={streams}: {result:?}"
        );
        let warnings = lines.roomkit(Level::WARN);
        assert_eq!(
            lines.warnings(),
            [Level::WARN],
            "streams={streams}: {:?}",
            lines.roomkit(Level::WARN)
        );
        assert!(
            warnings[0].1.contains("connection refused"),
            "streams={streams}: {warnings:?}"
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_turn_failing_before_any_round_is_no_roomkit_warning_when_headless
#[tokio::test]
async fn a_turn_failing_before_any_round_is_no_roomkit_warning_when_headless() {
    for streams in [true, false] {
        let (lines, _capturing) = Lines::capture();
        let rounds = vec![refusal(
            ScriptedRound::answering(""),
            None,
            "connection refused",
        )];
        let (result, _) = turn((streams, rounds, false), false).await;
        assert!(
            carries(&result, "connection refused"),
            "streams={streams}: {result:?}"
        );
        assert_eq!(
            lines.warnings(),
            Vec::<Level>::new(),
            "streams={streams}: {:?}",
            lines.roomkit(Level::WARN)
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_missing_model_or_a_server_fault_is_an_error_for_a_streaming_target
#[tokio::test]
async fn a_missing_model_or_a_server_fault_is_an_error_for_a_streaming_target() {
    for (status, streams) in [(404, true), (404, false), (503, true), (503, false)] {
        let (lines, _capturing) = Lines::capture();
        let rounds = vec![refusal(
            ScriptedRound::answering(""),
            Some(status),
            "model gone",
        )];
        turn((streams, rounds, false), true).await;
        let records = lines.roomkit(Level::WARN);
        assert_eq!(
            lines.warnings(),
            [Level::ERROR],
            "{status} {streams}: {records:?}"
        );
        assert!(records[0].1.contains("provider=mock"), "{records:?}");
        assert!(
            records[0].1.contains(&format!("status={status}")),
            "{records:?}"
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_send_event_caller_that_never_gets_the_failure_sees_it_logged
#[tokio::test]
async fn a_send_event_caller_that_never_gets_the_failure_sees_it_logged() {
    for streams in [true, false] {
        let rounds = vec![refusal(
            ScriptedRound::answering(""),
            Some(404),
            "model gone",
        )];
        let (kit, _errors) = kit((streams, rounds, false), false).await;
        let (lines, _capturing) = Lines::capture();
        kit.send_event(
            &room_id(),
            &channel("sms1"),
            TextContent::new("go"),
            SendEventOptions::default(),
        )
        .await
        .unwrap();
        roomkit::testing::idle(&kit).await;
        kit.close().await.unwrap();
        assert_eq!(
            lines.warnings(),
            [Level::ERROR],
            "streams={streams}: {:?}",
            lines.roomkit(Level::WARN)
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_deferred_call_sees_the_failure_logged_at_its_level
#[tokio::test]
async fn a_deferred_call_sees_the_failure_logged_at_its_level() {
    for streams in [true, false] {
        let rounds = vec![refusal(
            ScriptedRound::answering(""),
            Some(404),
            "model gone",
        )];
        let (kit, _errors) = kit((streams, rounds, false), false).await;
        let (lines, _capturing) = Lines::capture();
        let message = InboundMessage::new(channel("sms1"), SENDER, TextContent::new("go"));
        let result = kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    defer_delivery: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        result.delivery.unwrap().wait().await.unwrap();
        roomkit::testing::idle(&kit).await;
        kit.close().await.unwrap();
        let records = lines.roomkit(Level::WARN);
        assert_eq!(
            lines.warnings(),
            [Level::ERROR],
            "streams={streams}: {records:?}"
        );
        assert!(
            records[0].1.contains("stream consumption (no targets)"),
            "{records:?}"
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_streaming_channel_raising_its_own_error_is_one_line_more
#[tokio::test]
async fn a_streaming_channel_raising_its_own_error_is_one_line_more() {
    let rounds = vec![refusal(
        ScriptedRound::answering(""),
        None,
        "connection refused",
    )];
    let model = ScriptedProvider::new(rounds);
    let (kit, _errors) = kit_of(agent(model, false), Some(true)).await;
    let (lines, _capturing) = Lines::capture();
    let message = InboundMessage::new(channel("sms1"), SENDER, TextContent::new("go"));
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
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    assert!(carries(&result, "connection refused"), "{result:?}");
    let records = lines.roomkit(Level::WARN);
    assert_eq!(lines.warnings(), [Level::ERROR, Level::WARN], "{records:?}");
    let handling = "streaming channel ws1 handling the failed response of ai1 in room r1 failed";
    assert!(records[0].1.contains(handling), "{records:?}");
    let delivery = "streaming delivery of ai1 to ws1 for room r1 failed (provider=";
    assert!(records[1].1.contains(delivery), "{records:?}");
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_turn_failing_after_a_round_is_one_line_too
#[tokio::test]
async fn a_turn_failing_after_a_round_is_one_line_too() {
    for streams in [true, false] {
        let (lines, _capturing) = Lines::capture();
        let failure = refusal(ScriptedRound::answering(""), None, "connection reset");
        let rounds = fails_after_a_round(failure);
        let (result, _) = turn((streams, rounds, true), true).await;
        assert!(
            carries(&result, "connection reset"),
            "streams={streams}: {result:?}"
        );
        assert_eq!(
            lines.warnings(),
            [Level::WARN],
            "streams={streams}: {:?}",
            lines.roomkit(Level::WARN)
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_headless_turn_failing_after_a_round_is_no_roomkit_warning
#[tokio::test]
async fn a_headless_turn_failing_after_a_round_is_no_roomkit_warning() {
    for streams in [true, false] {
        let (lines, _capturing) = Lines::capture();
        let failure = refusal(ScriptedRound::answering(""), None, "connection reset");
        let rounds = fails_after_a_round(failure);
        let (result, _) = turn((streams, rounds, true), false).await;
        assert!(
            carries(&result, "connection reset"),
            "streams={streams}: {result:?}"
        );
        assert_eq!(
            lines.warnings(),
            Vec::<Level>::new(),
            "streams={streams}: {:?}",
            lines.roomkit(Level::WARN)
        );
    }
}

// Python: tests/test_inbound_stream_error_propagation.py::test_a_delegated_turn_failure_is_one_line_at_its_level
#[tokio::test]
async fn a_delegated_turn_failure_is_one_line_at_its_level() {
    let rounds = vec![refusal(
        ScriptedRound::answering(""),
        Some(503),
        "upstream down",
    )];
    let worker = AiChannel::new(
        channel("w1"),
        Whole(ScriptedProvider::new(rounds)),
        AiChannelOptions::default(),
    )
    .unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(worker).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (lines, _capturing) = Lines::capture();
    kit.delegate(
        &room_id(),
        &channel("w1"),
        "go",
        DelegateOptions {
            wait: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    let records = lines.roomkit(Level::WARN);
    assert_eq!(lines.warnings(), [Level::ERROR], "{records:?}");
    assert!(records[0].1.contains("status=503"), "{records:?}");
}
