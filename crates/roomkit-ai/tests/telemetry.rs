//! A turn's spans and metrics, read from a `MockTelemetryProvider`: Python's
//! `tests/test_telemetry.py::TestLLMTelemetry`, and the span tests of
//! `tests/test_ai_streaming_tool_loop.py`, `test_unified_tool_call.py`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, InboundMessage, Metadata, RoomId, TextContent};
use roomkit::telemetry::{Attr, MockTelemetryProvider, Span, SpanKind};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, AiToolCall,
    ProviderError, ProviderErrorOptions, ProviderStream, StreamDone, StreamEvent, StreamTextDelta,
    ToolError, ToolResult, Tools, tool_arguments,
};
use serde_json::{Value, json};

/// What a scripted generation does: answers `.0`, calling `.1`, or fails.
enum Round {
    Says(&'static str, Option<&'static str>),
    Fails(&'static str),
}

/// A model reading its rounds from a script, each using 10 tokens in and 5
/// out.
struct Scripted(Mutex<Vec<Round>>);

impl Scripted {
    fn new(rounds: Vec<Round>) -> Self {
        Self(Mutex::new(rounds))
    }

    fn next(&self) -> Round {
        let mut rounds = self.0.lock().unwrap();
        assert!(!rounds.is_empty(), "a round past the script");
        rounds.remove(0)
    }
}

struct Pieces(std::vec::IntoIter<Result<StreamEvent, ProviderError>>);

impl Stream for Pieces {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.next())
    }
}

impl AiProvider for Scripted {
    fn model_name(&self) -> &'static str {
        "scripted"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        ready(Ok(AiResponse::new("unused")))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let events = match self.next() {
            Round::Fails(message) => {
                let failure = ProviderError::new(message, ProviderErrorOptions::default());
                vec![Err(failure)]
            }
            Round::Says(text, call) => {
                let text = text.to_owned();
                let mut events = vec![Ok(StreamEvent::TextDelta(StreamTextDelta { text }))];
                if let Some(name) = call {
                    let mut call = AiToolCall::new("call-1", name);
                    call.arguments = tool_arguments("{}");
                    events.push(Ok(StreamEvent::ToolCall(call.into())));
                }
                let mut usage = Metadata::new();
                usage.insert("input_tokens".to_owned(), json!(10));
                usage.insert("output_tokens".to_owned(), json!(5));
                events.push(Ok(StreamEvent::Done(StreamDone {
                    usage,
                    ..StreamDone::default()
                })));
                events
            }
        };
        Box::pin(Pieces(events.into_iter()))
    }
}

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A room of a transport and a scripted agent, with a tool `lookup` and one
/// `broken` that fails, reporting to a mock; the first message sent.
async fn turn_of(rounds: Vec<Round>) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let mock = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::clone(&mock).into()),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let schema = json!({ "type": "object", "properties": {} });
    let lookup = AiTool {
        parameters: schema.clone(),
        ..AiTool::new("lookup", "Look it up.").unwrap()
    };
    let broken = AiTool {
        parameters: schema,
        ..AiTool::new("broken", "Fails.").unwrap()
    };
    let tools = Tools::new()
        .tool(lookup, |_call, _context| async {
            Ok(ToolResult::Text("42".to_owned()))
        })
        .tool(broken, |_call, _context| async {
            Err(ToolError::Failed("the database is down".to_owned()))
        });
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(id("ai"), Scripted::new(rounds), options).unwrap();
    kit.register_channel(agent).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["sms", "ai"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(id("sms"), "+15550000001", TextContent::new("Hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    (kit, mock)
}

fn one(spans: Vec<Span>) -> Span {
    assert_eq!(spans.len(), 1, "{spans:?}");
    spans.into_iter().next().unwrap()
}

// Python: tests/test_telemetry.py::TestLLMTelemetry::test_ai_channel_creates_llm_span,
// TestTelemetryPhase4::test_llm_span_parents_to_broadcast
#[tokio::test]
async fn a_turn_opens_a_generation_span_under_its_broadcast() {
    let (kit, mock) = turn_of(vec![Round::Says("Hi.", None)]).await;
    let span = one(mock.get_spans(SpanKind::LlmGenerate));
    assert_eq!(span.name, "llm.generate");
    assert_eq!(span.status, "ok");
    assert_eq!(span.room_id.as_deref(), Some("r1"));
    assert_eq!(span.channel_id.as_deref(), Some("ai"));
    assert_eq!(span.attributes[Attr::PROVIDER], json!("Scripted"));
    assert_eq!(span.attributes[Attr::LLM_STREAMING], json!(true));
    assert_eq!(span.attributes[Attr::LLM_INPUT_TOKENS], json!(10));
    assert_eq!(span.attributes[Attr::LLM_OUTPUT_TOKENS], json!(5));
    assert_eq!(span.attributes[Attr::LLM_TOOL_COUNT], json!(0));
    let broadcasts = mock.get_spans(SpanKind::Broadcast);
    assert!(
        broadcasts
            .iter()
            .any(|broadcast| Some(&broadcast.id) == span.parent_id.as_ref()),
        "{span:?} under {broadcasts:?}"
    );
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestLLMTelemetry::test_ai_channel_tool_call_span,
// tests/test_ai_streaming_tool_loop.py::test_span_accumulates_tokens_across_rounds
#[tokio::test]
async fn a_tool_call_opens_a_span_and_a_turn_adds_up_its_rounds() {
    let script = vec![
        Round::Says("Checking.", Some("lookup")),
        Round::Says("42.", None),
    ];
    let (kit, mock) = turn_of(script).await;
    let turn = one(mock.get_spans(SpanKind::LlmGenerate));
    assert_eq!(turn.attributes[Attr::LLM_INPUT_TOKENS], json!(20));
    assert_eq!(turn.attributes[Attr::LLM_OUTPUT_TOKENS], json!(10));
    assert_eq!(turn.attributes[Attr::LLM_TOOL_COUNT], json!(1));
    let call = one(mock.get_spans(SpanKind::LlmToolCall));
    assert_eq!(call.name, "tool.lookup");
    assert_eq!(call.status, "ok");
    assert_eq!(call.parent_id.as_ref(), Some(&turn.id));
    assert_eq!(call.attributes["tool.name"], json!("lookup"));
    assert_eq!(call.attributes["tool.id"], json!("call-1"));
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestLLMTelemetry (a raising tool ends its span in error)
#[tokio::test]
async fn a_tool_that_fails_ends_its_span_in_error() {
    let script = vec![
        Round::Says("Checking.", Some("broken")),
        Round::Says("Sorry.", None),
    ];
    let (kit, mock) = turn_of(script).await;
    let call = one(mock.get_spans(SpanKind::LlmToolCall));
    assert_eq!(call.status, "error");
    assert_eq!(call.error_message.as_deref(), Some("the database is down"));
    kit.close().await.unwrap();
}

// Python: tests/test_unified_tool_call.py::test_a_refused_call_does_not_trace_as_a_served_one
#[tokio::test]
async fn a_refused_call_opens_no_call_span() {
    let script = vec![
        Round::Says("Checking.", Some("unknown")),
        Round::Says("Sorry.", None),
    ];
    let (kit, mock) = turn_of(script).await;
    assert_eq!(mock.get_spans(SpanKind::LlmToolCall), vec![]);
    kit.close().await.unwrap();
}

// Python: tests/test_turn_that_did_not_complete.py (a provider's failure ends the turn's span)
#[tokio::test]
async fn a_provider_s_failure_ends_the_turn_s_span_in_error() {
    let (kit, mock) = turn_of(vec![Round::Fails("the model is down")]).await;
    let span = one(mock.get_spans(SpanKind::LlmGenerate));
    assert_eq!(span.status, "error");
    assert!(
        span.error_message
            .as_deref()
            .unwrap()
            .contains("the model is down")
    );
    kit.close().await.unwrap();
}

// Python: tests/test_ai_streaming_tool_loop.py (each generation records its tokens)
#[tokio::test]
async fn each_generation_records_its_tokens() {
    let script = vec![
        Round::Says("Checking.", Some("lookup")),
        Round::Says("42.", None),
    ];
    let (kit, mock) = turn_of(script).await;
    let tokens: Vec<(String, f64, String, Option<Value>)> = mock
        .metrics()
        .into_iter()
        .filter(|(name, ..)| name.starts_with("roomkit.llm."))
        .map(|(name, value, unit, attributes)| {
            (name, value, unit, attributes.get(Attr::CHANNEL_ID).cloned())
        })
        .collect();
    let of = |name: &str, value: f64| {
        (
            name.to_owned(),
            value,
            "tokens".to_owned(),
            Some(json!("ai")),
        )
    };
    assert_eq!(
        tokens,
        vec![
            of("roomkit.llm.input_tokens", 10.0),
            of("roomkit.llm.output_tokens", 5.0),
            of("roomkit.llm.input_tokens", 10.0),
            of("roomkit.llm.output_tokens", 5.0),
        ]
    );
    kit.close().await.unwrap();
}
