//! A turn's spans and metrics, read from a `MockTelemetryProvider`: Python's
//! `tests/test_telemetry.py::TestLLMTelemetry`, and the span tests of
//! `tests/test_ai_streaming_tool_loop.py`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::HookResult;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelId, ChannelType, InboundMessage, Metadata, RoomId, TextContent};
use roomkit::telemetry::{
    Attr, MockTelemetryProvider, Span, SpanKind, TelemetryConfig, TelemetryProvider,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions, TurnTap};
use roomkit::{
    AttachChannelOptions, CancelOptions, CreateRoomOptions, DelegateOptions, HookExecution,
    HookOptions, HookTrigger, MuteOptions, ProcessInboundOptions, RoomKit, RoomKitOptions, SendEventOptions,
    ToolCallEvent, current_turn_host,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiResponseEvent, AiTool,
    AiToolCall, ProviderError, ProviderErrorOptions, ProviderStream, StreamDone, StreamEvent,
    StreamTextDelta, ToolError, ToolResult, Tools, tool_arguments,
};
use serde_json::{Value, json};
use tokio::sync::Notify;

/// What a scripted generation does: answers `.0`, calling `.1`, or fails.
enum Round {
    Says(&'static str, Option<&'static str>),
    Fails(&'static str),
    FailsPastItsEnd(&'static str),
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
            Round::FailsPastItsEnd(message) => {
                let failure = ProviderError::new(message, ProviderErrorOptions::default());
                let mut usage = Metadata::new();
                usage.insert("input_tokens".to_owned(), json!(10));
                usage.insert("output_tokens".to_owned(), json!(5));
                vec![
                    Ok(StreamEvent::Done(StreamDone {
                        usage,
                        ..StreamDone::default()
                    })),
                    Err(failure),
                ]
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
    turn_recording(false, rounds).await
}

/// As [`turn_of`], the kit recording what can hold personal data when
/// `sensitive`.
async fn turn_recording(
    sensitive: bool,
    rounds: Vec<Round>,
) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let (kit, mock) = room_of(sensitive, rounds).await;
    says_hello(&kit).await;
    (kit, mock)
}

/// The room of [`turn_recording`], no message sent yet.
async fn room_of(sensitive: bool, rounds: Vec<Round>) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let mock = Arc::new(MockTelemetryProvider::default());
    let provider: Arc<dyn TelemetryProvider> = Arc::<MockTelemetryProvider>::clone(&mock);
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig {
            provider: Some(provider),
            record_sensitive_data: sensitive,
            ..TelemetryConfig::default()
        }),
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
    (kit, mock)
}

/// The first message of the room, answered before it returns.
async fn says_hello(kit: &RoomKit) {
    let message = InboundMessage::new(id("sms"), "+15550000001", TextContent::new("Hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
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

// Python: tests/test_telemetry.py::TestLLMTelemetry::test_a_stream_closed_unread_opens_no_llm_span
#[tokio::test]
async fn a_muted_agent_s_stream_opens_no_generation_span() {
    let (kit, mock) = room_of(false, vec![Round::Says("Never.", None)]).await;
    kit.mute(&room(), &id("ai"), MuteOptions::default())
        .await
        .unwrap();
    says_hello(&kit).await;
    kit.close().await.unwrap();
    let opened = mock.get_spans(SpanKind::LlmGenerate);
    assert_eq!(opened, Vec::new());
}

// Python: tests/test_telemetry.py::TestLLMTelemetry::test_a_stream_a_cut_left_unread_opens_no_llm_span:
// the first agent's stream is read while its tool holds, the delivery is
// cancelled there, and the second agent's is closed unread.
#[tokio::test]
async fn a_stream_a_cut_left_unread_opens_no_generation_span() {
    let (held, freed) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (kit, mock) = room_of(false, Vec::new()).await;
    let schema = json!({ "type": "object", "properties": {} });
    let tool = AiTool {
        parameters: schema,
        ..AiTool::new("held", "Holds.").unwrap()
    };
    let (holding, release) = (Arc::clone(&held), Arc::clone(&freed));
    let tools = Tools::new().tool(tool, move |_call, _context| {
        let (holding, release) = (Arc::clone(&holding), Arc::clone(&release));
        async move {
            holding.notify_one();
            release.notified().await;
            Ok(ToolResult::Text("42".to_owned()))
        }
    });
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let script = vec![
        Round::Says("Checking.", Some("held")),
        Round::Says("Done.", None),
    ];
    let first = AiChannel::new(id("first"), Scripted::new(script), options).unwrap();
    kit.register_channel(first).unwrap();
    let script = Scripted::new(vec![Round::Says("Never.", None)]);
    let second = AiChannel::new(id("second"), script, AiChannelOptions::default()).unwrap();
    kit.register_channel(second).unwrap();
    for channel in ["first", "second"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit.mute(&room(), &id("ai"), MuteOptions::default())
        .await
        .unwrap();
    let message = InboundMessage::new(id("sms"), "+15550000001", TextContent::new("Hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();
    let delivery = result.delivery.unwrap();
    held.notified().await;
    delivery.cancel(CancelOptions::default()).await.unwrap();
    freed.notify_one();
    delivery.wait().await.unwrap();
    kit.close().await.unwrap();
    let channels: Vec<_> = mock
        .get_spans(SpanKind::LlmGenerate)
        .into_iter()
        .filter_map(|span| span.channel_id)
        .collect();
    assert_eq!(channels, ["first"]);
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
    let script = || {
        vec![
            Round::Says("Checking.", Some("broken")),
            Round::Says("Sorry.", None),
        ]
    };
    let (kit, mock) = turn_of(script()).await;
    let call = one(mock.get_spans(SpanKind::LlmToolCall));
    assert_eq!(call.status, "error");
    assert_eq!(call.error_message.as_deref(), Some("ToolError"));
    kit.close().await.unwrap();
    let (kit, mock) = turn_recording(true, script()).await;
    let call = one(mock.get_spans(SpanKind::LlmToolCall));
    assert_eq!(call.error_message.as_deref(), Some("the database is down"));
    kit.close().await.unwrap();
}

// Python: channels/_ai_tools.py::_judged_call: a refusal past the gate is a
// raise like any other, its span ended in error; the guard's is one.
#[tokio::test]
async fn a_call_refused_past_its_gate_ends_its_span_in_error() {
    let script = vec![
        Round::Says("One.", Some("lookup")),
        Round::Says("Two.", Some("lookup")),
        Round::Says("Three.", Some("lookup")),
        Round::Says("Done.", None),
    ];
    let (kit, mock) = turn_of(script).await;
    let calls = mock.get_spans(SpanKind::LlmToolCall);
    let statuses: Vec<&str> = calls.iter().map(|call| call.status.as_str()).collect();
    assert_eq!(statuses, ["ok", "ok", "error"]);
    assert_eq!(calls[2].error_message.as_deref(), Some("ToolError"));
    kit.close().await.unwrap();
}

// Python: channels/_ai_tools.py::_serve_gated_call: a call its gate refuses
// never reaches `_judged_call`, which opens the span.
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
    assert_eq!(span.error_message.as_deref(), Some("ProviderError"));
    kit.close().await.unwrap();
    let (kit, mock) = turn_recording(true, vec![Round::Fails("the model is down")]).await;
    let span = one(mock.get_spans(SpanKind::LlmGenerate));
    assert!(
        span.error_message
            .as_deref()
            .unwrap()
            .contains("the model is down")
    );
    kit.close().await.unwrap();
}

/// Python's round records a generation's usage at its done event, so a
/// failure past it leaves the metrics written and the tokens on the span the
/// failure ends (`_record_stream_usage`, `_end_unfinished_turn`).
#[tokio::test]
async fn a_failure_past_a_done_event_keeps_its_tokens_on_the_span() {
    let (kit, mock) = turn_of(vec![Round::FailsPastItsEnd("the model is down")]).await;
    let span = one(mock.get_spans(SpanKind::LlmGenerate));
    assert_eq!(span.status, "error");
    assert_eq!(
        (
            span.attributes.get(Attr::LLM_INPUT_TOKENS).cloned(),
            span.attributes.get(Attr::LLM_OUTPUT_TOKENS).cloned()
        ),
        (Some(json!(10)), Some(json!(5)))
    );
    let tokens: Vec<(String, f64)> = mock
        .metrics()
        .into_iter()
        .filter(|(name, ..)| name.starts_with("roomkit.llm."))
        .map(|(name, value, ..)| (name, value))
        .collect();
    assert_eq!(
        tokens,
        [
            ("roomkit.llm.input_tokens".to_owned(), 10.0),
            ("roomkit.llm.output_tokens".to_owned(), 5.0)
        ]
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

/// The hooks of a tool call and of a turn open their spans, the sync ones and
/// the observers, each under the turn's call.
// Python: core/hooks.py runs each of them through its engine: hook.sync.<name>
// and hook.async.<name>, whichever door fires them.
#[tokio::test]
async fn the_hooks_of_a_tool_call_open_their_spans() {
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
    let lookup = AiTool {
        parameters: json!({ "type": "object", "properties": {} }),
        ..AiTool::new("lookup", "Look it up.").unwrap()
    };
    let tools = Tools::new().tool(lookup, |_call, _context| async {
        Ok(ToolResult::Text("42".to_owned()))
    });
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let script = vec![
        Round::Says("Checking.", Some("lookup")),
        Round::Says("42.", None),
    ];
    let agent = AiChannel::new(id("ai"), Scripted::new(script), options).unwrap();
    kit.register_channel(agent).unwrap();
    for (trigger, execution, name) in [
        (HookTrigger::BeforeToolUse, HookExecution::Sync, "gate"),
        (HookTrigger::OnToolCall, HookExecution::Sync, "judge"),
        (HookTrigger::OnToolCall, HookExecution::Async, "watch"),
    ] {
        let hook_options = HookOptions {
            name: name.to_owned(),
            execution,
            ..HookOptions::default()
        };
        kit.hook(
            trigger,
            |_: ToolCallEvent, _| ready(HookResult::allow(AllowOptions::default())),
            hook_options,
        )
        .unwrap();
    }
    let response = HookOptions {
        name: "heard".to_owned(),
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    kit.hook(
        HookTrigger::OnAiResponse,
        |_: AiResponseEvent, _| ready(()),
        response,
    )
    .unwrap();
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
    kit.close().await.unwrap();
    let spans = mock.completed_spans();
    for name in [
        "hook.sync.gate",
        "hook.sync.judge",
        "hook.async.watch",
        "hook.async.heard",
    ] {
        let found: Vec<&Span> = spans.iter().filter(|span| span.name == name).collect();
        assert_eq!(found.len(), 1, "{name} in {spans:?}");
        assert_eq!(found[0].status, "ok", "{:?}", found[0]);
        assert!(found[0].parent_id.is_some(), "{:?}", found[0]);
    }
}

/// A turn in a room whose channel serializes its turns runs under the
/// broadcast that asked it, whenever it starts.
// Python: core/_room_turns.py: `restored_span(span_id, ...)` around the turn.
#[tokio::test]
async fn a_serialized_turn_s_generation_is_a_child_of_its_broadcast() {
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
    let options = AiChannelOptions {
        serialize_room_turns: true,
        ..AiChannelOptions::default()
    };
    let script = vec![Round::Says("Hi.", None)];
    let agent = AiChannel::new(id("ai"), Scripted::new(script), options).unwrap();
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
    kit.close().await.unwrap();
    let generate = one(mock.get_spans(SpanKind::LlmGenerate));
    let broadcasts = mock.get_spans(SpanKind::Broadcast);
    assert!(
        broadcasts
            .iter()
            .any(|broadcast| Some(&broadcast.id) == generate.parent_id.as_ref()),
        "{generate:?} under {broadcasts:?}"
    );
}

/// A turn's host calling the kit stays in the turn's trace.
// Python: tests/test_opentelemetry.py::TestTraceContinuity's shape: one root.
#[tokio::test]
async fn a_turn_host_s_send_event_stays_in_the_trace() {
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
    let lookup = AiTool {
        parameters: json!({ "type": "object", "properties": {} }),
        ..AiTool::new("lookup", "Look it up.").unwrap()
    };
    let tools = Tools::new().tool(lookup, |_call, context| async move {
        let turn = current_turn_host().unwrap();
        turn.send_event(
            &context.room_id,
            &id("ai"),
            TextContent::new("(looking it up)"),
            SendEventOptions::default(),
        )
        .await
        .unwrap();
        Ok(ToolResult::Text("42".to_owned()))
    });
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let script = vec![
        Round::Says("Checking.", Some("lookup")),
        Round::Says("42.", None),
    ];
    let agent = AiChannel::new(id("ai"), Scripted::new(script), options).unwrap();
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
    kit.close().await.unwrap();
    let spans = mock.completed_spans();
    let roots: Vec<&str> = spans
        .iter()
        .filter(|span| span.parent_id.is_none())
        .map(|span| span.name.as_str())
        .collect();
    assert_eq!(roots, ["framework.inbound"], "{spans:?}");
    assert!(spans.iter().any(|span| span.name == "framework.send_event"));
}

// Python: core/mixins/lane_execution.py: `framework.broadcast` ends as
// `execute_plan` returns; the turn's stream is read after it, a delegated
// turn's as any other's.
#[tokio::test]
async fn a_delegated_turn_s_broadcast_ends_before_its_stream_is_read() {
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
    let lookup = AiTool {
        parameters: json!({ "type": "object", "properties": {} }),
        ..AiTool::new("lookup", "Look it up.").unwrap()
    };
    let (reading, seen) = (Arc::clone(&mock), Arc::new(Mutex::new(Vec::new())));
    let noted = Arc::clone(&seen);
    let tools = Tools::new().tool(lookup, move |_call, _context| {
        let active = reading.get_active_spans();
        let names = active.into_iter().map(|span| span.name);
        noted.lock().unwrap().extend(names);
        async { Ok(ToolResult::Text("42".to_owned())) }
    });
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let script = vec![
        Round::Says("Checking.", Some("lookup")),
        Round::Says("42.", None),
    ];
    let agent = AiChannel::new(id("ai"), Scripted::new(script), options).unwrap();
    kit.register_channel(agent).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    let wait = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    kit.delegate(&room(), &id("ai"), "go", wait).await.unwrap();
    kit.close().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert!(seen.iter().any(|name| name == "llm.generate"), "{seen:?}");
    assert!(
        !seen.iter().any(|name| name == "framework.broadcast"),
        "{seen:?}"
    );
}

// Python: tests/test_turns_wiring.py::test_a_turn_in_a_context_of_its_own_continues_the_owner_s_span
// and tests/test_opentelemetry.py: a turn's request names the span it runs
// under, the broadcast's (RFC §10.6.9).
#[tokio::test]
async fn a_turn_s_request_names_the_broadcast_it_runs_under() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::clone(&mock).into()),
        ..RoomKitOptions::default()
    });
    let tap = TurnTap::on(&kit);
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let script = vec![Round::Says("Yes.", None)];
    let agent = AiChannel::new(id("ai"), Scripted::new(script), AiChannelOptions::default());
    kit.register_channel(agent.unwrap()).unwrap();
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
    kit.close().await.unwrap();
    let requests: Vec<Value> = tap
        .messages()
        .into_iter()
        .filter(|message| message.family == "request")
        .map(|message| serde_json::from_str(&message.json).unwrap())
        .collect();
    let [request] = &requests[..] else {
        panic!("one request in {requests:?}");
    };
    let span_id = request["trace"]["span_id"].as_str().unwrap();
    let broadcasts = mock.get_spans(SpanKind::Broadcast);
    assert!(broadcasts.iter().any(|span| span.id == span_id));
    let generate = one(mock.get_spans(SpanKind::LlmGenerate));
    assert_eq!(generate.parent_id.as_deref(), Some(span_id));
}
