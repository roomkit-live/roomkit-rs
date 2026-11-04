//! The spans the kit reports, read from a `MockTelemetryProvider`: Python's
//! `tests/test_telemetry.py`.

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "a test fails on a broken fixture"
)]

use std::collections::BTreeSet;
use std::future::{Future, pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, HookResult,
    InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::tasks::WaitOptions;
use roomkit::telemetry::{
    Attr, MockTelemetryProvider, Span, SpanKind, TelemetryConfig, TelemetryProvider,
    get_current_span,
};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    AttachChannelOptions, CancelTaskOptions, Channel, ChannelError, CreateRoomOptions,
    DelegateOptions, DeliverOptions, DetachChannelOptions, HookExecution, HookOptions, HookTrigger,
    PostOptions, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, SendEventOptions,
    TransportChannel, TransportChannelOptions,
};
use serde_json::json;

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A channel keeping the span current during its `deliver()`.
#[derive(Clone)]
struct Reader {
    id: ChannelId,
    current: Arc<Mutex<Vec<Option<String>>>>,
}

impl Reader {
    fn new(channel: &str) -> Self {
        Self {
            id: id(channel),
            current: Arc::default(),
        }
    }
}

impl Channel for Reader {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let source = roomkit::model::EventSource::new(self.id.clone(), ChannelType::SMS);
        ready(Ok(RoomEvent::new(context.room.id, source, message.content)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.current.lock().unwrap().push(get_current_span());
        ready(Ok(ChannelOutput::empty()))
    }
}

/// A kit reporting to a mock, with a room of two transport channels: `src`
/// the source, `out` read back.
async fn kit_with_room(
    config: Option<TelemetryConfig>,
) -> (RoomKit, Arc<MockTelemetryProvider>, Reader) {
    let mock = Arc::new(MockTelemetryProvider::default());
    let provider: Arc<dyn TelemetryProvider> = Arc::<MockTelemetryProvider>::clone(&mock);
    let config = TelemetryConfig {
        provider: Some(provider),
        ..config.unwrap_or_default()
    };
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(config),
        ..RoomKitOptions::default()
    });
    let out = Reader::new("out");
    let src = TransportChannel::new(
        id("src"),
        ChannelType::SMS,
        TransportChannelOptions::default(),
    );
    kit.register_channel(src).unwrap();
    kit.register_channel(out.clone()).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["src", "out"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, mock, out)
}

fn hello() -> InboundMessage {
    InboundMessage::new(id("src"), "user1", TextContent::new("hello"))
}

async fn inbound(kit: &RoomKit) {
    let named = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(hello(), named).await.unwrap();
}

fn named<'a>(spans: &'a [Span], name: &str) -> &'a Span {
    let found: Vec<&Span> = spans.iter().filter(|span| span.name == name).collect();
    assert_eq!(found.len(), 1, "{name} in {:?}", names(spans));
    found[0]
}

fn names(spans: &[Span]) -> Vec<&str> {
    spans.iter().map(|span| span.name.as_str()).collect()
}

fn allow_hook(kit: &RoomKit, trigger: HookTrigger, name: &str, execution: HookExecution) {
    kit.hook(
        trigger,
        |_: RoomEvent, _| ready(HookResult::allow(AllowOptions::default())),
        HookOptions {
            name: name.to_owned(),
            execution,
            ..HookOptions::default()
        },
    )
    .unwrap();
}

// Python: tests/test_telemetry.py::TestInboundTelemetry::test_inbound_creates_span
// and TestTelemetryPhase4::test_inbound_span_gets_room_id
#[tokio::test]
async fn an_inbound_opens_a_span_with_its_steps_under_it() {
    let (kit, mock, _) = kit_with_room(None).await;
    inbound(&kit).await;
    let spans = mock.get_spans(SpanKind::InboundPipeline);
    let root = named(&spans, "framework.inbound");
    assert_eq!(root.status, "ok");
    assert_eq!(root.parent_id, None);
    assert_eq!(root.room_id.as_deref(), Some("r1"));
    assert_eq!(root.channel_id.as_deref(), Some("src"));
    assert_eq!(root.attributes["blocked"], json!(false));
    assert_eq!(root.attributes["deferred"], json!(false));
    let route = named(&spans, "framework.route");
    assert_eq!(route.parent_id.as_deref(), Some(root.id.as_str()));
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestHookTelemetry::test_sync_hook_creates_span,
// TestTelemetryPhase4::test_hook_span_parents_to_inbound
#[tokio::test]
async fn a_sync_hook_opens_a_span_under_the_inbound() {
    let (kit, mock, _) = kit_with_room(None).await;
    allow_hook(
        &kit,
        HookTrigger::BeforeBroadcast,
        "my_hook",
        HookExecution::Sync,
    );
    inbound(&kit).await;
    let spans = mock.completed_spans();
    let hook = named(&spans, "hook.sync.my_hook");
    assert_eq!(hook.kind, SpanKind::HookSync);
    assert_eq!(hook.status, "ok");
    assert_eq!(hook.room_id.as_deref(), Some("r1"));
    assert_eq!(hook.attributes[Attr::HOOK_NAME], json!("my_hook"));
    assert_eq!(
        hook.attributes[Attr::HOOK_TRIGGER],
        json!("before_broadcast")
    );
    assert_eq!(hook.attributes[Attr::HOOK_RESULT], json!("allow"));
    let root = named(&spans, "framework.inbound");
    assert_eq!(hook.parent_id.as_deref(), Some(root.id.as_str()));
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestHookTelemetry::test_hook_error_records_error_span
#[tokio::test]
async fn a_hook_that_fails_ends_its_span_in_error() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            let raised: Result<HookResult, roomkit::HookError> = Err("the scanner is down".into());
            raised
        },
        HookOptions {
            name: "bad_hook".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    inbound(&kit).await;
    let spans = mock.get_spans(SpanKind::HookSync);
    let hook = named(&spans, "hook.sync.bad_hook");
    assert_eq!(hook.status, "error");
    assert_eq!(hook.error_message.as_deref(), Some("HookError"));
    kit.close().await.unwrap();
}

/// A kit whose hook raises a text quoting an address, sent a message from
/// that address, under `config`.
async fn a_raising_hook(config: Option<TelemetryConfig>) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let (kit, mock, _) = kit_with_room(config).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            let raised: Result<HookResult, roomkit::HookError> =
                Err("+15145550123 is refused".into());
            raised
        },
        HookOptions {
            name: "scan".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let message = InboundMessage::new(id("src"), "+15145550123", TextContent::new("hi"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    (kit, mock)
}

// Python: tests/test_telemetry.py::TestSensitiveData::test_a_span_holds_no_address_and_no_error_text_by_default
#[tokio::test]
async fn a_span_holds_no_address_and_no_error_text_by_default() {
    let (kit, mock) = a_raising_hook(None).await;
    let spans = mock.completed_spans();
    let recorded = format!(
        "{:?}",
        spans
            .iter()
            .map(|span| (&span.attributes, &span.error_message))
            .collect::<Vec<_>>()
    );
    assert!(!recorded.contains("+15145550123"), "{recorded}");
    let hook = named(&spans, "hook.sync.scan");
    assert_eq!(hook.error_message.as_deref(), Some("HookError"));
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestSensitiveData::test_the_config_records_them_when_it_asks
#[tokio::test]
async fn the_config_records_them_when_it_asks() {
    let asks = TelemetryConfig {
        record_sensitive_data: true,
        ..TelemetryConfig::default()
    };
    let (kit, mock) = a_raising_hook(Some(asks)).await;
    let spans = mock.completed_spans();
    for name in ["framework.inbound", "framework.route"] {
        let span = named(&spans, name);
        assert_eq!(
            span.attributes["sender_id"],
            json!("+15145550123"),
            "{name}"
        );
    }
    let hook = named(&spans, "hook.sync.scan");
    assert_eq!(
        hook.error_message.as_deref(),
        Some("+15145550123 is refused")
    );
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestTelemetryPhase4::test_suppressed_hook_triggers_skip_spans,
// test_non_suppressed_hooks_still_create_spans, test_custom_suppressed_triggers,
// test_suppressed_async_hooks_skip_spans
#[tokio::test]
async fn a_suppressed_trigger_s_hooks_open_no_span() {
    let suppressed = TelemetryConfig {
        suppressed_hook_triggers: BTreeSet::from([
            "before_broadcast".to_owned(),
            "after_broadcast".to_owned(),
        ]),
        ..TelemetryConfig::default()
    };
    let (kit, mock, _) = kit_with_room(Some(suppressed)).await;
    allow_hook(
        &kit,
        HookTrigger::BeforeBroadcast,
        "quiet",
        HookExecution::Sync,
    );
    kit.hook(
        HookTrigger::AfterBroadcast,
        |_: RoomEvent, _| ready(()),
        HookOptions {
            name: "quiet_after".to_owned(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    inbound(&kit).await;
    assert_eq!(mock.get_spans(SpanKind::HookSync), vec![]);
    assert_eq!(mock.get_spans(SpanKind::HookAsync), vec![]);
    kit.close().await.unwrap();
    let (kit, mock, _) = kit_with_room(None).await;
    allow_hook(
        &kit,
        HookTrigger::BeforeBroadcast,
        "loud",
        HookExecution::Sync,
    );
    inbound(&kit).await;
    named(&mock.completed_spans(), "hook.sync.loud");
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestBroadcastTelemetry::test_broadcast_creates_span,
// TestTelemetryPhase4::test_inbound_broadcast_span_hierarchy, test_lane_side_spans_parent_to_inbound
#[tokio::test]
async fn a_broadcast_opens_a_span_under_the_inbound_with_its_counts() {
    let (kit, mock, out) = kit_with_room(None).await;
    inbound(&kit).await;
    let spans = mock.completed_spans();
    let root = named(&spans, "framework.inbound");
    let broadcast = named(&spans, "framework.broadcast");
    assert_eq!(broadcast.kind, SpanKind::Broadcast);
    assert_eq!(broadcast.parent_id.as_deref(), Some(root.id.as_str()));
    assert_eq!(broadcast.room_id.as_deref(), Some("r1"));
    assert_eq!(broadcast.attributes[Attr::CHANNEL_ID], json!("src"));
    assert_eq!(broadcast.attributes["target_count"], json!(1));
    assert_eq!(broadcast.attributes["delivered_count"], json!(1));
    assert_eq!(broadcast.attributes["failed_count"], json!(0));
    // What a channel's `deliver()` starts sits under the broadcast
    // (TestTelemetryPhase4::test_delivery_span_parents_to_broadcast).
    assert_eq!(
        out.current.lock().unwrap().clone(),
        vec![Some(broadcast.id.clone())]
    );
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestBroadcastTelemetry (a plan with no target)
#[tokio::test]
async fn a_broadcast_with_no_target_reports_none() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.detach_channel(&room(), &id("out"), DetachChannelOptions::default())
        .await
        .unwrap();
    inbound(&kit).await;
    let spans = mock.completed_spans();
    let broadcast = named(&spans, "framework.broadcast");
    assert_eq!(broadcast.attributes["target_count"], json!(0));
    assert!(!broadcast.attributes.contains_key("delivered_count"));
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestTelemetryPhase4::test_inbound_span_gets_session_id, test_broadcast_span_gets_session_id
#[tokio::test]
async fn a_voice_session_s_id_reaches_the_inbound_and_broadcast_spans() {
    let (kit, mock, _) = kit_with_room(None).await;
    let mut message = hello();
    message
        .metadata
        .insert("voice_session_id".to_owned(), json!("sess-abc"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    let spans = mock.completed_spans();
    let root = named(&spans, "framework.inbound");
    assert_eq!(root.attributes[Attr::SESSION_ID], json!("sess-abc"));
    let broadcast = named(&spans, "framework.broadcast");
    assert_eq!(broadcast.session_id.as_deref(), Some("sess-abc"));
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestTelemetryPhase4::test_context_var_reset_after_inbound
#[tokio::test]
async fn no_span_is_current_after_an_inbound() {
    let (kit, _, _) = kit_with_room(None).await;
    inbound(&kit).await;
    assert_eq!(get_current_span(), None);
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestTextMessageSpanHierarchy::test_full_text_pipeline_span_tree
#[tokio::test]
async fn a_text_message_s_spans_are_one_tree() {
    let (kit, mock, _) = kit_with_room(None).await;
    allow_hook(
        &kit,
        HookTrigger::BeforeBroadcast,
        "filter_hook",
        HookExecution::Sync,
    );
    inbound(&kit).await;
    let spans = mock.completed_spans();
    let root = named(&spans, "framework.inbound");
    for child in [
        "framework.route",
        "channel.handle_inbound",
        "framework.broadcast",
        "hook.sync.filter_hook",
    ] {
        let span = named(&spans, child);
        assert_eq!(span.parent_id.as_deref(), Some(root.id.as_str()), "{child}");
    }
    assert!(spans.iter().all(|span| span.status == "ok"), "{spans:?}");
    kit.close().await.unwrap();
}

// Python: tests/test_telemetry.py::TestTextMessageSpanHierarchy::test_send_event_creates_span
#[tokio::test]
async fn send_event_opens_a_span_over_its_broadcast() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.send_event(
        &room(),
        &id("src"),
        TextContent::new("hello"),
        SendEventOptions::default(),
    )
    .await
    .unwrap();
    let spans = mock.completed_spans();
    let send = named(&spans, "framework.send_event");
    assert_eq!(send.kind, SpanKind::InboundPipeline);
    assert_eq!(send.channel_id.as_deref(), Some("src"));
    assert_eq!(send.attributes["event_type"], json!("message"));
    let broadcast = named(&spans, "framework.broadcast");
    assert_eq!(broadcast.parent_id.as_deref(), Some(send.id.as_str()));
    kit.close().await.unwrap();
}

/// A worker that runs until the delegation is cancelled.
#[derive(Clone)]
struct Stuck(ChannelId);

impl Channel for Stuck {
    fn channel_id(&self) -> ChannelId {
        self.0.clone()
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
        ready(Err("a worker receives no message".into()))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        pending()
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// `(name, status)` of each delegation span `mock` ended.
fn delegations(mock: &MockTelemetryProvider) -> Vec<(String, String)> {
    let spans = mock.get_spans(SpanKind::Delegation);
    spans
        .into_iter()
        .map(|span| (span.name, span.status))
        .collect()
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_delegation_span_ends_with_its_task_status
#[tokio::test]
async fn a_delegation_span_ends_with_its_task_status() {
    for (reply, status) in [("Done.", "ok"), ("", "error")] {
        let (kit, mock, _) = kit_with_room(None).await;
        let worker = EchoAgent::new(id("worker"), reply, EchoAgentOptions::default());
        kit.register_channel(worker).unwrap();
        let wait = DelegateOptions {
            wait: true,
            ..DelegateOptions::default()
        };
        kit.delegate(&room(), &id("worker"), "go", wait)
            .await
            .unwrap();
        assert_eq!(
            delegations(&mock),
            vec![("delegation.inline".to_owned(), status.to_owned())]
        );
        let [span] = mock.get_spans(SpanKind::Delegation).try_into().unwrap();
        assert_eq!(span.room_id.as_deref(), Some("r1"));
        assert_eq!(span.channel_id.as_deref(), Some("worker"));
        assert_eq!(span.attributes[Attr::DELEGATION_MODE], json!("inline"));
        assert_eq!(span.attributes[Attr::DELEGATION_WORKER_ID], json!("worker"));
        kit.close().await.unwrap();
    }
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_cancelled_task_span_carries_how_long_it_ran
#[tokio::test(start_paused = true)]
async fn a_cancelled_task_span_carries_how_long_it_ran() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.register_channel(Stuck(id("worker"))).unwrap();
    let task = kit
        .delegate(&room(), &id("worker"), "go", DelegateOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cancelled = kit
        .cancel_task(&task.id, CancelTaskOptions::default())
        .await;
    assert!(cancelled);
    let [span] = mock.get_spans(SpanKind::Delegation).try_into().unwrap();
    assert_eq!(span.name, "delegation.background");
    assert_eq!(span.status, "cancelled");
    assert_eq!(span.attributes[Attr::DELEGATION_STATUS], json!("cancelled"));
    assert!(
        span.attributes[Attr::DURATION_MS].as_f64().unwrap() >= 40.0,
        "{span:?}"
    );
    kit.close().await.unwrap();
}

// Python: core/hooks.py (an async hook's return is never read)
#[tokio::test]
async fn an_async_hook_s_span_ends_ok_whatever_it_returns() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.hook(
        HookTrigger::AfterBroadcast,
        |_: RoomEvent, _| ready(()),
        HookOptions {
            name: "after".to_owned(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    inbound(&kit).await;
    kit.close().await.unwrap();
    let spans = mock.completed_spans();
    let hook = named(&spans, "hook.async.after");
    assert_eq!(hook.status, "ok", "{hook:?}");
    assert_eq!(hook.error_message, None);
}

// Python: core/hooks.py (an async hook that raises reads `failed`, whatever it raised)
#[tokio::test]
async fn an_async_hook_that_raises_reads_failed_never_its_text() {
    for sensitive in [false, true] {
        let asks = TelemetryConfig {
            record_sensitive_data: sensitive,
            ..TelemetryConfig::default()
        };
        let (kit, mock, _) = kit_with_room(Some(asks)).await;
        kit.hook(
            HookTrigger::AfterBroadcast,
            |_: RoomEvent, _| async {
                let raised: Result<HookResult, roomkit::HookError> =
                    Err("+15145550123 is refused".into());
                raised
            },
            HookOptions {
                name: "after".to_owned(),
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        inbound(&kit).await;
        kit.close().await.unwrap();
        let spans = mock.completed_spans();
        let hook = named(&spans, "hook.async.after");
        assert_eq!(hook.status, "error");
        assert_eq!(hook.error_message.as_deref(), Some("failed"));
    }
}

// Python: the broadcast of a delegated turn (RFC §23.3), which ends with its counts
#[tokio::test]
async fn a_delegated_turn_s_broadcast_ends_ok_with_its_counts() {
    let (kit, mock, _) = kit_with_room(None).await;
    let worker = EchoAgent::new(id("worker"), "Done.", EchoAgentOptions::default());
    kit.register_channel(worker).unwrap();
    let wait = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    kit.delegate(&room(), &id("worker"), "go", wait)
        .await
        .unwrap();
    let spans = mock.get_spans(SpanKind::Broadcast);
    assert_ne!(spans, vec![]);
    for span in &spans {
        assert_eq!(span.status, "ok", "{span:?}");
        assert_eq!(span.attributes["target_count"], json!(1), "{span:?}");
    }
    kit.close().await.unwrap();
}

// Python: core/mixins/delegation.py:799,835: the span ends before the task's callback
#[tokio::test]
async fn a_delegation_span_ends_before_the_task_s_callback() {
    let (kit, mock, _) = kit_with_room(None).await;
    let worker = EchoAgent::new(id("worker"), "Done.", EchoAgentOptions::default());
    kit.register_channel(worker).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (reading, noted) = (Arc::clone(&mock), Arc::clone(&seen));
    let wait = DelegateOptions {
        wait: true,
        on_complete: Some(Arc::new(move |_| {
            let active = reading.get_active_spans();
            let names = active.into_iter().map(|span| span.name);
            noted
                .lock()
                .unwrap()
                .extend(names.filter(|name| name.starts_with("delegation")));
            Box::pin(async {})
        })),
        ..DelegateOptions::default()
    };
    kit.delegate(&room(), &id("worker"), "go", wait)
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap().clone(), Vec::<String>::new());
    kit.close().await.unwrap();
}

// Python: core/_host_post.py: `post()` is a framework.send_event, its broadcast under it
#[tokio::test]
async fn a_post_opens_a_span_over_its_broadcast() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.post(&room(), TextContent::new("Bonjour"), PostOptions::default())
        .await
        .unwrap();
    let spans = mock.completed_spans();
    let post = named(&spans, "framework.send_event");
    assert_eq!(post.parent_id, None);
    assert_eq!(post.attributes["event_type"], json!("message"));
    let broadcast = named(&spans, "framework.broadcast");
    assert_eq!(broadcast.parent_id.as_deref(), Some(post.id.as_str()));
    kit.close().await.unwrap();
}

// Python: core/mixins/inbound.py:311-336: a call its channel refuses opens no span
#[tokio::test]
async fn a_message_on_no_channel_opens_no_span() {
    let (kit, mock, _) = kit_with_room(None).await;
    let lost = InboundMessage::new(id("nowhere"), "user1", TextContent::new("hello"));
    let refused = kit
        .process_inbound(lost, ProcessInboundOptions::default())
        .await;
    assert!(refused.is_err());
    assert_eq!(mock.completed_spans(), vec![]);
    kit.post(&room(), TextContent::new("Bonjour"), PostOptions::default())
        .await
        .unwrap();
    mock.reset();
    let lost = InboundMessage::new(ChannelId::host(), "user1", TextContent::new("hello"));
    let refused = kit
        .process_inbound(lost, ProcessInboundOptions::default())
        .await;
    assert!(refused.is_err(), "the host channel receives no message");
    assert_eq!(mock.completed_spans(), vec![]);
    kit.close().await.unwrap();
}

/// A background delegation from a hook: every span of the call, the task's
/// and its broadcast's, stays under the inbound.
// Python: asyncio.create_task copies the caller's span (tests/test_telemetry.py::
// TestTelemetryPhase4::test_lane_side_spans_parent_to_inbound's shape, on a task).
#[tokio::test]
async fn a_background_task_s_spans_stay_in_the_call_s_trace() {
    let (kit, mock, _) = kit_with_room(None).await;
    let worker = EchoAgent::new(id("worker"), "Done.", EchoAgentOptions::default());
    kit.register_channel(worker).unwrap();
    let started = Arc::new(Mutex::new(None));
    let (hook_kit, handle) = (kit.clone(), Arc::clone(&started));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| {
            let (kit, handle) = (hook_kit.clone(), Arc::clone(&handle));
            async move {
                let task = kit
                    .delegate(&room(), &id("worker"), "go", DelegateOptions::default())
                    .await
                    .unwrap();
                *handle.lock().unwrap() = Some(task);
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "delegates".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    inbound(&kit).await;
    let task = started.lock().unwrap().take().unwrap();
    task.wait(WaitOptions::default()).await.unwrap();
    kit.close().await.unwrap();
    let spans = mock.completed_spans();
    let roots: Vec<&str> = spans
        .iter()
        .filter(|span| span.parent_id.is_none())
        .map(|span| span.name.as_str())
        .collect();
    assert_eq!(roots, ["framework.inbound"], "{:?}", names(&spans));
}

// Python: core/hooks.py: an async hook its cascade's cap stopped reads `capped`
#[tokio::test]
async fn an_async_hook_a_cap_stopped_reads_capped() {
    let (kit, mock, _) = kit_with_room(None).await;
    kit.hook(
        HookTrigger::AfterBroadcast,
        |event: RoomEvent, _| async move {
            let capped = RoomKitError::ReentryLoopCap(event.room_id);
            let raised: Result<HookResult, roomkit::HookError> = Err(Box::new(capped));
            raised
        },
        HookOptions {
            name: "looped".to_owned(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    inbound(&kit).await;
    kit.close().await.unwrap();
    let spans = mock.completed_spans();
    let hook = named(&spans, "hook.async.looped");
    assert_eq!(hook.error_message.as_deref(), Some("capped"));
}

/// A hook a plain call fires reports its span: the call need not run under a
/// kit's span.
// Python: core/mixins/delegation.py:643, whose engine always holds the kit's telemetry
#[tokio::test]
async fn a_task_s_hook_a_plain_call_fires_opens_its_span() {
    let (kit, mock, _) = kit_with_room(None).await;
    let worker = EchoAgent::new(id("worker"), "Done.", EchoAgentOptions::default());
    kit.register_channel(worker).unwrap();
    kit.hook(
        HookTrigger::OnTaskDelegated,
        |_: RoomEvent, _| ready(()),
        HookOptions {
            name: "delegated".to_owned(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    let wait = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    kit.delegate(&room(), &id("worker"), "go", wait)
        .await
        .unwrap();
    kit.close().await.unwrap();
    let spans = mock.completed_spans();
    assert_eq!(named(&spans, "hook.async.delegated").status, "ok");
}

// Python: core/delivery.py: BEFORE_DELIVER runs through the engine
#[tokio::test]
async fn a_delivery_s_hook_opens_its_span() {
    let (kit, mock, _) = kit_with_room(None).await;
    allow_hook(
        &kit,
        HookTrigger::BeforeDeliver,
        "before",
        HookExecution::Sync,
    );
    kit.deliver(&room(), "result", DeliverOptions::default())
        .await
        .unwrap();
    let spans = mock.completed_spans();
    assert_eq!(named(&spans, "hook.sync.before").status, "ok");
    kit.close().await.unwrap();
}

// Python: core/hooks.py and telemetry.base.span_error: an error with no text reads its class
#[tokio::test]
async fn a_hook_raising_no_text_reads_its_class_even_recording() {
    let asks = TelemetryConfig {
        record_sensitive_data: true,
        ..TelemetryConfig::default()
    };
    let (kit, mock, _) = kit_with_room(Some(asks)).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            let raised: Result<HookResult, roomkit::HookError> = Err("".into());
            raised
        },
        HookOptions {
            name: "mute".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    inbound(&kit).await;
    let spans = mock.completed_spans();
    let hook = named(&spans, "hook.sync.mute");
    assert_eq!(hook.error_message.as_deref(), Some("HookError"));
    kit.close().await.unwrap();
}

// Python: core/mixins/inbound.py `_route_to_room`: its span reads the room
// routing came to, one the kit opened included.
#[tokio::test]
async fn a_route_reads_the_room_the_kit_creates() {
    let (kit, mock, _) = kit_with_room(None).await;
    let fresh = TransportChannel::new(
        id("fresh"),
        ChannelType::SMS,
        TransportChannelOptions::default(),
    );
    kit.register_channel(fresh).unwrap();
    let message = InboundMessage::new(id("fresh"), "user1", TextContent::new("hello"));
    let result = kit
        .process_inbound(message, ProcessInboundOptions::default())
        .await
        .unwrap();
    let created = result.event.unwrap().room_id.to_string();
    assert_ne!(created, "r1");
    let spans = mock.completed_spans();
    let route = named(&spans, "framework.route");
    assert_eq!(route.status, "ok");
    assert_eq!(route.attributes[Attr::ROOM_ID], json!(created));
    let root = named(&spans, "framework.inbound");
    assert_eq!(route.parent_id.as_deref(), Some(root.id.as_str()));
    kit.close().await.unwrap();
}

/// The span of the set of the message sent on `src`.
fn message_s_broadcast(mock: &MockTelemetryProvider) -> Span {
    let spans = mock.get_spans(SpanKind::Broadcast);
    let of_the_message = spans
        .into_iter()
        .filter(|span| span.attributes[Attr::CHANNEL_ID] == json!("src"));
    let found: Vec<Span> = of_the_message.collect();
    assert_eq!(found.len(), 1, "{found:?}");
    found.into_iter().next().unwrap()
}

/// `agent` registered and attached to `r1`.
async fn with_agent(kit: &RoomKit, agent: &str) {
    let echo = EchoAgent::new(id(agent), "Yes.", EchoAgentOptions::default());
    kit.register_channel(echo).unwrap();
    kit.attach_channel(&room(), &id(agent), AttachChannelOptions::default())
        .await
        .unwrap();
}

// Python: core/event_router.py `execute_plan`: `target_count` is
// `len(plan.targets)`, whom the event reaches, asked or not; the agent the
// address leaves out is one (RFC §19.3).
#[tokio::test]
async fn a_broadcast_counts_the_targets_it_planned_asked_or_not() {
    let (kit, mock, _) = kit_with_room(None).await;
    with_agent(&kit, "a1").await;
    with_agent(&kit, "a2").await;
    let mut message = hello();
    message.addressed_to = Some(vec![id("a1")]);
    let named = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, named).await.unwrap();
    let broadcast = message_s_broadcast(&mock);
    assert_eq!(broadcast.attributes["target_count"], json!(3));
    assert_eq!(broadcast.attributes["delivered_count"], json!(1));
    assert_eq!(broadcast.attributes["failed_count"], json!(0));
    kit.close().await.unwrap();
}

// Python: core/event_router.py `_process_target`: an agent the event asks,
// not registered, is a failure of the set, `channel_not_registered`; a
// transport absent here is none (RFC §10.2 step 3).
#[tokio::test]
async fn a_broadcast_counts_an_unregistered_agent_as_failed_a_transport_not() {
    let (kit, mock, _) = kit_with_room(None).await;
    with_agent(&kit, "gone").await;
    kit.unregister_channel(&id("gone")).unwrap();
    kit.unregister_channel(&id("out")).unwrap();
    inbound(&kit).await;
    let broadcast = message_s_broadcast(&mock);
    assert_eq!(broadcast.attributes["target_count"], json!(2));
    assert_eq!(broadcast.attributes["delivered_count"], json!(0));
    assert_eq!(broadcast.attributes["failed_count"], json!(1));
    kit.close().await.unwrap();
}

// Python: core/event_router.py `execute_plan`: a target that fails is one of
// `errors`, a send that goes through one of `delivery_outputs`.
#[tokio::test]
async fn a_broadcast_counts_what_its_targets_came_to() {
    let (kit, mock, _) = kit_with_room(None).await;
    let fails = TransportChannel::new(
        id("no-recipient"),
        ChannelType::SMS,
        TransportChannelOptions::default(),
    );
    kit.register_channel(fails).unwrap();
    kit.attach_channel(
        &room(),
        &id("no-recipient"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    inbound(&kit).await;
    let broadcast = message_s_broadcast(&mock);
    assert_eq!(broadcast.attributes["target_count"], json!(2));
    assert_eq!(broadcast.attributes["delivered_count"], json!(1));
    assert_eq!(broadcast.attributes["failed_count"], json!(1));
    kit.close().await.unwrap();
}

fn send_event_spans(mock: &MockTelemetryProvider) -> Vec<Span> {
    let active = mock.get_active_spans().into_iter();
    active
        .chain(mock.completed_spans())
        .filter(|span| span.name == "framework.send_event")
        .collect()
}

// Python: core/_host_post.py `post`: what it refuses before `_inject` opens
// `framework.send_event` leaves no span.
#[tokio::test]
async fn a_post_refused_before_its_injection_opens_no_span() {
    let (kit, mock, _) = kit_with_room(None).await;
    let not_a_host = PostOptions {
        channel_id: Some(id("out")),
        ..PostOptions::default()
    };
    let no_such_channel = PostOptions {
        channel_id: Some(id("nowhere")),
        ..PostOptions::default()
    };
    let unreached = PostOptions {
        participant_id: Some(roomkit::model::ParticipantId::new("nobody").unwrap()),
        ..PostOptions::default()
    };
    for options in [not_a_host, no_such_channel, unreached] {
        let refused = kit.post(&room(), TextContent::new("Hi"), options).await;
        assert!(refused.is_err());
    }
    let nowhere = RoomId::new("no-such-room").unwrap();
    let refused = kit
        .post(&nowhere, TextContent::new("Hi"), PostOptions::default())
        .await;
    assert!(refused.is_err());
    // A room the host channel was detached from.
    kit.post(&room(), TextContent::new("Hi"), PostOptions::default())
        .await
        .unwrap();
    mock.reset();
    kit.detach_channel(&room(), &ChannelId::host(), DetachChannelOptions::default())
        .await
        .unwrap();
    let refused = kit
        .post(&room(), TextContent::new("Hi"), PostOptions::default())
        .await;
    assert!(refused.is_err());
    assert_eq!(send_event_spans(&mock), vec![]);
    kit.close().await.unwrap();
}

// Python: core/framework.py `_send_event`: an instruction that names no agent
// is refused before it goes in.
#[tokio::test]
async fn a_send_event_refused_before_its_injection_opens_no_span() {
    let (kit, mock, _) = kit_with_room(None).await;
    let refused = kit
        .send_event(
            &room(),
            &id("nowhere"),
            TextContent::new("Hi"),
            SendEventOptions::default(),
        )
        .await;
    assert!(refused.is_err());
    assert_eq!(send_event_spans(&mock), vec![]);
    kit.close().await.unwrap();
}
