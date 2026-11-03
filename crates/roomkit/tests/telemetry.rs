//! The spans the kit reports, read from a `MockTelemetryProvider`: Python's
//! `tests/test_telemetry.py`.

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "a test fails on a broken fixture"
)]

use std::collections::BTreeSet;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, HookResult, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::telemetry::{
    Attr, MockTelemetryProvider, Span, SpanKind, TelemetryConfig, TelemetryProvider,
    get_current_span,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, DetachChannelOptions,
    HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions,
    SendEventOptions, TransportChannel, TransportChannelOptions,
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
    assert_eq!(root.attributes["sender_id"], json!("user1"));
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
    assert!(
        hook.error_message
            .as_deref()
            .unwrap()
            .contains("the scanner is down")
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
