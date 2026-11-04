//! The kit's spans through the feature `opentelemetry`, read from the SDK's
//! in-memory exporter: what a tracing backend shows, Python's
//! `tests/test_opentelemetry.py::TestTraceContinuity`.

#![cfg(feature = "opentelemetry")]
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::ready;
use std::sync::Arc;

use opentelemetry::trace::SpanId;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::telemetry::{OpenTelemetryProvider, OpenTelemetryProviderOptions};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookOptions, HookTrigger, PostOptions,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{AiChannel, AiChannelOptions, MockAiProvider, MockAiProviderOptions};

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

/// The spans a turn in a room of two transports and an agent exports, the
/// provider closed with the kit.
async fn exported(streaming: bool, defer_delivery: bool) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let telemetry = OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
        tracer_provider: Some(tracer_provider),
        ..OpenTelemetryProviderOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::new(telemetry).into()),
        ..RoomKitOptions::default()
    });
    for channel in ["ch1", "ch2"] {
        let transport = RecordingTransport::new(
            id(channel),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport).unwrap();
    }
    let provider = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["reply".to_owned()],
        streaming,
        ..MockAiProviderOptions::default()
    });
    let agent = AiChannel::new(id("ai1"), provider, AiChannelOptions::default()).unwrap();
    kit.register_channel(agent).unwrap();
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
    let room = RoomId::new("r1").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["ch1", "ch2"] {
        kit.attach_channel(&room, &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let agent = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..AttachChannelOptions::default()
    };
    kit.attach_channel(&room, &id("ai1"), agent).await.unwrap();
    let message = InboundMessage::new(id("ch1"), "user1", TextContent::new("hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room),
        defer_delivery,
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();
    if let Some(delivery) = result.delivery {
        delivery.wait().await.unwrap();
    }
    kit.close().await.unwrap();
    exporter.get_finished_spans().unwrap()
}

fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|span| span.name == name).collect()
}

// Python: tests/test_opentelemetry.py::TestTraceContinuity::test_turn_spans_share_the_inbound_trace
#[tokio::test]
async fn a_turn_s_spans_share_the_inbound_s_trace() {
    for (streaming, defer) in [(false, false), (true, false), (false, true), (true, true)] {
        let spans = exported(streaming, defer).await;
        let [inbound] = named(&spans, "roomkit.framework.inbound")[..] else {
            panic!("one framework.inbound in {spans:?}");
        };
        let trace = inbound.span_context.trace_id();
        let roots: Vec<&str> = spans
            .iter()
            .filter(|span| span.parent_span_id == SpanId::INVALID)
            .map(|span| span.name.as_ref())
            .collect();
        assert_eq!(roots, ["roomkit.framework.inbound"], "{streaming} {defer}");
        assert!(
            spans
                .iter()
                .all(|span| span.span_context.trace_id() == trace)
        );
        let broadcasts = named(&spans, "roomkit.framework.broadcast");
        assert_eq!(broadcasts.len(), 2, "{streaming} {defer}");
        let under_inbound =
            |span: &&SpanData| span.parent_span_id == inbound.span_context.span_id();
        assert!(broadcasts.iter().all(under_inbound), "{broadcasts:?}");
        let after = named(&spans, "roomkit.hook.async.after");
        assert!(
            !after.is_empty() && after.iter().all(under_inbound),
            "{after:?}"
        );
    }
}

// Python: tests/test_opentelemetry.py::test_a_turn_s_generation_is_a_child_of_the_broadcast_it_runs_under
#[tokio::test]
async fn a_turn_s_generation_is_a_child_of_the_broadcast_it_runs_under() {
    let spans = exported(false, false).await;
    let [generate] = named(&spans, "roomkit.llm.generate")[..] else {
        panic!("one llm.generate in {spans:?}");
    };
    let parent = spans
        .iter()
        .find(|span| span.span_context.span_id() == generate.parent_span_id)
        .unwrap();
    assert_eq!(parent.name, "roomkit.framework.broadcast");
}

// Python: core/mixins/inbound... `_inject`: a post's span opens as it goes in,
// and what follows nests under it in the trace.
#[tokio::test]
async fn a_post_s_broadcast_is_a_child_of_its_send_event() {
    let exporter = InMemorySpanExporter::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let telemetry = OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
        tracer_provider: Some(tracer_provider),
        ..OpenTelemetryProviderOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::new(telemetry).into()),
        ..RoomKitOptions::default()
    });
    let transport = RecordingTransport::new(
        id("ch1"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(transport).unwrap();
    let room = RoomId::new("r1").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room, &id("ch1"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.post(&room, TextContent::new("Bonjour"), PostOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let [post] = named(&spans, "roomkit.framework.send_event")[..] else {
        panic!("one framework.send_event in {spans:?}");
    };
    let [broadcast] = named(&spans, "roomkit.framework.broadcast")[..] else {
        panic!("one framework.broadcast in {spans:?}");
    };
    assert_eq!(broadcast.parent_span_id, post.span_context.span_id());
    assert_eq!(
        broadcast.span_context.trace_id(),
        post.span_context.trace_id()
    );
}
