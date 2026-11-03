//! The `opentelemetry` feature against the SDK's in-memory exporter:
//! Python's `tests/test_opentelemetry.py` and `test_opentelemetry_flush.py`.

#![expect(
    clippy::disallowed_methods,
    reason = "how soon a flush returns, on the real clock, is the behaviour"
)]

use std::collections::BTreeMap;
use std::future::{Future, ready};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::trace::{SpanId, Status, TraceContextExt};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, InMemorySpanExporter, SdkTracerProvider, SpanData,
    SpanExporter,
};
use serde_json::json;

use super::otel::ENDED_SPANS_KEPT;
use super::{
    EndSpanOptions, OpenTelemetryProvider, OpenTelemetryProviderOptions, RecordMetricOptions,
    SpanKind, StartSpanOptions, TelemetryProvider, get_current_span,
};

/// A provider whose spans land in an in-memory exporter.
fn exporting(
    options: OpenTelemetryProviderOptions,
) -> (OpenTelemetryProvider, InMemorySpanExporter) {
    let exporter = InMemorySpanExporter::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let provider = OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
        tracer_provider: Some(tracer_provider),
        ..options
    });
    (provider, exporter)
}

fn provider() -> (OpenTelemetryProvider, InMemorySpanExporter) {
    exporting(OpenTelemetryProviderOptions::default())
}

fn started(provider: &OpenTelemetryProvider, name: &str) -> String {
    provider.start_span(SpanKind::Custom, name, StartSpanOptions::default())
}

fn finished(exporter: &InMemorySpanExporter, name: &str) -> SpanData {
    let spans = exporter.get_finished_spans().unwrap();
    spans
        .into_iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("no span {name} was exported"))
}

fn attribute(span: &SpanData, key: &str) -> Option<opentelemetry::Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute: &KeyValue| attribute.value.clone())
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_constructor_and_name
#[test]
fn the_provider_s_name() {
    assert_eq!(provider().0.name(), "opentelemetry");
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_start_span_returns_string
#[test]
fn a_span_s_id_is_16_hex_digits() {
    let (provider, _) = provider();
    let id = started(&provider, "test.span");
    assert_eq!(id.len(), 16);
    assert!(id.chars().all(|digit| digit.is_ascii_hexdigit()));
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_end_span_ok
#[test]
fn a_span_ended_is_exported_ok_under_roomkit_s_names() {
    let (provider, exporter) = provider();
    let options = StartSpanOptions {
        room_id: Some("r1".to_owned()),
        channel_id: Some("sms".to_owned()),
        attributes: [
            ("n".to_owned(), json!(3)),
            ("dropped".to_owned(), json!([1])),
        ]
        .into_iter()
        .collect(),
        ..StartSpanOptions::default()
    };
    let id = provider.start_span(SpanKind::Broadcast, "framework.broadcast", options);
    let ended = EndSpanOptions {
        attributes: [("kept".to_owned(), json!([1]))].into_iter().collect(),
        ..EndSpanOptions::default()
    };
    provider.end_span(&id, ended);
    let span = finished(&exporter, "roomkit.framework.broadcast");
    assert_eq!(span.status, Status::Ok);
    assert_eq!(
        attribute(&span, "roomkit.span_kind"),
        Some("framework.broadcast".into())
    );
    assert_eq!(attribute(&span, "roomkit.room_id"), Some("r1".into()));
    assert_eq!(attribute(&span, "roomkit.channel_id"), Some("sms".into()));
    assert_eq!(attribute(&span, "roomkit.n"), Some(3_i64.into()));
    // What is no scalar is left out at the start and written out at the end.
    assert_eq!(attribute(&span, "roomkit.dropped"), None);
    assert_eq!(attribute(&span, "roomkit.kept"), Some("[1]".into()));
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_end_span_error
#[test]
fn a_span_ended_in_error_is_exported_with_its_message() {
    let (provider, exporter) = provider();
    let id = started(&provider, "test.err");
    let ended = EndSpanOptions {
        status: "error".to_owned(),
        error_message: Some("boom".to_owned()),
        ..EndSpanOptions::default()
    };
    provider.end_span(&id, ended);
    let span = finished(&exporter, "roomkit.test.err");
    assert_eq!(span.status, Status::error("boom"));
    assert_eq!(span.events.events.len(), 1, "the error is recorded");
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_end_span_nonexistent_is_noop
#[test]
fn ending_a_span_it_does_not_know_does_nothing() {
    provider()
        .0
        .end_span("nonexistent", EndSpanOptions::default());
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_record_metric_counter, test_record_metric_histogram
#[test]
fn a_metric_goes_to_the_meter_as_python_chooses_its_instrument() {
    let (provider, _) = provider();
    provider.record_metric("messages.count", 1.0, RecordMetricOptions::default());
    let options = RecordMetricOptions {
        unit: "ms".to_owned(),
        ..RecordMetricOptions::default()
    };
    provider.record_metric("stt.duration", 42.5, options);
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_close_ends_active_spans
#[test]
fn the_close_ends_the_spans_still_active() {
    let (provider, exporter) = provider();
    started(&provider, "active.1");
    started(&provider, "active.2");
    provider.close();
    assert_eq!(exporter.get_finished_spans().unwrap().len(), 2);
    assert_eq!(finished(&exporter, "roomkit.active.1").status, Status::Ok);
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_reset_clears_all
#[test]
fn a_reset_forgets_every_span() {
    let (provider, _) = provider();
    let id = started(&provider, "test");
    provider.end_span(&id, EndSpanOptions::default());
    assert!(provider.get_span_context(&id).is_some());
    provider.reset();
    assert!(provider.get_span_context(&id).is_none());
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_a_late_child_nests_under_its_ended_parent
#[test]
fn a_late_child_nests_under_its_ended_parent() {
    let (provider, exporter) = provider();
    let parent = started(&provider, "voice.session");
    provider.end_span(&parent, EndSpanOptions::default());
    let options = StartSpanOptions {
        parent_id: Some(parent.clone()),
        ..StartSpanOptions::default()
    };
    let child = provider.start_span(SpanKind::Custom, "after.tts", options);
    provider.end_span(&child, EndSpanOptions::default());
    let late = finished(&exporter, "roomkit.after.tts");
    let session = finished(&exporter, "roomkit.voice.session");
    assert_eq!(late.parent_span_id.to_string(), parent);
    assert_eq!(
        late.span_context.trace_id(),
        session.span_context.trace_id()
    );
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_ended_spans_kept_are_bounded
#[test]
fn the_ended_spans_kept_are_bounded() {
    let tracer_provider = SdkTracerProvider::builder().build();
    let provider = OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
        tracer_provider: Some(tracer_provider),
        ..OpenTelemetryProviderOptions::default()
    });
    let first = started(&provider, "span");
    provider.end_span(&first, EndSpanOptions::default());
    let mut last = first.clone();
    for _ in 0..ENDED_SPANS_KEPT {
        last = started(&provider, "span");
        provider.end_span(&last, EndSpanOptions::default());
    }
    assert!(provider.get_span_context(&first).is_none());
    assert!(provider.get_span_context(&last).is_some());
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_an_ended_span_is_kept_as_its_context_alone
#[test]
fn an_ended_span_is_kept_as_its_context_alone() {
    let (provider, _) = provider();
    let id = started(&provider, "big");
    provider.end_span(&id, EndSpanOptions::default());
    let kept = provider.get_span_context(&id).unwrap();
    let kept = kept.downcast_ref::<opentelemetry::Context>().unwrap();
    assert!(!kept.span().is_recording());
    assert_eq!(kept.span().span_context().span_id().to_string(), id);
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_set_attribute
#[test]
fn an_attribute_set_on_a_running_span_is_exported() {
    let (provider, exporter) = provider();
    let id = started(&provider, "test.attr");
    provider.set_attribute(&id, "custom_key", json!("custom_value"));
    provider.end_span(&id, EndSpanOptions::default());
    let span = finished(&exporter, "roomkit.test.attr");
    assert_eq!(
        attribute(&span, "roomkit.custom_key"),
        Some("custom_value".into())
    );
}

// Python: tests/test_opentelemetry.py::TestOpenTelemetryProvider::test_metadata_in_constructor
#[test]
fn the_metadata_tags_every_span() {
    let (provider, exporter) = exporting(OpenTelemetryProviderOptions {
        metadata: BTreeMap::from([("env".to_owned(), "test".to_owned())]),
        ..OpenTelemetryProviderOptions::default()
    });
    let id = started(&provider, "test.meta");
    provider.end_span(&id, EndSpanOptions::default());
    let span = finished(&exporter, "roomkit.test.meta");
    assert_eq!(attribute(&span, "roomkit.env"), Some("test".into()));
}

// Python: tests/test_opentelemetry.py::TestAcrossProcesses::test_a_span_another_provider_names_is_the_parent_of_its_spans
#[tokio::test]
async fn a_span_another_provider_names_is_the_parent_of_its_spans() {
    let (owner, exporter) = provider();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let executor: Arc<dyn TelemetryProvider> =
        Arc::new(OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
            tracer_provider: Some(tracer_provider),
            ..OpenTelemetryProviderOptions::default()
        }));
    let parent = owner.start_span(
        SpanKind::Broadcast,
        "broadcast",
        StartSpanOptions::default(),
    );
    let carrier = owner.inject_context(&parent);
    assert!(carrier.contains_key("traceparent"), "{carrier:?}");
    let context = executor.extract_context(&carrier);
    let child = {
        let executor = Arc::clone(&executor);
        let parent = parent.clone();
        super::within_extracted_span(Arc::clone(&executor), parent.clone(), context, async move {
            assert_eq!(get_current_span(), Some(parent.clone()));
            let options = StartSpanOptions {
                parent_id: Some(parent),
                ..StartSpanOptions::default()
            };
            executor.start_span(SpanKind::LlmGenerate, "generate", options)
        })
        .await
    };
    executor.end_span(&child, EndSpanOptions::default());
    owner.end_span(&parent, EndSpanOptions::default());
    let broadcast = finished(&exporter, "roomkit.broadcast");
    let generate = finished(&exporter, "roomkit.generate");
    assert_eq!(generate.parent_span_id, broadcast.span_context.span_id());
    assert_eq!(
        generate.span_context.trace_id(),
        broadcast.span_context.trace_id()
    );
}

// Python: tests/test_opentelemetry.py::TestAcrossProcesses::test_a_carrier_that_names_no_span_gives_no_context
#[test]
fn a_carrier_that_names_no_span_gives_no_context() {
    let (provider, _) = provider();
    assert!(provider.inject_context("0123456789abcdef").is_empty());
    let carrier = BTreeMap::from([("span_id".to_owned(), "0123456789abcdef".to_owned())]);
    assert!(provider.extract_context(&carrier).is_none());
    assert_ne!(SpanId::INVALID.to_string(), "0123456789abcdef");
}

/// A collector that accepts a batch once released, at most `delay` after
/// it was handed one.
#[derive(Clone, Debug)]
struct SlowCollector {
    delay: Duration,
    exported: Arc<Mutex<Vec<String>>>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl SlowCollector {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            exported: Arc::default(),
            released: Arc::default(),
        }
    }

    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }

    fn exported(&self) -> Vec<String> {
        self.exported.lock().unwrap().clone()
    }

    /// Waits at most `timeout` for a batch to have been exported.
    fn wait_exported(&self, timeout: Duration) -> Vec<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let exported = self.exported();
            if !exported.is_empty() || Instant::now() >= deadline {
                return exported;
            }
            std::thread::yield_now();
        }
    }
}

impl SpanExporter for SlowCollector {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        let released = self.released.0.lock().unwrap();
        let (_released, _) = self
            .released
            .1
            .wait_timeout_while(released, self.delay, |released| !*released)
            .unwrap();
        let mut exported = self.exported.lock().unwrap();
        exported.extend(batch.into_iter().map(|span| span.name.into_owned()));
        ready(Ok(()))
    }
}

/// A provider whose batch never leaves on its own schedule: only a flush
/// sends it.
fn slow_provider(collector: &SlowCollector, timeout: Duration) -> OpenTelemetryProvider {
    let config = BatchConfigBuilder::default()
        .with_scheduled_delay(Duration::from_secs(600))
        .build();
    let processor = BatchSpanProcessor::builder(collector.clone())
        .with_batch_config(config)
        .build();
    let tracer_provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();
    OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
        tracer_provider: Some(tracer_provider),
        shutdown_flush_timeout: timeout,
        ..OpenTelemetryProviderOptions::default()
    })
}

// Python: tests/test_opentelemetry_flush.py::test_a_flush_returns_at_once_and_the_spans_still_leave
#[test]
fn a_flush_returns_at_once_and_the_spans_still_leave() {
    let collector = SlowCollector::new(Duration::from_secs(5));
    let provider = slow_provider(&collector, Duration::from_secs(4));
    let id = provider.start_span(
        SpanKind::VoiceSession,
        "session",
        StartSpanOptions::default(),
    );
    provider.end_span(&id, EndSpanOptions::default());
    let started = Instant::now();
    provider.flush();
    // One already running: skipped, not queued behind it.
    provider.flush();
    assert!(started.elapsed() < Duration::from_millis(200));
    assert_eq!(collector.exported(), Vec::<String>::new());
    collector.release();
    assert_eq!(
        collector.wait_exported(Duration::from_secs(5)),
        vec!["roomkit.session".to_owned()]
    );
}

// Python: tests/test_opentelemetry_flush.py::test_close_waits_for_the_export_within_its_bound
#[test]
fn the_close_waits_for_the_export_within_its_bound() {
    let collector = SlowCollector::new(Duration::from_secs(5));
    let provider = slow_provider(&collector, Duration::from_millis(100));
    provider.start_span(
        SpanKind::VoiceSession,
        "still open",
        StartSpanOptions::default(),
    );
    let started = Instant::now();
    provider.close();
    assert!(started.elapsed() < Duration::from_secs(1));
    collector.release();
}

// Python: tests/test_opentelemetry_flush.py::test_close_exports_the_spans_it_ends_when_the_collector_answers
#[test]
fn the_close_exports_the_spans_it_ends_when_the_collector_answers() {
    let collector = SlowCollector::new(Duration::ZERO);
    let provider = slow_provider(&collector, Duration::from_secs(4));
    provider.start_span(
        SpanKind::VoiceSession,
        "still open",
        StartSpanOptions::default(),
    );
    provider.close();
    assert_eq!(collector.exported(), vec!["roomkit.still open".to_owned()]);
}
