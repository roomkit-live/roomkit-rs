//! The providers without a kit: Python's `tests/test_telemetry.py`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::*;
use roomkit_core::model::Metadata;

fn attributes(key: &str, value: serde_json::Value) -> Metadata {
    let mut attributes = Metadata::new();
    attributes.insert(key.to_owned(), value);
    attributes
}

fn error(message: &str) -> EndSpanOptions {
    EndSpanOptions {
        status: "error".to_owned(),
        error_message: Some(message.to_owned()),
        ..EndSpanOptions::default()
    }
}

// Python: tests/test_telemetry.py::TestSpanDataclass::test_span_defaults
#[test]
fn a_span_s_defaults_are_python_s() {
    let span = Span::default();
    assert_eq!(span.id.len(), 16);
    assert_eq!(span.kind, SpanKind::Custom);
    assert_eq!(span.name, "");
    assert_eq!(span.status, "ok");
    assert_eq!(span.parent_id, None);
    assert_eq!(span.end_time, None);
    assert_eq!(span.duration_ms(), None);
}

// Python: tests/test_telemetry.py::TestSpanDataclass::test_span_duration
#[test]
fn a_span_s_duration_is_its_end_less_its_start() {
    let start = roomkit_core::model::Timestamp::from_unix_millis(1_000).unwrap();
    let end = roomkit_core::model::Timestamp::from_unix_millis(1_250).unwrap();
    let span = Span {
        start_time: start,
        end_time: Some(end),
        ..Span::default()
    };
    assert_eq!(span.duration_ms(), Some(250.0));
}

// Python: tests/test_telemetry.py::TestAttr::test_attr_constants,
// TestPhase2SpanKinds::test_new_attr_constants, and
// TestDeliveryTelemetryPhase3::test_phase3_attr_constants
#[test]
fn an_attribute_key_is_python_s() {
    assert_eq!(Attr::STT_TEXT_LENGTH, "stt.text_length");
    assert_eq!(Attr::LLM_INPUT_TOKENS, "llm.input_tokens");
    assert_eq!(Attr::HOOK_RESULT, "hook.result");
    assert_eq!(Attr::DELIVERY_MESSAGE_ID, "delivery.message_id");
    assert_eq!(Attr::DELEGATION_CHILD_ROOM_ID, "delegation.child_room_id");
}

// Python: tests/test_telemetry.py::TestNoopProvider (test_noop_name,
// test_noop_returns_empty_string, test_noop_operations_succeed,
// test_noop_context_manager)
#[test]
fn the_noop_provider_records_nothing_and_names_each_span_empty() {
    let noop = NoopTelemetryProvider;
    assert_eq!(noop.name(), "noop");
    assert_eq!(
        noop.start_span(SpanKind::Custom, "x", StartSpanOptions::default()),
        ""
    );
    noop.end_span("", EndSpanOptions::default());
    noop.set_attribute("", "k", json!(1));
    noop.record_metric("m", 1.0, RecordMetricOptions::default());
    noop.flush();
    noop.close();
    noop.reset();
    let span = noop.span(SpanKind::Custom, "x", StartSpanOptions::default());
    assert_eq!(span.id(), "");
    span.end();
}

// Python: tests/test_telemetry.py::TestMockProvider::test_mock_name
#[test]
fn the_mock_provider_s_name() {
    assert_eq!(MockTelemetryProvider::default().name(), "mock");
}

// Python: tests/test_telemetry.py::TestMockProvider::test_start_end_span, test_end_span_with_attributes
#[test]
fn a_span_ended_is_recorded_with_its_end_s_attributes() {
    let mock = MockTelemetryProvider::default();
    let options = StartSpanOptions {
        attributes: attributes("a", json!(1)),
        ..StartSpanOptions::default()
    };
    let id = mock.start_span(SpanKind::SttTranscribe, "stt.batch", options);
    assert_eq!(mock.get_active_spans().len(), 1);
    assert_eq!(mock.spans(), vec![]);
    let ended = EndSpanOptions {
        attributes: attributes("b", json!(2)),
        ..EndSpanOptions::default()
    };
    mock.end_span(&id, ended);
    let [span] = mock.spans().try_into().unwrap();
    assert_eq!(span.id, id);
    assert_eq!(span.name, "stt.batch");
    assert_eq!(span.status, "ok");
    assert!(span.end_time.is_some());
    assert_eq!(span.attributes["a"], json!(1));
    assert_eq!(span.attributes["b"], json!(2));
    assert_eq!(mock.get_active_spans(), vec![]);
}

// Python: tests/test_telemetry.py::TestMockProvider::test_end_span_error
#[test]
fn a_span_ended_in_error_says_what_failed() {
    let mock = MockTelemetryProvider::default();
    let id = mock.start_span(SpanKind::Custom, "x", StartSpanOptions::default());
    mock.end_span(&id, error("boom"));
    let [span] = mock.completed_spans().try_into().unwrap();
    assert_eq!(span.status, "error");
    assert_eq!(span.error_message.as_deref(), Some("boom"));
}

// Python: tests/test_telemetry.py::TestMockProvider::test_set_attribute
#[test]
fn an_attribute_set_on_a_running_span_is_kept() {
    let mock = MockTelemetryProvider::default();
    let id = mock.start_span(SpanKind::Custom, "x", StartSpanOptions::default());
    mock.set_attribute(&id, "k", json!("v"));
    mock.end_span(&id, EndSpanOptions::default());
    assert_eq!(mock.spans()[0].attributes["k"], json!("v"));
}

// Python: tests/test_telemetry.py::TestMockProvider::test_record_metric
#[test]
fn a_metric_is_recorded_with_its_unit_and_attributes() {
    let mock = MockTelemetryProvider::default();
    let options = RecordMetricOptions {
        unit: "ms".to_owned(),
        attributes: attributes("p", json!("x")),
    };
    mock.record_metric("stt.ttfb_ms", 12.5, options);
    let [(name, value, unit, recorded)] = mock.metrics().try_into().unwrap();
    assert_eq!(
        (name.as_str(), value, unit.as_str()),
        ("stt.ttfb_ms", 12.5, "ms")
    );
    assert_eq!(recorded["p"], json!("x"));
}

// Python: tests/test_telemetry.py::TestMockProvider::test_get_spans_by_kind
#[test]
fn spans_are_read_by_kind() {
    let mock = MockTelemetryProvider::default();
    for (kind, name) in [(SpanKind::Custom, "a"), (SpanKind::HookSync, "b")] {
        let id = mock.start_span(kind, name, StartSpanOptions::default());
        mock.end_span(&id, EndSpanOptions::default());
    }
    let [span] = mock.get_spans(SpanKind::HookSync).try_into().unwrap();
    assert_eq!(span.name, "b");
}

// Python: tests/test_telemetry.py::TestMockProvider::test_context_manager
#[test]
fn the_guard_ends_its_span_ok() {
    let mock = MockTelemetryProvider::default();
    let span = mock.span(SpanKind::Custom, "x", StartSpanOptions::default());
    let id = span.id().to_owned();
    assert_eq!(mock.spans(), vec![]);
    span.end();
    assert_eq!(mock.spans()[0].id, id);
    assert_eq!(mock.spans()[0].status, "ok");
}

// Python: tests/test_telemetry.py::TestMockProvider::test_context_manager_error
#[test]
fn the_guard_of_a_failure_ends_its_span_in_error() {
    let mock = MockTelemetryProvider::default();
    mock.span(SpanKind::Custom, "x", StartSpanOptions::default())
        .fail("boom");
    assert_eq!(mock.spans()[0].status, "error");
    assert_eq!(mock.spans()[0].error_message.as_deref(), Some("boom"));
}

/// Rust's form of Python's block that raises: a guard dropped while the
/// thread panics ends in error, one dropped otherwise ends ok.
#[test]
fn a_guard_dropped_ends_its_span_by_how_the_thread_goes() {
    let mock = Arc::new(MockTelemetryProvider::default());
    drop(mock.span(SpanKind::Custom, "kept", StartSpanOptions::default()));
    let held = Arc::clone(&mock);
    let panicked = std::thread::spawn(move || {
        let _span = held.span(SpanKind::Custom, "panicked", StartSpanOptions::default());
        panic!("boom");
    })
    .join();
    assert!(panicked.is_err());
    let spans = mock.spans();
    assert_eq!(
        (spans[0].name.as_str(), spans[0].status.as_str()),
        ("kept", "ok")
    );
    assert_eq!(
        (spans[1].name.as_str(), spans[1].status.as_str()),
        ("panicked", "error")
    );
}

/// A guard crosses an `.await`, as Python's `with` in a coroutine.
#[tokio::test]
async fn a_guard_is_held_across_an_await_in_a_task() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let held = Arc::clone(&mock);
    tokio::spawn(async move {
        let span = held.span(SpanKind::Custom, "x", StartSpanOptions::default());
        let borrowed = &span;
        tokio::task::yield_now().await;
        assert_eq!(borrowed.id().len(), 16);
        span.end();
    })
    .await
    .unwrap();
    assert_eq!(mock.spans()[0].name, "x");
}

/// A provider held as an object starts a guard too.
#[test]
fn a_provider_held_as_an_object_starts_a_guard() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let cloned = Arc::clone(&mock);
    let held: Arc<dyn TelemetryProvider> = cloned;
    held.span(SpanKind::Custom, "x", StartSpanOptions::default())
        .end();
    assert_eq!(mock.spans()[0].name, "x");
}

// Python: tests/test_telemetry.py::TestMockProvider::test_span_with_parent, test_span_with_room_session_channel
#[test]
fn a_span_keeps_its_parent_and_what_it_ran_for() {
    let mock = MockTelemetryProvider::default();
    let options = StartSpanOptions {
        parent_id: Some("p".to_owned()),
        room_id: Some("r".to_owned()),
        session_id: Some("s".to_owned()),
        channel_id: Some("c".to_owned()),
        ..StartSpanOptions::default()
    };
    let id = mock.start_span(SpanKind::Custom, "x", options);
    mock.end_span(&id, EndSpanOptions::default());
    let span = &mock.spans()[0];
    assert_eq!(span.parent_id.as_deref(), Some("p"));
    assert_eq!(span.room_id.as_deref(), Some("r"));
    assert_eq!(span.session_id.as_deref(), Some("s"));
    assert_eq!(span.channel_id.as_deref(), Some("c"));
}

// Python: tests/test_telemetry.py::TestMockProvider::test_reset_clears_all
#[test]
fn a_reset_forgets_everything() {
    let mock = MockTelemetryProvider::default();
    let id = mock.start_span(SpanKind::Custom, "x", StartSpanOptions::default());
    mock.end_span(&id, EndSpanOptions::default());
    mock.start_span(SpanKind::Custom, "y", StartSpanOptions::default());
    mock.record_metric("m", 1.0, RecordMetricOptions::default());
    mock.reset();
    assert_eq!(mock.spans(), vec![]);
    assert_eq!(mock.get_active_spans(), vec![]);
    assert_eq!(mock.metrics(), vec![]);
}

// Python: tests/test_telemetry.py::TestMockProvider::test_end_nonexistent_span
#[test]
fn ending_a_span_it_does_not_know_does_nothing() {
    let mock = MockTelemetryProvider::default();
    mock.end_span("nope", EndSpanOptions::default());
    assert_eq!(mock.spans(), vec![]);
}

/// A subscriber keeping every event's message while it is the thread's
/// default, the test's `caplog`.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for Logs {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Message<'a>(&'a mut String);
        impl tracing::field::Visit for Message<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    *self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = String::new();
        event.record(&mut Message(&mut message));
        self.0.lock().unwrap().push(message);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// What `work` logged on `console`.
fn logged(work: impl FnOnce(&ConsoleTelemetryProvider)) -> String {
    let logs = Logs::default();
    let console = ConsoleTelemetryProvider::default();
    tracing::subscriber::with_default(logs.clone(), || work(&console));
    logs.0.lock().unwrap().join("\n")
}

// Python: tests/test_telemetry.py::TestConsoleProvider::test_console_name
#[test]
fn the_console_provider_s_name() {
    assert_eq!(ConsoleTelemetryProvider::default().name(), "console");
}

// Python: tests/test_telemetry.py::TestConsoleProvider::test_console_logs_spans
#[test]
fn the_console_logs_a_span_s_start_and_end() {
    let text = logged(|console| {
        let id = console.start_span(
            SpanKind::SttTranscribe,
            "stt.batch",
            StartSpanOptions::default(),
        );
        let ended = EndSpanOptions {
            attributes: attributes(Attr::STT_TEXT_LENGTH, json!(42)),
            ..EndSpanOptions::default()
        };
        console.end_span(&id, ended);
    });
    assert!(text.contains("SPAN START"), "{text}");
    assert!(text.contains("SPAN END"), "{text}");
    assert!(text.contains("stt.batch"), "{text}");
    assert!(text.contains("stt.text_length=42"), "{text}");
}

// Python: tests/test_telemetry.py::TestConsoleProvider::test_console_logs_errors
#[test]
fn the_console_logs_a_span_s_error() {
    let text = logged(|console| {
        let id = console.start_span(SpanKind::Custom, "fail", StartSpanOptions::default());
        console.end_span(&id, error("boom"));
    });
    assert!(text.contains("SPAN ERROR"), "{text}");
    assert!(text.contains("boom"), "{text}");
}

// Python: tests/test_telemetry.py::TestConsoleProvider::test_console_logs_metrics
#[test]
fn the_console_logs_a_metric() {
    let text = logged(|console| {
        let options = RecordMetricOptions {
            unit: "ms".to_owned(),
            ..RecordMetricOptions::default()
        };
        console.record_metric("test.metric", 42.0, options);
    });
    assert!(text.contains("METRIC"), "{text}");
    assert!(text.contains("42.00"), "{text}");
}

// Python: tests/test_telemetry.py::TestConsoleProvider::test_console_writes_out_what_could_forge_a_log_line
#[test]
fn the_console_writes_out_what_could_forge_a_log_line() {
    let forged = "x\n[SPAN END] custom forged";
    let logs = Logs::default();
    let console = ConsoleTelemetryProvider::default();
    tracing::subscriber::with_default(logs.clone(), || {
        let options = StartSpanOptions {
            parent_id: Some(forged.to_owned()),
            ..StartSpanOptions::default()
        };
        let id = console.start_span(SpanKind::Custom, forged, options);
        let ended = EndSpanOptions {
            attributes: attributes(forged, json!(forged)),
            ..EndSpanOptions::default()
        };
        console.end_span(&id, ended);
        let options = StartSpanOptions {
            attributes: attributes("a", json!("b, c=d")),
            ..StartSpanOptions::default()
        };
        let id = console.start_span(SpanKind::Custom, "fail", options);
        console.end_span(&id, error(forged));
        let metric = RecordMetricOptions {
            unit: forged.to_owned(),
            attributes: attributes(forged, json!(forged)),
        };
        console.record_metric(forged, 1.0, metric);
    });
    let lines = logs.0.lock().unwrap().clone();
    assert_eq!(lines.len(), 5, "{lines:?}");
    assert!(lines.iter().all(|line| !line.contains('\n')), "{lines:?}");
    assert!(lines[3].contains(r#"a="b, c=d""#), "{lines:?}");
    assert!(lines[3].contains(r#"error="x\n[SPAN END]"#), "{lines:?}");
}

// Python: tests/test_telemetry.py::TestConsoleProvider::test_console_close_warning
#[test]
fn the_console_warns_of_the_spans_open_at_its_close() {
    let text = logged(|console| {
        console.start_span(SpanKind::Custom, "leak", StartSpanOptions::default());
        console.close();
    });
    assert!(text.contains("active spans"), "{text}");
}

// Python: tests/test_telemetry.py::TestTelemetryConfig::test_default_config, test_config_with_provider,
// and TestTelemetryPhase4::test_default_suppression_includes_audio_levels
#[test]
fn a_config_s_defaults_are_python_s() {
    let config = TelemetryConfig::default();
    assert!(config.provider.is_none());
    assert_eq!(config.sample_rate, 1.0);
    assert!(config.enabled_spans.is_none());
    assert!(!config.record_sensitive_data);
    assert!(
        config
            .suppressed_hook_triggers
            .contains("on_vad_audio_level")
    );
    let mock = Arc::new(MockTelemetryProvider::default());
    let config = TelemetryConfig::from(Arc::clone(&mock));
    assert_eq!(config.provider.unwrap().name(), "mock");
}

/// The telemetry of a kit reporting to a mock.
fn kit_telemetry() -> KitTelemetry {
    KitTelemetry {
        provider: Arc::new(MockTelemetryProvider::default()),
        sensitive: false,
        suppressed: Arc::default(),
    }
}

// Python: tests/test_turns_wiring.py::test_a_turn_in_a_context_of_its_own_continues_the_owner_s_span
#[tokio::test]
async fn a_turn_s_work_in_a_context_of_its_own_continues_its_request_s_span() {
    let trace = BTreeMap::from([("span_id".to_owned(), "b1".to_owned())]);
    let read = within_turn_trace(Some(kit_telemetry()), Some(&trace), async {
        get_current_span()
    });
    assert_eq!(read.await.as_deref(), Some("b1"));
}

// Python: turns/_executor_turn.py `_run_work`: a request that names no span
// leaves its work where it stands, as a context under it already.
#[tokio::test]
async fn a_turn_s_work_under_its_request_s_span_or_with_none_stays_where_it_is() {
    let telemetry = kit_telemetry();
    let under = BTreeMap::from([("span_id".to_owned(), "b1".to_owned())]);
    let blank = BTreeMap::from([("span_id".to_owned(), String::new())]);
    let read = within_kit(&telemetry, Some("b1".to_owned()), {
        let telemetry = telemetry.clone();
        async move {
            let under = within_turn_trace(Some(telemetry.clone()), Some(&under), async {
                get_current_span()
            })
            .await;
            let blank = within_turn_trace(Some(telemetry.clone()), Some(&blank), async {
                get_current_span()
            })
            .await;
            let none = within_turn_trace(Some(telemetry), None, async { get_current_span() }).await;
            (under, blank, none)
        }
    });
    let b1 = Some("b1".to_owned());
    assert_eq!(read.await, (b1.clone(), b1.clone(), b1));
}

// Python: turns/owner.py `_trace`: under no span, or the no-op provider's,
// a request names none.
#[tokio::test]
async fn a_turn_request_under_no_span_names_none() {
    assert_eq!(current_trace(), None);
    let telemetry = kit_telemetry();
    let blank = within_kit(&telemetry, Some(String::new()), async { current_trace() });
    assert_eq!(blank.await, None);
}
