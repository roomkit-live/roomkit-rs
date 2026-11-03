//! Python's `MockTelemetryProvider`: the spans and the metrics, recorded.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::Metadata;
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::Value;

use super::{
    EndSpanOptions, RecordMetricOptions, Span, SpanKind, StartSpanOptions, TelemetryProvider,
};

/// A metric [`MockTelemetryProvider`] recorded: its name, value, unit and
/// attributes, as Python's dict of those four keys.
type Metric = (String, f64, String, Metadata);

/// Python `MockTelemetryProvider`: records every span and metric for a
/// test to read.
///
/// ```
/// use std::sync::Arc;
/// use roomkit::telemetry::{MockTelemetryProvider, SpanKind, TelemetryProvider};
///
/// let telemetry = Arc::new(MockTelemetryProvider::default());
/// let span = telemetry.span(SpanKind::Custom, "work", Default::default());
/// span.end();
/// assert_eq!(telemetry.get_spans(SpanKind::Custom)[0].name, "work");
/// ```
#[derive(Debug, Default)]
pub struct MockTelemetryProvider {
    recorded: Mutex<Recorded>,
}

#[derive(Debug, Default)]
struct Recorded {
    active: HashMap<String, Span>,
    completed: Vec<Span>,
    metrics: Vec<Metric>,
}

impl MockTelemetryProvider {
    fn recorded(&self) -> MutexGuard<'_, Recorded> {
        self.recorded.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Python `spans`: the spans ended, in the order they ended.
    #[must_use]
    pub fn spans(&self) -> Vec<Span> {
        self.completed_spans()
    }

    /// Python's attribute `completed_spans`: the spans ended, in the order
    /// they ended.
    #[must_use]
    pub fn completed_spans(&self) -> Vec<Span> {
        self.recorded().completed.clone()
    }

    /// Python's attribute `metrics`: each metric recorded, its name, value,
    /// unit and attributes.
    #[must_use]
    pub fn metrics(&self) -> Vec<(String, f64, String, Metadata)> {
        self.recorded().metrics.clone()
    }

    /// The spans of `kind` ended.
    #[must_use]
    pub fn get_spans(&self, kind: SpanKind) -> Vec<Span> {
        let recorded = self.recorded();
        recorded
            .completed
            .iter()
            .filter(|span| span.kind == kind)
            .cloned()
            .collect()
    }

    /// The spans started and not ended.
    #[must_use]
    pub fn get_active_spans(&self) -> Vec<Span> {
        self.recorded().active.values().cloned().collect()
    }
}

impl TelemetryProvider for MockTelemetryProvider {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn start_span(&self, kind: SpanKind, name: &str, options: StartSpanOptions) -> String {
        let span = Span {
            kind,
            name: name.to_owned(),
            parent_id: options.parent_id,
            attributes: options.attributes,
            room_id: options.room_id,
            session_id: options.session_id,
            channel_id: options.channel_id,
            ..Span::default()
        };
        let id = span.id.clone();
        self.recorded().active.insert(id.clone(), span);
        id
    }

    fn end_span(&self, span_id: &str, options: EndSpanOptions) {
        let mut recorded = self.recorded();
        let Some(mut span) = recorded.active.remove(span_id) else {
            return;
        };
        span.end_time = Some(SystemEnvironment.now());
        span.status = options.status;
        span.error_message = options.error_message;
        span.attributes.extend(options.attributes);
        recorded.completed.push(span);
    }

    fn set_attribute(&self, span_id: &str, key: &str, value: Value) {
        if let Some(span) = self.recorded().active.get_mut(span_id) {
            span.attributes.insert(key.to_owned(), value);
        }
    }

    fn record_metric(&self, name: &str, value: f64, options: RecordMetricOptions) {
        let metric = (name.to_owned(), value, options.unit, options.attributes);
        self.recorded().metrics.push(metric);
    }

    fn reset(&self) {
        *self.recorded() = Recorded::default();
    }
}
