//! Python's `NoopTelemetryProvider`, the kit's default.

use serde_json::Value;

use super::{EndSpanOptions, RecordMetricOptions, SpanKind, StartSpanOptions, TelemetryProvider};

/// Python `NoopTelemetryProvider`: records nothing, the kit's provider when
/// none is given. Every span's id is empty.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTelemetryProvider;

impl TelemetryProvider for NoopTelemetryProvider {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn start_span(&self, _kind: SpanKind, _name: &str, _options: StartSpanOptions) -> String {
        String::new()
    }

    fn end_span(&self, _span_id: &str, _options: EndSpanOptions) {}

    fn set_attribute(&self, _span_id: &str, _key: &str, _value: Value) {}

    fn record_metric(&self, _name: &str, _value: f64, _options: RecordMetricOptions) {}
}
