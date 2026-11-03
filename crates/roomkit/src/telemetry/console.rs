//! Python's `ConsoleTelemetryProvider`: spans and metrics as log lines.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::Metadata;
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::Value;
use tracing::Level;

use super::{
    EndSpanOptions, RecordMetricOptions, Span, SpanKind, StartSpanOptions, TelemetryProvider,
};

/// The options of [`ConsoleTelemetryProvider::new`], Python's keywords.
#[derive(Clone, Copy, Debug)]
pub struct ConsoleTelemetryProviderOptions {
    /// The level of its lines, `INFO` by default.
    pub level: Level,
}

impl Default for ConsoleTelemetryProviderOptions {
    fn default() -> Self {
        Self { level: Level::INFO }
    }
}

/// Python `ConsoleTelemetryProvider`: logs each span's start and end and
/// each metric through `tracing`, target `roomkit::telemetry`
/// (`[SPAN START]`, `[SPAN END]`, `[SPAN ERROR]`, `[METRIC]`).
#[derive(Debug)]
pub struct ConsoleTelemetryProvider {
    level: Level,
    spans: Mutex<HashMap<String, Span>>,
}

impl ConsoleTelemetryProvider {
    /// A provider logging at `options.level`.
    #[must_use]
    pub fn new(options: ConsoleTelemetryProviderOptions) -> Self {
        Self {
            level: options.level,
            spans: Mutex::default(),
        }
    }

    fn spans(&self) -> MutexGuard<'_, HashMap<String, Span>> {
        self.spans.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn log(&self, line: &str) {
        match self.level {
            Level::ERROR => tracing::error!(target: "roomkit::telemetry", "{line}"),
            Level::WARN => tracing::warn!(target: "roomkit::telemetry", "{line}"),
            Level::INFO => tracing::info!(target: "roomkit::telemetry", "{line}"),
            Level::DEBUG => tracing::debug!(target: "roomkit::telemetry", "{line}"),
            Level::TRACE => tracing::trace!(target: "roomkit::telemetry", "{line}"),
        }
    }
}

impl Default for ConsoleTelemetryProvider {
    fn default() -> Self {
        Self::new(ConsoleTelemetryProviderOptions::default())
    }
}

/// ` [k=v, k=v]`, or nothing for no attribute: each value its JSON, which
/// quotes and escapes a text, so that a value holding a delimiter or a line
/// break forges no attribute and no log line, as Python's `repr`.
fn listed(attributes: &Metadata) -> String {
    if attributes.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = attributes
        .iter()
        .map(|(key, value)| format!("{}={value}", key.escape_debug()))
        .collect();
    format!(" [{}]", parts.join(", "))
}

impl TelemetryProvider for ConsoleTelemetryProvider {
    fn name(&self) -> &'static str {
        "console"
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
        let mut line = format!(
            "[SPAN START] {kind} {} (id={}",
            name.escape_debug(),
            span.id
        );
        if let Some(parent) = &span.parent_id {
            let _ = write!(line, ", parent={}", parent.escape_debug());
        }
        line.push(')');
        let id = span.id.clone();
        self.spans().insert(id.clone(), span);
        self.log(&line);
        id
    }

    fn end_span(&self, span_id: &str, options: EndSpanOptions) {
        let Some(mut span) = self.spans().remove(span_id) else {
            return;
        };
        span.end_time = Some(SystemEnvironment.now());
        span.attributes.extend(options.attributes);
        let duration = span
            .duration_ms()
            .map(|ms| format!(" {ms:.1}ms"))
            .unwrap_or_default();
        let attributes = listed(&span.attributes);
        let name = span.name.escape_debug();
        let line = if options.status == "error" {
            let error = options.error_message.as_deref().unwrap_or("unknown");
            format!(
                "[SPAN ERROR] {} {name}{duration}{attributes} error={error:?}",
                span.kind
            )
        } else {
            format!("[SPAN END] {} {name}{duration}{attributes}", span.kind)
        };
        self.log(&line);
    }

    fn set_attribute(&self, span_id: &str, key: &str, value: Value) {
        if let Some(span) = self.spans().get_mut(span_id) {
            span.attributes.insert(key.to_owned(), value);
        }
    }

    fn record_metric(&self, name: &str, value: f64, options: RecordMetricOptions) {
        let unit = if options.unit.is_empty() {
            String::new()
        } else {
            format!(" {}", options.unit.escape_debug())
        };
        let attributes = listed(&options.attributes);
        let name = name.escape_debug();
        self.log(&format!("[METRIC] {name} = {value:.2}{unit}{attributes}"));
    }

    fn close(&self) {
        let mut spans = self.spans();
        if !spans.is_empty() {
            tracing::warn!(target: "roomkit::telemetry", active = spans.len(), "ConsoleTelemetryProvider closed with {} active spans", spans.len());
        }
        spans.clear();
    }

    fn reset(&self) {
        self.spans().clear();
    }
}
