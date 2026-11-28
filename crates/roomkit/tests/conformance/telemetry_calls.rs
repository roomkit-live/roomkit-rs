//! `telemetry.spans` and `telemetry.reset` (roomkit-specs
//! `conformance/README.md`, "Calls"): the completed spans of a kind at a
//! site, those the call selects, in the order they ended, each with its
//! members, or with `distinct` the members it lists, a span equal to one
//! before it left out; and the site's recorder emptied.

use std::sync::Arc;

use roomkit::model::Metadata;
use roomkit::telemetry::{Span, SpanKind, TelemetryProvider as _};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, optional};
use crate::run::Run;

/// The members a span may be answered with.
const MEMBERS: &[&str] = &[
    "name",
    "span_id",
    "parent_id",
    "room_id",
    "channel_id",
    "status",
    "attributes",
    "error_message",
];

/// The spans `spec` selects at its site.
pub(crate) fn spans(run: &Arc<Run>, spec: &Value) -> Result<Called, Unreadable> {
    let site: String = field(spec, "site")?;
    let kind: SpanKind = field(spec, "kind")?;
    let attributes: Metadata = optional(spec, "attributes")?.unwrap_or_default();
    let name: Option<String> = optional(spec, "name")?;
    let distinct: Option<Vec<String>> = optional(spec, "distinct")?;
    if let Some(unknown) = distinct
        .iter()
        .flatten()
        .find(|member| !MEMBERS.contains(&member.as_str()))
    {
        return Err(format!("telemetry.spans: no span member {unknown}"));
    }
    let recorders = crate::run::lock(&run.telemetry);
    let recorder = recorders
        .get(&site)
        .ok_or_else(|| format!("no telemetry at {site}"))?;
    let selected = recorder.get_spans(kind).into_iter().filter(|span| {
        name.as_ref().is_none_or(|name| &span.name == name)
            && attributes
                .iter()
                .all(|(key, value)| span.attributes.get(key) == Some(value))
    });
    let mut answered: Vec<Value> = Vec::new();
    for span in selected {
        let record = record(&span);
        let kept = match &distinct {
            None => record,
            Some(members) => members
                .iter()
                .map(|member| (member.clone(), record[member.as_str()].clone()))
                .collect::<serde_json::Map<_, _>>()
                .into(),
        };
        if distinct.is_none() || !answered.contains(&kept) {
            answered.push(kept);
        }
    }
    Ok(Ok(Value::Array(answered)))
}

/// The site's recorder forgets the spans it recorded (README,
/// `telemetry.reset`).
pub(crate) fn reset(run: &Arc<Run>, spec: &Value) -> Result<Called, Unreadable> {
    let site: String = field(spec, "site")?;
    let recorders = crate::run::lock(&run.telemetry);
    let recorder = recorders
        .get(&site)
        .ok_or_else(|| format!("no telemetry at {site}"))?;
    recorder.reset();
    Ok(Ok(Value::Null))
}

fn record(span: &Span) -> Value {
    json!({
        "name": span.name,
        "span_id": span.id,
        "parent_id": span.parent_id,
        "room_id": span.room_id,
        "channel_id": span.channel_id,
        "status": span.status,
        "attributes": span.attributes,
        "error_message": span.error_message,
    })
}

/// The kit's recording provider with `injects_context` (README, "Setup"):
/// it records as the mock does, and names the span it is handed to another
/// process, each of `traceparent`, `tracestate`, `baggage` and `span` mapped
/// to the span's id.
#[derive(Debug)]
pub(crate) struct InjectingTelemetry(pub(crate) Arc<roomkit::telemetry::MockTelemetryProvider>);

impl roomkit::telemetry::TelemetryProvider for InjectingTelemetry {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn start_span(
        &self,
        kind: SpanKind,
        name: &str,
        options: roomkit::telemetry::StartSpanOptions,
    ) -> String {
        self.0.start_span(kind, name, options)
    }

    fn end_span(&self, span_id: &str, options: roomkit::telemetry::EndSpanOptions) {
        self.0.end_span(span_id, options);
    }

    fn set_attribute(&self, span_id: &str, key: &str, value: Value) {
        self.0.set_attribute(span_id, key, value);
    }

    fn record_metric(
        &self,
        name: &str,
        value: f64,
        options: roomkit::telemetry::RecordMetricOptions,
    ) {
        self.0.record_metric(name, value, options);
    }

    fn inject_context(&self, span_id: &str) -> std::collections::BTreeMap<String, String> {
        ["traceparent", "tracestate", "baggage", "span"]
            .into_iter()
            .map(|key| (key.to_owned(), span_id.to_owned()))
            .collect()
    }

    fn reset(&self) {
        self.0.reset();
    }
}
