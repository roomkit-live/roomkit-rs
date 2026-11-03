//! Python's `OpenTelemetryProvider`, behind the cargo feature
//! `opentelemetry` (HITL 2796129a): the kit's spans and metrics handed to
//! an OpenTelemetry SDK tracer and meter provider.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use opentelemetry::trace::{Status, TraceContextExt, Tracer as _, TracerProvider as _};
use opentelemetry::{Context, InstrumentationScope, KeyValue, global};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use serde_json::Value;

use super::context::current_span_context;
use super::{EndSpanOptions, RecordMetricOptions, SpanKind, StartSpanOptions, TelemetryProvider};

/// How many ended spans a provider keeps as parents for late children, the
/// latest to end: a context each, as Python's `_ENDED_SPANS_KEPT`.
pub(super) const ENDED_SPANS_KEPT: usize = 10_000;

/// The options of [`OpenTelemetryProvider::new`], Python's keywords.
#[derive(Clone, Debug)]
pub struct OpenTelemetryProviderOptions {
    /// The SDK tracer provider the spans go to; the global tracer when
    /// `None`, and then the kit's close exports nothing itself.
    pub tracer_provider: Option<SdkTracerProvider>,
    /// The SDK meter provider the metrics go to; the global meter when
    /// `None`.
    pub meter_provider: Option<SdkMeterProvider>,
    /// The tracer's and meter's name, `roomkit` by default.
    pub service_name: String,
    /// Tags every span carries, each as `roomkit.<key>`.
    pub metadata: BTreeMap<String, String>,
    /// How long [`close`](TelemetryProvider::close) waits for the export,
    /// 4 seconds by default.
    pub shutdown_flush_timeout: Duration,
}

impl Default for OpenTelemetryProviderOptions {
    fn default() -> Self {
        Self {
            tracer_provider: None,
            meter_provider: None,
            service_name: "roomkit".to_owned(),
            metadata: BTreeMap::new(),
            shutdown_flush_timeout: Duration::from_secs(4),
        }
    }
}

/// Where the spans start: the SDK provider's tracer, or the global one.
enum Tracer {
    Sdk(SdkTracer),
    Global(global::BoxedTracer),
}

impl Tracer {
    /// A span named `name` started under `parent`, as a context holding it.
    fn start(&self, name: String, parent: &Context, attributes: Vec<KeyValue>) -> Context {
        match self {
            Self::Sdk(tracer) => {
                let span = tracer
                    .span_builder(name)
                    .with_attributes(attributes)
                    .start_with_context(tracer, parent);
                parent.with_span(span)
            }
            Self::Global(tracer) => {
                let span = tracer
                    .span_builder(name)
                    .with_attributes(attributes)
                    .start_with_context(tracer, parent);
                parent.with_span(span)
            }
        }
    }
}

/// A running span: its context, and what it was written so far, for an
/// attribute written again with its value not to show twice: the SDK adds, where
/// Python's overwrites.
struct Active {
    context: Context,
    written: HashMap<String, opentelemetry::Value>,
}

impl Active {
    /// Writes `key`, `value` unless it holds it already.
    fn write(&mut self, key: &str, value: opentelemetry::Value) {
        let key = format!("roomkit.{key}");
        if self.written.get(&key) != Some(&value) {
            self.context
                .span()
                .set_attribute(KeyValue::new(key.clone(), value.clone()));
            self.written.insert(key, value);
        }
    }
}

/// The spans a provider holds: those running, and the latest ended, as
/// their contexts alone.
#[derive(Default)]
struct Spans {
    active: HashMap<String, Active>,
    ended: HashMap<String, Context>,
    ended_order: VecDeque<String>,
}

impl Spans {
    fn parent(&self, span_id: &str) -> Option<Context> {
        self.active
            .get(span_id)
            .map(|active| &active.context)
            .or_else(|| self.ended.get(span_id))
            .cloned()
    }

    /// Keeps ended `span_id`'s context for its late children, forgetting
    /// the oldest kept past [`ENDED_SPANS_KEPT`]: the context alone, never
    /// the span and its attributes.
    fn keep_ended(&mut self, span_id: String, context: &Context) {
        let span_context = context.span().span_context().clone();
        let kept = Context::new().with_remote_span_context(span_context);
        if self.ended.insert(span_id.clone(), kept).is_none() {
            self.ended_order.push_back(span_id);
        }
        while self.ended_order.len() > ENDED_SPANS_KEPT {
            if let Some(oldest) = self.ended_order.pop_front() {
                self.ended.remove(&oldest);
            }
        }
    }
}

/// Python `OpenTelemetryProvider`: each span of the kit an OpenTelemetry
/// span named `roomkit.<name>`, its attributes `roomkit.<key>`, its kind
/// `roomkit.span_kind`; a metric whose name holds `duration` or `ttfb` a
/// histogram, any other a counter.
///
/// ```
/// use std::sync::Arc;
/// use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
/// use roomkit::telemetry::{OpenTelemetryProvider, OpenTelemetryProviderOptions};
/// use roomkit::{RoomKit, RoomKitOptions};
///
/// let exporter = InMemorySpanExporter::default();
/// let provider = SdkTracerProvider::builder()
///     .with_simple_exporter(exporter.clone())
///     .build();
/// let telemetry = OpenTelemetryProvider::new(OpenTelemetryProviderOptions {
///     tracer_provider: Some(provider),
///     ..Default::default()
/// });
/// let kit = RoomKit::new(RoomKitOptions {
///     telemetry: Some(Arc::new(telemetry).into()),
///     ..Default::default()
/// });
/// # drop(kit);
/// ```
///
/// Exporting never runs on the kit's tasks: [`flush`](TelemetryProvider::flush)
/// hands the SDK's `force_flush` to a thread and returns at once, and
/// [`close`](TelemetryProvider::close) waits for it at most
/// `shutdown_flush_timeout`, then gives up the spans not yet out.
pub struct OpenTelemetryProvider {
    tracer: Tracer,
    tracer_provider: Option<SdkTracerProvider>,
    meter: Meter,
    metadata: Mutex<BTreeMap<String, String>>,
    shutdown_flush_timeout: Duration,
    spans: Mutex<Spans>,
    /// Set while a flush thread runs: a flush asked meanwhile is skipped,
    /// the running one exports what it asked for and more.
    flushing: Arc<AtomicBool>,
}

impl OpenTelemetryProvider {
    /// A provider exporting through `options.tracer_provider`.
    #[must_use]
    pub fn new(options: OpenTelemetryProviderOptions) -> Self {
        let scope = InstrumentationScope::builder(options.service_name).build();
        let tracer = match &options.tracer_provider {
            Some(provider) => Tracer::Sdk(provider.tracer_with_scope(scope.clone())),
            None => Tracer::Global(global::tracer_with_scope(scope.clone())),
        };
        let meter = match &options.meter_provider {
            Some(provider) => provider.meter_with_scope(scope),
            None => global::meter_with_scope(scope),
        };
        Self {
            tracer,
            tracer_provider: options.tracer_provider,
            meter,
            metadata: Mutex::new(options.metadata),
            shutdown_flush_timeout: options.shutdown_flush_timeout,
            spans: Mutex::default(),
            flushing: Arc::default(),
        }
    }

    fn spans(&self) -> MutexGuard<'_, Spans> {
        self.spans.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The context a span named `name` starts under, as a child of
    /// `parent_id`: the context the caller carries when its span is that
    /// parent, else the parent this provider started, else the carried
    /// context, the span then staying in its trace under an ancestor.
    fn parent_context(&self, parent_id: &str, name: &str) -> Context {
        let carried =
            current_span_context().and_then(|context| context.downcast_ref::<Context>().cloned());
        if let Some(context) = &carried
            && span_id_of(context) == parent_id
        {
            return context.clone();
        }
        let spans = self.spans();
        if let Some(parent) = spans.parent(parent_id) {
            return parent;
        }
        if carried.is_none() {
            tracing::warn!(target: "roomkit::telemetry::otel", parent_id, name, active = spans.active.len(), ended = spans.ended.len(), "parent span not found");
        }
        drop(spans);
        carried.unwrap_or_else(Context::current)
    }

    /// What a span named by `options` starts with, each key once, the last
    /// writing it, as Python's dict.
    fn attributes(
        &self,
        kind: SpanKind,
        options: &StartSpanOptions,
    ) -> HashMap<String, opentelemetry::Value> {
        let mut attributes = HashMap::new();
        attributes.insert("roomkit.span_kind".to_owned(), kind.as_str().into());
        let metadata = self.metadata.lock().unwrap_or_else(PoisonError::into_inner);
        for (key, value) in metadata.iter() {
            attributes.insert(format!("roomkit.{key}"), value.clone().into());
        }
        drop(metadata);
        for (key, value) in [
            ("room_id", &options.room_id),
            ("session_id", &options.session_id),
            ("channel_id", &options.channel_id),
        ] {
            if let Some(value) = value.as_ref().filter(|value| !value.is_empty()) {
                attributes.insert(format!("roomkit.{key}"), value.clone().into());
            }
        }
        for (key, value) in &options.attributes {
            if let Some(value) = scalar(value) {
                attributes.insert(format!("roomkit.{key}"), value);
            }
        }
        attributes
    }

    fn force_flush(provider: &SdkTracerProvider) {
        if let Err(error) = provider.force_flush() {
            tracing::debug!(target: "roomkit::telemetry::otel", %error, "flushing the OpenTelemetry tracer provider failed");
        }
    }
}

/// What writes a trace's fields when the application set no propagator
/// (OpenTelemetry's for Rust default to none): W3C trace context and
/// baggage, Python's `OTEL_PROPAGATORS` default.
fn default_propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ])
}

/// `value` as an OpenTelemetry attribute's when it is a string, a number or
/// a boolean, as Python keeps a start's attributes of those types alone.
fn scalar(value: &Value) -> Option<opentelemetry::Value> {
    match value {
        Value::String(text) => Some(text.clone().into()),
        Value::Bool(flag) => Some((*flag).into()),
        Value::Number(number) => number
            .as_i64()
            .map(opentelemetry::Value::from)
            .or_else(|| number.as_f64().map(opentelemetry::Value::from)),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

/// `value` as an attribute set on a running span: a scalar as itself, any
/// other but `null` written out, as Python's `str(value)`.
fn coerced(value: &Value) -> Option<opentelemetry::Value> {
    match value {
        Value::Null => None,
        Value::Array(_) | Value::Object(_) => Some(value.to_string().into()),
        Value::String(_) | Value::Bool(_) | Value::Number(_) => scalar(value),
    }
}

/// The id of the span `context` holds, as the kit names span ids: 16 hex
/// digits.
fn span_id_of(context: &Context) -> String {
    context.span().span_context().span_id().to_string()
}

/// The error an OpenTelemetry span records for an `error` end.
#[derive(Debug)]
struct Failed(String);

impl fmt::Display for Failed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Failed {}

impl TelemetryProvider for OpenTelemetryProvider {
    fn name(&self) -> &'static str {
        "opentelemetry"
    }

    fn start_span(&self, kind: SpanKind, name: &str, options: StartSpanOptions) -> String {
        let parent = match options.parent_id.as_deref() {
            Some(parent_id) if !parent_id.is_empty() => self.parent_context(parent_id, name),
            _ => Context::current(),
        };
        let written = self.attributes(kind, &options);
        let attributes = written
            .iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
            .collect();
        let context = self
            .tracer
            .start(format!("roomkit.{name}"), &parent, attributes);
        let span_id = span_id_of(&context);
        let active = Active { context, written };
        self.spans().active.insert(span_id.clone(), active);
        span_id
    }

    fn end_span(&self, span_id: &str, options: EndSpanOptions) {
        let mut spans = self.spans();
        let Some(mut active) = spans.active.remove(span_id) else {
            return;
        };
        spans.keep_ended(span_id.to_owned(), &active.context);
        drop(spans);
        for (key, value) in &options.attributes {
            if let Some(value) = coerced(value) {
                active.write(key, value);
            }
        }
        let span = active.context.span();
        if options.status == "error" {
            let message = options.error_message.unwrap_or_default();
            if !message.is_empty() {
                span.record_error(&Failed(message.clone()));
            }
            span.set_status(Status::error(message));
        } else {
            span.set_status(Status::Ok);
        }
        span.end();
    }

    fn set_attribute(&self, span_id: &str, key: &str, value: Value) {
        let mut spans = self.spans();
        if let (Some(active), Some(value)) = (spans.active.get_mut(span_id), coerced(&value)) {
            active.write(key, value);
        }
    }

    fn record_metric(&self, name: &str, value: f64, options: RecordMetricOptions) {
        let attributes: Vec<KeyValue> = options
            .attributes
            .iter()
            .filter_map(|(key, value)| {
                coerced(value).map(|value| KeyValue::new(key.clone(), value))
            })
            .collect();
        let description = format!("RoomKit metric: {name}");
        if name.contains("duration") || name.contains("ttfb") {
            let unit = if options.unit.is_empty() {
                "ms".to_owned()
            } else {
                options.unit
            };
            self.meter
                .f64_histogram(name.to_owned())
                .with_unit(unit)
                .with_description(description)
                .build()
                .record(value, &attributes);
        } else {
            self.meter
                .f64_counter(name.to_owned())
                .with_unit(options.unit)
                .with_description(description)
                .build()
                .add(value, &attributes);
        }
    }

    fn get_span_context(&self, span_id: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        let context = self.spans().parent(span_id)?;
        Some(Arc::new(context))
    }

    fn inject_context(&self, span_id: &str) -> BTreeMap<String, String> {
        let Some(context) = self.spans().parent(span_id) else {
            return BTreeMap::new();
        };
        let mut carrier = HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut carrier);
        });
        if carrier.is_empty() {
            default_propagator().inject_context(&context, &mut carrier);
        }
        carrier.into_iter().collect()
    }

    fn extract_context(
        &self,
        carrier: &BTreeMap<String, String>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        let carrier: HashMap<String, String> = carrier.clone().into_iter().collect();
        let context = global::get_text_map_propagator(|propagator| propagator.extract(&carrier));
        if context.span().span_context().is_valid() {
            return Some(Arc::new(context));
        }
        let context = default_propagator().extract(&carrier);
        if !context.span().span_context().is_valid() {
            return None;
        }
        Some(Arc::new(context))
    }

    fn flush(&self) {
        let Some(provider) = self.tracer_provider.clone() else {
            return;
        };
        if self.flushing.swap(true, Ordering::AcqRel) {
            return;
        }
        let flushing = Arc::clone(&self.flushing);
        let spawned = std::thread::Builder::new()
            .name("otel-flush".to_owned())
            .spawn(move || {
                Self::force_flush(&provider);
                flushing.store(false, Ordering::Release);
            });
        if let Err(error) = spawned {
            // No thread to be had: let a later flush try again.
            self.flushing.store(false, Ordering::Release);
            tracing::debug!(target: "roomkit::telemetry::otel", %error, "could not start the OpenTelemetry flush thread");
        }
    }

    fn close(&self) {
        let active: Vec<String> = self.spans().active.keys().cloned().collect();
        for span_id in active {
            self.end_span(&span_id, EndSpanOptions::default());
        }
        let Some(provider) = self.tracer_provider.clone() else {
            return;
        };
        let (done, flushed) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("otel-flush".to_owned())
            .spawn(move || {
                Self::force_flush(&provider);
                let _ = done.send(());
            });
        match spawned {
            Ok(_) => {
                if flushed.recv_timeout(self.shutdown_flush_timeout).is_err() {
                    tracing::warn!(target: "roomkit::telemetry::otel", timeout_seconds = self.shutdown_flush_timeout.as_secs_f64(), "the OpenTelemetry flush did not finish within its bound at shutdown; spans may be lost");
                }
            }
            // No thread to be had: flush here.
            Err(_) => {
                if let Some(provider) = &self.tracer_provider {
                    Self::force_flush(provider);
                }
            }
        }
    }

    fn reset(&self) {
        *self.spans() = Spans::default();
    }

    fn set_metadata(&self, metadata: BTreeMap<String, String>) {
        *self.metadata.lock().unwrap_or_else(PoisonError::into_inner) = metadata;
    }
}

impl fmt::Debug for OpenTelemetryProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenTelemetryProvider")
            .field("shutdown_flush_timeout", &self.shutdown_flush_timeout)
            .finish_non_exhaustive()
    }
}
