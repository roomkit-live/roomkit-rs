//! The provider trait, its span record and its options: Python's
//! `TelemetryProvider` and `Span`.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use roomkit_core::model::{Metadata, Timestamp};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::Value;

use super::SpanKind;

/// Python `Span`: a span a recording provider keeps
/// ([`MockTelemetryProvider`](super::MockTelemetryProvider)).
#[derive(Clone, Debug, PartialEq)]
pub struct Span {
    /// Its id, 16 hex digits.
    pub id: String,
    /// What it measures.
    pub kind: SpanKind,
    /// Its name (`framework.inbound`, `hook.sync.<name>`).
    pub name: String,
    /// The id of the span it runs under.
    pub parent_id: Option<String>,
    /// Its attributes, those its end added included.
    pub attributes: Metadata,
    /// When it started.
    pub start_time: Timestamp,
    /// When it ended, `None` while it runs.
    pub end_time: Option<Timestamp>,
    /// How it ended: `ok`, `error` or `cancelled`.
    pub status: String,
    /// What failed, for an `error`.
    pub error_message: Option<String>,
    /// The room it ran for.
    pub room_id: Option<String>,
    /// The voice session it ran for.
    pub session_id: Option<String>,
    /// The channel it ran for.
    pub channel_id: Option<String>,
}

impl Default for Span {
    /// A `custom` span started now, its id drawn at random.
    fn default() -> Self {
        Self {
            id: new_span_id(),
            kind: SpanKind::Custom,
            name: String::new(),
            parent_id: None,
            attributes: Metadata::new(),
            start_time: SystemEnvironment.now(),
            end_time: None,
            status: "ok".to_owned(),
            error_message: None,
            room_id: None,
            session_id: None,
            channel_id: None,
        }
    }
}

impl Span {
    /// Python `duration_ms`: how long it ran, in milliseconds, `None` until
    /// it ended.
    #[must_use]
    pub fn duration_ms(&self) -> Option<f64> {
        let ended = self.end_time?.duration_since(self.start_time)?;
        Some(ended.as_secs_f64() * 1000.0)
    }
}

/// A span id, 16 hex digits drawn at random, as Python's
/// `uuid4().hex[:16]`.
pub(crate) fn new_span_id() -> String {
    format!("{:016x}", uuid::Uuid::new_v4().as_u64_pair().0)
}

/// The options of [`TelemetryProvider::start_span`], Python's keywords.
#[derive(Clone, Debug, Default)]
pub struct StartSpanOptions {
    /// The id of the span it runs under.
    pub parent_id: Option<String>,
    /// Its first attributes.
    pub attributes: Metadata,
    /// The room it runs for.
    pub room_id: Option<String>,
    /// The voice session it runs for.
    pub session_id: Option<String>,
    /// The channel it runs for.
    pub channel_id: Option<String>,
}

/// The options of [`TelemetryProvider::end_span`], Python's keywords.
#[derive(Clone, Debug)]
pub struct EndSpanOptions {
    /// How it ended: `ok` by default, `error` or `cancelled`.
    pub status: String,
    /// What failed, for an `error`.
    pub error_message: Option<String>,
    /// The attributes its end adds.
    pub attributes: Metadata,
}

impl Default for EndSpanOptions {
    fn default() -> Self {
        Self {
            status: "ok".to_owned(),
            error_message: None,
            attributes: Metadata::new(),
        }
    }
}

/// The options of [`TelemetryProvider::record_metric`], Python's keywords.
#[derive(Clone, Debug, Default)]
pub struct RecordMetricOptions {
    /// The value's unit (`ms`, `tokens`), none by default.
    pub unit: String,
    /// The metric's attributes.
    pub attributes: Metadata,
}

/// Python `TelemetryProvider`: where the kit reports the spans and the
/// metrics of its work (RMK-1213). The kit takes one as
/// [`RoomKitOptions::telemetry`](crate::RoomKitOptions::telemetry), a
/// [`NoopTelemetryProvider`](super::NoopTelemetryProvider) by default.
///
/// A provider is called from the kit's tasks and MUST NOT block them: an
/// export that waits on the network belongs on a thread of its own, as
/// [`OpenTelemetryProvider`](super) does.
pub trait TelemetryProvider: Any + Send + Sync {
    /// Python `name`: the provider's name (`noop`, `mock`, `console`).
    fn name(&self) -> &str;

    /// Starts a span and returns its id.
    fn start_span(&self, kind: SpanKind, name: &str, options: StartSpanOptions) -> String;

    /// Ends the span `span_id` started; nothing for one it does not know.
    fn end_span(&self, span_id: &str, options: EndSpanOptions);

    /// Sets an attribute of a span still running.
    fn set_attribute(&self, span_id: &str, key: &str, value: Value);

    /// Records a metric's value.
    fn record_metric(&self, name: &str, value: f64, options: RecordMetricOptions);

    /// The backend's own context for `span_id`, opaque (an OpenTelemetry
    /// `Context`), which the kit carries along with the span's id for its
    /// children: `None` by default.
    fn get_span_context(&self, _span_id: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    /// The fields that name `span_id` to another process, for its spans to
    /// continue the same trace (W3C `traceparent` for OpenTelemetry):
    /// empty by default.
    fn inject_context(&self, _span_id: &str) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// The backend context the fields of [`inject_context`](Self::inject_context)
    /// name; `None` by default, and when `carrier` names no span.
    fn extract_context(
        &self,
        _carrier: &BTreeMap<String, String>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    /// Exports the ended spans without closing the provider; nothing by
    /// default.
    fn flush(&self) {}

    /// Closes the provider, exporting what is pending: the kit's close
    /// calls it last. Nothing by default.
    fn close(&self) {}

    /// Forgets what the provider holds; nothing by default.
    fn reset(&self) {}

    /// Python's `with provider.span(kind, name) as span_id:`: starts a span
    /// ended by the guard (see [`SpanGuard`]).
    fn span(&self, kind: SpanKind, name: &str, options: StartSpanOptions) -> SpanGuard<'_> {
        let span_id = self.start_span(kind, name, options);
        let id = span_id.clone();
        SpanGuard {
            span_id,
            end: Some(Box::new(move |options| self.end_span(&id, options))),
        }
    }

    /// The global metadata a provider tags its spans with
    /// (`TelemetryConfig.metadata`), which Python sets on a provider that
    /// keeps some; nothing by default.
    #[doc(hidden)]
    fn set_metadata(&self, _metadata: BTreeMap<String, String>) {}
}

impl fmt::Debug for dyn TelemetryProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryProvider")
            .field("name", &self.name())
            .finish_non_exhaustive()
    }
}

/// A span [`TelemetryProvider::span`] started, Python's `with` block: ended
/// `ok` by [`end`](Self::end), `error` by [`fail`](Self::fail). Rust has
/// no exception the guard could see: dropped unended, it ends `ok`, or
/// `error` while the thread panics.
#[must_use = "a guard dropped at once ends its span at once"]
pub struct SpanGuard<'a> {
    span_id: String,
    end: Option<Box<dyn FnOnce(EndSpanOptions) + Send + 'a>>,
}

impl SpanGuard<'_> {
    /// The span's id, what Python's `with` binds.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.span_id
    }

    /// Ends the span `ok`.
    pub fn end(mut self) {
        self.finish(EndSpanOptions::default());
    }

    /// Ends the span `error`, `message` its error, as Python's block that
    /// raises.
    pub fn fail(mut self, message: impl fmt::Display) {
        self.finish(EndSpanOptions {
            status: "error".to_owned(),
            error_message: Some(message.to_string()),
            ..EndSpanOptions::default()
        });
    }

    fn finish(&mut self, options: EndSpanOptions) {
        if let Some(end) = self.end.take() {
            end(options);
        }
    }
}

impl Drop for SpanGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.finish(EndSpanOptions {
                status: "error".to_owned(),
                error_message: Some("panicked".to_owned()),
                ..EndSpanOptions::default()
            });
        } else {
            self.finish(EndSpanOptions::default());
        }
    }
}

impl fmt::Debug for SpanGuard<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpanGuard")
            .field("span_id", &self.span_id)
            .finish_non_exhaustive()
    }
}
