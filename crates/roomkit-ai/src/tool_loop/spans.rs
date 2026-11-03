//! The spans of a turn's loop, Python's `llm.generate` and `tool.<name>`,
//! and its token metrics. The kit's provider and the current span are read
//! where the turn starts, under the kit's call: its stream may be read from
//! another task.

use std::any::type_name;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::telemetry::{
    Attr, EndSpanOptions, RecordMetricOptions, SpanKind, StartSpanOptions, TelemetryProvider,
    current_telemetry, get_current_span,
};
use serde_json::{Value, json};

use roomkit::model::LoopEndReason;

/// Where a turn reports: the kit's provider and the span the turn runs
/// under, as read when the turn started.
#[derive(Clone, Default)]
pub(crate) struct TurnTelemetry {
    provider: Option<Arc<dyn TelemetryProvider>>,
    parent: Option<String>,
}

impl std::fmt::Debug for TurnTelemetry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnTelemetry")
            .field("parent", &self.parent)
            .finish_non_exhaustive()
    }
}

impl TurnTelemetry {
    /// What the code running now reports to.
    pub(crate) fn current() -> Self {
        Self {
            provider: current_telemetry(),
            parent: get_current_span(),
        }
    }

    /// The turn's `llm.generate` span, naming `P` as its provider, as
    /// Python's names its provider's class.
    pub(super) fn start_turn<P: ?Sized>(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Option<TurnSpan> {
        let provider = self.provider.clone()?;
        let name = type_name::<P>().rsplit("::").next().unwrap_or_default();
        let mut attributes = Metadata::new();
        attributes.insert(Attr::PROVIDER.to_owned(), Value::from(name));
        attributes.insert(Attr::LLM_STREAMING.to_owned(), Value::from(true));
        let options = StartSpanOptions {
            parent_id: self.parent.clone(),
            room_id: Some(room_id.to_string()),
            channel_id: Some(channel_id.to_string()),
            attributes,
            ..StartSpanOptions::default()
        };
        let id = provider.start_span(SpanKind::LlmGenerate, "llm.generate", options);
        Some(TurnSpan {
            provider,
            id,
            channel_id: channel_id.to_string(),
            ended: AtomicBool::new(false),
        })
    }
}

/// A turn's `llm.generate` span, ended once. Dropped unended, its reader
/// stopped reading: it ends `cancelled`, as Python's a stream closed.
pub(crate) struct TurnSpan {
    provider: Arc<dyn TelemetryProvider>,
    id: String,
    channel_id: String,
    ended: AtomicBool,
}

impl TurnSpan {
    /// Records what a generation used, Python's `roomkit.llm.<counter>`.
    pub(super) fn generation_used(&self, usage: &Metadata) {
        for counter in ["input_tokens", "output_tokens"] {
            let used = usage.get(counter).and_then(Value::as_f64).unwrap_or(0.0);
            let mut attributes = Metadata::new();
            attributes.insert(
                Attr::CHANNEL_ID.to_owned(),
                Value::from(self.channel_id.as_str()),
            );
            let options = RecordMetricOptions {
                unit: "tokens".to_owned(),
                attributes,
            };
            self.provider
                .record_metric(&format!("roomkit.llm.{counter}"), used, options);
        }
    }

    /// Ends it as the loop ended for `reason`, with what its rounds used:
    /// `cancelled` and `error` as themselves, any other `ok`.
    pub(super) fn ended(&self, reason: LoopEndReason, calls: u32, usage: &Metadata) {
        let status = match reason {
            LoopEndReason::Cancelled => "cancelled",
            LoopEndReason::Error => "error",
            _ => "ok",
        };
        self.finish(status, None, calls, usage);
    }

    /// Ends it `error`, `message` what failed.
    pub(super) fn failed(&self, message: String, calls: u32, usage: &Metadata) {
        self.finish("error", Some(message), calls, usage);
    }

    fn finish(&self, status: &str, error: Option<String>, calls: u32, usage: &Metadata) {
        if self.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut attributes = Metadata::new();
        attributes.insert(Attr::LLM_TOOL_COUNT.to_owned(), json!(calls));
        let token = |counter: &str| usage.get(counter).and_then(Value::as_u64).unwrap_or(0);
        let (input, output) = (token("input_tokens"), token("output_tokens"));
        if input > 0 || output > 0 {
            attributes.insert(Attr::LLM_INPUT_TOKENS.to_owned(), json!(input));
            attributes.insert(Attr::LLM_OUTPUT_TOKENS.to_owned(), json!(output));
        }
        let options = EndSpanOptions {
            status: status.to_owned(),
            error_message: error,
            attributes,
        };
        self.provider.end_span(&self.id, options);
    }

    /// A call's `tool.<name>` span, a child of the turn's.
    pub(super) fn start_call(&self, name: &str, id: &str) -> CallSpan {
        let mut attributes = Metadata::new();
        attributes.insert("tool.name".to_owned(), Value::from(name));
        attributes.insert("tool.id".to_owned(), Value::from(id));
        let options = StartSpanOptions {
            parent_id: Some(self.id.clone()),
            attributes,
            ..StartSpanOptions::default()
        };
        let span =
            self.provider
                .start_span(SpanKind::LlmToolCall, &format!("tool.{name}"), options);
        CallSpan {
            provider: Arc::clone(&self.provider),
            id: span,
            ended: false,
        }
    }
}

impl Drop for TurnSpan {
    fn drop(&mut self) {
        self.finish("cancelled", None, 0, &Metadata::new());
    }
}

/// A call's `tool.<name>` span. Dropped unended, the call was cancelled.
pub(crate) struct CallSpan {
    provider: Arc<dyn TelemetryProvider>,
    id: String,
    ended: bool,
}

impl CallSpan {
    /// Ends it `ok`, or `error` with what the tool raised.
    pub(super) fn end(mut self, error: Option<String>) {
        self.ended = true;
        let options = match error {
            Some(message) => EndSpanOptions {
                status: "error".to_owned(),
                error_message: Some(message),
                ..EndSpanOptions::default()
            },
            None => EndSpanOptions::default(),
        };
        self.provider.end_span(&self.id, options);
    }
}

impl Drop for CallSpan {
    fn drop(&mut self) {
        if !self.ended {
            let options = EndSpanOptions {
                status: "cancelled".to_owned(),
                ..EndSpanOptions::default()
            };
            self.provider.end_span(&self.id, options);
        }
    }
}
