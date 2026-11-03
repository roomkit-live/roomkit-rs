//! The spans of a turn's loop, Python's `llm.generate` and `tool.<name>`,
//! and its token metrics. The kit's provider and the current span are read
//! where the turn starts, under the kit's call: its stream may be read from
//! another task.

use std::any::type_name;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::telemetry::{
    Attr, EndSpanOptions, RecordMetricOptions, SpanKind, StartSpanOptions, TelemetryProvider,
    current_records_sensitive_data, current_telemetry, get_current_span,
};
use serde_json::{Value, json};

use roomkit::model::LoopEndReason;

/// Where a turn reports: the kit's provider and the span the turn runs
/// under, as read when the turn started.
#[derive(Clone, Default)]
pub(crate) struct TurnTelemetry {
    provider: Option<Arc<dyn TelemetryProvider>>,
    parent: Option<String>,
    sensitive: bool,
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
            sensitive: current_records_sensitive_data(),
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
        let name = type_name::<P>()
            .split('<')
            .next()
            .and_then(|path| path.rsplit("::").next())
            .unwrap_or_default();
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
            sensitive: self.sensitive,
            used: Mutex::default(),
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
    sensitive: bool,
    /// What its rounds used so far, which its end reports whenever it comes.
    used: Mutex<Used>,
    ended: AtomicBool,
}

/// A turn's calls and tokens so far.
#[derive(Default)]
struct Used {
    calls: u32,
    input_tokens: u64,
    output_tokens: u64,
}

impl TurnSpan {
    fn used(&self) -> MutexGuard<'_, Used> {
        self.used.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts the `calls` a round ran.
    pub(super) fn calls_ran(&self, calls: u32) {
        let mut used = self.used();
        used.calls = used.calls.saturating_add(calls);
    }

    /// Records what a generation used, Python's `roomkit.llm.<counter>`.
    pub(super) fn generation_used(&self, usage: &Metadata) {
        {
            let token = |counter: &str| usage.get(counter).and_then(Value::as_u64).unwrap_or(0);
            let mut used = self.used();
            used.input_tokens = used.input_tokens.saturating_add(token("input_tokens"));
            used.output_tokens = used.output_tokens.saturating_add(token("output_tokens"));
        }
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

    /// Ends it as the loop ended for `reason`: `cancelled` and `error` as
    /// themselves, any other `ok`.
    pub(super) fn ended(&self, reason: LoopEndReason) {
        let status = if reason == LoopEndReason::Cancelled {
            "cancelled"
        } else if reason == LoopEndReason::Error {
            "error"
        } else {
            "ok"
        };
        self.finish(status, None);
    }

    /// Ends it `error`, the provider's failure saying `text`: the class's
    /// name alone, unless the kit records what can hold personal data.
    pub(super) fn failed(&self, text: String) {
        let message = if self.sensitive && !text.is_empty() {
            text
        } else {
            "ProviderError".to_owned()
        };
        self.finish("error", Some(message));
    }

    /// Ends it, what its rounds used on it, whether or not the turn reached
    /// its end, as Python's `_turn_span_attributes`.
    fn finish(&self, status: &str, error: Option<String>) {
        if self.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut attributes = Metadata::new();
        let used = self.used();
        attributes.insert(Attr::LLM_TOOL_COUNT.to_owned(), json!(used.calls));
        if used.input_tokens > 0 || used.output_tokens > 0 {
            attributes.insert(Attr::LLM_INPUT_TOKENS.to_owned(), json!(used.input_tokens));
            attributes.insert(
                Attr::LLM_OUTPUT_TOKENS.to_owned(),
                json!(used.output_tokens),
            );
        }
        drop(used);
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
            sensitive: self.sensitive,
            ended: false,
        }
    }
}

impl Drop for TurnSpan {
    fn drop(&mut self) {
        self.finish("cancelled", None);
    }
}

/// A call's `tool.<name>` span. Dropped unended, the call was cancelled.
pub(crate) struct CallSpan {
    provider: Arc<dyn TelemetryProvider>,
    id: String,
    sensitive: bool,
    ended: bool,
}

impl CallSpan {
    /// Ends it `ok`, or `error` with what the tool raised: its class's name
    /// alone, unless the kit records what can hold personal data.
    pub(super) fn end(mut self, error: Option<String>) {
        self.ended = true;
        let options = match error {
            Some(text) => EndSpanOptions {
                status: "error".to_owned(),
                error_message: Some(if self.sensitive && !text.is_empty() {
                    text
                } else {
                    "ToolError".to_owned()
                }),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roomkit::model::{ChannelId, Metadata, RoomId};
    use roomkit::telemetry::{MockTelemetryProvider, SpanKind, TelemetryProvider};

    use super::{Attr, LoopEndReason, TurnSpan, TurnTelemetry, json};

    fn started(mock: &Arc<MockTelemetryProvider>) -> TurnSpan {
        let cloned = Arc::clone(mock);
        let provider: Arc<dyn TelemetryProvider> = cloned;
        let telemetry = TurnTelemetry {
            provider: Some(provider),
            parent: None,
            sensitive: false,
        };
        let room = RoomId::new("r1").unwrap();
        let channel = ChannelId::new("ai").unwrap();
        telemetry.start_turn::<u8>(&room, &channel).unwrap()
    }

    /// The status of a turn's span, by how its loop ended.
    // Python: tests/test_turn_that_did_not_complete.py::
    // test_a_steering_cancel_closes_its_span_cancelled,
    // test_a_turn_cancelled_from_outside_closes_its_span, and
    // tests/test_parallel_tool_cleanup.py::test_cancelling_tool_turn_closes_span
    #[test]
    fn a_turn_s_span_ends_as_its_loop_ended() {
        let mock = Arc::new(MockTelemetryProvider::default());
        started(&mock).ended(LoopEndReason::Completed);
        started(&mock).ended(LoopEndReason::Cancelled);
        started(&mock).ended(LoopEndReason::Error);
        // A reader that stopped reading drops it.
        drop(started(&mock));
        let statuses: Vec<String> = mock
            .get_spans(SpanKind::LlmGenerate)
            .into_iter()
            .map(|span| span.status)
            .collect();
        assert_eq!(statuses, ["ok", "cancelled", "error", "cancelled"]);
    }

    // Python: tests/test_turn_that_did_not_complete.py::test_a_barge_in_keeps_what_the_rounds_used_on_the_span
    #[test]
    fn a_turn_cut_short_keeps_what_its_rounds_used_on_its_span() {
        let mock = Arc::new(MockTelemetryProvider::default());
        let turn = started(&mock);
        let mut usage = Metadata::new();
        usage.insert("input_tokens".to_owned(), json!(10));
        usage.insert("output_tokens".to_owned(), json!(5));
        turn.generation_used(&usage);
        turn.calls_ran(1);
        drop(turn);
        let [span] = mock.get_spans(SpanKind::LlmGenerate).try_into().unwrap();
        assert_eq!(span.status, "cancelled");
        assert_eq!(span.attributes[Attr::LLM_INPUT_TOKENS], json!(10));
        assert_eq!(span.attributes[Attr::LLM_OUTPUT_TOKENS], json!(5));
        assert_eq!(span.attributes[Attr::LLM_TOOL_COUNT], json!(1));
    }

    // A call cut with its turn ends cancelled.
    #[test]
    fn a_call_dropped_ends_cancelled() {
        let mock = Arc::new(MockTelemetryProvider::default());
        let turn = started(&mock);
        drop(turn.start_call("lookup", "c1"));
        let [span] = mock.get_spans(SpanKind::LlmToolCall).try_into().unwrap();
        assert_eq!(span.status, "cancelled");
        turn.ended(LoopEndReason::Completed);
    }
}
