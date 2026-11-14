//! Per-prompt usage and provenance, copied at the transport return boundary.
//! Python: `channels/_acp_usage.py` and `ACPTurnMixin._run_prompt`.

use std::sync::{Mutex, PoisonError};

use agent_client_protocol_schema::v1::PromptResponse;
use roomkit::model::Metadata;
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::{Value, json};

const SESSION_FIELDS: &[&str] = &[
    "session_id",
    "session_epoch",
    "usage_protocol",
    "node_id",
    "agent_id",
    "adapter_info",
];
const RESULT_FIELDS: &[&str] = &["result_id", "turn_id", "generation", "replayed"];

#[derive(Clone, Default)]
pub(super) struct Snapshot {
    pub(super) counters: Metadata,
    pub(super) metadata: Metadata,
}

struct State {
    metadata: Metadata,
    context: Metadata,
    finalized: bool,
}

pub(super) struct Usage {
    session: String,
    trusted: bool,
    state: Mutex<State>,
}

impl Usage {
    pub(super) fn new(
        session: &str,
        event: &str,
        transport: &str,
        trusted: bool,
        adapter: Option<Value>,
        model: Option<Value>,
    ) -> Self {
        let mut prompt = Metadata::from_iter([
            ("source".to_owned(), json!("session/prompt")),
            ("scope".to_owned(), json!("unspecified")),
        ]);
        if let Some(model) = model.filter(Value::is_string) {
            prompt.insert("model_at_start".to_owned(), model);
        }
        let mut metadata = Metadata::from_iter([
            ("protocol".to_owned(), json!("acp")),
            ("transport".to_owned(), json!(transport)),
            ("session_id".to_owned(), json!(session)),
            ("event_id".to_owned(), json!(event)),
            ("prompt".to_owned(), Value::Object(prompt)),
        ]);
        if let Some(adapter) = adapter {
            metadata.insert("adapter_info".to_owned(), adapter);
        }
        Self {
            session: session.to_owned(),
            trusted,
            state: Mutex::new(State {
                metadata,
                context: Metadata::new(),
                finalized: false,
            }),
        }
    }

    /// A live observation cannot change a response whose transport already returned.
    pub(super) fn observe(&self, update: &Value, model: Option<Value>) -> Value {
        let envelope = envelope(update, self.trusted);
        let report = envelope.map_or_else(
            || observe(update, model),
            |envelope| envelope.get("usage_report").cloned().unwrap_or(Value::Null),
        );
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.finalized
            && envelope.is_none_or(|e| e.get("session_id") == Some(&json!(self.session)))
        {
            state.context = context(&report);
            if let Some(envelope) = envelope {
                apply(&mut state.metadata, envelope, false);
            } else {
                state
                    .metadata
                    .insert("usage_report".to_owned(), report.clone());
            }
        }
        envelope.map_or_else(
            || json!({"session_id":self.session,"usage_report":report}),
            |e| json!(e),
        )
    }

    /// Freeze on the producer task, before its join or stream is polled again.
    pub(super) fn finish(&self, response: &PromptResponse) -> Snapshot {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(prompt) = state
            .metadata
            .get_mut("prompt")
            .and_then(Value::as_object_mut)
        {
            prompt.insert("stop_reason".to_owned(), json!(response.stop_reason));
        }
        if let Some(envelope) = response
            .meta
            .as_ref()
            .filter(|_| self.trusted)
            .and_then(|meta| meta.get("roomkit.live/usage"))
            .and_then(Value::as_object)
        {
            apply(&mut state.metadata, envelope, true);
            if let Some(prompt) = state
                .metadata
                .get_mut("prompt")
                .and_then(Value::as_object_mut)
            {
                prompt.shift_remove("model_at_start");
                if let Some(model) = envelope.get("model").filter(|value| value.is_string()) {
                    prompt.insert("model".to_owned(), model.clone());
                }
            }
            state.context = envelope
                .get("usage_report")
                .map(context)
                .unwrap_or_default();
        }
        state.finalized = true;
        let mut counters = tokens(response);
        counters.extend(state.context.clone());
        Snapshot {
            counters,
            metadata: state.metadata.clone(),
        }
    }
}

fn envelope(update: &Value, trusted: bool) -> Option<&Metadata> {
    trusted
        .then_some(update)?
        .get("_meta")?
        .get("roomkit.live/usage")?
        .as_object()
}

fn apply(metadata: &mut Metadata, envelope: &Metadata, terminal: bool) {
    for key in SESSION_FIELDS
        .iter()
        .copied()
        .chain(RESULT_FIELDS.iter().copied().filter(|_| terminal))
        .chain(["usage_report"])
    {
        metadata.shift_remove(key);
        if let Some(value) = envelope.get(key) {
            metadata.insert(key.to_owned(), value.clone());
        }
    }
    metadata.insert("identity_source".to_owned(), json!("transport"));
}

fn observe(update: &Value, model: Option<Value>) -> Value {
    let environment = SystemEnvironment;
    let mut report = json!({
        "report_id": environment.new_observation_id().to_string(),
        "identity_source": "roomkit", "observed_at_ms": environment.now().as_unix_millis(),
        "source":"session/update", "scope":"session", "update": update,
    });
    if let Some(model) = model.filter(Value::is_string)
        && let Some(report) = report.as_object_mut()
    {
        report.insert("model_at_observation".to_owned(), model);
    }
    report
}

fn context(report: &Value) -> Metadata {
    let mut values = Metadata::new();
    let Some(update) = report.get("update") else {
        return values;
    };
    for (source, target) in [("used", "context_used"), ("size", "context_size")] {
        if let Some(value) = update.get(source).filter(|v| v.is_i64() || v.is_u64()) {
            values.insert(target.to_owned(), value.clone());
        }
    }
    let cost = update.get("cost");
    if let Some(value) = cost
        .and_then(|cost| cost.get("amount"))
        .and_then(Value::as_f64)
    {
        values.insert("cost".to_owned(), json!(value));
    }
    if let Some(value) = cost
        .and_then(|cost| cost.get("currency"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        values.insert("currency".to_owned(), json!(value));
    }
    values
}

/// The counters the agent returned with its answer, unaltered, under
/// Python's names; none when it returned none.
fn tokens(answer: &PromptResponse) -> Metadata {
    let mut usage = Metadata::new();
    let Some(counters) = &answer.usage else {
        return usage;
    };
    usage.insert(
        "total_tokens".to_owned(),
        Value::from(counters.total_tokens),
    );
    usage.insert(
        "input_tokens".to_owned(),
        Value::from(counters.input_tokens),
    );
    usage.insert(
        "output_tokens".to_owned(),
        Value::from(counters.output_tokens),
    );
    let optional = [
        ("thought_tokens", counters.thought_tokens),
        ("cached_read_tokens", counters.cached_read_tokens),
        ("cached_write_tokens", counters.cached_write_tokens),
    ];
    for (name, value) in optional {
        if let Some(value) = value {
            usage.insert(name.to_owned(), Value::from(value));
        }
    }
    usage
}

#[cfg(test)]
mod tests;
