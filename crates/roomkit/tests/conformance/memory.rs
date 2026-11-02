//! The scripted memory of a scenario's scripted agent (roomkit-specs
//! `conformance/README.md`, "Scripted agents"): the room's latest
//! `max_events` events as a sliding window reads them, with the messages,
//! notes and speakers the scenario gives, wrapped in the memories it names.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::ChannelError;
use roomkit::model::{RoomContext, RoomEvent, RoomId};
use roomkit_ai::memory::{
    BudgetAwareMemory, BudgetAwareMemoryOptions, MemoryProvider, MemoryResult, RetrieveOptions,
    SlidingWindowMemory, SlidingWindowMemoryOptions,
};
use roomkit_ai::{AiMessage, AiRole};
use serde_json::Value;

/// The room's latest events, as `SlidingWindowMemory` reads them, and
/// what the scenario says the memory built, retrieved and names.
struct Scripted {
    window: SlidingWindowMemory,
    messages: Vec<AiMessage>,
    notes: Vec<String>,
    speakers: Vec<String>,
}

impl MemoryProvider for Scripted {
    fn recent_events_window(&self) -> usize {
        self.window.recent_events_window()
    }

    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        Box::pin(async move {
            let window = self
                .window
                .retrieve(room_id, current_event, context, options)
                .await?;
            Ok(MemoryResult {
                messages: self.messages.clone(),
                events: window.events,
                notes: self.notes.clone(),
                speakers: self.speakers.clone(),
            })
        })
    }
}

/// The memory `spec` describes; `max_events` is its window by default, the
/// agent's `max_context_events`.
pub(crate) fn scripted_memory(
    spec: &Value,
    max_events: usize,
) -> Result<Arc<dyn MemoryProvider>, String> {
    let max_events = match spec.get("max_events") {
        Some(count) => count_of(count, "max_events")?,
        None => max_events,
    };
    let messages = spec["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .map(message)
        .collect::<Result<Vec<_>, _>>()?;
    let memory: Arc<dyn MemoryProvider> = Arc::new(Scripted {
        window: SlidingWindowMemory::new(SlidingWindowMemoryOptions { max_events }),
        messages,
        notes: texts(&spec["notes"]),
        speakers: texts(&spec["speakers"]),
    });
    match spec.get("budget_aware") {
        Some(budget) => budget_aware(budget, memory),
        None => Ok(memory),
    }
}

/// `inner` in a budget-aware memory of `spec`'s parameters.
fn budget_aware(
    spec: &Value,
    inner: Arc<dyn MemoryProvider>,
) -> Result<Arc<dyn MemoryProvider>, String> {
    let defaults = BudgetAwareMemoryOptions::default();
    let window = count_of(&spec["max_context_tokens"], "max_context_tokens")?;
    let options = BudgetAwareMemoryOptions {
        safety_margin_ratio: spec
            .get("safety_margin_ratio")
            .map_or(Ok(defaults.safety_margin_ratio), |ratio| {
                ratio.as_f64().ok_or("safety_margin_ratio: not a number")
            })?,
        min_events: spec
            .get("min_events")
            .map_or(Ok(defaults.min_events), |count| {
                count_of(count, "min_events")
            })?,
        reserved_tokens: spec
            .get("reserved_tokens")
            .map_or(Ok(defaults.reserved_tokens), |count| {
                count_of(count, "reserved_tokens")
            })?,
    };
    Ok(Arc::new(BudgetAwareMemory::new(inner, window, options)))
}

fn message(spec: &Value) -> Result<AiMessage, String> {
    let role = match spec["role"].as_str() {
        Some("system") => AiRole::System,
        Some("user") => AiRole::User,
        Some("assistant") => AiRole::Assistant,
        other => return Err(format!("memory message role: {other:?}")),
    };
    Ok(AiMessage::new(
        role,
        spec["text"].as_str().unwrap_or_default(),
    ))
}

fn texts(spec: &Value) -> Vec<String> {
    spec.as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn count_of(value: &Value, name: &str) -> Result<usize, String> {
    let count = value.as_u64().ok_or(format!("{name}: not a count"))?;
    usize::try_from(count).map_err(|e| e.to_string())
}
