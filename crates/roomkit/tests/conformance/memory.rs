//! The scripted memory of a scenario's scripted agent (roomkit-specs
//! `conformance/README.md`, "Scripted agents"): the room's latest
//! `max_events` events as a sliding window reads them, with the messages,
//! notes and speakers the scenario gives, wrapped in the memories it names.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::ChannelError;
use roomkit::model::{RoomContext, RoomEvent, RoomId};
use roomkit_ai::knowledge::{KnowledgeResult, KnowledgeSource, SearchOptions};
use roomkit_ai::memory::{
    BudgetAwareMemory, BudgetAwareMemoryOptions, CompactingMemory, CompactingMemoryOptions,
    MemoryProvider, MemoryResult, RetrievalMemory, RetrievalMemoryOptions, RetrieveOptions,
    SlidingWindowMemory, SlidingWindowMemoryOptions, SummarizingMemory, SummarizingMemoryOptions,
};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{AiMessage, AiRole};
use serde_json::Value;

use crate::agents::write_record;

/// The room's latest events, as `SlidingWindowMemory` reads them, and
/// what the scenario says the memory built, retrieved and names.
struct Scripted {
    window: SlidingWindowMemory,
    messages: Vec<AiMessage>,
    notes: Vec<String>,
    speakers: Vec<String>,
    /// What each read writes on the turn's record.
    spec: Value,
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
            write_record(&self.spec);
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

/// The memory `spec` describes, its wrappers from the innermost;
/// `max_events` is its window by default, the agent's
/// `max_context_events`, and `summarizer` the model a compacting or
/// summarizing memory summarizes with.
pub(crate) fn scripted_memory(
    spec: &Value,
    max_events: usize,
    summarizer: Option<ScriptedProvider>,
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
        spec: spec.clone(),
    });
    let memory = match spec.get("budget_aware") {
        Some(budget) => budget_aware(budget, memory)?,
        None => memory,
    };
    let memory = match spec.get("compacting") {
        Some(compacting) => self::compacting(compacting, memory, needed(summarizer.clone())?)?,
        None => memory,
    };
    let memory = match spec.get("summarizing") {
        Some(summarizing) => self::summarizing(summarizing, memory, needed(summarizer)?)?,
        None => memory,
    };
    match spec.get("retrieval") {
        Some(retrieval) => self::retrieval(retrieval, memory),
        None => Ok(memory),
    }
}

/// A knowledge source answering every search with its results, at most
/// the search's limit, or failing with its `fails`.
struct ScriptedSource {
    name: String,
    results: Vec<KnowledgeResult>,
    fails: Option<String>,
}

impl KnowledgeSource for ScriptedSource {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn search<'a>(
        &'a self,
        _query: &'a str,
        options: SearchOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<KnowledgeResult>, ChannelError>> + Send + 'a>> {
        let answer = match &self.fails {
            Some(fails) => Err(fails.clone().into()),
            None => Ok(self.results.iter().take(options.limit).cloned().collect()),
        };
        Box::pin(std::future::ready(answer))
    }
}

/// `inner` in a retrieval memory of `spec`'s sources and parameters.
fn retrieval(
    spec: &Value,
    inner: Arc<dyn MemoryProvider>,
) -> Result<Arc<dyn MemoryProvider>, String> {
    let sources = spec["sources"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|source| {
            let results = source["results"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|result| KnowledgeResult {
                    score: result["score"].as_f64().unwrap_or(0.0),
                    source: result["source"].as_str().unwrap_or_default().to_owned(),
                    ..KnowledgeResult::new(result["content"].as_str().unwrap_or_default())
                })
                .collect();
            let source: Arc<dyn KnowledgeSource> = Arc::new(ScriptedSource {
                name: source["name"].as_str().unwrap_or_default().to_owned(),
                results,
                fails: source["fails"].as_str().map(str::to_owned),
            });
            source
        })
        .collect();
    let defaults = RetrievalMemoryOptions::default();
    let options = RetrievalMemoryOptions {
        max_results: count(spec, "max_results", defaults.max_results)?,
        min_query_length: count(spec, "min_query_length", defaults.min_query_length)?,
    };
    Ok(Arc::new(RetrievalMemory::new(sources, inner, options)))
}

fn needed(summarizer: Option<ScriptedProvider>) -> Result<ScriptedProvider, String> {
    summarizer.ok_or_else(|| "a compacting or summarizing memory needs its summarizer".to_owned())
}

/// `inner` in a compacting memory of `spec`'s parameters.
fn compacting(
    spec: &Value,
    inner: Arc<dyn MemoryProvider>,
    summarizer: ScriptedProvider,
) -> Result<Arc<dyn MemoryProvider>, String> {
    let defaults = CompactingMemoryOptions::default();
    let options = CompactingMemoryOptions {
        summary_ratio: ratio(spec, "summary_ratio", defaults.summary_ratio)?,
        safety_margin_ratio: ratio(spec, "safety_margin_ratio", defaults.safety_margin_ratio)?,
        min_events: count(spec, "min_events", defaults.min_events)?,
        ..defaults
    };
    let window = count_of(&spec["max_context_tokens"], "max_context_tokens")?;
    Ok(Arc::new(CompactingMemory::new(
        inner, summarizer, window, options,
    )))
}

/// `inner` in a summarizing memory of `spec`'s parameters.
fn summarizing(
    spec: &Value,
    inner: Arc<dyn MemoryProvider>,
    summarizer: ScriptedProvider,
) -> Result<Arc<dyn MemoryProvider>, String> {
    let defaults = SummarizingMemoryOptions::default();
    let summary_max_tokens = count(
        spec,
        "summary_max_tokens",
        defaults.summary_max_tokens as usize,
    )?;
    let options = SummarizingMemoryOptions {
        tier1_ratio: ratio(spec, "tier1_ratio", defaults.tier1_ratio)?,
        tier2_ratio: ratio(spec, "tier2_ratio", defaults.tier2_ratio)?,
        truncate_chars: count(spec, "truncate_chars", defaults.truncate_chars)?,
        summary_max_tokens: u32::try_from(summary_max_tokens).map_err(|e| e.to_string())?,
        min_events: count(spec, "min_events", defaults.min_events)?,
        ..defaults
    };
    let window = count_of(&spec["max_context_tokens"], "max_context_tokens")?;
    Ok(Arc::new(SummarizingMemory::new(
        inner, summarizer, window, options,
    )))
}

fn ratio(spec: &Value, name: &str, default: f64) -> Result<f64, String> {
    spec.get(name).map_or(Ok(default), |ratio| {
        ratio.as_f64().ok_or(format!("{name}: not a number"))
    })
}

fn count(spec: &Value, name: &str, default: usize) -> Result<usize, String> {
    spec.get(name)
        .map_or(Ok(default), |count| count_of(count, name))
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
