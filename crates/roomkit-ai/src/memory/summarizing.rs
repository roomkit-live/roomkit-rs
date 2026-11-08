//! A two-tier memory that keeps the context to its window (RFC §20.3):
//! Python's `SummarizingMemory`. Tier 1 truncates the large bodies of the
//! older events, with no model call; tier 2 has a model summarize the
//! older events, a prior summary carried into the new one.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit::ChannelError;
use roomkit::model::{EventContent, Metadata, RoomContext, RoomEvent, RoomId, TextContent};
use tokio::time::Instant;

use super::footprint::{history_event_costs, turn_history_budget};
use super::summary::{
    SUMMARIZER_PROMPT, SUMMARY_FAILED, SummaryLines, is_summary, message_lines, prior_summary,
    summary_message, unavailable,
};
use super::token_estimator::{estimate_message_tokens, estimate_notes_tokens};
use super::{CompactOptions, IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use crate::provider::{AiContext, AiMessage, AiProvider, AiRole};
use crate::turn_budget::TurnBudget;

/// How many summaries the cache keeps.
const MAX_CACHE_ENTRIES: usize = 200;

/// What [`SummarizingMemory::new`] takes beside its inner memory, its
/// summarizer and its window: Python's keywords and their defaults.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SummarizingMemoryOptions {
    /// The share of the history's budget, what the turn leaves of the
    /// window, past which tier 1 truncates: 0.50.
    pub tier1_ratio: f64,
    /// The share of the history's budget past which tier 2 summarizes:
    /// 0.85.
    pub tier2_ratio: f64,
    /// The most characters an older event's body keeps in tier 1: 2,000.
    pub truncate_chars: usize,
    /// The most tokens the summary is asked for: 1,000.
    pub summary_max_tokens: u32,
    /// The fewest events kept after a summary: 5.
    pub min_events: usize,
    /// How long a summary is reused: 30 minutes.
    pub summary_cache_ttl: Duration,
}

impl Default for SummarizingMemoryOptions {
    fn default() -> Self {
        Self {
            tier1_ratio: 0.50,
            tier2_ratio: 0.85,
            truncate_chars: 2000,
            summary_max_tokens: 1000,
            min_events: 5,
            summary_cache_ttl: Duration::from_mins(30),
        }
    }
}

/// Wraps a memory and keeps what it retrieved within a window of tokens
/// in two tiers (RFC §20.3): Python's `SummarizingMemory`.
///
/// The history's budget is what `max_context_tokens`, the model's window,
/// leaves once the rest of the turn is in it (RFC §20): the turn's input
/// the channel measured
/// ([`current_turn_footprint`](super::current_turn_footprint)), its reply
/// budget and the current event. The tiers are fractions of that budget:
///
/// - Tier 1, past `tier1_ratio` of it: the text bodies of the
///   older half of the events are cut to `truncate_chars`, with no model
///   call.
/// - Tier 2, past `tier2_ratio`: the older events are summarized by
///   `provider`, a prior summary of the inner memory's carried into the
///   new one, the latest events kept whole, `min_events` of them at least;
///   the summary's message replaces the prior one (RFC §6.4).
///
/// Then the oldest events are dropped until the result fits the budget.
/// The inner memory's messages and notes are paid for first and handed on
/// as they are. A summary is reused for the same lines, for
/// `summary_cache_ttl`. A summarizer that fails, or answers nothing, leaves
/// a summary that says so.
pub struct SummarizingMemory<P> {
    inner: Arc<dyn MemoryProvider>,
    provider: P,
    max_context_tokens: usize,
    options: SummarizingMemoryOptions,
    /// The summaries made, oldest first: the lines' key, when, the summary.
    cache: Mutex<Vec<(String, Instant, String)>>,
}

impl<P: AiProvider> SummarizingMemory<P> {
    /// `inner`, kept to what the turn leaves of a window of
    /// `max_context_tokens`, the model's, its older events summarized by
    /// `provider`.
    pub fn new(
        inner: Arc<dyn MemoryProvider>,
        provider: P,
        max_context_tokens: usize,
        options: SummarizingMemoryOptions,
    ) -> Self {
        Self {
            inner,
            provider,
            max_context_tokens,
            options,
            cache: Mutex::default(),
        }
    }

    /// The memory's result, `inner` the inner memory's, kept within the
    /// history's `budget`.
    async fn reduced(
        &self,
        room_id: &RoomId,
        inner: MemoryResult,
        budget: usize,
        lines: SummaryLines<'_>,
    ) -> MemoryResult {
        let injected = injected_tokens(&inner);
        let mut events = inner.events.clone();
        let mut total = injected + events_tokens(&events);
        if total > share(budget, self.options.tier1_ratio) && events.len() > 1 {
            events = self.truncated(events);
            total = injected + events_tokens(&events);
        }
        let tier2 = share(budget, self.options.tier2_ratio);
        if total > tier2 && events.len() > self.options.min_events {
            let summarized = self.summarized(room_id, events, inner, tier2, lines).await;
            return within_budget(summarized, budget);
        }
        within_budget(MemoryResult { events, ..inner }, budget)
    }

    /// Tier 1: the text bodies of the older half of `events` cut to
    /// `truncate_chars`, an annotation replacing their tail.
    fn truncated(&self, events: Vec<RoomEvent>) -> Vec<RoomEvent> {
        let midpoint = events.len() / 2;
        events
            .into_iter()
            .enumerate()
            .map(|(at, event)| {
                if at < midpoint {
                    self.cut(event)
                } else {
                    event
                }
            })
            .collect()
    }

    /// `event` with its text body cut to `truncate_chars`, the annotation
    /// replacing its tail, never making it longer; any other content as it
    /// is.
    fn cut(&self, event: RoomEvent) -> RoomEvent {
        let EventContent::Text(text) = &event.content else {
            return event;
        };
        let length = text.body.chars().count();
        if length <= self.options.truncate_chars {
            return event;
        }
        let annotation = format!("\n[... truncated from {length} chars ...]");
        let cutoff = self
            .options
            .truncate_chars
            .saturating_sub(annotation.chars().count());
        let body: String = text.body.chars().take(cutoff).collect();
        let content = TextContent {
            body: format!("{body}{annotation}"),
            ..text.clone()
        };
        RoomEvent {
            content: EventContent::Text(content),
            ..event
        }
    }

    /// Tier 2: the older events summarized, the latest kept whole, within
    /// `budget`; `inner`'s messages kept but a prior summary, which the new
    /// one carries.
    async fn summarized(
        &self,
        room_id: &RoomId,
        events: Vec<RoomEvent>,
        inner: MemoryResult,
        budget: usize,
        mut lines: SummaryLines<'_>,
    ) -> MemoryResult {
        let costs: Vec<i64> = history_event_costs(&events)
            .into_iter()
            .map(to_i64)
            .collect();
        let event_budget = to_i64(budget) - i64::from(self.options.summary_max_tokens);
        let mut keep_from = 0;
        let mut running = 0;
        for at in (0..events.len()).rev() {
            running += costs.get(at).copied().unwrap_or(0);
            if running > event_budget && events.len() - at >= self.options.min_events {
                keep_from = at;
                break;
            }
        }
        if keep_from == 0 {
            return MemoryResult { events, ..inner };
        }
        let mut trimmed = events;
        let kept = trimmed.split_off(keep_from);
        lines.kept.clone_from(&kept);
        let prior = prior_summary(&inner.messages);
        let texts = lines.lines(&trimmed);
        let summary = self
            .summary(room_id, &texts, prior.as_deref(), trimmed.len())
            .await;
        let mut messages: Vec<AiMessage> = inner
            .messages
            .iter()
            .filter(|message| !is_summary(message))
            .cloned()
            .collect();
        messages.push(summary_message(&summary));
        let mut speakers = inner.speakers.clone();
        speakers.extend(lines.named(&trimmed));
        speakers.sort();
        speakers.dedup();
        MemoryResult {
            messages,
            events: kept,
            speakers,
            ..inner
        }
    }

    /// Tier 2 between two tool rounds, on the size the round's generation
    /// measured (RFC §20.3): Python's `compact`. Past `tier2_ratio` of
    /// `max_context_tokens`, what `usage` counts (the round's request and
    /// its reply), the older messages of `history` are summarized, a prior
    /// summary among them carried into the new one, and the latest kept
    /// whole: those that fit what the history must give back to come under
    /// the threshold, the summary's `summary_max_tokens` paid for,
    /// `min_events` of them at least, a call's results never kept without
    /// their call. Tier 1 does not run between rounds; a generation that
    /// reported no usage compacts nothing.
    async fn compacted(
        &self,
        room_id: &RoomId,
        history: &[AiMessage],
        usage: &Metadata,
    ) -> Option<Vec<AiMessage>> {
        let measured = i64::try_from(TurnBudget::tokens_of(usage)).unwrap_or(i64::MAX);
        let threshold = to_i64(share(self.max_context_tokens, self.options.tier2_ratio));
        if measured <= threshold || history.len() <= self.options.min_events {
            return None;
        }
        let costs: Vec<i64> = history
            .iter()
            .map(|message| to_i64(estimate_message_tokens(message)))
            .collect();
        let kept_budget = costs.iter().sum::<i64>()
            - (measured - threshold)
            - i64::from(self.options.summary_max_tokens);
        let keep_from = with_its_call(
            history,
            kept_within(&costs, kept_budget, self.options.min_events),
        );
        if keep_from == 0 {
            return None;
        }
        let (summarized, kept) = history.split_at(keep_from);
        let prior = prior_summary(summarized);
        let read: Vec<AiMessage> = summarized
            .iter()
            .filter(|message| !is_summary(message))
            .cloned()
            .collect();
        let texts = message_lines(&read);
        let summary = self
            .summary(room_id, &texts, prior.as_deref(), summarized.len())
            .await;
        Some(
            std::iter::once(summary_message(&summary))
                .chain(kept.iter().cloned())
                .collect(),
        )
    }

    /// The summary of `count` turns the summarizer reads as `texts`, from
    /// the cache when the same lines were summarized within its time to
    /// live.
    async fn summary(
        &self,
        room_id: &RoomId,
        texts: &[String],
        prior: Option<&str>,
        count: usize,
    ) -> String {
        let key = std::iter::once(room_id.as_str().to_owned())
            .chain(texts.iter().cloned())
            .collect::<Vec<_>>()
            .join("\n");
        let now = Instant::now();
        if let Some(summary) = self.cached(&key, now) {
            return summary;
        }
        let mut parts = vec![
            "Summarize this conversation concisely. Focus on: decisions made, key findings, \
             tool results, errors encountered, and current task state. Be specific about names, \
             error messages, and action outcomes."
                .to_owned(),
        ];
        if let Some(prior) = prior.filter(|prior| !prior.is_empty()) {
            parts.push(format!(
                "\nA prior summary exists. Incorporate its key points:\n{prior}"
            ));
        }
        parts.push(format!("\n{}", texts.join("\n")));
        let context = AiContext {
            messages: vec![AiMessage::new(AiRole::User, parts.join("\n"))],
            system_prompt: Some(SUMMARIZER_PROMPT.to_owned()),
            temperature: 0.0,
            max_tokens: Some(self.options.summary_max_tokens),
            ..AiContext::default()
        };
        let summary = match self.provider.generate(&context).await {
            Ok(response) => match response.content.trim() {
                "" => SUMMARY_FAILED.to_owned(),
                summary => summary.to_owned(),
            },
            Err(error) => {
                tracing::warn!(target: "roomkit::memory", %error, "failed to generate a summary");
                unavailable(count)
            }
        };
        self.cache_lock().push((key, now, summary.clone()));
        summary
    }

    /// The summary cached under `key`, still within its time to live; one
    /// past it is dropped, and the oldest when the cache is full.
    fn cached(&self, key: &str, now: Instant) -> Option<String> {
        let mut cache = self.cache_lock();
        if let Some(at) = cache.iter().position(|(cached, _, _)| cached == key) {
            let (_, made, summary) = cache.get(at)?;
            if now.duration_since(*made) < self.options.summary_cache_ttl {
                return Some(summary.clone());
            }
            cache.remove(at);
        }
        if cache.len() >= MAX_CACHE_ENTRIES {
            cache.remove(0);
        }
        None
    }

    fn cache_lock(&self) -> std::sync::MutexGuard<'_, Vec<(String, Instant, String)>> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Where the kept items start: the latest whose `costs` fit `budget`,
/// `min_kept` of them at least; 0 when they all fit. Unlike tier 2 in
/// `retrieve`, no budget is enforced after it, so the item that passes the
/// budget is not kept.
fn kept_within(costs: &[i64], budget: i64, min_kept: usize) -> usize {
    let mut running = 0;
    let mut start = costs.len();
    for at in (0..costs.len()).rev() {
        running += costs.get(at).copied().unwrap_or(0);
        if running > budget && costs.len() - at > min_kept {
            break;
        }
        start = at;
    }
    start
}

/// `index`, moved back past the tool results there to the call they
/// answer, which the kept messages then start with.
fn with_its_call(history: &[AiMessage], mut index: usize) -> usize {
    while index > 0
        && history
            .get(index)
            .is_some_and(|message| message.role == AiRole::Tool)
    {
        index -= 1;
    }
    index
}

/// `result` with its oldest events dropped until it fits `budget`, one at
/// least kept.
fn within_budget(mut result: MemoryResult, budget: usize) -> MemoryResult {
    let injected = injected_tokens(&result);
    while injected + events_tokens(&result.events) > budget && result.events.len() > 1 {
        result.events.remove(0);
    }
    result
}

/// `budget * ratio`, cut toward zero, as Python's `int()`.
fn share(budget: usize, ratio: f64) -> usize {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        reason = "Python's int(budget * ratio) of a ratio not below 0"
    )]
    let share = (budget as f64 * ratio).max(0.0) as usize;
    share
}

fn to_i64(tokens: usize) -> i64 {
    i64::try_from(tokens).unwrap_or(i64::MAX)
}

/// `events`' tokens, with what the history replays with them (RFC §20).
fn events_tokens(events: &[RoomEvent]) -> usize {
    history_event_costs(events).into_iter().sum()
}

/// What `result` puts in the window besides its events: its messages and
/// the turn's notes, neither of which a tier trims.
fn injected_tokens(result: &MemoryResult) -> usize {
    result
        .messages
        .iter()
        .map(estimate_message_tokens)
        .sum::<usize>()
        + estimate_notes_tokens(&result.notes)
}

impl<P: AiProvider> MemoryProvider for SummarizingMemory<P> {
    fn name(&self) -> String {
        format!("SummarizingMemory({})", self.inner.name())
    }

    fn recent_events_window(&self) -> usize {
        self.inner.recent_events_window()
    }

    fn retrieve<'a>(
        &'a self,
        room_id: &'a RoomId,
        current_event: &'a RoomEvent,
        context: &'a RoomContext,
        options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        Box::pin(async move {
            let channel_id = options.channel_id.clone();
            let inner = self
                .inner
                .retrieve(room_id, current_event, context, options)
                .await?;
            // What the turn leaves the history of the window: no margin,
            // the tiers' ratios are the headroom when no reply budget is set.
            let budget = turn_history_budget(self.max_context_tokens, 0.0, current_event, 0, None);
            let lines = SummaryLines {
                context,
                current: Some(current_event),
                channel_id: channel_id.as_ref(),
                kept: Vec::new(),
            };
            Ok(self.reduced(room_id, inner, budget, lines).await)
        })
    }

    /// Tier 2 on the round's measured size; the inner memory is not asked.
    fn compact<'a>(
        &'a self,
        room_id: &'a RoomId,
        history: &'a [AiMessage],
        usage: &'a Metadata,
        _options: CompactOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<AiMessage>>, ChannelError>> + Send + 'a>>
    {
        Box::pin(async move { Ok(self.compacted(room_id, history, usage).await) })
    }

    fn ingest<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: &'a RoomEvent,
        options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.inner.ingest(room_id, event, options)
    }

    /// Clears the inner memory, then every cached summary: their keys are
    /// the lines, the room's among others.
    fn clear<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        Box::pin(async move {
            self.inner.clear(room_id).await?;
            self.cache_lock().clear();
            Ok(())
        })
    }

    /// Closes the inner memory, then the summarizer.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.inner.close().await;
            self.provider.close().await;
        })
    }
}

impl<P> fmt::Debug for SummarizingMemory<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SummarizingMemory")
            .field("inner", &self.inner.name())
            .field("max_context_tokens", &self.max_context_tokens)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
