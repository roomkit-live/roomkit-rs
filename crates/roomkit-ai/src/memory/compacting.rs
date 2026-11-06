//! A budget-aware memory that summarizes the events it trims (RFC §20.3):
//! Python's `CompactingMemory`.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit::ChannelError;
use roomkit::model::{ChannelId, RoomContext, RoomEvent, RoomId};
use tokio::time::Instant;

use super::footprint::turn_history_budget;
use super::summary::{
    SUMMARIZER_PROMPT, SUMMARY_FAILED, SummaryLines, summary_message, unavailable,
};
use super::token_estimator::{EstimateEventTokensOptions, estimate_event_tokens};
use super::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use crate::provider::{AiContext, AiMessage, AiProvider, AiRole};

/// How many summaries the cache keeps, the least recently used dropped.
const MAX_CACHE_ENTRIES: usize = 200;

/// What [`CompactingMemory::new`] takes beside its inner memory, its
/// summarizer and its window: Python's keywords and their defaults.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompactingMemoryOptions {
    /// The share of the window the summary is given: 0.10.
    pub summary_ratio: f64,
    /// The share of the window kept for the model's reply: 0.15.
    pub safety_margin_ratio: f64,
    /// The fewest events kept after the summary: 5.
    pub min_events: usize,
    /// How long a room's summary is reused for the same events: 5 minutes.
    pub summary_cache_ttl: Duration,
}

impl Default for CompactingMemoryOptions {
    fn default() -> Self {
        Self {
            summary_ratio: 0.10,
            safety_margin_ratio: 0.15,
            min_events: 5,
            summary_cache_ttl: Duration::from_mins(5),
        }
    }
}

/// A room's cached summary for one agent: when, the prompt it answered,
/// the summary.
type Cached = ((RoomId, Option<ChannelId>), Instant, String, String);

/// Wraps a memory and, when its events exceed what the window leaves the
/// history, has `provider` summarize the oldest, which the summary's
/// message stands for at the start of the conversation (RFC §20.3):
/// Python's `CompactingMemory`.
///
/// The budget is what `max_context_tokens`, the model's window, leaves
/// once the rest of the turn is in it, as [`BudgetAwareMemory`] reckons it
/// (RFC §20): the turn's input the channel measured
/// ([`current_turn_footprint`](super::current_turn_footprint)), the reply,
/// the larger of the safety margin and the turn's reply budget, the
/// current event, and the inner memory's messages and notes, which are
/// handed on. The latest events that fit the budget, less the summary's
/// share of the window, are kept whole, `min_events` of them at least. A
/// room's summary is reused, for the agent asking, while the same events
/// are summarized, for `summary_cache_ttl`; a clear or a close during a
/// summary's making keeps it out of the cache.
///
/// [`BudgetAwareMemory`]: super::BudgetAwareMemory
pub struct CompactingMemory<P> {
    inner: Arc<dyn MemoryProvider>,
    provider: P,
    max_context_tokens: usize,
    options: CompactingMemoryOptions,
    /// The summaries made, least recently used first.
    cache: Mutex<Vec<Cached>>,
    /// Counts the clears and closes, so that a summary made across one is
    /// not cached.
    generation: AtomicU64,
}

impl<P: AiProvider> CompactingMemory<P> {
    /// `inner`, its oldest events summarized by `provider` past a window of
    /// `max_context_tokens`.
    pub fn new(
        inner: Arc<dyn MemoryProvider>,
        provider: P,
        max_context_tokens: usize,
        options: CompactingMemoryOptions,
    ) -> Self {
        Self {
            inner,
            provider,
            max_context_tokens,
            options,
            cache: Mutex::default(),
            generation: AtomicU64::new(0),
        }
    }

    /// The memory's result, `inner` the inner memory's, its events kept
    /// within `budget` beside the summary's share.
    async fn compacted(
        &self,
        room_id: &RoomId,
        inner: MemoryResult,
        budget: usize,
        mut lines: SummaryLines<'_>,
    ) -> MemoryResult {
        let budget = to_i64(budget);
        let costs: Vec<i64> = inner
            .events
            .iter()
            .map(|event| {
                to_i64(estimate_event_tokens(
                    event,
                    EstimateEventTokensOptions::default(),
                ))
            })
            .collect();
        if costs.iter().sum::<i64>() <= budget {
            return inner;
        }
        let events_budget = budget - share(self.max_context_tokens, self.options.summary_ratio);
        let count = inner.events.len();
        let mut keep_from = 0;
        let mut running = 0;
        for at in (0..count).rev() {
            let cost = costs.get(at).copied().unwrap_or(0);
            if running + cost > events_budget && count - at > self.options.min_events {
                keep_from = at + 1;
                break;
            }
            running += cost;
        }
        if keep_from == 0 {
            return inner;
        }
        let mut trimmed = inner.events.clone();
        let kept = trimmed.split_off(keep_from);
        lines.kept.clone_from(&kept);
        let summary = self.summary(room_id, &trimmed, &lines).await;
        let mut messages = inner.messages.clone();
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

    /// The summary of `events`, from the cache when the room's agent asked
    /// for the same within its time to live.
    async fn summary(
        &self,
        room_id: &RoomId,
        events: &[RoomEvent],
        lines: &SummaryLines<'_>,
    ) -> String {
        let prompt = format!(
            "Summarize this conversation concisely. Focus on: decisions made, key findings, tool \
             results, errors encountered, and current task state. Be specific about file names, \
             error messages, and action outcomes.\n\n{}",
            lines.lines(events).join("\n")
        );
        let key = (room_id.clone(), lines.channel_id.cloned());
        let now = Instant::now();
        if let Some(summary) = self.cached(&key, &prompt, now) {
            return summary;
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let context = AiContext {
            messages: vec![AiMessage::new(AiRole::User, prompt.clone())],
            system_prompt: Some(SUMMARIZER_PROMPT.to_owned()),
            temperature: 0.0,
            max_tokens: Some(1000),
            ..AiContext::default()
        };
        let summary = match self.provider.generate(&context).await {
            Ok(response) => match response.content.trim() {
                "" => SUMMARY_FAILED.to_owned(),
                summary => summary.to_owned(),
            },
            Err(error) => {
                tracing::warn!(target: "roomkit::memory", %error, "failed to generate a summary");
                unavailable(events.len())
            }
        };
        // A clear or a close during the summary must not bring back an
        // erased one.
        if generation == self.generation.load(Ordering::SeqCst) {
            let mut cache = self.cache_lock();
            cache.retain(|(cached, ..)| cached != &key);
            cache.push((key, now, prompt, summary.clone()));
            let over = cache.len().saturating_sub(MAX_CACHE_ENTRIES);
            cache.drain(..over);
        }
        summary
    }

    /// The summary cached for `key` that answered `prompt`, within its time
    /// to live, made the most recently used.
    fn cached(
        &self,
        key: &(RoomId, Option<ChannelId>),
        prompt: &str,
        now: Instant,
    ) -> Option<String> {
        let mut cache = self.cache_lock();
        let at = cache.iter().position(|(cached, ..)| cached == key)?;
        let (_, made, asked, summary) = cache.get(at)?;
        if now.duration_since(*made) >= self.options.summary_cache_ttl || asked != prompt {
            return None;
        }
        let summary = summary.clone();
        let entry = cache.remove(at);
        cache.push(entry);
        Some(summary)
    }

    fn cache_lock(&self) -> std::sync::MutexGuard<'_, Vec<Cached>> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// `max_context_tokens * ratio`, cut toward zero, as Python's `int()`.
fn share(max_context_tokens: usize, ratio: f64) -> i64 {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        reason = "Python's int(max_context_tokens * ratio), cut toward zero"
    )]
    let share = (max_context_tokens as f64 * ratio) as i64;
    share
}

fn to_i64(tokens: usize) -> i64 {
    i64::try_from(tokens).unwrap_or(i64::MAX)
}

impl<P: AiProvider> MemoryProvider for CompactingMemory<P> {
    fn name(&self) -> String {
        format!("CompactingMemory({})", self.inner.name())
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
            // What the inner memory carries besides its events is not
            // compacted: it is paid for first, with the rest of the turn.
            let budget = turn_history_budget(
                self.max_context_tokens,
                self.options.safety_margin_ratio,
                current_event,
                0,
                Some(&inner),
            );
            let lines = SummaryLines {
                context,
                current: Some(current_event),
                channel_id: channel_id.as_ref(),
                kept: Vec::new(),
            };
            Ok(self.compacted(room_id, inner, budget, lines).await)
        })
    }

    fn ingest<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: &'a RoomEvent,
        options: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.inner.ingest(room_id, event, options)
    }

    /// Forgets the room's summaries, then clears the inner memory.
    fn clear<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.cache_lock().retain(|((room, _), ..)| room != room_id);
        self.inner.clear(room_id)
    }

    /// Forgets every summary, then closes the inner memory and the
    /// summarizer.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.cache_lock().clear();
        Box::pin(async move {
            self.inner.close().await;
            self.provider.close().await;
        })
    }
}

impl<P> fmt::Debug for CompactingMemory<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompactingMemory")
            .field("inner", &self.inner.name())
            .field("max_context_tokens", &self.max_context_tokens)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
