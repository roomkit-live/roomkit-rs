//! A memory that trims the oldest events to what the window leaves (RFC
//! §20.3): Python's `BudgetAwareMemory`.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::ChannelError;
use roomkit::model::{RoomContext, RoomEvent, RoomId};

use super::footprint::{history_event_costs, turn_history_budget};
use super::token_estimator::{EstimateEventTokensOptions, estimate_event_tokens};
use super::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use crate::provider::{ProviderError, ProviderErrorOptions};

/// What [`BudgetAwareMemory::new`] takes beside its inner memory and its
/// window: Python's `safety_margin_ratio=0.15, min_events=3,
/// reserved_tokens=0`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BudgetAwareMemoryOptions {
    /// The share of the window kept for the model's reply.
    pub safety_margin_ratio: f64,
    /// How many of the latest events are kept, even when they do not fit.
    pub min_events: usize,
    /// A floor on the turn's input the channel measures (the system
    /// prompt, the tool schemas, the channel's notes), for a caller that
    /// knows a part the channel cannot: the larger of the two is reserved,
    /// never their sum.
    pub reserved_tokens: usize,
}

impl Default for BudgetAwareMemoryOptions {
    fn default() -> Self {
        Self {
            safety_margin_ratio: 0.15,
            min_events: 3,
            reserved_tokens: 0,
        }
    }
}

/// Wraps a memory and trims what it retrieved to a window of tokens (RFC
/// §20.3): Python's `BudgetAwareMemory`.
///
/// The history's budget is what is left of the window once the rest of
/// the turn is paid for ([`history_budget`](super::history_budget)): the
/// turn's input the channel measured
/// ([`current_turn_footprint`](super::current_turn_footprint)), or the
/// reserved floor when larger; the inner memory's notes and messages; the
/// current event; and the reply budget when larger than the safety margin.
/// Events are dropped from the oldest; the latest `min_events` are kept
/// even when they do not fit, since an empty history is worse than one
/// that needs compaction.
///
/// A current event that alone exceeds a known window fails the read with a
/// [`ProviderError`] of `context_overflow`, not retryable: no trimming can
/// make it fit.
pub struct BudgetAwareMemory {
    inner: Arc<dyn MemoryProvider>,
    max_context_tokens: usize,
    options: BudgetAwareMemoryOptions,
}

impl fmt::Debug for BudgetAwareMemory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BudgetAwareMemory")
            .field("inner", &self.inner.name())
            .field("max_context_tokens", &self.max_context_tokens)
            .field("options", &self.options)
            .finish()
    }
}

impl BudgetAwareMemory {
    /// `inner`, its retrievals trimmed to a window of `max_context_tokens`
    /// (0 for a window not known), as `options` say.
    pub fn new(
        inner: Arc<dyn MemoryProvider>,
        max_context_tokens: usize,
        options: BudgetAwareMemoryOptions,
    ) -> Self {
        Self {
            inner,
            max_context_tokens,
            options,
        }
    }

    /// Refuses a current event that alone exceeds a known window.
    fn refuse_overflow(&self, current_event: &RoomEvent) -> Result<(), ChannelError> {
        let text_only = EstimateEventTokensOptions { text_only: true };
        let current = estimate_event_tokens(current_event, text_only);
        if self.max_context_tokens == 0 || current <= self.max_context_tokens {
            return Ok(());
        }
        let message = format!(
            "The current message alone exceeds the model's context window (~{current} tokens, \
             window {}). Shorten the message or choose a model with a larger context window.",
            self.max_context_tokens
        );
        let options = ProviderErrorOptions {
            context_overflow: Some(true),
            ..ProviderErrorOptions::default()
        };
        Err(Box::new(ProviderError::new(message, options)))
    }

    /// What the history may take of the window for `current_event`, once
    /// `inner`'s messages and notes, and the turn, are paid for.
    fn budget(&self, inner: &MemoryResult, current_event: &RoomEvent) -> usize {
        turn_history_budget(
            self.max_context_tokens,
            self.options.safety_margin_ratio,
            current_event,
            self.options.reserved_tokens,
            Some(inner),
        )
    }

    /// `events`' latest that fit `budget`, the latest `min_events` always.
    fn trimmed(&self, mut events: Vec<RoomEvent>, budget: usize) -> Vec<RoomEvent> {
        let mut total = 0;
        let mut keep_from = 0;
        // Each event with what the history replays with it: a turn is kept or
        // cut whole, its rounds of tool calls with it (RFC §20).
        let costs = history_event_costs(&events);
        for (at, cost) in costs.iter().copied().enumerate().rev() {
            if total + cost > budget && events.len() - at > self.options.min_events {
                keep_from = at + 1;
                break;
            }
            total += cost;
        }
        if keep_from > 0 {
            tracing::info!(target: "roomkit::memory", trimmed = keep_from, kept_tokens = total, budget, "trimmed the oldest events to fit the context budget");
        }
        events.split_off(keep_from)
    }
}

impl MemoryProvider for BudgetAwareMemory {
    fn name(&self) -> String {
        format!("BudgetAwareMemory({})", self.inner.name())
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
            self.refuse_overflow(current_event)?;
            let inner = self
                .inner
                .retrieve(room_id, current_event, context, options)
                .await?;
            let budget = self.budget(&inner, current_event);
            let events = self.trimmed(inner.events, budget);
            Ok(MemoryResult { events, ..inner })
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

    fn clear<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.inner.clear(room_id)
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests;
