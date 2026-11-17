//! Calls already served by a provider: report first, then hand their rows.

use std::sync::Arc;

use roomkit::model::{RoomId, ToolCall, ToolCallOutcome};
use roomkit::{ToolCallReport, TurnHost};
use serde_json::Value;
use tokio::time::Instant;

use super::tool_events::{Ended, ToolEvents};
use super::{Loop, ToolEnd, TurnItem};
use crate::provider::{AiToolCall, ProviderError, ServedCall, StreamToolCall};
use crate::rows::end_row;
use crate::stream::Yielder;
use crate::tool_eviction::ToolEviction;
use crate::tools::ToolResult;

/// An attempt's provider-call bridge, independent of local tool authorization.
pub(super) struct ProviderCalls {
    turn: Option<TurnHost>,
    events: Option<ToolEvents>,
    eviction: Arc<ToolEviction>,
    room_id: RoomId,
}

impl Loop {
    pub(super) fn provider_calls(&self, round: u32) -> ProviderCalls {
        ProviderCalls {
            turn: self.turn.clone(),
            events: ToolEvents::new(
                self.turn.as_ref(),
                (&self.context.room_id, &self.context.channel_id),
                round,
                (
                    self.thinking_publication.answers_room,
                    self.thinking_publication.publishes_tool_call_text,
                ),
            ),
            eviction: Arc::clone(&self.eviction),
            room_id: self.context.room_id.clone(),
        }
    }
}

impl ProviderCalls {
    /// The provider already acted. A SYNC report cannot replace its result;
    /// only copies delivered to the model and timeline are bounded.
    pub(super) async fn hand(
        &self,
        call: StreamToolCall,
        served: ServedCall,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> (AiToolCall, ToolEnd) {
        let started = Instant::now();
        let ran = ToolCall::new(call.id.clone(), call.name.clone(), call.arguments.clone());
        self.report(&ran, &served).await;
        out.give(Ok(TurnItem::ToolStart(ran.clone()))).await;
        if let Some(events) = &self.events {
            events.started([&ran]);
        }
        let end = self.end(ran, &served, started.elapsed());
        if let Some(turn) = &self.turn {
            turn.call_ended(0, end_row(end.clone()));
        }
        let closing = Closing {
            events: self.events.clone(),
            turn: self.turn.clone(),
            ended: Ended {
                call: end.call.clone(),
                result: end.result.as_text(),
                failed: served.is_error,
            },
            duration_ms: u64::try_from(end.duration.as_millis()).unwrap_or(u64::MAX),
        };
        out.give(Ok(TurnItem::ToolEnd(end.clone()))).await;
        closing.handed();
        (call.into(), end)
    }
    async fn report(&self, ran: &ToolCall, served: &ServedCall) {
        if let Some(turn) = &self.turn {
            let report = ToolCallReport::new(ran.clone())
                .ended(outcome(served), Value::String(served.result.clone()));
            turn.report_provider_tool_call(report).await;
        }
    }

    fn end(&self, ran: ToolCall, served: &ServedCall, duration: std::time::Duration) -> ToolEnd {
        let result = self.eviction.bounded(
            self.room_id.as_str(),
            ToolResult::Text(served.result.clone()),
            &ran.id,
        );
        ToolEnd {
            call: ran,
            outcome: outcome(served),
            result,
            duration,
            announced_only: false,
            structured_content: None,
            references: Vec::new(),
        }
    }
}

/// A provider's END handed to the reader still needs its publication when
/// that reader stops without asking for the next item.
struct Closing {
    events: Option<ToolEvents>,
    turn: Option<TurnHost>,
    ended: Ended,
    duration_ms: u64,
}

impl Closing {
    fn handed(mut self) {
        if let Some(events) = self.events.take() {
            events.ended(std::slice::from_ref(&self.ended), Some(self.duration_ms));
        }
    }
}

impl Drop for Closing {
    fn drop(&mut self) {
        let Some(events) = self.events.take() else {
            return;
        };
        if self.turn.as_ref().is_some_and(TurnHost::stream_was_cut) {
            events.ended(&[Ended::aborted(self.ended.call.clone())], None);
        } else {
            events.ended(std::slice::from_ref(&self.ended), Some(self.duration_ms));
        }
    }
}

fn outcome(served: &ServedCall) -> ToolCallOutcome {
    if served.is_error {
        ToolCallOutcome::Failed
    } else {
        ToolCallOutcome::Served
    }
}

impl super::generation::Generation {
    pub(super) fn provider_only(&self) -> bool {
        self.calls.is_empty() && !self.provider_calls.is_empty()
    }

    /// One assistant message, then the provider results, before local results.
    pub(super) fn replay(self) -> Vec<roomkit_core::rules::transcript::Message> {
        use roomkit_core::rules::transcript::Message;
        let made = self
            .provider_results
            .iter()
            .map(|end| end.call.clone())
            .chain(self.calls.into_iter().map(|called| called.call))
            .collect();
        let mut assistant = Message::assistant_calls(self.text, made);
        assistant.metadata = super::replayed(self.done.metadata);
        let mut read = vec![assistant];
        read.extend(self.provider_results.into_iter().map(|end| {
            let mut result = Message::tool_result(end.call.id.clone(), end.result.as_text());
            result.is_error = end.outcome != ToolCallOutcome::Served;
            result.outcome = Some(end.outcome);
            result
        }));
        read
    }
}
