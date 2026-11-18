//! Provider-owned calls: original outcomes, or decisions by an external handler.

mod closing;
mod decision;
mod reports;

use std::collections::HashSet;
use std::sync::Arc;

use roomkit::model::{RoomId, ToolCall, ToolCallOutcome};
use roomkit::{ToolCallReport, TurnHost};
use serde_json::Value;
use tokio::time::Instant;

use super::tool_events::ToolEvents;
use super::{Loop, ToolEnd, TurnItem};
use crate::provider::{AiToolCall, ProviderError, StreamToolCall};
use crate::rows::end_row;
use crate::steering::Steering;
use crate::stream::Yielder;
use crate::tool_eviction::ToolEviction;
use crate::tools::{ExternalToolHandler, Occurrence, ToolResult};
use closing::Closing;
use decision::Decided;
use reports::Recovery;

/// An attempt's external-call bridge, independent of local authorization.
pub(super) struct ProviderCalls {
    turn: Option<TurnHost>,
    events: Option<ToolEvents>,
    eviction: Arc<ToolEviction>,
    room_id: RoomId,
    handler: Option<Arc<dyn ExternalToolHandler>>,
    local: HashSet<String>,
    reports: Arc<crate::shutdown::Reports>,
}

impl Loop {
    pub(super) fn provider_calls(&self, round: u32) -> ProviderCalls {
        ProviderCalls {
            turn: self.turn.clone(),
            events: ToolEvents::new(
                self.context.callbacks.as_ref(),
                (&self.context.room_id, &self.context.channel_id),
                round,
                (
                    self.thinking_publication.answers_room,
                    self.thinking_publication.publishes_tool_call_text,
                ),
            ),
            eviction: Arc::clone(&self.eviction),
            room_id: self.context.room_id.clone(),
            handler: self.external_tool_handler.clone(),
            reports: Arc::clone(&self.loops.reports),
            local: self
                .toolbox
                .own_names()
                .cloned()
                .chain(self.adopted.withdrawn.iter().cloned())
                .collect(),
        }
    }
}

impl ProviderCalls {
    pub(super) fn takes(&self, call: &StreamToolCall) -> bool {
        call.served.is_some() || self.handler.is_some() && !self.local.contains(&call.name)
    }

    pub(super) async fn hand(
        &self,
        call: StreamToolCall,
        steering: &Steering,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> (AiToolCall, ToolEnd) {
        let entry = crate::tools::announce_tool_call(&call.id);
        crate::tools::within_reporting(
            Arc::clone(&entry),
            self.hand_entry(call, &entry, steering, out),
        )
        .await
    }

    /// Known results are reported before START. Pending decisions start only
    /// once the reader asks past START; their known end precedes their report.
    async fn hand_entry(
        &self,
        mut call: StreamToolCall,
        entry: &Arc<Occurrence>,
        steering: &Steering,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> (AiToolCall, ToolEnd) {
        let started = Instant::now();
        let ran = ToolCall::new(call.id.clone(), call.name.clone(), call.arguments.clone());
        let mut recovery = Recovery::new(self, ran.clone(), Arc::clone(entry));
        let known = call.served.as_ref().map(|served| Decided {
            report: ToolCallReport::new(ran.clone()).ended(
                if served.is_error {
                    ToolCallOutcome::Failed
                } else {
                    ToolCallOutcome::Served
                },
                Value::String(served.result.clone()),
            ),
            by_channel: false,
        });
        if let Some(known) = &known {
            recovery.known(&known.report);
            self.report(known, entry).await;
            recovery.complete();
        }
        out.give(Ok(TurnItem::ToolStart(ran.clone()))).await;
        let mut closing = Closing::started(self.events.clone(), self.turn.clone(), &ran);
        let pending = known.is_none();
        let decided = match known {
            Some(known) => Some(known),
            None => self.settle(&call, entry, steering).await,
        };
        let report = decided.as_ref().map_or_else(
            || {
                let text = reports::cancelled(&ran.name, true);
                ToolCallReport::new(ran).ended(ToolCallOutcome::Cancelled, Value::String(text))
            },
            |decided| decided.report.clone(),
        );
        let end = self.end(&report, started.elapsed());
        if let Some(turn) = &self.turn {
            turn.call_ended(0, end_row(end.clone()));
        }
        closing.ended(&end);
        if pending && let Some(decided) = &decided {
            recovery.known(&report);
            self.report(decided, entry).await;
            recovery.complete();
        }
        closing.handing();
        out.give(Ok(TurnItem::ToolEnd(end.clone()))).await;
        closing.handed();
        call.arguments.clone_from(&end.call.arguments);
        (call.into(), end)
    }

    fn end(&self, report: &ToolCallReport, duration: std::time::Duration) -> ToolEnd {
        let result = self.eviction.bounded(
            self.room_id.as_str(),
            ToolResult::Text(report.result.as_str().unwrap_or_default().to_owned()),
            &report.call.id,
        );
        ToolEnd {
            call: report.call.clone(),
            outcome: report.outcome.unwrap_or(ToolCallOutcome::Failed),
            result,
            duration,
            announced_only: false,
            structured_content: None,
            references: Vec::new(),
        }
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
