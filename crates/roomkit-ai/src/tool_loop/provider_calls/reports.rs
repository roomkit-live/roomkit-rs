//! One report per occurrence, including work a dropped producer still owes.

use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{RoomId, ToolCall, ToolCallOutcome};
use roomkit::{ToolCallReport, TurnHost};
use serde_json::Value;

use crate::tools::{
    ExternalToolHandler, Occurrence, OnToolCancelledOptions, OnToolRefusedOptions,
    OnToolResultOptions, ToolError,
};

use super::ProviderCalls;
use super::decision::Decided;

impl ProviderCalls {
    pub(super) async fn report(&self, decided: &Decided, entry: &Arc<Occurrence>) {
        if !decided.by_channel
            && let Some(handler) = &self.handler
        {
            let (result, reached) = roomkit::external_tools::watch_external_tool_report(
                &decided.report.call.id,
                delegate(&**handler, &self.room_id, &decided.report),
            )
            .await;
            match result {
                Ok(()) => {
                    entry.claim();
                    return;
                }
                Err(error) => {
                    tracing::error!(target: "roomkit::ai", tool_call_id = %decided.report.call.id, %error, "External tool handler failed reporting the call");
                    if reached {
                        entry.claim();
                        return;
                    }
                }
            }
        }
        if let Some(turn) = &self.turn {
            let turn = turn.with_tool_report_claim(entry.callback());
            if decided.by_channel || decided.report.outcome == Some(ToolCallOutcome::Refused) {
                turn.observe_external_tool_call(&decided.report).await;
            } else {
                turn.report_provider_tool_call(decided.report.clone()).await;
            }
        }
        entry.claim();
    }
}

async fn delegate(
    handler: &dyn ExternalToolHandler,
    room: &RoomId,
    report: &ToolCallReport,
) -> Result<(), ToolError> {
    let call = &report.call;
    let result = report.result.as_str().unwrap_or_default();
    if report.outcome == Some(ToolCallOutcome::Refused) {
        handler
            .on_tool_refused(
                &call.name,
                &call.arguments,
                result,
                OnToolRefusedOptions {
                    tool_call_id: call.id.clone(),
                    room_id: Some(room.clone()),
                    detail: report.error_detail.clone(),
                    ..OnToolRefusedOptions::default()
                },
            )
            .await
    } else {
        handler
            .on_tool_result(
                &call.name,
                &call.arguments,
                result,
                OnToolResultOptions {
                    tool_call_id: call.id.clone(),
                    room_id: Some(room.clone()),
                    is_error: report.outcome != Some(ToolCallOutcome::Served),
                    ..OnToolResultOptions::default()
                },
            )
            .await
    }
}

/// Holds the original outcome before a report can suspend. Cleanup is captured
/// now and scheduled as tracked observer work if the producer is dropped.
pub(super) struct Recovery {
    known: Arc<Mutex<Option<ToolCallReport>>>,
    cleanup: Option<Box<dyn FnOnce() + Send>>,
}

impl Recovery {
    pub(super) fn new(calls: &ProviderCalls, call: ToolCall, entry: Arc<Occurrence>) -> Self {
        let known = Arc::new(Mutex::new(None));
        let reading = Arc::clone(&known);
        let handler = calls.handler.clone();
        let room = calls.room_id.clone();
        let turn = calls.turn.clone();
        let work = crate::tools::within_reporting(Arc::clone(&entry), async move {
            recover(turn, handler, room, call, entry, reading).await;
        });
        let work = Box::pin(work);
        let cleanup = Some(match &calls.turn {
            Some(turn) => turn.tool_report_cleanup(work),
            None => calls.reports.prepare(work),
        });
        Self { known, cleanup }
    }

    pub(super) fn known(&self, report: &ToolCallReport) {
        *self.known.lock().unwrap_or_else(PoisonError::into_inner) = Some(report.clone());
    }

    pub(super) fn complete(&mut self) {
        self.cleanup.take();
    }
}

impl Drop for Recovery {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

async fn recover(
    turn: Option<TurnHost>,
    handler: Option<Arc<dyn ExternalToolHandler>>,
    room: RoomId,
    call: ToolCall,
    entry: Arc<Occurrence>,
    known: Arc<Mutex<Option<ToolCallReport>>>,
) {
    if entry.reported() {
        return;
    }
    let known = known.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let report = if let Some(report) = known {
        report
    } else {
        if let Some(handler) = handler {
            let (result, reached) = roomkit::external_tools::watch_external_tool_report(
                &call.id,
                handler.on_tool_cancelled(
                    &call.name,
                    &call.arguments,
                    OnToolCancelledOptions {
                        tool_call_id: call.id.clone(),
                        room_id: Some(room),
                        ..OnToolCancelledOptions::default()
                    },
                ),
            )
            .await;
            match result {
                Ok(()) => {
                    entry.claim();
                    return;
                }
                Err(error) => {
                    tracing::error!(target: "roomkit::ai", tool_call_id = %call.id, %error, "External tool handler failed on the cut call");
                    if reached {
                        entry.claim();
                        return;
                    }
                }
            }
        }
        let text = cancelled(&call.name, false);
        ToolCallReport::new(call).ended(ToolCallOutcome::Cancelled, Value::String(text))
    };
    if let Some(turn) = turn {
        turn.with_tool_report_claim(entry.callback())
            .observe_external_tool_call(&report)
            .await;
    }
    entry.claim();
}

pub(super) fn cancelled(name: &str, closed: bool) -> String {
    let hint = if closed {
        "The channel closed before its result."
    } else {
        "The turn ended before its result."
    };
    crate::dumps(&serde_json::json!({"error":"Tool call cancelled","tool":name,"hint":hint}))
}
