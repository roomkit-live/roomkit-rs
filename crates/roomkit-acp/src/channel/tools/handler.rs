//! Delegate one closed call's report, falling back only before observers heard it.

use roomkit::model::{RoomId, ToolCallOutcome};
use roomkit::{ToolCallReport, TurnHost};
use roomkit_ai::{
    ExternalToolHandler, OnToolCancelledOptions, OnToolRefusedOptions, OnToolResultOptions,
    ToolError,
};

pub(super) async fn report(
    host: Option<&TurnHost>,
    handler: Option<&dyn ExternalToolHandler>,
    room: &RoomId,
    report: ToolCallReport,
    observe_only: bool,
) {
    if !observe_only && let Some(handler) = handler {
        let (result, reached) = roomkit::external_tools::watch_external_tool_report(
            &report.call.id,
            delegate(handler, room, &report),
        )
        .await;
        match result {
            Ok(()) => return,
            Err(error) => {
                tracing::error!(target: "roomkit_acp", %error, "External tool handler failed reporting the ACP call");
                if reached {
                    return;
                }
            }
        }
    }
    if let Some(host) = host {
        if observe_only {
            host.observe_external_tool_call(&report).await;
        } else {
            host.report_external_tool_call(&report).await;
        }
    }
}

async fn delegate(
    handler: &dyn ExternalToolHandler,
    room: &RoomId,
    report: &ToolCallReport,
) -> Result<(), ToolError> {
    let call = &report.call;
    let result = report.result.as_str().unwrap_or_default();
    match report.outcome {
        Some(ToolCallOutcome::Cancelled) => {
            handler
                .on_tool_cancelled(
                    &call.name,
                    &call.arguments,
                    OnToolCancelledOptions {
                        tool_call_id: call.id.clone(),
                        room_id: Some(room.clone()),
                        ..OnToolCancelledOptions::default()
                    },
                )
                .await
        }
        Some(ToolCallOutcome::Refused) => {
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
        }
        _ => {
            handler
                .on_tool_result(
                    &call.name,
                    &call.arguments,
                    result,
                    OnToolResultOptions {
                        tool_call_id: call.id.clone(),
                        room_id: Some(room.clone()),
                        is_error: report.outcome != Some(ToolCallOutcome::Served),
                        refused_but_ran: report.refused_but_ran,
                        error_detail: report
                            .refused_but_ran
                            .then(|| report.error_detail.clone())
                            .flatten(),
                        ..OnToolResultOptions::default()
                    },
                )
                .await
        }
    }
}
