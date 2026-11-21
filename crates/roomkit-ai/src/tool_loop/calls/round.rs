//! A failed gate ends the concurrent round before reporting its unclaimed calls.

use std::sync::Arc;

use roomkit::ToolCallReport;
use roomkit::model::{ToolCall, ToolCallOutcome};
use serde_json::Value;

use super::super::generation::Called;
use super::super::{Loop, ToolEnd, place_under_its_id};
use crate::provider::ProviderError;
use crate::steering::Steering;
use crate::stream::try_join_all;

impl Loop {
    pub(in crate::tool_loop) async fn serve_round(
        &self,
        calls: &[Called],
        steering: &Steering,
    ) -> Result<Vec<ToolEnd>, ProviderError> {
        // Keep the executed arguments after dropping the futures: a failed gate
        // cuts its pending siblings, whose reports still describe what ran.
        let mut ran: Vec<ToolCall> = calls.iter().map(|called| called.call.clone()).collect();
        let serving = calls
            .iter()
            .zip(&mut ran)
            .enumerate()
            .map(|(position, (called, ran))| {
                self.serve(called, place_under_its_id(calls, position), steering, ran)
            });
        let result = try_join_all(serving.collect()).await;
        if result.is_err() {
            for (called, ran) in calls.iter().zip(ran) {
                if !called.report.reported() {
                    crate::tools::within_reporting(Arc::clone(&called.report), async {
                        self.report_abandoned(called, ran).await;
                    })
                    .await;
                }
            }
        }
        result
    }

    /// Python's `_report_unreported_calls`: observers alone see this cancellation;
    /// the reader closes the announced rows when it reads the round's failure.
    async fn report_abandoned(&self, called: &Called, ran: ToolCall) {
        let tool = serde_json::to_string(&ran.name).unwrap_or_default();
        let text = format!(
            "{{\"error\": \"Tool call cancelled\", \"tool\": {tool}, \"hint\": \"The turn ended before its result.\"}}"
        );
        let report =
            ToolCallReport::new(ran).ended(ToolCallOutcome::Cancelled, Value::String(text));
        let Some(turn) = self.report_host().await else {
            if let Some(hooks) = &self.context.callbacks {
                hooks.report_abandoned_wired_tool(report, called.report.callback());
            }
            return;
        };
        let turn = turn.with_tool_report_claim(called.report.callback());
        if called.duplicate {
            turn.report_call_under_held_id(&report);
        } else {
            turn.report_tool_call(&report);
        }
    }
}
