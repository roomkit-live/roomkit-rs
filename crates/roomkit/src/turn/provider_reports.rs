//! A provider's known outcome is reported once, even when its SYNC hook is cut.

use super::{Arc, ExecutionContext, HookTrigger, ToolCallReport, TurnHost};

impl TurnHost {
    /// Reports a provider-owned call, without authorizing or rewriting it.
    /// The occurrence owns its report; another call holding its wire id is untouched.
    #[doc(hidden)]
    pub async fn report_provider_tool_call(&self, report: ToolCallReport) {
        let mut reporting = Reporting {
            host: self.clone(),
            report: Some(report),
            execution: ExecutionContext::current(),
            context_loaded: false,
        };
        if !self.hooks(HookTrigger::OnToolCall).is_empty() {
            let context = self
                .shared
                .kit
                .external_tool_context(self.room_id().clone())
                .await;
            let Some(context) = context else {
                if let Some(report) = reporting.report.take() {
                    self.report_event_only(&report);
                }
                return;
            };
            reporting.host.tool_report_context = Some(Arc::new(context));
        }
        reporting.context_loaded = true;
        if let Some(report) = &reporting.report {
            reporting.host.external_call_chain(report).await;
        }
        // The same claim handles normal completion and cancellation: there is
        // no await between claiming the report and scheduling its observers.
        reporting.finish();
    }
}

struct Reporting {
    host: TurnHost,
    report: Option<ToolCallReport>,
    execution: ExecutionContext,
    context_loaded: bool,
}

impl Reporting {
    fn finish(&mut self) {
        let Some(report) = self.report.take() else {
            return;
        };
        let floor = self.execution.turn_depth().unwrap_or(0);
        self.host.observe(&report, &self.execution, floor);
    }
}

impl Drop for Reporting {
    fn drop(&mut self) {
        if self.context_loaded {
            self.finish();
            return;
        }
        let Some(report) = self.report.take() else {
            return;
        };
        // A cut while loading hook context still reports the known result.
        // This is observer work: a turn's cut cannot cancel it with its tools.
        let host = self.host.clone();
        let execution = self.execution.clone();
        self.host
            .shared
            .kit
            .spawn_observer(Box::pin(execution.scope(async move {
                let mut reporting = host.clone();
                if !host.hooks(HookTrigger::OnToolCall).is_empty() {
                    let Some(context) = host
                        .shared
                        .kit
                        .external_tool_context(host.room_id().clone())
                        .await
                    else {
                        host.report_event_only(&report);
                        return;
                    };
                    reporting.tool_report_context = Some(Arc::new(context));
                }
                reporting.report_call_under_held_id(&report);
            })));
    }
}
