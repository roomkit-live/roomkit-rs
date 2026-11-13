//! An external agent already consumed its call's result. Hooks can chain
//! their own rewrites, but observers still hear what the agent consumed.

use super::{
    HookExecution, HookTrigger, ToolCallChain, ToolCallOutcome, ToolCallReport, TurnHost, hooks,
};

impl TurnHost {
    /// Reports an external call (RFC §9.3), as Python's executor wiring
    /// `report_tool_call`: run the SYNC chain for a call that ran, discard
    /// its verdict, then report the original outcome to ASYNC observers.
    /// Cancelled or refused calls reach the observers alone.
    #[doc(hidden)]
    pub async fn report_external_tool_call(&self, report: &ToolCallReport) {
        crate::context::ExecutionContext::current()
            .external_agent(self.room_id())
            .scope(self.external_tool_report(report))
            .await;
    }

    async fn external_tool_report(&self, report: &ToolCallReport) {
        if !matches!(
            report.outcome,
            Some(ToolCallOutcome::Cancelled | ToolCallOutcome::Refused)
        ) {
            self.external_call_chain(report).await;
        }
        self.report_tool_call(report);
    }

    async fn external_call_chain(&self, report: &ToolCallReport) {
        let hooks = self.hooks(HookTrigger::OnToolCall);
        let mut chain = ToolCallChain::new(self.event_of(report));
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run_traced(
                spec,
                std::sync::Arc::clone(handler),
                chain.current.clone(),
                self.shared.context.clone(),
                |_| None,
            )
            .await;
            self.report_timeout(spec, &ran);
            if chain.read(spec, ran).is_some() {
                break;
            }
        }
    }
}
