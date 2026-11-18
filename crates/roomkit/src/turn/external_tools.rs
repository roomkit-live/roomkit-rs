//! An external agent already consumed its call's result. Hooks can chain
//! their own rewrites, but observers still hear what the agent consumed.

use super::{
    Arc, BoxFuture, ConversationStore, HookExecution, HookTrigger, Inner, RoomContext, RoomId,
    ToolCallChain, ToolCallOutcome, ToolCallReport, TurnHost, Weak, hooks,
};

impl crate::turns::ChannelTurnHooks {
    /// Capture the execution context before starting the external agent's callback tasks.
    #[doc(hidden)]
    pub fn external_agent_context<F: Future>(
        &self,
        future: F,
    ) -> impl Future<Output = F::Output> + use<F> {
        self.host()
            .map_or_else(
                crate::context::ExecutionContext::current,
                TurnHost::external_context,
            )
            .scope(future)
    }
}

impl TurnHost {
    /// Captures an external agent's callback context while its own stream
    /// is being read. The copy keeps live admission and budget accounting,
    /// but only the inherited depth: ACP does not run the local AI loop.
    /// Capture once, before the stream ends, so a late report never removes
    /// a still-running parent's depth in place of its own expired frame.
    #[doc(hidden)]
    pub fn external_agent_context<F: Future>(
        &self,
        future: F,
    ) -> impl Future<Output = F::Output> + use<F> {
        self.external_context().scope(future)
    }

    fn external_context(&self) -> crate::context::ExecutionContext {
        crate::context::ExecutionContext::current().external_agent(self.room_id())
    }

    /// Reports an external call (RFC §9.3), as Python's executor wiring
    /// `report_tool_call`: run the SYNC chain for a call that ran, discard
    /// its verdict, then report the original outcome to ASYNC observers.
    /// Cancelled or refused calls reach the observers alone.
    #[doc(hidden)]
    pub async fn report_external_tool_call(&self, report: &ToolCallReport) {
        let mut reporting = self.clone();
        if !self.hooks(HookTrigger::OnToolCall).is_empty() {
            let context = self
                .shared
                .kit
                .external_tool_context(self.room_id().clone())
                .await;
            let Some(context) = context else {
                // As Python: no stale hook context on a failed room read,
                // but the framework still hears the call's one outcome.
                self.emit_tool_call(&self.event_of(report));
                return;
            };
            reporting.tool_report_context = Some(Arc::new(context));
        }
        if !matches!(
            report.outcome,
            Some(ToolCallOutcome::Cancelled | ToolCallOutcome::Refused)
        ) {
            reporting.external_call_chain(report).await;
        }
        reporting.report_tool_call(report);
    }

    /// A failure decided by the channel itself reaches observers alone.
    #[doc(hidden)]
    pub async fn observe_external_tool_call(&self, report: &ToolCallReport) {
        let mut reporting = self.clone();
        if !self.hooks(HookTrigger::OnToolCall).is_empty() {
            let Some(context) = self
                .shared
                .kit
                .external_tool_context(self.room_id().clone())
                .await
            else {
                self.emit_tool_call(&self.event_of(report));
                return;
            };
            reporting.tool_report_context = Some(Arc::new(context));
        }
        reporting.report_tool_call(report);
    }

    pub(super) fn reported_tool_context(&self) -> RoomContext {
        self.tool_report_context
            .as_deref()
            .unwrap_or(&self.shared.context)
            .clone()
    }

    pub(super) async fn external_call_chain(&self, report: &ToolCallReport) {
        let hooks = self.hooks(HookTrigger::OnToolCall);
        let mut chain = ToolCallChain::new(self.event_of(report));
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run_traced(
                spec,
                Arc::clone(handler),
                chain.current.clone(),
                self.reported_tool_context(),
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

pub(super) fn current_context<S: ConversationStore>(
    kit: Weak<Inner<S>>,
    room_id: RoomId,
) -> BoxFuture<'static, Option<RoomContext>> {
    Box::pin(async move {
        let inner = kit.upgrade()?;
        inner
            .wiring()
            .hook_context(&room_id, HookTrigger::OnToolCall, None)
            .await
    })
}
