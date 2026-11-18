//! A wired channel's ordinary tool hooks when it runs without an admitted turn.
use super::Notification;
use crate::model::{ToolCall, ToolCallOutcome};
use crate::turn::{ToolCallChain, ToolCallReport, ToolGate, refused, withheld};
use crate::turns::ChannelTurnHooks;
use crate::{HookExecution, HookTrigger};
use std::sync::Arc;

impl ChannelTurnHooks {
    async fn tool_notification(&self, trigger: HookTrigger) -> Option<Notification> {
        let (wiring, room, channel) = self.wired()?;
        Some(
            Notification::new(
                Arc::clone(&wiring.kit),
                wiring.clone(),
                room.clone(),
                channel.clone(),
                trigger,
            )
            .await,
        )
    }

    /// Run an external agent's report hooks against the consumed value. Only
    /// observers hear a channel-decided permission failure, refusal or cancellation.
    #[doc(hidden)]
    pub async fn report_external_tool_call(&self, report: &ToolCallReport, observe_only: bool) {
        if let Some(host) = self.host() {
            if observe_only {
                host.observe_external_tool_call(report).await;
            } else {
                host.report_external_tool_call(report).await;
            }
            return;
        }
        if let Some(notification) = self.tool_notification(HookTrigger::OnToolCall).await {
            let event = notification.event(report);
            if !observe_only && !event.cancelled && !event.refused {
                notification.report_chain(&event).await;
            }
            notification.observe_completed(&event).await;
        }
    }

    /// Run the ordinary tool gate, claiming the occurrence on a turn when it starts.
    #[doc(hidden)]
    pub async fn before_tool_use(
        &self,
        call: &ToolCall,
        claim: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> ToolGate {
        if let Some(host) = self.host() {
            return host
                .with_tool_report_claim(claim)
                .before_tool_use(call)
                .await;
        }
        let gate = self.tool_notification(HookTrigger::BeforeToolUse).await;
        if let Some(gate) = gate {
            let event = gate.event(&ToolCallReport::new(call.clone()));
            if let Some(chain) = gate.before_chain(&event).await {
                return chain.decision(&call.name);
            }
        }
        refused(&call.name, None, None)
    }

    /// Apply synchronous result hooks before the model reads the result.
    #[doc(hidden)]
    pub async fn on_tool_call(&self, report: ToolCallReport) -> ToolCallReport {
        if let Some(host) = self.host() {
            return host.on_tool_call(report).await;
        }
        if !matches!(
            report.outcome,
            Some(ToolCallOutcome::Served | ToolCallOutcome::Unserved)
        ) {
            return report;
        }
        let Some(notification) = self.tool_notification(HookTrigger::OnToolCall).await else {
            return report;
        };
        let Some(context) = notification.context.as_ref() else {
            return match notification
                .hooks
                .iter()
                .find(|(spec, _)| spec.execution == HookExecution::Sync && spec.fail_closed)
            {
                Some((spec, _)) => withheld(report, &format!("hook_error:{}", spec.name)),
                None => report,
            };
        };
        let event = notification.event(&report);
        ToolCallChain::judge(report, event, &notification.hooks, context, |spec, ran| {
            notification.timeout(spec, ran);
        })
        .await
    }

    /// Notify the wired channel's observers and event outlet before the next round.
    #[doc(hidden)]
    pub async fn report_wired_tool(
        &self,
        report: &ToolCallReport,
        claim: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        if let Some(notification) = self.tool_notification(HookTrigger::OnToolCall).await
            && claim()
        {
            let event = notification.event(report);
            notification.observe_completed(&event).await;
        }
    }

    /// Report a call cancelled by synchronous cleanup on the executor's observer pool.
    #[doc(hidden)]
    pub fn report_abandoned_wired_tool(
        &self,
        report: ToolCallReport,
        claim: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        let hooks = self.clone();
        self.spawn(Box::pin(async move {
            hooks.report_wired_tool(&report, claim).await;
        }));
    }
}
