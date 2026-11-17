//! The AI loop's per-occurrence right, carried through runtime cancellation.

use std::sync::Arc;

use super::{ToolCall, TurnHost};

#[derive(Clone)]
pub(super) struct ReportClaim {
    right: Arc<dyn Fn() -> bool + Send + Sync>,
    scope: Option<Arc<dyn crate::ToolCallScope>>,
}

impl std::fmt::Debug for ReportClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReportClaim")
            .finish_non_exhaustive()
    }
}

impl ReportClaim {
    pub(super) fn claim(&self) -> bool {
        (self.right)()
    }

    fn in_context<T>(&self, work: impl FnOnce() -> T) -> T {
        match &self.scope {
            Some(scope) => crate::tool_call_scope::in_tool_call_scope(Arc::clone(scope), work),
            None => work(),
        }
    }
}

#[derive(Debug)]
pub(super) struct RunningCall {
    pub(super) call: ToolCall,
    pub(super) claim: Option<ReportClaim>,
}

impl TurnHost {
    pub(super) fn in_report_context<T>(&self, work: impl FnOnce() -> T) -> T {
        match &self.report_claim {
            Some(claim) => claim.in_context(work),
            None => work(),
        }
    }

    pub(super) fn claim_tool_report(&self) -> bool {
        self.report_claim.as_ref().is_none_or(ReportClaim::claim)
    }

    /// Internal AI-loop bridge: this occurrence's observer report and its
    /// cancellation fallback share one right. Model results remain independent.
    #[doc(hidden)]
    #[must_use]
    pub fn with_tool_report_claim(&self, claim: Arc<dyn Fn() -> bool + Send + Sync>) -> Self {
        let mut host = self.clone();
        host.report_claim = Some(ReportClaim {
            right: claim,
            scope: crate::tool_call_scope::current_tool_call_scope(),
        });
        host
    }
}
