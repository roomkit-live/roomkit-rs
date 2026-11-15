//! Skill access is narrower than the policy-admitted catalogue (RFC §24.2).

use std::sync::PoisonError;

use roomkit::model::{ToolCall, ToolCallOutcome};

use super::Loop;
use crate::tools::AiTool;

impl Loop {
    /// Ordinary schemas, search and hints all read the same reachable set.
    /// Deferred providers still hold the full admitted definitions unseen.
    pub(super) fn reachable(&self) -> Vec<AiTool> {
        self.declared()
            .into_iter()
            .filter(|tool| !self.skill_gated(&tool.name))
            .collect()
    }

    pub(super) fn skill_gated(&self, name: &str) -> bool {
        if self.toolbox.is_exempt(name) {
            return false;
        }
        let Some(skills) = &self.skills else {
            return false;
        };
        let activated = self
            .activated_skills
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        skills.gates(name, &self.context, &activated)
    }

    /// Settle before references are collected and before sibling calls can
    /// finish. Only a served, existing skill changes activation state.
    pub(super) fn settle_skill(&self, call: &ToolCall, outcome: ToolCallOutcome) {
        if let Some(name) = self
            .skills
            .as_ref()
            .and_then(|skills| skills.record_activation(call, outcome, &self.context))
        {
            self.activated_skills
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(name);
        }
    }
}
