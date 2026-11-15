//! Skill access is narrower than the policy-admitted catalogue (RFC §24.2).

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{ToolCall, ToolCallOutcome};

use super::Loop;
use crate::skills::tools::SkillTools;
use crate::tools::{AiTool, ToolContext};

/// The selected registry and activations owned by this turn, together.
pub(crate) struct SkillGates {
    skills: Option<Arc<SkillTools>>,
    activated: Mutex<HashSet<String>>,
}

impl SkillGates {
    pub(crate) fn new(skills: Option<Arc<SkillTools>>) -> Self {
        Self {
            skills,
            activated: Mutex::default(),
        }
    }

    fn gates(&self, name: &str, context: &ToolContext) -> bool {
        let Some(skills) = &self.skills else {
            return false;
        };
        let activated = self
            .activated
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        skills.gates(name, context, &activated)
    }

    fn settle(&self, call: &ToolCall, outcome: ToolCallOutcome, context: &ToolContext) {
        if let Some(name) = self
            .skills
            .as_ref()
            .and_then(|skills| skills.record_activation(call, outcome, context))
        {
            self.activated
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(name);
        }
    }
}

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
        self.skill_gates.gates(name, &self.context)
    }

    /// Settle before references are collected and before sibling calls can
    /// finish. Only a served, existing skill changes activation state.
    pub(super) fn settle_skill(&self, call: &ToolCall, outcome: ToolCallOutcome) {
        self.skill_gates.settle(call, outcome, &self.context);
    }
}
