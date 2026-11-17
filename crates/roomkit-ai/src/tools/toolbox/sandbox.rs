//! Sandbox declarations follow skills, remain policy governed, and may change per turn.
use std::sync::Arc;

use super::{AiError, AiTool, DynToolset, Toolbox};

impl Toolbox {
    pub(crate) fn add_sandbox(
        &mut self,
        tools: Arc<crate::sandbox::tools::SandboxTools>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        let position = self.toolsets.len();
        let toolset: Arc<dyn DynToolset> =
            Arc::<crate::sandbox::tools::SandboxTools>::clone(&tools);
        self.insert(toolset, declared, false)?;
        self.sandbox = Some((position, tools));
        Ok(())
    }

    pub(crate) fn with_sandbox(&self, declared: Vec<AiTool>) -> Self {
        let Some((position, _)) = &self.sandbox else {
            return self.clone();
        };
        let position = *position;
        let mut turn = self.clone();
        let mut before = 0;
        let mut host = 0;
        turn.tools = self.tools.iter().enumerate().filter_map(|(index, (tool, at))| {
            let collision = *at != position && declared.iter().any(|given| given.name == tool.name);
            if *at == position || collision {
                if collision {
                    tracing::warn!(target: "roomkit::ai", tool = %tool.name, "a tool under a name the sandbox serves is not declared");
                }
                before += usize::from(index < self.injected_at);
                host += usize::from(index < self.host_count);
                turn.served.remove(&tool.name);
                None
            } else { Some((tool.clone(), *at)) }
        }).collect();
        turn.host_count -= host;
        turn.injected_at -= before;
        let before_planner = turn
            .tools
            .iter()
            .position(|(tool, _)| turn.planning && tool.name == crate::planning::PLAN_TOOL)
            .unwrap_or(turn.tools.len());
        for (offset, tool) in declared.into_iter().enumerate() {
            turn.served.insert(tool.name.clone(), position);
            turn.tools.insert(before_planner + offset, (tool, position));
        }
        turn
    }

    /// Catalogue reads are fallible even when a call eventually belongs to the host.
    pub(crate) fn live_sandbox(&self, name: &str) -> Result<bool, AiError> {
        let Some((_, sandbox)) = &self.sandbox else {
            return Ok(false);
        };
        Ok(sandbox.names()?.iter().any(|tool| tool.name == name))
    }

    /// A declaration remains in this turn's context after its sandbox stops serving it.
    pub(crate) fn serving_now(&self, name: &str) -> Result<Option<Arc<dyn DynToolset>>, AiError> {
        let original = || self.find(name).map(|(_, tools)| tools);
        if self.reserved.contains(name) || self.injected.contains(name) {
            return Ok(original());
        }
        if self.live_sandbox(name)? {
            return Ok(self
                .sandbox
                .as_ref()
                .and_then(|(at, _)| self.toolsets.get(*at).cloned()));
        }
        if self.is_sandbox(name) {
            return Ok(self.handler.and_then(|at| self.toolsets.get(at).cloned()));
        }
        Ok(original())
    }

    /// Python exempts Bash's default bound whenever a sandbox is attached,
    /// even if that exact name belongs to the host rather than its catalogue.
    pub(crate) fn sandbox_waits(&self, name: &str) -> bool {
        self.sandbox.is_some() && name == "sandbox_bash"
    }

    pub(crate) fn is_sandbox(&self, name: &str) -> bool {
        self.sandbox
            .as_ref()
            .is_some_and(|(at, _)| self.served.get(name) == Some(at))
    }

    pub(crate) fn sandbox_allowed(&self, policy: &super::ToolPolicy) -> bool {
        self.tools
            .iter()
            .any(|(tool, _)| self.is_sandbox(&tool.name) && self.permits(policy, &tool.name))
    }
}
