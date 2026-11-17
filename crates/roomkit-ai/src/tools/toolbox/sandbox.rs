//! Sandbox declarations follow skills, remain policy governed, and may change per turn.
use std::sync::Arc;

use super::{AiError, AiTool, DynToolset, Toolbox};

impl Toolbox {
    pub(crate) fn add_sandbox(
        &mut self,
        tools: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        let position = self.toolsets.len();
        self.insert(tools, declared, false)?;
        self.sandbox = Some(position);
        Ok(())
    }

    pub(crate) fn with_sandbox(&self, declared: Vec<AiTool>) -> Self {
        let Some(position) = self.sandbox else {
            return self.clone();
        };
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
        for tool in declared {
            turn.served.insert(tool.name.clone(), position);
            turn.tools.push((tool, position));
        }
        turn
    }

    /// Python exempts Bash's default bound whenever a sandbox is attached,
    /// even if that exact name belongs to the host rather than its catalogue.
    pub(crate) fn sandbox_waits(&self, name: &str) -> bool {
        self.sandbox.is_some() && name == "sandbox_bash"
    }

    pub(crate) fn is_sandbox(&self, name: &str) -> bool {
        self.sandbox
            .is_some_and(|at| self.served.get(name) == Some(&at))
    }

    pub(crate) fn sandbox_allowed(&self, policy: &super::ToolPolicy) -> bool {
        self.tools
            .iter()
            .any(|(tool, at)| Some(*at) == self.sandbox && self.permits(policy, &tool.name))
    }
}
