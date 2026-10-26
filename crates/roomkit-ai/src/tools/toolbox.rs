//! An agent's tools: its toolsets, and which serves each name (RFC §21.1
//! "One tool per name").

use std::collections::HashMap;
use std::sync::Arc;

use super::{DynToolset, ToolDefinition, ToolPolicy, Toolset};
use crate::error::AiError;

/// The toolsets an agent took, and the tools they declare, in the order
/// they were given: each name served by one toolset.
#[derive(Clone, Default)]
pub(crate) struct Toolbox {
    toolsets: Vec<Arc<dyn DynToolset>>,
    tools: Vec<(ToolDefinition, usize)>,
    served: HashMap<String, usize>,
}

impl std::fmt::Debug for Toolbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self
            .tools
            .iter()
            .map(|(tool, _)| tool.name.as_str())
            .collect();
        formatter
            .debug_struct("Toolbox")
            .field("tools", &names)
            .finish_non_exhaustive()
    }
}

impl Toolbox {
    /// Takes `toolset`, whose tools join the agent's.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::DuplicateTool`] for a name the agent serves
    /// already, or that the toolset declares twice; the toolbox is left as
    /// it was.
    pub(crate) fn add(&mut self, toolset: impl Toolset) -> Result<(), AiError> {
        let declared = toolset.tools();
        let mut names: Vec<&str> = Vec::with_capacity(declared.len());
        for tool in &declared {
            if self.served.contains_key(&tool.name) || names.contains(&tool.name.as_str()) {
                return Err(AiError::DuplicateTool {
                    name: tool.name.clone(),
                });
            }
            names.push(&tool.name);
        }
        let position = self.toolsets.len();
        self.toolsets.push(Arc::new(toolset));
        for tool in declared {
            self.served.insert(tool.name.clone(), position);
            self.tools.push((tool, position));
        }
        Ok(())
    }

    /// The tools `policy` lets the model call, in order: what each round
    /// declares (RFC §21.1).
    pub(crate) fn declared(&self, policy: &ToolPolicy) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .filter(|(tool, _)| policy.permits(&tool.name))
            .map(|(tool, _)| tool.clone())
            .collect()
    }

    /// The tool `name`, and the toolset serving it.
    pub(crate) fn find(&self, name: &str) -> Option<(&ToolDefinition, Arc<dyn DynToolset>)> {
        let position = *self.served.get(name)?;
        let tool = self
            .tools
            .iter()
            .find(|(tool, _)| tool.name == name)
            .map(|(tool, _)| tool)?;
        let toolset = self.toolsets.get(position)?;
        Some((tool, Arc::clone(toolset)))
    }
}
