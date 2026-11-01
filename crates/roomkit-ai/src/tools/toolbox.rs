//! An agent's tools: its toolsets, and which serves each name (RFC §21.1
//! "One tool per name").

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use roomkit::model::{ToolCall, ToolCallOutcome};

use super::{DynToolset, ToolContext, ToolDefinition, ToolPolicy, Toolset};
use crate::error::AiError;

/// The toolsets an agent took, and the tools they declare, in the order
/// they were given: each name served by one toolset.
#[derive(Clone, Default)]
pub(crate) struct Toolbox {
    toolsets: Vec<Arc<dyn DynToolset>>,
    tools: Vec<(ToolDefinition, usize)>,
    served: HashMap<String, usize>,
    /// The tools the agent serves itself that only read or unlock, which
    /// the policy does not govern (RFC §21.1 "Tools the channel provides").
    exempt: HashSet<String>,
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
        self.insert(Arc::new(toolset), declared, false)
    }

    /// Takes `toolset`, the agent's own, declaring `declared`: tools that
    /// only read or unlock, exempt from the policy (RFC §21.1).
    ///
    /// # Errors
    ///
    /// As [`add`](Self::add).
    pub(crate) fn add_own(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<ToolDefinition>,
    ) -> Result<(), AiError> {
        self.insert(toolset, declared, true)
    }

    fn insert(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<ToolDefinition>,
        exempt: bool,
    ) -> Result<(), AiError> {
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
        self.toolsets.push(toolset);
        for tool in declared {
            self.served.insert(tool.name.clone(), position);
            if exempt {
                self.exempt.insert(tool.name.clone());
            }
            self.tools.push((tool, position));
        }
        Ok(())
    }

    /// Whether the model may call the tool `name` under `policy`: the
    /// policy's word, save for a tool of the agent's own that it does not
    /// govern (RFC §21.1).
    pub(crate) fn permits(&self, policy: &ToolPolicy, name: &str) -> bool {
        self.exempt.contains(name) || policy.is_allowed(name)
    }

    /// Whether a call to `name` is work the agent did, which its later
    /// turns replay (RFC §6.4 "The tools of earlier turns"): not a call to
    /// a tool of its own, which only reads or unlocks (its skills'), as the
    /// reference implementation's `in_tool_memory` trait reads them.
    pub(crate) fn in_tool_memory(&self, name: &str) -> bool {
        !self.exempt.contains(name)
    }

    /// The tools `policy` lets the model call, in order: what each round
    /// declares (RFC §21.1).
    pub(crate) fn declared(&self, policy: &ToolPolicy) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .filter(|(tool, _)| self.permits(policy, &tool.name))
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

    /// Closes each toolset, one after the other, in the order the agent
    /// took them.
    pub(crate) async fn close(&self) {
        for toolset in &self.toolsets {
            toolset.close().await;
        }
    }

    /// Tells the toolset serving `call`'s tool how the call ended, once its
    /// hooks ran.
    pub(crate) fn settled(&self, call: &ToolCall, outcome: ToolCallOutcome, context: &ToolContext) {
        if let Some((_, toolset)) = self.find(&call.name) {
            toolset.settled(call, outcome, context);
        }
    }
}
