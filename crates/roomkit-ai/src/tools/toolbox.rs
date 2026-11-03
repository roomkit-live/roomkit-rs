//! An agent's tools: its toolsets, and which serves each name (RFC §21.1
//! "One tool per name").

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use roomkit::model::{ToolCall, ToolCallOutcome};

use super::{AiTool, DynToolset, ToolContext, ToolPolicy};
use crate::error::AiError;
use crate::turn_config::Collisions;

/// The toolsets an agent took, and the tools they declare, in the order
/// they were given: each name served by one toolset.
#[derive(Clone, Default)]
pub(crate) struct Toolbox {
    toolsets: Vec<Arc<dyn DynToolset>>,
    tools: Vec<(AiTool, usize)>,
    served: HashMap<String, usize>,
    /// The tools the agent serves itself that only read or unlock, which
    /// the policy does not govern (RFC §21.1 "Tools the channel provides").
    exempt: HashSet<String>,
    /// The toolset of the host's tool handler, with the tools serving
    /// themselves behind it: what serves a tool a generation hook adds
    /// (RFC §6.4).
    handler: Option<usize>,
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
    pub(crate) fn add_dyn(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        self.insert(toolset, declared, false)
    }

    /// Takes `toolset`, the host's tool handler's, whose tools join the
    /// agent's, and which serves a tool a generation hook adds (RFC §6.4).
    ///
    /// # Errors
    ///
    /// As [`add_dyn`](Self::add_dyn).
    pub(crate) fn add_handler(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        let position = self.toolsets.len();
        self.insert(toolset, declared, false)?;
        self.handler = Some(position);
        Ok(())
    }

    /// The toolbox as a turn's `BEFORE_AI_GENERATION` hooks left the tools
    /// it declares (RFC §6.4, What the generation hooks see): a tool of the
    /// host's they redefined is declared and checked as they left it, and
    /// one they added is declared after the others, served by the host's
    /// handler. A name the agent serves itself (its skills', a person's,
    /// the owner's) keeps its definition, and no tool is added under it
    /// (RFC §21.1). What they took out is the loop's to withdraw. Python's
    /// `_adopt_hook_toolset`.
    pub(crate) fn as_left(&self, left: &[AiTool]) -> Self {
        let mut adopted = self.clone();
        for tool in left {
            let served = self.served.get(&tool.name).copied();
            match (served, self.handler) {
                (Some(at), Some(handler)) if at == handler => {
                    for (own, _) in adopted
                        .tools
                        .iter_mut()
                        .filter(|(own, _)| own.name == tool.name)
                    {
                        own.clone_from(tool);
                    }
                }
                (Some(_), _) => {
                    if self.find(&tool.name).is_some_and(|(own, _)| own != tool) {
                        tracing::warn!(target: "roomkit::ai", tool = %tool.name, "a generation hook redefined a tool the channel serves: its definition stands");
                    }
                }
                (None, Some(handler)) => {
                    adopted.served.insert(tool.name.clone(), handler);
                    adopted.tools.push((tool.clone(), handler));
                }
                (None, None) => {
                    tracing::warn!(target: "roomkit::ai", tool = %tool.name, "a generation hook added a tool no handler of the channel serves");
                }
            }
        }
        adopted
    }

    /// The toolbox of one turn (RFC Appendix A.9, per-turn settings):
    /// `base`, the turn's configuration's tools or its binding's, declared
    /// before the agent's own and served by the host's handler, and
    /// `handler`, the turn's own, serving the host's tools in place of the
    /// agent's handler and its tools serving themselves. A base tool under
    /// a name the agent declares already is not declared, the agent's
    /// standing at its place, and a name given twice keeps the later
    /// (Python's `_turn_base_tools`, `declared_once`). Never installed on
    /// the agent: the turn reads its own copy.
    pub(crate) fn for_turn(
        &self,
        base: Vec<AiTool>,
        handler: Option<Arc<dyn DynToolset>>,
        (channel, collisions): (&str, &Collisions),
    ) -> Self {
        let mut turn = self.clone();
        let Some(at) = self.handler else {
            return turn;
        };
        if let (Some(handler), Some(slot)) = (handler, turn.toolsets.get_mut(at)) {
            *slot = handler;
        }
        let mut kept: Vec<AiTool> = Vec::with_capacity(base.len());
        for tool in base {
            match self.served.get(&tool.name) {
                Some(&served) if served == at => collisions.duplicate(channel, &tool.name),
                Some(_) => collisions.served(channel, &tool.name),
                None => {
                    if let Some(earlier) = kept.iter().position(|kept| kept.name == tool.name) {
                        collisions.duplicate(channel, &tool.name);
                        kept.remove(earlier);
                    }
                    kept.push(tool);
                }
            }
        }
        for tool in &kept {
            turn.served.insert(tool.name.clone(), at);
        }
        turn.tools
            .splice(0..0, kept.into_iter().map(|tool| (tool, at)));
        turn
    }

    pub(crate) fn add_own(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        self.insert(toolset, declared, true)
    }

    fn insert(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
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

    /// Serves the names of `names` the agent declares already with
    /// `toolset`, before the toolset that served them (RFC §9.3: a person's
    /// tools before the host's handler).
    pub(crate) fn intercept(&mut self, toolset: Arc<dyn DynToolset>, names: &[String]) {
        let position = self.toolsets.len();
        self.toolsets.push(toolset);
        for name in names {
            if let Some(served) = self.served.get_mut(name) {
                *served = position;
                for (tool, at) in &mut self.tools {
                    if &tool.name == name {
                        *at = position;
                    }
                }
            }
        }
    }

    /// Whether the model may call the tool `name` under `policy`: the
    /// policy's word, save for a tool of the agent's own that it does not
    /// govern (RFC §21.1).
    pub(crate) fn permits(&self, policy: &ToolPolicy, name: &str) -> bool {
        self.exempt.contains(name) || super::policy::call_admitted(policy, name)
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
    pub(crate) fn declared(&self, policy: &ToolPolicy) -> Vec<AiTool> {
        self.tools
            .iter()
            .filter(|(tool, _)| self.permits(policy, &tool.name))
            .map(|(tool, _)| tool.clone())
            .collect()
    }

    /// The tool `name`, and the toolset serving it.
    pub(crate) fn find(&self, name: &str) -> Option<(&AiTool, Arc<dyn DynToolset>)> {
        let position = *self.served.get(name)?;
        let tool = self
            .tools
            .iter()
            .find(|(tool, _)| tool.name == name)
            .map(|(tool, _)| tool)?;
        let toolset = self.toolsets.get(position)?;
        Some((tool, Arc::clone(toolset)))
    }

    /// Tells the toolset serving `call`'s tool how the call ended, once its
    /// hooks ran.
    pub(crate) fn settled(&self, call: &ToolCall, outcome: ToolCallOutcome, context: &ToolContext) {
        if let Some((_, toolset)) = self.find(&call.name) {
            toolset.settled(call, outcome, context);
        }
    }
}
