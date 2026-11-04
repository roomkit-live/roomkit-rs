//! An agent's tools: its toolsets, and which serves each name (RFC §21.1
//! "One tool per name").

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use roomkit::model::{ToolCall, ToolCallOutcome};

use super::{AiTool, DynToolset, ToolContext, ToolPolicy};
use crate::error::AiError;
use crate::tool_search::search_tool_defs;
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
    /// The names of the tools the agent may serve itself in a turn, which no
    /// other tool may take (RFC §21.1): Tool Search's, and the skill tools'
    /// once it was given a registry.
    reserved: HashSet<String>,
    /// The agent, which a refusal of one of those names names.
    agent: String,
    /// The tools that ask a person (RFC §9.3).
    person_tools: HashSet<String>,
    /// The tools orchestration injected for the turn (RFC §21.1).
    injected: HashSet<String>,
    /// Where the tools orchestration injected are declared: after the host's,
    /// before a person's and the skills' (Python's `_turn_base_tools`).
    injected_at: usize,
    /// The toolset of the skill tools, once the agent was given a registry.
    skills: Option<usize>,
    /// The names a registry given keeps for them, a tool under one refused as
    /// any tool under a name served.
    skill_names: HashSet<String>,
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
    /// Takes `toolset`, the host's tool handler's, whose tools join the
    /// agent's, and which serves a tool a generation hook adds (RFC §6.4).
    ///
    /// # Errors
    ///
    /// [`AiError::DuplicateTool`] for a name the agent serves already, or
    /// that the toolset declares twice, [`AiError::ToolSearchName`] for one
    /// reserved; the toolbox is left as it was.
    pub(crate) fn add_handler(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        let position = self.toolsets.len();
        self.insert(toolset, declared, false)?;
        self.handler = Some(position);
        self.injected_at = self.tools.len();
        Ok(())
    }

    /// Takes `toolset`, whose tools ask a person (RFC §9.3): Tool Search never
    /// hides nor counts them.
    ///
    /// # Errors
    ///
    /// As [`add_handler`](Self::add_handler).
    pub(crate) fn add_person(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        // A name the agent serves already is named before one given twice,
        // the first in the host's order, as Python refuses the host's tools
        // against the person's names first.
        let taken = self
            .tools
            .iter()
            .find(|(served, _)| declared.iter().any(|tool| tool.name == served.name));
        if let Some((served, _)) = taken {
            return Err(AiError::DuplicateTool {
                name: served.name.clone(),
            });
        }
        let names: Vec<String> = declared.iter().map(|tool| tool.name.clone()).collect();
        self.insert(toolset, declared, false)?;
        self.person_tools.extend(names);
        Ok(())
    }

    /// Takes `toolset`, whose tools orchestration injected for the turn (RFC
    /// §21.1): Tool Search never hides nor counts them.
    ///
    /// # Errors
    ///
    /// As [`add_handler`](Self::add_handler).
    pub(crate) fn add_injected(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
    ) -> Result<(), AiError> {
        let names: Vec<String> = declared.iter().map(|tool| tool.name.clone()).collect();
        let end = self.tools.len();
        self.insert(toolset, declared, false)?;
        let added = self.tools.split_off(end);
        let at = self.injected_at;
        self.injected_at += added.len();
        self.tools.splice(at..at, added);
        self.injected.extend(names);
        Ok(())
    }

    /// Keeps `names` for `agent` to serve in a turn: no other tool may take
    /// one (RFC §21.1). Before any tool is added.
    pub(crate) fn reserve(&mut self, agent: &str, names: impl IntoIterator<Item = String>) {
        agent.clone_into(&mut self.agent);
        self.reserved.extend(names);
    }

    /// The names of the agent's tools a turn may not hand it under, those
    /// reserved among them: what its kit may not set up on it (RFC §21.1).
    pub(crate) fn own_names(&self) -> impl Iterator<Item = &String> {
        self.tools
            .iter()
            .map(|(tool, _)| &tool.name)
            .chain(&self.reserved)
    }

    /// Takes `toolset`, a registry's skill tools, which declares `declared`,
    /// none for a registry with no skill: the names they take are kept all the
    /// same, `names`, for what a turn may give (RFC §21.1, §24.3). After the
    /// host's tools and a person's.
    ///
    /// # Errors
    ///
    /// [`AiError::DuplicateTool`] for a name the agent serves already.
    pub(crate) fn add_skills(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
        names: &[&str],
    ) -> Result<(), AiError> {
        let position = self.toolsets.len();
        self.insert(toolset, declared, true)?;
        self.skills = Some(position);
        self.skill_names
            .extend(names.iter().map(|name| (*name).to_owned()));
        self.reserved.extend(self.skill_names.iter().cloned());
        Ok(())
    }

    /// The toolbox of a turn that reads a registry of its own (RFC §24.3):
    /// `toolset` serving the skill tools, which it declares as `declared`, last
    /// as the agent's stand, in place of the agent's. Never installed on the
    /// agent: the turn reads its own copy.
    pub(crate) fn with_skills(&self, toolset: Arc<dyn DynToolset>, declared: Vec<AiTool>) -> Self {
        let mut turn = self.clone();
        let Some(position) = self.skills else {
            return turn;
        };
        if let Some(served) = turn.toolsets.get_mut(position) {
            *served = toolset;
        }
        turn.tools.retain(|(_, at)| *at != position);
        turn.served.retain(|_, at| *at != position);
        for tool in declared {
            turn.served.insert(tool.name.clone(), position);
            turn.exempt.insert(tool.name.clone());
            turn.tools.push((tool, position));
        }
        turn
    }

    /// The toolbox with `search` serving Tool Search's tools, declared after
    /// all the others, for the turn it hides a catalogue (RFC §6.4).
    ///
    /// # Errors
    ///
    /// [`AiError::DuplicateTool`] for a name already served: never, once
    /// reserved.
    pub(crate) fn with_discovery(&self, search: Arc<dyn DynToolset>) -> Result<Self, AiError> {
        let mut turn = self.clone();
        turn.insert(search, search_tool_defs(), true)?;
        Ok(turn)
    }

    /// The names of its tools.
    pub(crate) fn names(&self) -> HashSet<String> {
        self.tools
            .iter()
            .map(|(tool, _)| tool.name.clone())
            .collect()
    }

    /// The tools Tool Search may hide, whatever the policy says of them: what
    /// is measured against its thresholds (RFC §21.1).
    pub(crate) fn deferrable(&self) -> Vec<&AiTool> {
        self.tools
            .iter()
            .map(|(tool, _)| tool)
            .filter(|tool| !self.never_hidden_name(&tool.name))
            .collect()
    }

    /// Whether the channel itself serves `name`, past the tool policy.
    pub(crate) fn is_exempt(&self, name: &str) -> bool {
        self.exempt.contains(name)
    }

    /// Whether Tool Search never hides `name`.
    fn never_hidden_name(&self, name: &str) -> bool {
        self.exempt.contains(name)
            || self.person_tools.contains(name)
            || self.injected.contains(name)
    }

    /// Whether a skill activation's hint may name `name`: not a tool the agent
    /// serves itself nor one that asks a person (RFC §24.4).
    pub(crate) fn hintable(&self, name: &str) -> bool {
        !self.exempt.contains(name) && !self.person_tools.contains(name)
    }

    /// What Tool Search never hides: the agent's own tools, a person's, what
    /// orchestration injected. Python's `_never_hidden`.
    pub(crate) fn never_hidden(&self) -> HashSet<String> {
        self.names()
            .into_iter()
            .filter(|name| self.never_hidden_name(name))
            .collect()
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
                (None, _) if self.reserved.contains(&tool.name) => {
                    tracing::warn!(target: "roomkit::ai", tool = %tool.name, "a generation hook added a tool under a name the channel serves: it is not declared");
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
            if self.reserved.contains(&tool.name) {
                collisions.served(channel, &tool.name);
                continue;
            }
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

    fn insert(
        &mut self,
        toolset: Arc<dyn DynToolset>,
        declared: Vec<AiTool>,
        exempt: bool,
    ) -> Result<(), AiError> {
        let mut names: Vec<&str> = Vec::with_capacity(declared.len());
        for tool in &declared {
            if !exempt && self.reserved.contains(&tool.name) {
                return Err(self.refused_name(&tool.name));
            }
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

    /// The refusal of a tool under `name`, a name kept for the agent's own.
    fn refused_name(&self, name: &str) -> AiError {
        if self.skill_names.contains(name) {
            return AiError::DuplicateTool {
                name: name.to_owned(),
            };
        }
        AiError::ToolSearchName {
            name: name.to_owned(),
            channel: self.agent.clone(),
        }
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
    /// a tool of its own, which only reads or unlocks (its skills', Tool
    /// Search's), as the reference implementation's `in_tool_memory` trait
    /// reads them.
    pub(crate) fn in_tool_memory(&self, name: &str) -> bool {
        !self.exempt.contains(name) && !self.reserved.contains(name)
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::tools::{BoxFuture, ToolError, ToolResult};

    struct Serving;

    impl DynToolset for Serving {
        fn call(
            &self,
            _call: ToolCall,
            _context: ToolContext,
        ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
            Box::pin(async { Ok(ToolResult::Text(String::new())) })
        }
    }

    fn tool(name: &str) -> AiTool {
        AiTool::new(name, "").unwrap()
    }

    // Python: turns/executor.py _refuse_clashing_owner_tools: a turn whose
    // owner's tool bears a name the channel serves is refused, its registry's
    // own names holding the skill tools' for a registry given, an empty one too.
    #[test]
    fn an_owner_s_tool_under_a_kept_skill_name_is_refused_as_a_tool_given_twice() {
        let mut toolbox = Toolbox::default();
        toolbox
            .add_skills(Arc::new(Serving), Vec::new(), &["activate_skill"])
            .unwrap();
        assert_eq!(
            toolbox.add_injected(Arc::new(Serving), vec![tool("activate_skill")]),
            Err(AiError::DuplicateTool {
                name: "activate_skill".to_owned()
            })
        );
    }
}
