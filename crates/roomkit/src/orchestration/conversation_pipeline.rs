//! The conversation pipeline (RFC §19.5): Python's
//! `roomkit.orchestration.pipeline`. A workflow of stages generating a
//! router's rules, one per stage, and the transitions a handoff keeps to.
//! A voice channel's and a realtime channel's wiring are not here yet.

use std::collections::{BTreeSet, HashMap, HashSet};

use roomkit_core::model::ChannelId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::agents::{HeldAgent, held};
use super::handoff::{HandoffHandler, HandoffHandlerOptions, handoff_tool_definition};
use super::router::{
    ConversationRouter, ConversationRouterOptions, RoutingConditions, RoutingRule,
};
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::orchestration::StrategyAgent;
use crate::store::ConversationStore;

/// A stage in a conversation pipeline: Python's `PipelineStage`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineStage {
    /// The phase it holds.
    pub phase: String,
    /// The agent handling that phase.
    pub agent_id: ChannelId,
    /// The phase a handoff moves on to; `None`, or empty, for a terminal one.
    #[serde(default)]
    pub next: Option<String>,
    /// The earlier phases a handoff may go back to.
    #[serde(default)]
    pub can_return_to: BTreeSet<String>,
    /// How a handoff tool names it, for an agent with no description.
    #[serde(default)]
    pub description: Option<String>,
}

impl PipelineStage {
    /// The terminal stage of `agent_id` in `phase`, the rest Python's
    /// defaults.
    pub fn new(phase: impl Into<String>, agent_id: ChannelId) -> Self {
        Self {
            phase: phase.into(),
            agent_id,
            next: None,
            can_return_to: BTreeSet::new(),
            description: None,
        }
    }

    /// The phases a handoff may lead to from here, Python's truth test
    /// reading an empty `next` as none.
    fn reachable(&self) -> BTreeSet<&str> {
        self.next
            .iter()
            .map(String::as_str)
            .filter(|next| !next.is_empty())
            .chain(self.can_return_to.iter().map(String::as_str))
            .collect()
    }
}

/// What [`ConversationPipeline::new`] takes beside the stages: Python's
/// `default_phase=None, supervisor_id=None`.
#[derive(Clone, Debug, Default)]
pub struct ConversationPipelineOptions {
    /// The phase naming the agent a phase no stage holds asks; the first
    /// stage's when `None` or empty.
    pub default_phase: Option<String>,
    /// The supervisor every event also asks.
    pub supervisor_id: Option<ChannelId>,
}

/// What [`ConversationPipeline::install`] takes beside the kit and the
/// agents: Python's `install(kit, agents, *, agent_aliases=None,
/// hook_priority=-100)`.
#[derive(Clone, Debug)]
pub struct InstallOptions {
    /// Each alias's agent id.
    pub agent_aliases: Option<HashMap<String, String>>,
    /// The priority of the router's hook.
    pub hook_priority: i32,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            agent_aliases: None,
            hook_priority: -100,
        }
    }
}

/// Generates routing rules for a sequential agent workflow: Python's
/// `ConversationPipeline`.
///
/// ```
/// use roomkit::model::ChannelId;
/// use roomkit::{ConversationPipeline, PipelineStage};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let triage = PipelineStage {
///     next: Some("billing".to_owned()),
///     ..PipelineStage::new("triage", ChannelId::new("agent-triage")?)
/// };
/// let billing = PipelineStage::new("billing", ChannelId::new("agent-billing")?);
/// let pipeline = ConversationPipeline::new(vec![triage, billing], Default::default())?;
/// assert_eq!(pipeline.stages().len(), 2);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ConversationPipeline {
    stages: Vec<PipelineStage>,
    default_phase: Option<String>,
    supervisor_id: Option<ChannelId>,
}

impl ConversationPipeline {
    /// The pipeline of `stages`, in their order.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for a stage whose `next` or `can_return_to`
    /// names no phase of a stage.
    pub fn new(
        stages: Vec<PipelineStage>,
        options: ConversationPipelineOptions,
    ) -> Result<Self, RoomKitError> {
        let default_phase = options
            .default_phase
            .filter(|phase| !phase.is_empty())
            .or_else(|| stages.first().map(|stage| stage.phase.clone()));
        let pipeline = Self {
            stages,
            default_phase,
            supervisor_id: options.supervisor_id,
        };
        pipeline.validate()?;
        Ok(pipeline)
    }

    /// Refuses a stage naming a phase no stage holds.
    fn validate(&self) -> Result<(), RoomKitError> {
        let phases: HashSet<&str> = self.stages.iter().map(|s| s.phase.as_str()).collect();
        for stage in &self.stages {
            let next = stage.next.as_deref().filter(|next| !next.is_empty());
            if let Some(next) = next
                && !phases.contains(next)
            {
                return Err(RoomKitError::Value(format!(
                    "Stage '{}' has next='{next}' which is not a valid phase",
                    stage.phase
                )));
            }
            if let Some(phase) = stage
                .can_return_to
                .iter()
                .find(|phase| !phases.contains(phase.as_str()))
            {
                return Err(RoomKitError::Value(format!(
                    "Stage '{}' has can_return_to='{phase}' which is not a valid phase",
                    stage.phase
                )));
            }
        }
        Ok(())
    }

    /// The pipeline's stages: Python's `stages`.
    #[must_use]
    pub fn stages(&self) -> Vec<PipelineStage> {
        self.stages.clone()
    }

    /// The router of this pipeline: one rule per stage, asking its agent in
    /// its phase, in the stages' order; the default phase's agent when no
    /// rule applies; and the supervisor.
    #[must_use]
    pub fn to_router(&self) -> ConversationRouter {
        let rules = self
            .stages
            .iter()
            .zip(0..)
            .map(|(stage, priority)| RoutingRule {
                agent_id: stage.agent_id.clone(),
                conditions: RoutingConditions {
                    phases: Some(BTreeSet::from([stage.phase.clone()])),
                    ..RoutingConditions::default()
                },
                priority,
            })
            .collect();
        ConversationRouter::new(ConversationRouterOptions {
            rules,
            default_agent_id: self.default_agent_id(),
            supervisor_id: self.supervisor_id.clone(),
        })
    }

    /// The agent of the default phase's stage, the last of several.
    fn default_agent_id(&self) -> Option<ChannelId> {
        let default_phase = self.default_phase.as_deref().unwrap_or_default();
        self.stages
            .iter()
            .rev()
            .find(|stage| stage.phase == default_phase)
            .map(|stage| stage.agent_id.clone())
    }

    /// Each agent's phase, the last stage's for an agent holding several:
    /// Python's `get_phase_map`.
    #[must_use]
    pub fn get_phase_map(&self) -> HashMap<String, String> {
        self.stages
            .iter()
            .map(|stage| (stage.agent_id.to_string(), stage.phase.clone()))
            .collect()
    }

    /// Each phase's phases a handoff may lead to: its `next` and its
    /// `can_return_to`. Python's `get_allowed_transitions`.
    #[must_use]
    pub fn get_allowed_transitions(&self) -> HashMap<String, HashSet<String>> {
        self.stages
            .iter()
            .map(|stage| {
                let allowed = stage.reachable().into_iter().map(str::to_owned).collect();
                (stage.phase.clone(), allowed)
            })
            .collect()
    }

    /// The handoff handler of this pipeline, routing with `router`.
    pub(super) fn handler<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        router: ConversationRouter,
        agent_aliases: Option<HashMap<String, String>>,
    ) -> HandoffHandler<S> {
        HandoffHandler::new(
            kit,
            router,
            HandoffHandlerOptions {
                agent_aliases,
                phase_map: Some(self.get_phase_map()),
                allowed_transitions: Some(self.get_allowed_transitions()),
                ..HandoffHandlerOptions::default()
            },
        )
    }

    /// The handoff tool `agent` serves: naming the agents its stage reaches
    /// other than itself, in the stages' order, each by its own description
    /// else its stage's; the generic one when it reaches none or holds no
    /// stage. Python's `_wire_handoff`, for one agent.
    pub(super) fn handoff_definition(&self, agent: &ChannelId, agents: &[HeldAgent]) -> Value {
        let Some(stage) = self.stages.iter().rev().find(|s| s.agent_id == *agent) else {
            return handoff_tool_definition(&[]);
        };
        let reachable = stage.reachable();
        let targets: Vec<(String, Option<String>)> = self
            .stages
            .iter()
            .filter(|s| reachable.contains(s.phase.as_str()) && s.agent_id != *agent)
            .map(|s| {
                let own = agents
                    .iter()
                    .rev()
                    .find(|held| held.channel_id == s.agent_id)
                    .and_then(|held| held.description.clone());
                (
                    s.agent_id.to_string(),
                    own.or_else(|| s.description.clone()),
                )
            })
            .collect();
        handoff_tool_definition(&targets)
    }

    /// Wires routing and handoff in one call: a router from this pipeline,
    /// its hook on every `BEFORE_BROADCAST` of the kit, a [`HandoffHandler`]
    /// keeping to the stages, and on every agent a handoff tool naming the
    /// agents it can reach. Each agent is live or named by the id of one the
    /// kit holds, a live one registered once every one was read, since it is
    /// given to the kit. Returns the router and the
    /// handler for further customisation.
    ///
    /// # Errors
    ///
    /// Before anything is installed, [`RoomKitError::ChannelNotRegistered`]
    /// for an id naming no channel of the kit, [`RoomKitError::Value`] or
    /// [`RoomKitError::Type`] for an agent that takes no handoff,
    /// [`RoomKitError::Runtime`] for one set up already or named twice, and
    /// [`RoomKitError::ToolNameCollision`] for a name its tools take.
    pub fn install<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        agents: &[StrategyAgent],
        options: InstallOptions,
    ) -> Result<(ConversationRouter, HandoffHandler<S>), RoomKitError> {
        // Every agent read before anything is installed.
        let held_agents = agents
            .iter()
            .map(|agent| held(kit, agent))
            .collect::<Result<Vec<HeldAgent>, _>>()?;
        kit.refuse_handoffs(agents)?;
        kit.register_strategy_agents(agents)?;
        let router = self.to_router();
        kit.install_router_hook(&router, options.hook_priority)?;
        let handler = self.handler(kit, router.clone(), options.agent_aliases);
        handler.keep_agents(agents, &held_agents);
        for agent in agents {
            let definition = self.handoff_definition(agent.channel_id(), &held_agents);
            handler.serve_tool(agent, Some(definition), None)?;
        }
        Ok((router, handler))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, HashSet};

    use roomkit_core::model::ChannelId;

    use super::{ConversationPipeline, ConversationPipelineOptions, PipelineStage};
    use crate::error::RoomKitError;

    fn id(name: &str) -> ChannelId {
        ChannelId::new(name).unwrap_or_else(|error| panic!("{error}"))
    }

    fn stage(phase: &str, agent: &str, next: Option<&str>) -> PipelineStage {
        PipelineStage {
            next: next.map(str::to_owned),
            ..PipelineStage::new(phase, id(agent))
        }
    }

    fn pipeline(stages: Vec<PipelineStage>) -> ConversationPipeline {
        ConversationPipeline::new(stages, ConversationPipelineOptions::default())
            .unwrap_or_else(|error| panic!("{error}"))
    }

    fn refusal(stages: Vec<PipelineStage>) -> String {
        match ConversationPipeline::new(stages, ConversationPipelineOptions::default()) {
            Err(RoomKitError::Value(message)) => message,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    // Python: tests/orchestration/test_pipeline.py::TestPipelineValidation::test_invalid_next_phase
    // Python: tests/orchestration/test_pipeline.py::TestPipelineValidation::test_invalid_can_return_to
    #[test]
    fn a_stage_naming_no_phase_is_refused_with_python_s_message() {
        assert_eq!(
            refusal(vec![stage("a", "x", Some("nonexistent"))]),
            "Stage 'a' has next='nonexistent' which is not a valid phase"
        );
        let returns = PipelineStage {
            can_return_to: BTreeSet::from(["nonexistent".to_owned()]),
            ..PipelineStage::new("a", id("x"))
        };
        assert_eq!(
            refusal(vec![returns]),
            "Stage 'a' has can_return_to='nonexistent' which is not a valid phase"
        );
    }

    // Python: tests/orchestration/test_pipeline.py::TestToRouter::test_default_agent_is_first_stage
    // Python: tests/orchestration/test_pipeline.py::TestToRouter::test_custom_default_phase
    #[test]
    fn its_default_agent_is_the_default_phase_s_an_empty_one_the_first_stage_s() {
        let stages = vec![stage("a", "x", Some("b")), stage("b", "y", None)];
        let default = |phase: Option<&str>| {
            ConversationPipeline::new(
                stages.clone(),
                ConversationPipelineOptions {
                    default_phase: phase.map(str::to_owned),
                    ..ConversationPipelineOptions::default()
                },
            )
            .unwrap_or_else(|error| panic!("{error}"))
            .default_agent_id()
        };
        assert_eq!(default(None), Some(id("x")));
        assert_eq!(default(Some("")), Some(id("x")));
        assert_eq!(default(Some("b")), Some(id("y")));
        assert_eq!(default(Some("no such phase")), None);
    }

    // Python: tests/orchestration/test_pipeline.py::TestGetAllowedTransitions::test_terminal_stage
    #[test]
    fn an_empty_next_is_no_next_and_refuses_nothing() {
        let read = pipeline(vec![stage("a", "x", Some(""))]);
        assert_eq!(
            read.get_allowed_transitions(),
            HashMap::from([("a".to_owned(), HashSet::new())])
        );
    }
}
