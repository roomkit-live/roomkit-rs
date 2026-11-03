//! The pipeline strategy (RFC §19.7.1): Python's
//! `orchestration/strategies/pipeline.py`. Agents chained in order, the first
//! the entry point, each reachable by a handoff from the one before it.

use roomkit_core::model::{ChannelId, RoomId};
use serde_json::Value;

use crate::error::RoomKitError;
use crate::hooks::HookRegistration;
use crate::kit::{RoomKit, strategy_address};
use crate::orchestration::agents::{HeldAgent, held};
use crate::orchestration::{
    ConversationPipeline, ConversationPipelineOptions, ConversationState, Orchestration,
    PipelineStage, StrategyAgent, StrategyFuture, save_conversation_state,
};
use crate::store::ConversationStore;
use crate::{HookExecution, HookTrigger};

/// What [`Pipeline::new`] takes beside the agents: Python's
/// `Pipeline(agents, *, supervisor_id=None)`. Its voice channel's options are
/// not here yet.
#[derive(Clone, Debug, Default)]
pub struct PipelineOptions {
    /// The supervisor every event also asks, where the room binds it.
    pub supervisor_id: Option<ChannelId>,
}

/// The pipeline strategy (RFC §19.7.1): Python's `roomkit.Pipeline(agents)`.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use roomkit::model::ChannelId;
/// use roomkit::orchestration::PipelineOptions;
/// use roomkit::{Pipeline, RoomKit, RoomKitOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let pipeline = Pipeline::new(
///     vec![ChannelId::new("triage")?.into(), ChannelId::new("billing")?.into()],
///     PipelineOptions::default(),
/// );
/// let kit: RoomKit = RoomKit::new(RoomKitOptions {
///     orchestration: Some(Arc::new(pipeline)),
///     ..RoomKitOptions::default()
/// });
/// # drop(kit);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Pipeline {
    agents: Vec<StrategyAgent>,
    supervisor_id: Option<ChannelId>,
}

impl Pipeline {
    /// The ordered `agents`, each live or named by the id of one the kit
    /// holds: the first is the entry point, each hands off to the next. With
    /// none, its install refuses, as Python's.
    #[must_use]
    pub fn new(agents: Vec<StrategyAgent>, options: PipelineOptions) -> Self {
        Self {
            agents,
            supervisor_id: options.supervisor_id,
        }
    }

    async fn install_in<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
    ) -> Result<(), RoomKitError> {
        let Some(first) = self.agents.first() else {
            return Err(RoomKitError::Value(
                "Pipeline needs at least one agent".to_owned(),
            ));
        };
        let owner = strategy_address::<S>(self);
        kit.claim_for(room_id, owner)?;
        let agents = self
            .agents
            .iter()
            .map(|agent| held(kit, agent))
            .collect::<Result<Vec<HeldAgent>, _>>()?;
        kit.register_strategy_agents(&self.agents)?;
        let pipeline = ConversationPipeline::new(
            build_stages(&agents),
            ConversationPipelineOptions {
                supervisor_id: self.supervisor_id.clone(),
                ..ConversationPipelineOptions::default()
            },
        )?;
        let router = pipeline.to_router();
        let handler = pipeline.handler(kit, router.clone(), None);
        handler.keep_agents(&self.agents, &agents);
        let hook = HookRegistration {
            priority: -100,
            name: format!("pipeline_router_{room_id}"),
            ..HookRegistration::new(
                HookTrigger::BeforeBroadcast,
                HookExecution::Sync,
                router.as_hook(),
            )
        };
        kit.hook_engine().replace_room_hook(room_id, hook)?;
        let handoffs: Vec<(ChannelId, Value)> = agents
            .iter()
            .map(|agent| {
                let definition = pipeline.handoff_definition(&agent.channel_id, &agents);
                (agent.channel_id.clone(), definition)
            })
            .collect();
        handler.serve_in_room(kit, &handoffs, room_id, owner)?;
        let mut state = ConversationState::new();
        state.phase = first.channel_id().to_string();
        state.active_agent_id = Some(first.channel_id().clone());
        save_conversation_state(kit.store(), room_id, &state).await
    }
}

/// The stages of `agents`, each its own phase, handing off to the next:
/// Python's `_build_stages`.
fn build_stages(agents: &[HeldAgent]) -> Vec<PipelineStage> {
    agents
        .iter()
        .enumerate()
        .map(|(index, agent)| PipelineStage {
            next: agents
                .get(index + 1)
                .map(|next| next.channel_id.to_string()),
            description: agent.description.clone(),
            ..PipelineStage::new(agent.channel_id.as_str(), agent.channel_id.clone())
        })
        .collect()
}

impl<S: ConversationStore> Orchestration<S> for Pipeline {
    fn agents(&self) -> Vec<StrategyAgent> {
        self.agents.clone()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(self.install_in(kit, room_id))
    }
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::ChannelId;

    use super::{Pipeline, PipelineOptions, build_stages};
    use crate::orchestration::Orchestration;
    use crate::orchestration::agents::tests::held_agent;
    use crate::store::InMemoryStore;

    fn id(name: &str) -> ChannelId {
        ChannelId::new(name).unwrap_or_else(|error| panic!("{error}"))
    }

    // Python: tests/orchestration/test_strategy_pipeline.py::TestPipelineAgents::test_agents_returns_all
    // Python: tests/orchestration/test_strategy_pipeline.py::TestPipelineAgents::test_agents_returns_copy
    #[test]
    fn its_agents_are_all_of_them_in_their_order() {
        let pipeline = Pipeline::new(
            vec![id("a").into(), id("b").into(), id("c").into()],
            PipelineOptions::default(),
        );
        let ids: Vec<String> = Orchestration::<InMemoryStore>::agents(&pipeline)
            .iter()
            .map(|agent| agent.channel_id().to_string())
            .collect();
        assert_eq!(ids, ["a", "b", "c"]);
    }

    // Python: tests/orchestration/test_strategy_pipeline.py::TestPipelineBuildStages::test_linear_chain
    // Python: tests/orchestration/test_strategy_pipeline.py::TestPipelineBuildStages::test_single_agent
    #[test]
    fn its_stages_chain_its_agents_the_last_one_terminal() {
        let mut described = held_agent("b");
        described.description = Some("Bills.".to_owned());
        let stages = build_stages(&[held_agent("a"), described, held_agent("c")]);
        let read: Vec<(&str, Option<&str>, Option<&str>)> = stages
            .iter()
            .map(|s| {
                (
                    s.phase.as_str(),
                    s.next.as_deref(),
                    s.description.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            read,
            [
                ("a", Some("b"), None),
                ("b", Some("c"), Some("Bills.")),
                ("c", None, None),
            ]
        );
        let single = build_stages(&[held_agent("solo")]);
        assert_eq!(single.len(), 1);
        assert_eq!(single.first().and_then(|s| s.next.clone()), None);
    }
}
