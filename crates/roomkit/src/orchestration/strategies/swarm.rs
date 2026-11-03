//! The swarm strategy (RFC §19.7): Python's
//! `orchestration/strategies/swarm.py`. Every agent can hand off to every
//! other agent, with no phase constraint; the router keeps the conversation
//! with the agent that holds it until it hands off.

use std::collections::HashMap;

use roomkit_core::model::{ChannelId, RoomId};

use crate::error::RoomKitError;
use crate::hooks::HookRegistration;
use crate::kit::{RoomKit, strategy_address};
use crate::orchestration::agents::{HeldAgent, held};
use crate::orchestration::handoff::{
    HANDOFF_TOOL_NAME, HandoffHandler, HandoffHandlerOptions, handoff_tool_definition,
};
use crate::orchestration::{
    ConversationRouter, ConversationRouterOptions, ConversationState, Orchestration, StrategyAgent,
    StrategyFuture, save_conversation_state,
};
use crate::store::ConversationStore;
use crate::{HookExecution, HookTrigger};

/// The swarm strategy (RFC §19.7): Python's `roomkit.Swarm(agents,
/// entry=None)`.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use roomkit::model::ChannelId;
/// use roomkit::{RoomKit, RoomKitOptions, Swarm};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let swarm = Swarm::new(
///     vec![ChannelId::new("sales")?.into(), ChannelId::new("support")?.into()],
///     None,
/// );
/// let kit: RoomKit = RoomKit::new(RoomKitOptions {
///     orchestration: Some(Arc::new(swarm)),
///     ..RoomKitOptions::default()
/// });
/// # drop(kit);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Swarm {
    agents: Vec<StrategyAgent>,
    entry: Option<ChannelId>,
}

impl Swarm {
    /// All the swarm's `agents`, each live or named by the id of one the kit
    /// holds, and the channel id of the agent it starts with, `entry`, the
    /// first agent's when `None`.
    #[must_use]
    pub fn new(agents: Vec<StrategyAgent>, entry: Option<ChannelId>) -> Self {
        let entry = entry.or_else(|| agents.first().map(|agent| agent.channel_id().clone()));
        Self { agents, entry }
    }

    async fn install_in<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
    ) -> Result<(), RoomKitError> {
        let owner = strategy_address::<S>(self);
        kit.claim_for(room_id, owner)?;
        let agents = self
            .agents
            .iter()
            .map(|agent| held(kit, agent))
            .collect::<Result<Vec<HeldAgent>, _>>()?;
        kit.register_strategy_agents(&self.agents)?;
        let router = ConversationRouter::new(ConversationRouterOptions {
            default_agent_id: self.entry.clone(),
            ..ConversationRouterOptions::default()
        });
        let handler = HandoffHandler::new(kit, router.clone(), HandoffHandlerOptions::default());
        handler.set_greeting_map(
            agents
                .iter()
                .filter_map(|agent| {
                    let greeting = agent.greeting.clone()?;
                    Some((agent.channel_id.clone(), greeting))
                })
                .collect::<HashMap<_, _>>(),
        );
        handler.set_agents(
            self.agents
                .iter()
                .map(|agent| (agent.channel_id().clone(), agent.clone()))
                .collect(),
        );
        let hook = HookRegistration {
            priority: -100,
            name: format!("swarm_router_{room_id}"),
            ..HookRegistration::new(
                HookTrigger::BeforeBroadcast,
                HookExecution::Sync,
                router.as_hook(),
            )
        };
        kit.hook_engine().replace_room_hook(room_id, hook)?;
        set_up_handoffs(kit, &handler, &agents, room_id, owner)?;
        let mut state = ConversationState::new();
        "swarm".clone_into(&mut state.phase);
        state.active_agent_id.clone_from(&self.entry);
        save_conversation_state(kit.store(), room_id, &state).await
    }
}

/// Serves each agent's handoff in `room_id` for the install `owner`, each
/// reaching all the others: all of them, or none when one is refused.
/// Python's `set_up_handoffs`.
fn set_up_handoffs<S: ConversationStore>(
    kit: &RoomKit<S>,
    handler: &HandoffHandler<S>,
    agents: &[HeldAgent],
    room_id: &RoomId,
    owner: usize,
) -> Result<(), RoomKitError> {
    for (index, agent) in agents.iter().enumerate() {
        let targets: Vec<(String, Option<String>)> = agents
            .iter()
            .filter(|other| other.channel_id != agent.channel_id)
            .map(|other| (other.channel_id.to_string(), other.description.clone()))
            .collect();
        let definition = handoff_tool_definition(&targets);
        let entry = handler.entry(&agent.channel_id, definition, Some(owner));
        let served = kit.register_room_tools(&agent.channel_id, room_id, vec![entry], owner);
        if let Err(error) = served {
            let done = agents.get(..index).unwrap_or_default();
            for agent in done {
                kit.inner.owner_tools.unregister_in_room(
                    &agent.channel_id,
                    room_id,
                    HANDOFF_TOOL_NAME,
                );
            }
            return Err(error);
        }
    }
    Ok(())
}

impl<S: ConversationStore> Orchestration<S> for Swarm {
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

    use super::Swarm;
    use crate::orchestration::Orchestration;
    use crate::store::InMemoryStore;

    fn id(name: &str) -> ChannelId {
        ChannelId::new(name).unwrap_or_else(|error| panic!("{error}"))
    }

    fn ids(swarm: &Swarm) -> Vec<String> {
        Orchestration::<InMemoryStore>::agents(swarm)
            .iter()
            .map(|agent| agent.channel_id().to_string())
            .collect()
    }

    // Python: tests/orchestration/test_strategy_swarm.py::TestSwarmAgents::test_agents_returns_all
    // Python: tests/orchestration/test_strategy_swarm.py::TestSwarmAgents::test_default_entry_is_first_agent
    #[test]
    fn its_agents_are_all_of_them_and_its_entry_the_first() {
        let swarm = Swarm::new(vec![id("a").into(), id("b").into()], None);
        assert_eq!(ids(&swarm), ["a", "b"]);
        assert_eq!(swarm.entry, Some(id("a")));
    }

    // Python: tests/orchestration/test_strategy_swarm.py::TestSwarmAgents::test_custom_entry
    #[test]
    fn its_entry_is_the_one_it_is_given() {
        let swarm = Swarm::new(vec![id("a").into(), id("b").into()], Some(id("b")));
        assert_eq!(swarm.entry, Some(id("b")));
    }
}
