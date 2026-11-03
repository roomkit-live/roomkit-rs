//! The supervisor strategy's class (RFC §19.7.3): its options, its
//! install, and the team its delegations read. Python's
//! `orchestration/strategies/supervisor/core.py`.

use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{ChannelId, RoomId};

use super::{WorkerStrategy, per_worker, team};
use crate::error::RoomKitError;
use crate::hooks::HookRegistration;
use crate::kit::{DelegateOptions, RoomKit, strategy_address};
use crate::orchestration::agents::{HeldAgent, held};
use crate::orchestration::{
    ConversationRouter, ConversationRouterOptions, ConversationState, Orchestration, StrategyAgent,
    StrategyFuture, save_conversation_state,
};
use crate::store::ConversationStore;
use crate::{HookExecution, HookTrigger};

/// What [`Supervisor::new`] takes beside its agents: Python's
/// `Supervisor(supervisor, workers, *, strategy=None, ..., share_channels=None,
/// task_timeout=120.0, max_revisions=3)`.
#[derive(Clone, Debug)]
pub struct SupervisorOptions {
    /// How the team runs: [`WorkerStrategy::Sequential`], the supervisor
    /// framing and judging each step, or [`WorkerStrategy::Parallel`], all
    /// workers at once on the same task, both behind one `delegate_workers`
    /// tool; `None`, one `delegate_to_<worker>` tool per worker, the model
    /// deciding when to delegate.
    pub strategy: Option<WorkerStrategy>,
    /// The room's channels attached to each child room a delegation opens,
    /// so that a worker's events reach them.
    pub share_channels: Option<Vec<ChannelId>>,
    /// Each worker's delegation budget: 120 seconds by default. A worker
    /// past it is cut and read as timed out: a parallel team's others go
    /// on, a supervised step stops the chain there.
    pub task_timeout: Duration,
    /// How many times a sequential team's step is reviewed at most: 3 by
    /// default, so two reworks; 0 reviews it once, as 1 does. A step still
    /// rejected then stops the chain, flagged unvalidated.
    pub max_revisions: u32,
}

impl Default for SupervisorOptions {
    fn default() -> Self {
        Self {
            strategy: None,
            share_channels: None,
            task_timeout: Duration::from_secs(120),
            max_revisions: 3,
        }
    }
}

/// The supervisor strategy (RFC §19.7.3): Python's `roomkit.Supervisor`.
/// The supervisor answers the room, a router asking it for every message;
/// its workers are registered on the kit, never attached, and run in child
/// rooms (RFC §23.3). In the room it is installed in, the supervisor is
/// given the tools to delegate, served by the room's owner: one
/// `delegate_to_<worker>` per worker with no strategy, else one
/// `delegate_workers` that runs the whole team. A sequential team is
/// supervised: the supervisor frames the first task, judges each step
/// through `submit_verdict` and frames the next one, a rejected step
/// reworked with its feedback. The call answers once the workers did.
///
/// Its agents are each given live or named by the id of an agent the kit
/// holds. Elsewhere, the supervisor answers as itself.
///
/// ```
/// use std::sync::Arc;
///
/// use roomkit::model::{ChannelId, RoomId};
/// use roomkit::orchestration::SupervisorOptions;
/// use roomkit::orchestration::strategies::supervisor::WorkerStrategy;
/// use roomkit::{RoomKit, Supervisor};
///
/// # async fn install(kit: &RoomKit) -> Result<(), roomkit::RoomKitError> {
/// let team = Supervisor::new(
///     ChannelId::new("coordinator")?,
///     vec![ChannelId::new("researcher")?.into(), ChannelId::new("writer")?.into()],
///     SupervisorOptions {
///         strategy: Some(WorkerStrategy::Sequential),
///         ..SupervisorOptions::default()
///     },
/// )?;
/// kit.install_strategy(&RoomId::new("r1")?, Arc::new(team), Default::default())
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Supervisor {
    /// The supervisor, then the workers, as given.
    named: Vec<StrategyAgent>,
    pub(super) options: SupervisorOptions,
}

/// One install's team, as its delegations read it.
#[derive(Debug)]
pub(crate) struct Team {
    pub(super) supervisor: HeldAgent,
    pub(super) workers: Vec<HeldAgent>,
    pub(super) share_channels: Vec<ChannelId>,
    pub(super) task_timeout: Duration,
    pub(super) max_revisions: u32,
}

impl Team {
    /// A delegation of the team's: the room's shared channels attached to
    /// its child room.
    pub(super) fn delegation(&self) -> DelegateOptions {
        DelegateOptions {
            share_channels: (!self.share_channels.is_empty()).then(|| self.share_channels.clone()),
            ..DelegateOptions::default()
        }
    }
}

impl Supervisor {
    /// A supervisor strategy of `supervisor` over `workers`, in their
    /// order.
    ///
    /// # Errors
    ///
    /// None yet: [`RoomKitError::Value`], Python's `ValueError`, is kept for
    /// a combination of options a supervisor refuses.
    pub fn new(
        supervisor: impl Into<StrategyAgent>,
        workers: Vec<StrategyAgent>,
        options: SupervisorOptions,
    ) -> Result<Self, RoomKitError> {
        Ok(Self {
            named: std::iter::once(supervisor.into()).chain(workers).collect(),
            options,
        })
    }

    /// The workers, as given.
    pub(super) fn workers(&self) -> &[StrategyAgent] {
        self.named.get(1..).unwrap_or_default()
    }

    /// Wires the supervisor in `room_id`: its router, its workers
    /// registered, its delegation tools, the initial state saved.
    async fn install_in<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
    ) -> Result<(), RoomKitError> {
        let owner = strategy_address::<S>(self);
        kit.claim_for(room_id, owner)?;
        let mut agents = self
            .named
            .iter()
            .map(|agent| held(kit, agent))
            .collect::<Result<Vec<HeldAgent>, _>>()?;
        let supervisor = agents.remove(0);
        install_router(kit, room_id, &supervisor.channel_id)?;
        // The workers are the kit's, never attached to the room.
        kit.register_strategy_agents(self.workers())?;
        let team = Arc::new(Team {
            supervisor: supervisor.clone(),
            workers: agents,
            share_channels: self.options.share_channels.clone().unwrap_or_default(),
            task_timeout: self.options.task_timeout,
            max_revisions: self.options.max_revisions,
        });
        let entries = match self.options.strategy {
            Some(strategy) => vec![team::entry(team, strategy)],
            None => per_worker::entries(&team),
        };
        kit.register_room_tools(&supervisor.channel_id, room_id, entries, owner)?;
        let mut state = ConversationState::new();
        "supervisor".clone_into(&mut state.phase);
        state.active_agent_id = Some(supervisor.channel_id);
        save_conversation_state(kit.store(), room_id, &state).await
    }
}

/// Asks the supervisor for every message of `room_id` (RFC §19.4): the
/// router's hook, which a second install of the room replaces.
fn install_router<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    supervisor: &ChannelId,
) -> Result<(), RoomKitError> {
    let router = ConversationRouter::new(ConversationRouterOptions {
        default_agent_id: Some(supervisor.clone()),
        ..ConversationRouterOptions::default()
    });
    let hook = HookRegistration {
        priority: -100,
        name: format!("supervisor_router_{room_id}"),
        ..HookRegistration::new(
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
            router.as_hook(),
        )
    };
    kit.hook_engine().replace_room_hook(room_id, hook).map(drop)
}

impl<S: ConversationStore> Orchestration<S> for Supervisor {
    /// The supervisor: it handles the room's conversation.
    fn agents(&self) -> Vec<StrategyAgent> {
        self.named.first().cloned().into_iter().collect()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(self.install_in(kit, room_id))
    }
}
