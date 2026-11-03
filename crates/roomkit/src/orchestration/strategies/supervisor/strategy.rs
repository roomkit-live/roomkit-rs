//! The supervisor strategy's class (RFC §19.7.3): its options, its
//! install, and the team its delegations read. Python's
//! `orchestration/strategies/supervisor/core.py`.

use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{ChannelId, RoomId};

use super::auto::DelegatingTurns;
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
use crate::turn_runners::TurnRunner;
use crate::{HookExecution, HookTrigger};

/// What [`Supervisor::new`] takes beside its agents: Python's
/// `Supervisor(supervisor, workers, *, strategy=None, auto_delegate=False,
/// refine_task=True, refine_instruction=None,
/// delegation_message="I'm dispatching my team to work on this.", ...,
/// share_channels=None, task_timeout=120.0, max_revisions=3)`.
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
    /// Whether the framework delegates each message itself, no tool
    /// needed: `false` by default. The supervisor's turns in the room run
    /// the team, then the supervisor answers from their results. It needs
    /// a `strategy`.
    pub auto_delegate: bool,
    /// With `auto_delegate`, whether a first pass of the supervisor's own
    /// turn turns the message into the team's task: `true` by default; else
    /// the team is handed the message's text.
    pub refine_task: bool,
    /// The instruction that first pass is given after the supervisor's
    /// prompt, when not the implementation's own.
    pub refine_instruction: Option<String>,
    /// The message meant for a delegation run in the background, which no
    /// delegation of this implementation posts yet: kept as given.
    pub delegation_message: Option<String>,
}

impl Default for SupervisorOptions {
    fn default() -> Self {
        Self {
            strategy: None,
            share_channels: None,
            task_timeout: Duration::from_secs(120),
            max_revisions: 3,
            auto_delegate: false,
            refine_task: true,
            refine_instruction: None,
            delegation_message: Some("I'm dispatching my team to work on this.".to_owned()),
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
/// With `auto_delegate`, no tool: the supervisor's turns in the room run
/// the team on each message, a first pass of its own turn turning the
/// message into the team's task unless `refine_task` is off, then the
/// supervisor answers the message from the workers' results.
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
    /// [`RoomKitError::Value`], Python's `ValueError`, for `auto_delegate`
    /// with no `strategy`.
    pub fn new(
        supervisor: impl Into<StrategyAgent>,
        workers: Vec<StrategyAgent>,
        options: SupervisorOptions,
    ) -> Result<Self, RoomKitError> {
        if options.auto_delegate && options.strategy.is_none() {
            return Err(RoomKitError::Value(
                "auto_delegate=True requires strategy to be set".to_owned(),
            ));
        }
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
        match self.options.strategy {
            Some(strategy) if self.options.auto_delegate => {
                // Kept by the room's owner, never on the shared supervisor
                // (RFC §10.6.1): it answers as itself in every other room.
                let turns: Arc<dyn TurnRunner<S>> = Arc::new(DelegatingTurns {
                    team,
                    strategy,
                    refine: self.options.refine_task,
                    instruction: self.options.refine_instruction.clone(),
                });
                kit.inner
                    .turn_runners
                    .take(room_id, &supervisor.channel_id, turns, owner)?;
            }
            Some(strategy) => {
                let entries = vec![team::entry(team, strategy)];
                kit.register_room_tools(&supervisor.channel_id, room_id, entries, owner)?;
            }
            None => {
                let entries = per_worker::entries(&team);
                kit.register_room_tools(&supervisor.channel_id, room_id, entries, owner)?;
            }
        }
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
