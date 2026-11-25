//! The turn backend (RFC §10.6): every intelligence turn runs through it,
//! between the room owner, the driver, which commits and broadcasts, and
//! the kit's own turn executor, which builds the channel's turn and reads
//! its stream, as serialized messages and nothing else (step 4, decision
//! 10).
//!
//! - this module: the backend, in memory: placement on the kit's own
//!   executor, the owner's record of each room's previous turn, its
//!   timeouts, its tap, its close;
//! - `wire`: the messages, in the serialized form of RFC §1.4;
//! - `pipe`: one direction of a turn's link, numbered and bounded;
//! - `codec` and `error`: what a turn's values and errors become on the
//!   wire, and back;
//! - `pulls`: the window a reader grants its stream (RFC §10.6.4);
//! - `owner`: the owner's side of a turn, its output, its stream, its
//!   liveness, the verbs its turn asks;
//! - `executor`: the executor's side of a turn, its human-input requests
//!   among it;
//! - `owner_inputs`: the owner's record of its turns' human-input
//!   requests, which the kit's `answer_input` and `reject_input` answer.
//!
//! In memory, the events a request carries, the event it answers and the
//! room's history, cross as they are (RFC §10.6.1): records nothing changes
//! once made. Everything else is serialized and parsed anew. Beside the
//! messages the backend carries the owner's execution context alone (RFC
//! §10.6.5): a turn's work runs in a copy of the context of the call that
//! submitted it, and its stream advances in a copy of the context of the
//! call that first read it, so a tool that calls the kit keeps the
//! reentrancy rules it would have in the owner's own code.

mod client;
pub use client::{
    OwnerEvents, OwnerHost, OwnerTools, TurnClient, TurnClientInputTurn, TurnClientInputs,
    TurnClientOptions, TurnClientOutput, TurnRunOptions,
};
mod served_executor;
pub use served_executor::ServedExecutor;
pub(crate) use served_executor::owner_executor;
mod sync_pipeline;
pub use sync_pipeline::SyncPipelineResult;
mod plan_event;
mod plan_task;
// The public SDK names for these observer types are re-exported by roomkit-ai.
#[doc(hidden)]
pub use plan_event::PlanUpdatedEvent;
#[doc(hidden)]
pub use plan_task::PlanTaskHandle;
mod public_options;
pub use public_options::{HookOptions, RoomHookOptions, TakesOptions};
#[cfg(feature = "redis")]
pub(crate) mod redis;
#[cfg(feature = "redis")]
pub use redis::{RedisTurnBackend, RedisTurnBackendOptions};
mod backend_handle;
pub use backend_handle::TurnBackendHandle;
mod local_presence_claims;
mod presence;
use local_presence_claims::LocalPresenceClaims;
pub(crate) use presence::LocalHosts;
pub use presence::{AgentPresence, ExecutorInfo, ExecutorInfoOfOptions, ExecutorInfoOptions};

/// The innermost turn whose work is running, including a call served by its
/// room owner. Its ID can be passed to [`crate::RoomKit::steer`] to address
/// that turn in its room. Outside turn work the result is `None`. Pass a spawned
/// child future through [`crate::keep_context`] to retain this ID.
pub fn current_turn_id() -> Option<String> {
    context::TurnScope::current()
        .innermost()
        .map(|id| id.to_string())
}

pub(crate) mod room_owner;
pub use room_owner::RoomOwner;
mod application;
mod transport;
pub use transport::TurnBackend;
mod executor_watchers;
pub use executor_watchers::{ExecutorWatchers, ExecutorsListener};
mod in_memory;
pub use in_memory::InMemoryTurnBackend;
mod backend;
pub use backend::{ExecutorLink, TurnFuture, TurnLink};
pub use context::TurnContext;
mod call_state;
mod channel_calls;
mod channel_routing;
pub(crate) mod declared;
pub(crate) mod wiring;
pub use crate::turn::ToolHooks;
pub use wiring::{
    ChannelTurnHooks, DirectPrompt, EmitOptions, FrameworkEmitter, TurnRealtime, TurnWiring,
    WiringEnv,
};
mod detach_state;
pub use declared::{DeclareHostVerbOptions, DeclaredVerb};
pub(crate) use detach_state::{DetachTicket, PendingDetaches};
mod codec;
pub(crate) mod context;
mod error;
mod error_classes;
pub use error_classes::{ErrorClasses, RemoteTurnDetails};
pub(crate) mod executor;
mod memory;
mod observe;
mod outside;
pub(crate) mod owner;
mod owner_inputs;
mod pipe;
mod pulls;
mod records;
mod rooms;
pub(crate) use channel_calls::Handover;
pub use channel_calls::{ChannelCall, ChannelResult};
#[cfg(feature = "redis")]
pub(crate) use error::rebuild_kit_error;
pub(crate) use rooms::{state_key, stored_state};
pub(crate) mod served;
mod started;
mod submission;
#[cfg(test)]
mod tests;
mod timeouts;
pub mod wire;
#[doc(inline)]
pub use wire::{ChannelControl, OwnerTool, PreviousTurn, RoomCall, RoomPublication, TurnRequest};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent,
    RoomId,
};
use roomkit_core::rules::transcript::TurnAuthor;
use serde::Serialize;
use tokio::sync::{Notify, watch};
use uuid::Uuid;

pub(crate) use self::codec::send_event_arguments;
pub(crate) use self::error::{
    CHANNEL_ERROR, RemoteError, TURN_LINK_ERROR, cause_records, record_of, variant_name,
};
pub(crate) use self::error::{TurnError, TurnFailure, to_record};
pub(crate) use self::executor::{ExecutorInputs, HostLink, InputSettler, WorkerTurn};
pub(crate) use self::memory::{CarriedRequest, MemoryTurn};
use self::observe::Observers;
pub(crate) use self::observe::{Carried, Tap};
pub(crate) use self::owner::{Canceller, OwedEnd, OwnerTurn, RemoteItem, WeakCanceller};
pub(crate) use self::owner_inputs::InputTurn;
use self::owner_inputs::OwnerInputs;
use self::pipe::MAX_MESSAGE_BYTES;
use self::records::RoomRecords;
use self::started::StartedTurns;
pub(crate) use self::started::SteerAddress;
pub use self::timeouts::TurnTimeouts;
pub(crate) use self::wire::SEND_EVENT;
use self::wire::{ChannelControlEnvelope, OwnerMessage, OwnerMessageEnvelope, RoomCallResult};
use crate::channel::DynChannel;
use crate::error::RoomKitError;
use crate::steering::SteeringDirective;
use crate::tasks::{Marks, Unit};
use crate::turn::TurnKit;

/// What a refusal names beside the channel: the room's hooks and its
/// organization, each when it applies.
fn needs(hooks: &[String], organization: Option<&str>) -> String {
    let hooks = if hooks.is_empty() {
        String::new()
    } else {
        let names = hooks
            .iter()
            .map(|s| crate::error::repr(s))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" with room hooks [{names}]")
    };
    let org = organization.map_or_else(String::new, |o| {
        format!(" in organization {}", crate::error::repr(o))
    });
    hooks + &org
}

fn no_executor(channel: &str, hooks: &[String], organization: Option<&str>) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "no_executor".into(),
        message: Some(format!(
            "No executor serves channel {}{}",
            crate::error::repr(channel),
            needs(hooks, organization)
        )),
    }
}

/// The refusal of a turn submitted to `executor` alone, which does not take
/// it: nothing was handed to it, nor to any other (RFC §10.6.7).
fn not_taken(
    executor: &ExecutorInfo,
    channel: &str,
    hooks: &[String],
    organization: Option<&str>,
) -> RoomKitError {
    let node = executor
        .node
        .as_ref()
        .map_or_else(String::new, |node| format!(" at {node}"));
    RoomKitError::TurnLink {
        reason: "no_executor".into(),
        message: Some(format!(
            "Executor {}{node} does not take channel {}{}",
            executor.executor_id,
            crate::error::repr(channel),
            needs(hooks, organization)
        )),
    }
}

/// Whether `executor` names the worker `executor_id` of `node`, a process
/// its backend names; an executor named with no node is any process's.
fn names(executor: &ExecutorInfo, executor_id: &str, node: Option<&str>) -> bool {
    executor.executor_id == executor_id
        && executor
            .node
            .as_deref()
            .is_none_or(|named| Some(named) == node)
}

/// What a turn answered on the owner.
#[derive(Debug)]
pub(crate) enum Answer {
    /// Its output, no stream following.
    Output(Box<ChannelOutput>),
    /// Its output, its stream following, read through the turn.
    Streamed(Box<ChannelOutput>, OwnerTurn),
}

/// The kit's turn backend, in memory, with the kit's own executor (RFC
/// §10.6.1, §10.6.7): every intelligence channel the kit holds is one it
/// registered live, whose turns its own executor takes.
#[derive(Debug)]
pub(crate) struct Turns {
    client: Option<Arc<client::Services>>,
    pub(crate) application: Option<application::Application>,
    #[cfg(feature = "redis")]
    pub(crate) remote: Option<RedisTurnBackend>,
    worker_owner: Mutex<Option<room_owner::OwnerPort>>,
    error_classes: RwLock<ErrorClasses>,
    timeouts: RwLock<TurnTimeouts>,
    observers: RwLock<Observers>,
    /// The id the kit's own executor answers with.
    executor_id: String,
    closed: AtomicBool,
    pub(crate) executor_lifecycle: Arc<executor::Lifecycle>,
    /// Each room's last turn of each channel the owner submitted.
    records: Mutex<RoomRecords>,
    rooms: Arc<rooms::ExecutorRooms>,
    call_states: call_state::CallStates,
    pub(crate) pending_detaches: Arc<PendingDetaches>,
    /// How many of the turns' streams the owner reads now.
    reads: Mutex<usize>,
    /// Told each time a read ends.
    reads_changed: Notify,
    /// Set as the kit's close starts: every turn is cut (RFC §10.6.8,
    /// §12.10.4).
    cut: watch::Sender<bool>,
    /// The turns submitted and not over, which steering reaches (RFC
    /// §10.6.8).
    started: Mutex<StartedTurns>,
    /// The open human-input requests of the turns, and the answers held
    /// for ones not open yet (RFC §10.6.5).
    pub(crate) inputs: OwnerInputs,
    /// A test's: the executor starts in a context of its own, as one in
    /// another process does.
    #[cfg(test)]
    pub(crate) executor_apart: AtomicBool,
}

impl Default for Turns {
    fn default() -> Self {
        Self {
            #[cfg(feature = "redis")]
            remote: None,
            application: None,
            client: None,
            worker_owner: Mutex::default(),
            error_classes: RwLock::default(),
            timeouts: RwLock::new(TurnTimeouts::default()),
            observers: RwLock::new(Observers::default()),
            executor_id: executor_id(),
            closed: AtomicBool::new(false),
            executor_lifecycle: Arc::default(),
            records: Mutex::new(RoomRecords::default()),
            rooms: Arc::default(),
            call_states: call_state::CallStates::default(),
            pending_detaches: Arc::default(),
            reads: Mutex::new(0),
            reads_changed: Notify::new(),
            cut: watch::Sender::new(false),
            started: Mutex::new(StartedTurns::default()),
            inputs: OwnerInputs::default(),
            #[cfg(test)]
            executor_apart: AtomicBool::new(false),
        }
    }
}

/// The owner's read of a turn's stream, counted while it lasts: the kit's
/// close waits, within its grace, for the reads of the turns it cut,
/// each storing what its turn said.
#[derive(Debug)]
pub(crate) struct Reading(Arc<Turns>);

impl Drop for Reading {
    fn drop(&mut self) {
        let mut reads = lock(&self.0.reads);
        *reads = reads.saturating_sub(1);
        drop(reads);
        self.0.reads_changed.notify_waiters();
    }
}

impl Turns {
    pub(crate) fn with_backend(backend: Option<TurnBackendHandle>) -> Self {
        let mut turns = Self::default();
        if let Some(backend) = backend {
            if let Some(timeouts) = backend.timeouts() {
                turns.set_timeouts(timeouts);
            }
            #[cfg(feature = "redis")]
            {
                let concrete: &dyn std::any::Any = &*backend;
                if let Some(redis) = concrete.downcast_ref::<RedisTurnBackend>() {
                    turns.remote = Some(redis.clone());
                }
            }
            turns.application = Some(application::Application::new(backend));
        }
        turns
    }

    pub(crate) fn configured_backend(&self) -> Option<&TurnBackendHandle> {
        self.application.as_ref().map(|app| &app.backend)
    }

    pub(crate) fn set_error_classes(&self, classes: ErrorClasses) {
        *self
            .error_classes
            .write()
            .unwrap_or_else(PoisonError::into_inner) = classes;
    }

    pub(crate) fn error_classes(&self) -> ErrorClasses {
        self.error_classes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The bounds of the turns submitted from now on.
    pub(crate) fn set_timeouts(&self, timeouts: TurnTimeouts) {
        *self
            .timeouts
            .write()
            .unwrap_or_else(PoisonError::into_inner) = timeouts;
    }

    /// Has `tap` told every message of the turns submitted from now on.
    pub(crate) fn set_tap(&self, tap: Tap) {
        let mut observers = self
            .observers
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        *observers = observers.clone().with_tap(tap);
    }

    /// Has `fault` decide the fate of every message of the turns submitted
    /// from now on.
    #[cfg(test)]
    pub(crate) fn set_fault(&self, fault: observe::Fault) {
        let mut observers = self
            .observers
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        *observers = observers.clone().with_fault(fault);
    }

    fn observers(&self) -> Observers {
        self.observers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Counts the owner's read of a turn's stream until the guard drops.
    pub(crate) fn reading(self: &Arc<Self>) -> Reading {
        *lock(&self.reads) += 1;
        Reading(Arc::clone(self))
    }

    /// Whether the owner reads no turn's stream.
    pub(crate) fn reads_none(&self) -> bool {
        *lock(&self.reads) == 0
    }

    pub(crate) fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// What is told each time a read ends.
    pub(crate) fn reads_changed(&self) -> &Notify {
        &self.reads_changed
    }

    /// The signal every turn's owner cuts its turn on: `true` once the kit's
    /// close started.
    pub(crate) fn cutting(&self) -> watch::Receiver<bool> {
        self.cut.subscribe()
    }

    /// Cuts every turn at once, the kit's close starting (RFC §10.6.8: the
    /// turn's cascade cancelled; §12.10.4): the owner's side of each, its
    /// output awaited or its stream read, crosses `cancel`, `kit_closed`,
    /// and a reader stores what it read; a turn submitted from now on
    /// fails `backend_closed`. The executor of a turn a `close()` is called
    /// from inside lets its owner go and its work run on.
    pub(crate) fn cut_all(&self) {
        self.cut.send_replace(true);
    }

    /// Takes no more turns: one submitted from now on fails `backend_closed`.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// The creation mark of the room the owner's last turn or channel call
    /// of `room_id` ran in, `None` when it holds none of the id.
    pub(crate) fn room_mark(&self, room_id: &RoomId) -> Option<roomkit_core::model::Timestamp> {
        lock(&self.records).room_mark(room_id)
    }

    pub(crate) fn timeouts(&self) -> TurnTimeouts {
        *self.timeouts.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn records(&self) -> MutexGuard<'_, RoomRecords> {
        lock(&self.records)
    }

    /// Runs one turn of `binding`'s channel on `event`: submits it, and
    /// reads its output back (RFC §10.6.3, §10.6.4).
    ///
    /// # Errors
    ///
    /// The turn's failure as a record: the channel's error, the link's under
    /// `TurnLinkError`, or for a turn the kit's close cut, `TURN_CUT`, no
    /// failure.
    pub(crate) async fn run<G: Send>(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        context: RoomContext,
        strategy_turn: bool,
        submitted: impl FnOnce(Canceller) -> G,
    ) -> Result<Answer, TurnFailure> {
        self.run_with_history(
            kit,
            binding,
            |_| std::future::ready((event, context)),
            strategy_turn,
            submitted,
        )
        .await
    }

    /// Prepare a kit-owned turn's history for its placement before submitting it.
    pub(crate) fn run_with_history<G: Send, F: Future<Output = (RoomEvent, RoomContext)> + Send>(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        prepare: impl FnOnce(bool) -> F + Send,
        strategy_turn: bool,
        submitted: impl FnOnce(Canceller) -> G,
    ) -> std::pin::Pin<Box<impl Future<Output = Result<Answer, TurnFailure>>>> {
        Box::pin(async move {
            // The turn's tasks run in a unit of their own, within the call's
            // run's: a close from the turn's work spares them all (RFC
            // §12.10.4).
            self.ready()
                .await
                .map_err(|e| TurnFailure::crossed(&self.error_classes().record(&e)))?;
            let channel = kit.channel(&binding.channel_id);
            let separate_rows = self.client.is_none()
                && binding.category == ChannelCategory::Intelligence
                && self.sends_channel_rows(channel.as_deref());
            let (event, mut context) = prepare(separate_rows).await;
            let channel_rows = if separate_rows {
                crate::history::distributed::distributed_rows(
                    kit.as_ref(),
                    binding,
                    &event,
                    &mut context,
                )
                .await
            } else {
                Vec::new()
            };
            let marks = Marks::current().with(Unit::new());
            let input = submission::Input {
                event,
                context,
                strategy_turn,
                channel_rows,
            };
            // A configured backend takes every turn by its public submit, a
            // live channel's too (RFC §10.6.7).
            let (mut turn, _following) = if let Some(app) = &self.application {
                marks
                    .scoped(self.start_application(app, kit, binding, input, submitted))
                    .await?
            } else {
                marks.sync_scope(|| self.start_turn(kit, binding, input, submitted))?
            };
            let (output, streams) = turn.output(self.cutting()).await?;
            Ok(if streams {
                Answer::Streamed(Box::new(output), turn)
            } else {
                Answer::Output(Box::new(output))
            })
        })
    }

    /// The turns of `channel_id` this owner runs in `room_id`, oldest
    /// first: each one's id and who wrote the message it answers (RFC §6.4
    /// "A message for the running turn").
    pub(crate) fn running_in(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Vec<(String, TurnAuthor)> {
        lock(&self.started).running_in(room_id, channel_id)
    }

    /// Steers the running turns `address` names (RFC §21.3, §10.6.8): a
    /// `Cancel` reaches every turn of a room, any other directive its most
    /// recent; each turn's executor applies it through the channel. How
    /// many loops it reached, summed.
    pub(crate) async fn steer(
        &self,
        directive: &SteeringDirective,
        address: SteerAddress<'_>,
    ) -> usize {
        let targets = lock(&self.started).targets(directive, address);
        let mut reached = 0usize;
        for target in targets {
            reached = reached.saturating_add(target.steer(directive).await);
        }
        reached
    }

    /// Answers the human-input request `request_id` of a turn with
    /// `value` (RFC §10.6.5): whether the tool waiting on it reads the
    /// answer, one for a request not open yet held for the hold bound.
    pub(crate) async fn answer_input(&self, request_id: &str, value: &str) -> bool {
        let hold = self.timeouts().bounded().input_hold_timeout;
        self.inputs.answer(request_id, value, hold).await
    }

    /// Rejects the human-input request `request_id` for `reason`: whether
    /// the waiting tool reads the rejection.
    pub(crate) async fn reject_input(&self, request_id: &str, reason: &str) -> bool {
        let hold = self.timeouts().bounded().input_hold_timeout;
        self.inputs.reject(request_id, reason, hold).await
    }

    /// The executor that took `turn_id` in `room_id`, as it said so.
    fn took(&self, channel_id: &ChannelId, room_id: &RoomId, turn_id: &str, executor_id: &str) {
        self.records()
            .took(channel_id, room_id, turn_id, executor_id);
    }

    /// Carries a verb an executor made after its turn ended to the owner,
    /// and its answer back, each serialized and parsed anew (RFC §10.6.3
    /// `room_call`): the owner's side runs in a context of its own, never
    /// the executor's.
    ///
    /// # Errors
    ///
    /// What the verb raised, rebuilt, or its refusal.
    pub(crate) async fn room_call(
        self: &Arc<Self>,
        kit: Arc<dyn TurnKit>,
        message: RoomCall,
    ) -> Result<serde_json::Value, RoomKitError> {
        let observers = self.observers();
        let parsed: OwnerMessageEnvelope = cross(
            &observers,
            Carried::RoomCall,
            &OwnerMessageEnvelope {
                message: OwnerMessage::RoomCall(message),
            },
        )?;
        let answered = self
            .receive_owner_message(kit, parsed)
            .await
            .ok_or_else(|| RoomKitError::Value("a room call did not receive its result".into()))?;
        let answered: RoomCallResult = cross(&observers, Carried::RoomCallResult, &answered)?;
        match answered.error {
            Some(error) => Err(self.error_classes().rebuild(&error)),
            None => Ok(answered.result),
        }
    }

    /// Notify the executors that serve this room’s intelligence channel after attach.
    pub(crate) async fn attached(
        &self,
        binding: &ChannelBinding,
        organization_id: Option<String>,
        channel: Option<Arc<dyn DynChannel>>,
    ) -> Result<(), RoomKitError> {
        self.pending_detaches
            .wait(&binding.channel_id, &binding.room_id)
            .await;
        let message = ChannelControl::RoomAttached {
            channel_id: binding.channel_id.clone(),
            room_id: binding.room_id.clone(),
            organization_id,
            binding: Box::new(binding.clone()),
        };
        self.send_channel_notification(message, channel).await
    }

    /// Forget the previous turn before notifying its executors of the detach.
    /// An executor missing the notification will still discard its stale state.
    pub(crate) async fn detached(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        organization_id: Option<String>,
        channel: Option<Arc<dyn DynChannel>>,
    ) -> Result<(), RoomKitError> {
        self.records().pop(channel_id, room_id);
        self.rooms.forget(channel_id, room_id);
        let message = ChannelControl::RoomDetached {
            channel_id: channel_id.clone(),
            room_id: room_id.clone(),
            organization_id,
        };
        self.send_channel_notification(message, channel).await
    }

    /// Hand a committed event to the memory of an eligible executor without
    /// starting a turn (RFC §10.6.3, §19.7.5 rule 3).
    pub(crate) async fn committed(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        event: &RoomEvent,
        organization_id: Option<String>,
        room_hooks: Vec<String>,
        channel: Option<Arc<dyn DynChannel>>,
    ) -> Result<(), RoomKitError> {
        let message = ChannelControl::CommittedEvent {
            channel_id: channel_id.clone(),
            room_id: room_id.clone(),
            organization_id,
            event: Box::new(event.clone()),
            room_hooks,
        };
        self.send_channel_notification(message, channel).await
    }

    /// Carries a channel control message to the kit's own executor: the
    /// message as the executor parses it, `None` when it could not cross.
    fn control(&self, message: &ChannelControl) -> Option<ChannelControl> {
        let envelope = serde_json::to_string(&ChannelControlEnvelope {
            message: message.clone(),
        });
        let Ok(raw) = envelope else {
            tracing::warn!(target: "roomkit::turns", "a channel control message could not be written");
            return None;
        };
        self.observers().tell(Carried::ChannelControl, &raw);
        match serde_json::from_str::<ChannelControlEnvelope>(&raw) {
            Ok(parsed) => Some(parsed.message),
            Err(error) => {
                tracing::warn!(target: "roomkit::turns", %error, "a channel control message could not be read");
                None
            }
        }
    }
}

/// Applies a room's attach or detach `message`, or a committed event, on
/// the executor to `channel`, the executor's (`None`: none registered), as
/// the Python implementation's `ExecutorRooms.apply` and `_hand_event`:
/// what the channel raises is logged, never returned (RFC §10.6.3).
pub(crate) async fn apply(channel: Option<Arc<dyn DynChannel>>, message: ChannelControl) {
    let Some(channel) = channel else {
        return;
    };
    let (kind, channel_id, room_id, applied) = match message {
        // A local kit holds its live channel through registration. Its release
        // applies only to builds owned by independently hosted executors.
        ChannelControl::ChannelReleased { .. } => return,
        ChannelControl::ChannelCall(_) => unreachable!("channel calls require an answer"),
        ChannelControl::RoomAttached {
            channel_id,
            room_id,
            binding,
            ..
        } => {
            let applied = channel.on_room_attached(room_id.clone(), *binding).await;
            ("room_attached", channel_id, room_id, applied)
        }
        ChannelControl::RoomDetached {
            channel_id,
            room_id,
            ..
        } => {
            let applied = channel.on_room_detached(room_id.clone()).await;
            ("room_detached", channel_id, room_id, applied)
        }
        ChannelControl::CommittedEvent {
            channel_id,
            room_id,
            event,
            ..
        } => {
            let applied = channel.on_room_event(room_id.clone(), *event).await;
            ("committed_event", channel_id, room_id, applied)
        }
    };
    if let Err(error) = applied {
        tracing::error!(
            target: "roomkit::turns",
            channel_id = %channel_id,
            room_id = %room_id,
            %error,
            "Channel {channel_id} failed to apply {kind} for room {room_id}"
        );
    }
}

/// `message`, serialized within its bound and parsed anew, as a message
/// outside a turn's link crosses.
fn cross<T: Serialize + serde::de::DeserializeOwned>(
    observers: &Observers,
    carried: Carried,
    message: &T,
) -> Result<T, RoomKitError> {
    let failed = |why: String| TurnError::new("frame_invalid", why).into_kit_error();
    let raw = serde_json::to_string(message).map_err(|error| failed(error.to_string()))?;
    if raw.len() > MAX_MESSAGE_BYTES {
        return Err(failed(format!(
            "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
            raw.len()
        )));
    }
    observers.tell(carried, &raw);
    serde_json::from_str(&raw).map_err(|error| failed(error.to_string()))
}

/// A new executor id: `executor-` and twelve hex digits, as the Python
/// implementation's.
fn executor_id() -> String {
    let drawn = Uuid::new_v4().simple().to_string();
    format!("executor-{}", drawn.get(..12).unwrap_or(&drawn))
}

/// Resolve placement before changing the previous-turn record. A refusal
/// before handover must preserve the last valid turn (RFC §10.6.2).
fn request_room_hooks(
    kit: &dyn TurnKit,
    room: &RoomId,
    channel: &ChannelId,
    local: bool,
) -> Result<Vec<String>, ErrorRecord> {
    let names = kit.room_hook_names(room);
    // The target worker, not the owner, supplies the hooks of a distributed turn.
    if local && !kit.defines_room_hooks(&names) {
        return Err(TurnError::new(
            "no_executor",
            format!("No executor takes {channel} with room hooks {names:?}"),
        )
        .to_record());
    }
    Ok(names)
}

/// The error of a turn the backend no longer takes, or still open as the
/// kit closes: the turn link's `backend_closed`, naming the backend's
/// close (RFC §10.6.8).
pub(crate) fn closed() -> ErrorRecord {
    TurnError::link_reason("backend_closed").to_record()
}

/// A request that cannot cross.
fn uncarried(why: &str) -> ErrorRecord {
    ErrorRecord::new(
        CHANNEL_ERROR,
        format!("the turn's request cannot cross the turn backend: {why}"),
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
