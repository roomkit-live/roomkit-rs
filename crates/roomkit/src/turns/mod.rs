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

mod codec;
mod error;
mod executor;
mod memory;
mod observe;
mod owner;
mod owner_inputs;
mod pipe;
mod pulls;
mod records;
pub(crate) mod served;
mod started;
#[cfg(test)]
mod tests;
mod timeouts;
pub(crate) mod wire;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent, RoomId,
};
use serde::Serialize;
use tokio::sync::{Notify, watch};
use uuid::Uuid;

pub(crate) use self::codec::send_event_arguments;
pub(crate) use self::error::{CHANNEL_ERROR, TURN_LINK_ERROR, record_of, variant_name};
pub(crate) use self::error::{TurnError, TurnFailure, to_record};
pub(crate) use self::executor::{ExecutorInputs, HostLink, InputSettler};
use self::memory::Request;
pub(crate) use self::memory::{CarriedRequest, MemoryTurn};
use self::observe::Observers;
pub(crate) use self::observe::{Carried, Tap};
pub(crate) use self::owner::{Canceller, OwnerTurn, RemoteItem};
pub(crate) use self::owner_inputs::InputTurn;
use self::owner_inputs::OwnerInputs;
use self::pipe::{MAX_MESSAGE_BYTES, pipe};
use self::records::RoomRecords;
use self::started::StartedTurns;
pub(crate) use self::started::SteerAddress;
pub use self::timeouts::TurnTimeouts;
pub(crate) use self::wire::SEND_EVENT;
use self::wire::{
    ChannelControl, ChannelControlEnvelope, OwnerMessage, OwnerMessageEnvelope, PreviousTurn,
    RoomCallResult,
};
use crate::channel::DynChannel;
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::steering::SteeringDirective;
use crate::tasks::{Marks, Unit};
use crate::telemetry::current_trace;
use crate::turn::TurnKit;

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
    timeouts: RwLock<TurnTimeouts>,
    observers: RwLock<Observers>,
    /// The id the kit's own executor answers with.
    executor_id: String,
    closed: AtomicBool,
    /// Each room's last turn of each channel the owner submitted.
    records: Mutex<RoomRecords>,
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
            timeouts: RwLock::new(TurnTimeouts::default()),
            observers: RwLock::new(Observers::default()),
            executor_id: executor_id(),
            closed: AtomicBool::new(false),
            records: Mutex::new(RoomRecords::default()),
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

    fn timeouts(&self) -> TurnTimeouts {
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
    pub(crate) async fn run(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        context: RoomContext,
        submitted: impl FnOnce(Canceller),
    ) -> Result<Answer, TurnFailure> {
        // The turn's tasks run in a unit of their own, within the call's
        // run's: a close from the turn's work spares them all (RFC
        // §12.10.4).
        let unit = Unit::new();
        let mut turn = Marks::current()
            .with(unit)
            .sync_scope(|| self.submit(kit, binding, event, context))?;
        submitted(turn.canceller());
        let (output, streams) = turn.output(self.cutting()).await?;
        Ok(if streams {
            Answer::Streamed(Box::new(output), turn)
        } else {
            Answer::Output(Box::new(output))
        })
    }

    /// Hands a turn to the kit's own executor, and opens its owner's side,
    /// the turn's tasks in the unit the current code runs in.
    fn submit(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        mut context: RoomContext,
    ) -> Result<OwnerTurn, ErrorRecord> {
        if self.closed.load(Ordering::Acquire) || *self.cut.borrow() {
            return Err(closed());
        }
        let turn_id: Arc<str> = Arc::from(format!("turn-{}", Uuid::new_v4().simple()));
        let previous = self.records().put(
            &binding.channel_id,
            &event.room_id,
            PreviousTurn {
                turn_id: turn_id.to_string(),
                executor_id: None,
            },
        );
        let history = std::mem::take(&mut context.recent_events);
        let observers = self.observers();
        let owner_tools = kit.owner_tools(&binding.channel_id, &event.room_id);
        let trace = current_trace();
        let request = Request {
            turn_id: &turn_id,
            binding,
            event: &event,
            context: &context,
            history: &history,
            previous: previous.as_ref(),
            owner_tools: &owner_tools,
            trace: trace.as_ref(),
        };
        let rest = request.write(&observers).map_err(|why| uncarried(&why))?;
        let timeouts = self.timeouts();
        let (frames, frames_read) = pipe(Arc::clone(&turn_id), Carried::Frame, observers.clone());
        let (controls, controls_read) = pipe(Arc::clone(&turn_id), Carried::Control, observers);
        let memory = Arc::new(MemoryTurn::new(turn_id, frames, controls, timeouts));
        let facts = owner::Facts::of(binding, &event, &context.room);
        let (room_id, organization_id) = (facts.room_id.clone(), facts.organization_id.clone());
        let (turn, pump) = owner::open(
            Arc::clone(&memory),
            frames_read,
            facts,
            Arc::clone(kit),
            Arc::clone(self),
        );
        let carried = CarriedRequest {
            rest,
            event,
            history,
        };
        let taken = kit.spawn(pump)
            && executor::take(self, Arc::clone(&memory), controls_read, carried, kit);
        if !taken {
            memory.shut();
            return Err(closed());
        }
        lock(&self.started).start(
            (&memory.turn_id, &room_id, &binding.channel_id),
            organization_id.as_deref(),
            turn.steerable(),
        );
        Ok(turn)
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
        message: OwnerMessage,
    ) -> Result<serde_json::Value, RoomKitError> {
        let observers = self.observers();
        let parsed: OwnerMessageEnvelope = cross(
            &observers,
            Carried::RoomCall,
            &OwnerMessageEnvelope { message },
        )?;
        let OwnerMessage::RoomCall {
            channel_id,
            room_id,
            ..
        } = &parsed.message;
        let (channel_id, room_id) = (channel_id.clone(), room_id.clone());
        let last = || owner::last_turn(&self.records(), &channel_id, &room_id);
        let answering = owner::answer_room_call(&kit, last, parsed.message);
        let answered = ExecutionContext::default().scope(answering).await;
        let answered: RoomCallResult = cross(&observers, Carried::RoomCallResult, &answered)?;
        match answered.error {
            Some(error) => Err(error::rebuild_kit_error(&error)),
            None => Ok(answered.result),
        }
    }

    /// Tells the kit's own executor that `binding`'s intelligence channel
    /// was attached to its room (RFC §10.6.3 `room_attached`): it calls the
    /// channel's `on_room_attached`, `channel` the executor's, and logs
    /// what the channel raises.
    pub(crate) async fn attached(
        &self,
        binding: &ChannelBinding,
        channel: Option<Arc<dyn DynChannel>>,
    ) {
        let message = ChannelControl::RoomAttached {
            channel_id: binding.channel_id.clone(),
            room_id: binding.room_id.clone(),
            binding: Box::new(binding.clone()),
        };
        if let Some(message) = self.control(&message) {
            apply(channel, message).await;
        }
    }

    /// Tells the kit's own executor that `channel_id` was detached from
    /// `room_id` (RFC §10.6.3 `room_detached`): the room's next turn of the
    /// channel names no previous turn, and work its last turn began is
    /// refused; the executor calls the channel's `on_room_detached`,
    /// `channel` the executor's, and logs what the channel raises.
    pub(crate) async fn detached(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        channel: Option<Arc<dyn DynChannel>>,
    ) {
        self.records().pop(channel_id, room_id);
        let message = ChannelControl::RoomDetached {
            channel_id: channel_id.clone(),
            room_id: room_id.clone(),
        };
        if let Some(message) = self.control(&message) {
            apply(channel, message).await;
        }
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

/// Applies a room's attach or detach `message` on the executor to
/// `channel`, the executor's (`None`: none registered), as the Python
/// implementation's `ExecutorRooms.apply`: what the channel raises is
/// logged, never returned (RFC §10.6.3).
async fn apply(channel: Option<Arc<dyn DynChannel>>, message: ChannelControl) {
    let Some(channel) = channel else {
        return;
    };
    let (kind, channel_id, room_id, applied) = match message {
        ChannelControl::RoomAttached {
            channel_id,
            room_id,
            binding,
        } => {
            let applied = channel.on_room_attached(room_id.clone(), *binding).await;
            ("room_attached", channel_id, room_id, applied)
        }
        ChannelControl::RoomDetached {
            channel_id,
            room_id,
        } => {
            let applied = channel.on_room_detached(room_id.clone()).await;
            ("room_detached", channel_id, room_id, applied)
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
