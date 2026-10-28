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
//! - `executor`: the executor's side of a turn.
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
mod pipe;
mod pulls;
mod records;
#[cfg(test)]
mod tests;
mod timeouts;
mod wire;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent, RoomId,
};
use serde::Serialize;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use uuid::Uuid;

pub(crate) use self::codec::send_event_arguments;
pub use self::error::TurnError;
pub(crate) use self::error::record_of;
pub(crate) use self::executor::HostLink;
use self::memory::Request;
pub(crate) use self::memory::{CarriedRequest, MemoryTurn};
use self::observe::Observers;
pub(crate) use self::observe::{Carried, Tap};
pub(crate) use self::owner::{Canceller, OwnerTurn, RemoteItem};
use self::pipe::{MAX_MESSAGE_BYTES, pipe};
use self::records::RoomRecords;
pub use self::timeouts::TurnTimeouts;
pub(crate) use self::wire::SEND_EVENT;
use self::wire::{
    ChannelControl, ChannelControlEnvelope, OwnerMessage, OwnerMessageEnvelope, PreviousTurn,
    RoomCallResult,
};
use crate::context::ExecutionContext;
use crate::error::KitError;
use crate::tasks::{Marks, Unit};
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
    /// The turns the owner is working on, by their unit: how many times,
    /// awaiting their output or reading their stream, and the unit of the
    /// run that works on them.
    working: Mutex<HashMap<Unit, (usize, Option<Unit>)>>,
    idle: Notify,
    /// Set as the kit shuts down: every turn is cut but the spared ones.
    cut: watch::Sender<Cut>,
}

/// The units of the kit's work a turn is: its own, which its tasks run
/// in, and the run's whose delivery set runs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TurnUnits {
    pub(crate) turn: Unit,
    pub(crate) run: Option<Unit>,
}

impl TurnUnits {
    /// Whether the kit's close spares the turn, whose units are in
    /// `spared`: a close called from inside it, or from inside the run
    /// that waits for it (RFC §12.10.4).
    fn spared_by(&self, spared: &HashSet<Unit>) -> bool {
        spared.contains(&self.turn) || self.run.is_some_and(|run| spared.contains(&run))
    }
}

/// The kit's cut of its turns (RFC §10.6.8): none until the kit shuts
/// down, then every turn but those of the units a caller of the kit's
/// close runs within, which the shutdown spares (RFC §12.10.4).
#[derive(Clone, Debug, Default)]
pub(crate) struct Cut(Option<Arc<HashSet<Unit>>>);

impl Cut {
    /// Whether the kit's shutdown began cutting its turns.
    fn began(&self) -> bool {
        self.0.is_some()
    }

    /// Whether the kit's shutdown cuts the turn of `units`; `None`, a turn
    /// whose units are not known, is cut.
    pub(crate) fn cuts(&self, units: Option<TurnUnits>) -> bool {
        self.0
            .as_ref()
            .is_some_and(|spared| units.is_none_or(|units| !units.spared_by(spared)))
    }
}

impl Default for Turns {
    fn default() -> Self {
        Self {
            timeouts: RwLock::new(TurnTimeouts::default()),
            observers: RwLock::new(Observers::default()),
            executor_id: executor_id(),
            closed: AtomicBool::new(false),
            records: Mutex::new(RoomRecords::default()),
            working: Mutex::new(HashMap::new()),
            idle: Notify::new(),
            cut: watch::Sender::new(Cut::default()),
        }
    }
}

/// The owner's work on the turn of a unit, counted while it lasts.
#[derive(Debug)]
pub(crate) struct Working(Arc<Turns>, Unit);

impl Drop for Working {
    fn drop(&mut self) {
        let mut working = lock(&self.0.working);
        if let Some((count, _)) = working.get_mut(&self.1) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                working.remove(&self.1);
            }
        }
        drop(working);
        self.0.idle.notify_waiters();
    }
}

/// What the kit's shutdown cut of its turns: the units of the runs that
/// worked on them, which commit what their readers read, and until when
/// they may.
#[derive(Debug)]
pub(crate) struct CutTurns {
    pub(crate) runs: HashSet<Unit>,
    pub(crate) until: Instant,
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

    /// Counts the owner's work on the turn of `units` until the guard
    /// drops.
    pub(crate) fn working(self: &Arc<Self>, units: TurnUnits) -> Working {
        let mut working = lock(&self.working);
        working.entry(units.turn).or_insert((0, units.run)).0 += 1;
        drop(working);
        Working(Arc::clone(self), units.turn)
    }

    /// The signal every turn's reader cuts its turn on, once the kit shuts
    /// down.
    pub(crate) fn cutting(&self) -> watch::Receiver<Cut> {
        self.cut.subscribe()
    }

    /// Cuts every turn but those of the units in `spared`, which a caller
    /// of the kit's close runs within (the kit's shutdown, RFC §10.6.8:
    /// the turn's cascade cancelled; RFC §12.10.4): each crosses as
    /// `cancel`, and its reader stores what it read; returns once the owner
    /// works on no turn it cut, or after twice the cancel bound, with the
    /// runs that worked on them and the end of that bound.
    pub(crate) async fn cut_all(&self, spared: HashSet<Unit>) -> CutTurns {
        let spared = Arc::new(spared);
        self.cut.send_replace(Cut(Some(Arc::clone(&spared))));
        let bound = self.timeouts().cancel.saturating_mul(2);
        let cut_turns = || {
            lock(&self.working)
                .iter()
                .map(|(&turn, &(_, run))| TurnUnits { turn, run })
                .filter(|units| !units.spared_by(&spared))
                .collect::<Vec<_>>()
        };
        let cut = CutTurns {
            runs: cut_turns()
                .into_iter()
                .filter_map(|units| units.run)
                .collect(),
            until: Instant::now() + bound,
        };
        let settled = async {
            loop {
                let mut idle = std::pin::pin!(self.idle.notified());
                idle.as_mut().enable();
                if cut_turns().is_empty() {
                    return;
                }
                idle.await;
            }
        };
        if tokio::time::timeout_at(cut.until, settled).await.is_err() {
            tracing::warn!(
                target: "roomkit::turns",
                bound_s = bound.as_secs_f64(),
                "turns still ran when the kit shut down: their work is dropped"
            );
        }
        cut
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
    /// The turn's failure as a record: the channel's error, or the link's
    /// under `TurnLinkError`.
    pub(crate) async fn run(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        context: RoomContext,
        submitted: impl FnOnce(Canceller),
    ) -> Result<Answer, ErrorRecord> {
        // The turn's tasks run in a unit of their own, within the call's
        // run's: a close from the turn's work, or from its run's, spares
        // them all (RFC §12.10.4).
        let marks = Marks::current();
        let units = TurnUnits {
            turn: Unit::new(),
            run: marks.innermost(),
        };
        let _working = self.working(units);
        let mut turn = marks
            .with(units.turn)
            .sync_scope(|| self.submit(kit, binding, event, context, units))?;
        submitted(turn.canceller());
        let (output, streams) = turn.output(self.cutting()).await?;
        Ok(if streams {
            Answer::Streamed(Box::new(output), turn)
        } else {
            Answer::Output(Box::new(output))
        })
    }

    /// Hands a turn to the kit's own executor, and opens its owner's side,
    /// the turn's tasks in its `units`' own, the one the current code runs
    /// in.
    fn submit(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        mut context: RoomContext,
        units: TurnUnits,
    ) -> Result<OwnerTurn, ErrorRecord> {
        if self.closed.load(Ordering::Acquire) || self.cut.borrow().began() {
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
        let request = Request {
            turn_id: &turn_id,
            binding,
            event: &event,
            context: &context,
            history: &history,
            previous: previous.as_ref(),
        };
        let rest = request.write(&observers).map_err(|why| uncarried(&why))?;
        let timeouts = self.timeouts();
        let (frames, frames_read) = pipe(Arc::clone(&turn_id), Carried::Frame, observers.clone());
        let (controls, controls_read) = pipe(Arc::clone(&turn_id), Carried::Control, observers);
        let memory = Arc::new(MemoryTurn::new(turn_id, frames, controls, timeouts));
        let facts = owner::Facts::of(binding, &event, &context.room);
        let (turn, pump) = owner::open(
            Arc::clone(&memory),
            frames_read,
            facts,
            Arc::clone(kit),
            Arc::clone(self),
            units,
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
        Ok(turn)
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
    ) -> Result<serde_json::Value, KitError> {
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
    /// was attached to its room (RFC §10.6.3 `room_attached`): a channel of
    /// 0.1 keeps no state per room, so the executor applies nothing.
    pub(crate) fn attached(&self, binding: &ChannelBinding) {
        self.control(&ChannelControl::RoomAttached {
            channel_id: binding.channel_id.clone(),
            room_id: binding.room_id.clone(),
            binding: Box::new(binding.clone()),
        });
    }

    /// Tells the kit's own executor that `channel_id` was detached from
    /// `room_id` (RFC §10.6.3 `room_detached`): the room's next turn of the
    /// channel names no previous turn, and work its last turn began is
    /// refused.
    pub(crate) fn detached(&self, channel_id: &ChannelId, room_id: &RoomId) {
        self.records().pop(channel_id, room_id);
        self.control(&ChannelControl::RoomDetached {
            channel_id: channel_id.clone(),
            room_id: room_id.clone(),
        });
    }

    /// Carries a channel control message to the kit's own executor, which
    /// parses it and applies it as the channel's own attach or detach
    /// would: no channel of 0.1 holds state per room.
    fn control(&self, message: &ChannelControl) {
        let envelope = serde_json::to_string(&ChannelControlEnvelope {
            message: message.clone(),
        });
        let Ok(raw) = envelope else {
            tracing::warn!(target: "roomkit::turns", "a channel control message could not be written");
            return;
        };
        self.observers().tell(Carried::ChannelControl, &raw);
        match serde_json::from_str::<ChannelControlEnvelope>(&raw) {
            Ok(parsed) => {
                tracing::trace!(target: "roomkit::turns", message = ?parsed.message, "channel control applied");
            }
            Err(error) => {
                tracing::warn!(target: "roomkit::turns", %error, "a channel control message could not be read");
            }
        }
    }
}

/// `message`, serialized within its bound and parsed anew, as a message
/// outside a turn's link crosses.
fn cross<T: Serialize + serde::de::DeserializeOwned>(
    observers: &Observers,
    carried: Carried,
    message: &T,
) -> Result<T, KitError> {
    let failed = |why: String| KitError::Turn(TurnError::new("frame_invalid", why));
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

/// A turn the backend no longer takes (RFC §10.6.8 `backend_closed`).
fn closed() -> ErrorRecord {
    TurnError::new("backend_closed", "the turn backend closed").to_record()
}

/// A request that cannot cross.
fn uncarried(why: &str) -> ErrorRecord {
    ErrorRecord::new(
        error::CHANNEL_ERROR,
        format!("the turn's request cannot cross the turn backend: {why}"),
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
