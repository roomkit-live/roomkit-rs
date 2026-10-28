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
use std::time::Duration;

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
    /// The turns the owner is working on, by their unit, and how many
    /// times: awaiting their output, then reading their stream.
    working: Mutex<HashMap<(Unit, TurnStage), usize>>,
    idle: Notify,
    /// The streams the owner reads, by their turn's unit.
    reads: Mutex<HashMap<Unit, Read>>,
    /// Told each time a read starts or ends, and each time a turn's stream
    /// starts or stops waiting on its channel.
    reads_changed: Arc<Notify>,
    /// Set as the kit shuts down: the streams read then first, then every
    /// turn, but the spared ones.
    cut: watch::Sender<Cut>,
}

/// Where the owner works on a turn: awaiting its output, the channel's
/// `on_event()`, or reading its stream, the model's rounds and the tools'
/// calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TurnStage {
    /// The owner awaits the turn's output.
    Output,
    /// The owner reads the turn's stream.
    Stream,
}

/// The kit's cut of its turns (RFC §10.6.8), but those of the units a
/// caller of the kit's close runs within, which the shutdown spares (RFC
/// §12.10.4): none until the kit shuts down; then, at once, the turns whose
/// streams are read as the close starts, and those whose read begins as
/// the close waits for them, their work cut with no grace, as the Python
/// implementation cuts the stream readers it has then; then, once the
/// lanes are idle or their grace is over, every turn, as Python's
/// `turns.close` follows its `lanes.aclose`.
#[derive(Clone, Debug, Default)]
pub(crate) struct Cut {
    /// The units of the turns whose streams were read as the close
    /// started: a turn submitted, or a stream read, from then on runs on.
    streams: Arc<HashSet<Unit>>,
    /// Whether every turn is cut, the lanes idle or their grace over.
    all: bool,
    spared: Arc<HashSet<Unit>>,
}

impl Cut {
    /// Whether the kit's shutdown cuts the turn of `unit` where the owner
    /// works on it at `stage`; `None`, a turn whose unit is not known, is
    /// cut with every turn.
    pub(crate) fn cuts(&self, unit: Option<Unit>, stage: TurnStage) -> bool {
        let Some(unit) = unit else {
            return self.all;
        };
        !self.spared.contains(&unit)
            && (self.all || (stage == TurnStage::Stream && self.streams.contains(&unit)))
    }

    /// Whether the cut of the stream of `unit`'s turn lets its reader read
    /// what the turn sent first: the cut of every turn, once the lanes are
    /// idle, as the Python implementation's `turns.close` abandons the
    /// turns left, their readers reading what came; not the cut of the
    /// streams read as the close starts, which stops their readers at once,
    /// as Python's cancels them.
    pub(crate) fn reads_what_came(&self, unit: Option<Unit>) -> bool {
        unit.is_none_or(|unit| !self.streams.contains(&unit))
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
            reads: Mutex::new(HashMap::new()),
            reads_changed: Arc::default(),
            cut: watch::Sender::new(Cut::default()),
        }
    }
}

/// The owner's work on the turn of a unit at a stage, counted while it
/// lasts.
#[derive(Debug)]
pub(crate) struct Working(Arc<Turns>, (Unit, TurnStage));

/// A stream the owner reads: the unit of the run reading it, and the turn
/// as its two sides share it.
#[derive(Debug)]
struct Read {
    run: Option<Unit>,
    turn: Arc<MemoryTurn>,
}

/// The owner's read of a turn's stream, known while it lasts: the turn
/// counted as worked on at its stream (`Working`), and the read recorded
/// with the run reading it.
#[derive(Debug)]
pub(crate) struct Reading {
    turns: Arc<Turns>,
    unit: Unit,
    _working: Working,
}

impl Drop for Reading {
    fn drop(&mut self) {
        lock(&self.turns.reads).remove(&self.unit);
        self.turns.reads_changed.notify_waiters();
    }
}

impl Drop for Working {
    fn drop(&mut self) {
        let mut working = lock(&self.0.working);
        if let Some(count) = working.get_mut(&self.1) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                working.remove(&self.1);
            }
        }
        drop(working);
        self.0.idle.notify_waiters();
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

    /// Counts the owner's work on the turn of `unit` at `stage` until the
    /// guard drops.
    pub(crate) fn working(self: &Arc<Self>, unit: Unit, stage: TurnStage) -> Working {
        *lock(&self.working).entry((unit, stage)).or_default() += 1;
        Working(Arc::clone(self), (unit, stage))
    }

    /// Counts the owner's read of `turn`'s stream, by the run the current
    /// code runs within, until the guard drops.
    pub(crate) fn reading(self: &Arc<Self>, turn: &OwnerTurn) -> Reading {
        let unit = turn.unit();
        let run = Marks::current().innermost();
        let working = self.working(unit, TurnStage::Stream);
        let read = Read {
            run,
            turn: Arc::clone(turn.memory()),
        };
        lock(&self.reads).insert(unit, read);
        self.reads_changed.notify_waiters();
        Reading {
            turns: Arc::clone(self),
            unit,
            _working: working,
        }
    }

    /// Whether the run of `run` reads streams, and each of them waits on
    /// its channel, what it sent before read or on its way: the run's turns
    /// say nothing more for now, and the kit's close may cut them (RFC
    /// §10.6.8).
    pub(crate) fn reads_wait(&self, run: Unit) -> bool {
        let reads = lock(&self.reads);
        let mut of_run = reads
            .values()
            .filter(|read| read.run == Some(run))
            .peekable();
        of_run.peek().is_some() && of_run.all(|read| read.turn.waits_on_channel())
    }

    /// What is told each time a read starts or ends, and each time a
    /// turn's stream starts or stops waiting on its channel.
    pub(crate) fn reads_changed(&self) -> &Notify {
        &self.reads_changed
    }

    /// The signal every turn's reader cuts its turn on, once the kit shuts
    /// down.
    pub(crate) fn cutting(&self) -> watch::Receiver<Cut> {
        self.cut.subscribe()
    }

    /// Cuts at once the turns whose streams the owner reads now, but those
    /// of the units in `spared`, which a caller of the kit's close runs
    /// within (the kit's shutdown, RFC §10.6.8: the turn's cascade
    /// cancelled; RFC §12.10.4): each crosses as `cancel`, and its reader
    /// stores what it read. Once the owner reads no stream it cut, the
    /// streams whose read began meanwhile are cut in turn, until none is
    /// read, as the Python implementation's close cuts its stream readers
    /// again until none is left; all within twice the cancel bound. A turn
    /// whose output the owner still awaits, the channel's `on_event()`, a
    /// turn submitted and a stream read from then on run on, as part of
    /// their delivery set, until [`cut_all`](Self::cut_all).
    pub(crate) async fn cut_streams(&self, spared: HashSet<Unit>) {
        let deadline = Instant::now() + self.cut_bound();
        let spared = Arc::new(spared);
        let mut streams = HashSet::new();
        loop {
            let reading: Vec<Unit> = lock(&self.working)
                .keys()
                .filter(|&&(unit, stage)| {
                    stage == TurnStage::Stream
                        && !spared.contains(&unit)
                        && !streams.contains(&unit)
                })
                .map(|&(unit, _)| unit)
                .collect();
            if reading.is_empty() {
                return;
            }
            streams.extend(reading);
            let cut = Cut {
                streams: Arc::new(streams.clone()),
                all: false,
                spared: Arc::clone(&spared),
            };
            if !self.cut_with(cut, deadline).await {
                return;
            }
        }
    }

    /// Cuts every turn, but those of the units in `spared`, as
    /// [`cut_streams`](Self::cut_streams) does, within twice the cancel
    /// bound; a turn submitted from now on fails `backend_closed`.
    pub(crate) async fn cut_all(&self, spared: HashSet<Unit>) {
        let cut = Cut {
            streams: Arc::default(),
            all: true,
            spared: Arc::new(spared),
        };
        self.cut_with(cut, Instant::now() + self.cut_bound()).await;
    }

    /// How long the close waits for the turns it cut: twice the cancel
    /// bound.
    fn cut_bound(&self) -> Duration {
        self.timeouts().cancel.saturating_mul(2)
    }

    /// Sets `cut`, and waits until the owner works on no turn it cuts, by
    /// `deadline`: whether it did.
    async fn cut_with(&self, cut: Cut, deadline: Instant) -> bool {
        self.cut.send_replace(cut.clone());
        let settled = async {
            loop {
                let mut idle = std::pin::pin!(self.idle.notified());
                idle.as_mut().enable();
                let cutting = lock(&self.working)
                    .keys()
                    .any(|&(unit, at)| cut.cuts(Some(unit), at));
                if !cutting {
                    return;
                }
                idle.await;
            }
        };
        let settled = tokio::time::timeout_at(deadline, settled).await.is_ok();
        if !settled {
            tracing::warn!(
                target: "roomkit::turns",
                bound_s = self.cut_bound().as_secs_f64(),
                "turns still ran when the kit shut down: their work is dropped"
            );
        }
        settled
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
        // run's: a close from the turn's work spares them all (RFC
        // §12.10.4).
        let unit = Unit::new();
        let _working = self.working(unit, TurnStage::Output);
        let mut turn = Marks::current()
            .with(unit)
            .sync_scope(|| self.submit(kit, binding, event, context, unit))?;
        submitted(turn.canceller());
        let (output, streams) = turn.output(self.cutting()).await?;
        Ok(if streams {
            Answer::Streamed(Box::new(output), turn)
        } else {
            Answer::Output(Box::new(output))
        })
    }

    /// Hands a turn to the kit's own executor, and opens its owner's side,
    /// the turn's tasks in `unit`, the one the current code runs in.
    fn submit(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        mut context: RoomContext,
        unit: Unit,
    ) -> Result<OwnerTurn, ErrorRecord> {
        if self.closed.load(Ordering::Acquire) || self.cut.borrow().all {
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
        let memory = Arc::new(MemoryTurn::new(
            turn_id,
            frames,
            controls,
            timeouts,
            Arc::clone(&self.reads_changed),
        ));
        let facts = owner::Facts::of(binding, &event, &context.room);
        let (turn, pump) = owner::open(
            Arc::clone(&memory),
            frames_read,
            facts,
            Arc::clone(kit),
            Arc::clone(self),
            unit,
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
/// The refusal of a turn the backend no longer takes, its link's error
/// naming the backend's close (RFC §10.6.8 `backend_closed`).
pub(crate) fn closed() -> ErrorRecord {
    TurnError::new("backend_closed", "the turn backend closed").to_record()
}

/// A turn whose stream ended before it said anything: the owner let go of
/// it, the kit closing (RFC §10.6.8), as the Python implementation's
/// `turns.close` abandons it.
pub(crate) fn ended_mid_stream() -> ErrorRecord {
    TurnError::new(
        "executor_lost",
        "the turn ended in the middle of its stream",
    )
    .to_record()
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
