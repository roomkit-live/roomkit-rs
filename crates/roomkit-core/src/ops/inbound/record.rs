//! The commit of a record the room refused before any hook (RFC §8.3).
//!
//! A channel not asked at the chain-depth limit, a response past its
//! cascade's reentry budget, a response a transport returned at the limit:
//! each is stored `BLOCKED`, under the room's lock, with the next index, and
//! runs no hook. The room's due timer transitions apply first (gap G-045),
//! and a room that no longer accepts events refuses the record, as it
//! refuses any write (RFC §5.1).

use std::mem;

use crate::model::{ErrorRecord, EventStatus, RoomContext, RoomEvent, RoomUpdates};
use crate::ops::lifecycle::gate::{self, Gate, Pending};
use crate::ops::protocol::{EffectId, Outcome, Request, RoomCounters};
use crate::ops::{self, Abort, Cx, Progress};

/// What became of a record.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum RecordResult {
    /// Stored, with its index.
    Committed(Box<RoomEvent>),
    /// The room no longer accepts events: nothing was stored.
    Refused,
}

/// Everything a record's commit produced.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RecordReport {
    pub(super) result: RecordResult,
    /// The failure of the lock's release, which changes nothing the commit
    /// decided (gap G-025).
    pub(super) release_error: Option<ErrorRecord>,
}

/// The commit of one blocked record.
#[derive(Debug)]
pub(super) struct RecordPass {
    record: RoomEvent,
    state: State,
    release_error: Option<ErrorRecord>,
}

#[derive(Debug)]
enum State {
    Start,
    Locking(EffectId),
    LoadingRoom(EffectId),
    ApplyingTimers {
        effect: EffectId,
        pending: Box<Pending>,
    },
    Committing(EffectId),
    Releasing {
        effect: EffectId,
        result: Result<RecordResult, Abort>,
    },
    Finished(Result<RecordResult, Abort>),
    Reported,
}

impl State {
    fn awaits(&self) -> Option<EffectId> {
        match self {
            Self::Locking(effect)
            | Self::LoadingRoom(effect)
            | Self::ApplyingTimers { effect, .. }
            | Self::Committing(effect)
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Finished(_) | Self::Reported => None,
        }
    }
}

impl RecordPass {
    /// The commit of `record`, which already carries its `blocked_by`.
    pub(super) fn new(mut record: RoomEvent) -> Self {
        record.status = EventStatus::Blocked;
        Self {
            record,
            state: State::Start,
            release_error: None,
        }
    }

    /// Takes the room's lock.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            let room_id = self.record.room_id.clone();
            self.state = State::Locking(cx.io.issue(Request::AcquireLock { room_id }));
        }
    }

    /// Takes the driver's answer to the pass's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<RecordReport, Abort>> {
        if self.state.awaits() != Some(effect) {
            debug_assert!(false, "record pass received {effect:?} in {:?}", self.state);
            return Progress::Waiting;
        }
        self.state = match mem::replace(&mut self.state, State::Reported) {
            // The context holds the recent events the hooks of a due timer
            // transition read (gap G-027).
            State::Locking(_) => match outcome.into_locked() {
                Ok(()) => State::LoadingRoom(cx.io.issue(Request::LoadContext {
                    room_id: self.record.room_id.clone(),
                    recent_events: cx.config.context_window,
                })),
                Err(error) => State::Finished(Err(Abort::Lock(error))),
            },
            State::LoadingRoom(_) => self.on_room(cx, outcome),
            State::ApplyingTimers { pending, .. } => match pending.stored(cx, outcome) {
                Ok(opened) => self.on_gate(cx, &opened.context),
                Err(error) => self.release(cx, Err(Abort::Store(error))),
            },
            State::Committing(_) => match outcome.into_committed() {
                Ok(()) => {
                    let committed = RecordResult::Committed(Box::new(self.record.clone()));
                    self.release(cx, Ok(committed))
                }
                Err(error) => self.release(cx, Err(Abort::Store(error))),
            },
            State::Releasing { result, .. } => {
                let (result, release_error) =
                    ops::after_release(&self.record.room_id, result, outcome.into_released());
                self.release_error = release_error;
                State::Finished(result)
            }
            state @ (State::Start | State::Finished(_) | State::Reported) => state,
        };
        if !matches!(self.state, State::Finished(_)) {
            return Progress::Waiting;
        }
        let State::Finished(result) = mem::replace(&mut self.state, State::Reported) else {
            return Progress::Waiting;
        };
        Progress::Done(result.map(|result| RecordReport {
            result,
            release_error: self.release_error.take(),
        }))
    }

    /// The room as the lock found it goes through the timer gate first.
    fn on_room(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return self.release(cx, Err(Abort::RoomNotFound)),
            Err(error) => return self.release(cx, Err(Abort::Store(error))),
        };
        match gate::open(cx, context) {
            Gate::Clear(context) => self.on_gate(cx, &context),
            Gate::Writing { effect, pending } => State::ApplyingTimers { effect, pending },
        }
    }

    /// The status gate, then the commit with the next index.
    fn on_gate(&mut self, cx: &mut Cx<'_>, context: &RoomContext) -> State {
        if !context.room.status.accepts_events() {
            return self.release(cx, Ok(RecordResult::Refused));
        }
        self.record.index = context.room.event_count;
        let counters = RoomCounters {
            event_count: context.room.event_count.saturating_add(1),
            latest_index: self.record.index,
            last_activity_at: context.room.timers.last_activity_at,
            room: RoomUpdates::default(),
        };
        State::Committing(cx.io.issue(Request::Commit {
            event: self.record.clone(),
            counters,
            target: None,
        }))
    }

    fn release(&mut self, cx: &mut Cx<'_>, result: Result<RecordResult, Abort>) -> State {
        let effect = cx.io.issue(Request::ReleaseLock {
            room_id: self.record.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}
