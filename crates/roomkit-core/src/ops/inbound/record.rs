//! The commit of a record the room refused before any hook (RFC §8.3).
//!
//! A channel not asked at the chain-depth limit, a response past its
//! cascade's reentry budget, a response a transport returned at the limit,
//! an entry adopted past it, an injected event past it: each is stored
//! `BLOCKED`, under the room's lock, with the next index, and runs no hook.
//! A channel's record keeps the visibility step 12 would have given it when
//! its source is bound (gap G-036), a hook's injected event the targets the
//! room binds (gap G-031). The
//! room's due timer transitions apply first (gap G-045), and a room that no
//! longer accepts events refuses the record, as it refuses any write, with
//! `room_refused_event`: `reentry` for the framework's records, `inbound`
//! for an adopted entry's (RFC §5.1, gap G-043). A direct injection adopted
//! past the budget is refused first, as its pass would have refused it, when
//! the room is another organization's or binds no channel it names: nothing
//! is stored (gap G-069).

use std::mem;

use super::direct::DirectCall;
use super::{commit, report};
use crate::model::{ChannelId, ErrorRecord, EventStatus, RoomContext, RoomEvent};
use crate::ops::lifecycle::gate::{self, Gate, Pending};
use crate::ops::protocol::{EffectId, HoldRequest, Outcome, RoomCounters, StoreRequest};
use crate::ops::{self, Abort, Cx, Progress, RefusedOperation};
use crate::rules::permissions;

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
    /// What a refusal of the record by the room's status reports it as.
    refused_as: RefusedOperation,
    /// What the record's visibility is narrowed to at its commit.
    narrowing: Narrowing,
    /// What a direct injection's record is checked for at the gate (gap
    /// G-069).
    direct: Option<DirectCall>,
    state: State,
    release_error: Option<ErrorRecord>,
}

/// What a record's visibility is narrowed to at its commit, on the room as
/// the lock finds it.
#[derive(Debug)]
enum Narrowing {
    /// A channel's record: what its source binding allows, as step 12 would
    /// have narrowed it (gap G-036).
    SourceBinding,
    /// A hook's injected event for these targets, `None` stored only: the
    /// targets the room binds (gap G-031).
    Targets(Option<Vec<ChannelId>>),
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
    /// The commit of `record`, a channel's, which already carries its
    /// `blocked_by`, in place of what the framework refused: a turn or a
    /// response (RFC §8.3).
    pub(super) fn new(record: RoomEvent) -> Self {
        Self::refusing(record, RefusedOperation::Reentry)
    }

    /// The commit of `record`, a channel's, which already carries its
    /// `blocked_by`, reported as `refused_as` when the room's status
    /// refuses it.
    pub(super) fn refusing(mut record: RoomEvent, refused_as: RefusedOperation) -> Self {
        record.status = EventStatus::Blocked;
        Self {
            record,
            refused_as,
            narrowing: Narrowing::SourceBinding,
            direct: None,
            state: State::Start,
            release_error: None,
        }
    }

    /// The record of a direct injection for `direct`, which the room refuses
    /// before anything is stored as its pass would have (gap G-069); `None`
    /// for any other record.
    pub(super) fn checking(self, direct: Option<DirectCall>) -> Self {
        Self { direct, ..self }
    }

    /// The commit of `record`, an event a hook injected for `targets`,
    /// visible to the targets the room binds (gap G-031).
    pub(super) fn injected(record: RoomEvent, targets: Option<Vec<ChannelId>>) -> Self {
        Self {
            narrowing: Narrowing::Targets(targets),
            ..Self::new(record)
        }
    }

    /// Whether the record's commit waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.state.awaits() == Some(effect)
    }

    /// Takes the room's lock.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            let room_id = self.record.room_id.clone();
            self.state = State::Locking(cx.io.issue(HoldRequest::AcquireLock { room_id }));
        }
    }

    /// Takes the driver's answer to the pass's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<RecordReport, Abort>> {
        if !self.awaits(effect) {
            debug_assert!(false, "record pass received {effect:?} in {:?}", self.state);
            return Progress::Waiting;
        }
        self.state = match mem::replace(&mut self.state, State::Reported) {
            // The context holds the recent events the hooks of a due timer
            // transition read (gap G-027).
            State::Locking(_) => match outcome.into_locked() {
                Ok(()) => State::LoadingRoom(cx.io.issue(StoreRequest::LoadContext {
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

    /// The status gate, then the commit with the next index and the
    /// visibility step 12 would have given the record (gap G-036); a direct
    /// injection the room refuses is refused first (gap G-069).
    fn on_gate(&mut self, cx: &mut Cx<'_>, context: &RoomContext) -> State {
        let refusal = self
            .direct
            .as_ref()
            .and_then(|direct| direct.refusal(&self.record, context));
        if let Some(refusal) = refusal {
            return self.release(cx, Err(Abort::Refused(refusal)));
        }
        let status = context.room.status;
        if !status.accepts_events() {
            report::room_refused_as(cx, &self.record, self.refused_as, status);
            return self.release(cx, Ok(RecordResult::Refused));
        }
        self.record.index = context.room.event_count;
        self.record.visibility = match &self.narrowing {
            Narrowing::SourceBinding => commit::stored_visibility(&self.record, context),
            Narrowing::Targets(targets) => {
                permissions::targets_visibility(targets.as_deref(), &context.bindings)
            }
        };
        let counters = RoomCounters::after_commit(&context.room, self.record.index);
        let request = StoreRequest::Commit {
            event: Box::new(self.record.clone()),
            counters,
            target: None,
        };
        State::Committing(cx.io.issue_locked(request, self.record.room_id.clone()))
    }

    fn release(&mut self, cx: &mut Cx<'_>, result: Result<RecordResult, Abort>) -> State {
        let effect = cx.io.issue(HoldRequest::ReleaseLock {
            room_id: self.record.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}
