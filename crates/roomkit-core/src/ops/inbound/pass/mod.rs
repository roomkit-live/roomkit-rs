//! One event's way through the locked section of the pipeline (RFC §10.1
//! steps 6 to 13).
//!
//! Under the room's lock, a pass applies the room's due timer transitions
//! (gap G-045), checks the room still accepts events, answers a duplicate,
//! assigns the index, checks an edit or a delete against its target (step
//! 8a), runs the `BEFORE_BROADCAST` chain (`gates`), then reads the room
//! again as the hooks left it: the code a hook ran under the lock may have
//! committed an event ahead of this one, closed the room or muted the
//! source (RFC §13.5, gap G-070). A room that no longer accepts events
//! refuses the event; otherwise it takes the room's next index, its source
//! is checked, and it is committed with the room's counters, resuming a
//! paused room and updating an edit's or a delete's target, and its
//! broadcast planned (`decision`). An instruction takes every step but the
//! write: it is decided, never committed (RFC §10.1.1). An event delivered
//! takes its place in the room's delivery lane before the lock is released
//! (RFC §10.1 step 12, "appended to the room's delivery lane"), so the
//! places follow the commits and the sets run in index order (RFC §10.2).
//! Every path that took the lock releases it.
//!
//! Every request a pass makes while it holds the lock says so
//! (`Effect::locked`), so that the code its hooks run may take the lock
//! again (RFC §13.5).
//!
//! The trigger's pass is the end of the call's pre-commit phase: it carries
//! the call's deadline on every request up to the commit, and a passed
//! deadline ends it, the lock released, with nothing stored (RFC §13.6). It
//! also carries the outcome of the trigger's off-lock check, which step 9
//! applies first, running only the hooks that need the lock (RFC §9.5.1).

mod decision;
mod gates;

use std::mem;

use super::direct::DirectCall;
use super::edits::Accepted;
use crate::model::{
    ChannelId, ErrorRecord, InjectedEvent, Observation, RoomContext, RoomEvent, RoomStatus, Task,
    Visibility,
};
use crate::ops::io::Bound;
use crate::ops::lifecycle::gate::{Pending, TimerHooks};
use crate::ops::protocol::{EffectId, EntryId, HoldRequest, Outcome, StoreRequest};
use crate::ops::{self, Abort, Cx, Progress};
use crate::rules::edits::EditRefusal;
use crate::rules::hook_chain::HookChain;
use crate::rules::permissions::DeliveryPlan;

use super::chain::HookRun;

/// Where the event of a pass comes from.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum PassOrigin {
    /// The call's own entry: the inbound message, or the event the
    /// integrator injected directly (RFC §10.5).
    Trigger,
    /// An entry the operation adopted from its own work: a message or a
    /// direct injection into the room, made by a hook, a turn or a delivery
    /// of the operation (RFC §13.5, gap G-070).
    Adopted(EntryId),
    /// A channel's response, re-entering (RFC §10.1 step 14).
    Response {
        /// Where the answers to the event it answers may go, the scope it
        /// was stamped within, which its visibility is narrowed back to
        /// once its hooks are over (RFC §8.5, gap G-073); `None` for
        /// anywhere. What answers the response goes where its own
        /// `response_visibility` says.
        answers_within: Option<Visibility>,
    },
    /// An event a hook injected, delivered to its targets only (gap G-021).
    Injected {
        /// The channels it is delivered to; `None` stores it only.
        targets: Option<Vec<ChannelId>>,
    },
}

impl PassOrigin {
    /// Whether the pass is an entry's, a caller's, rather than the
    /// framework's re-injection: its key is checked (RFC §13.4), a refusal
    /// is reported to its caller, and the room's refusal is `inbound`.
    pub(super) fn is_entry(&self) -> bool {
        matches!(self, Self::Trigger | Self::Adopted(_))
    }
}

/// What a pass did with its event.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum PassResult {
    /// Committed delivered, with its broadcast planned; an instruction is
    /// delivered without being committed (RFC §10.1.1 rule 5).
    Delivered {
        /// The committed event, or the uncommitted instruction.
        event: Box<RoomEvent>,
        /// Whom its broadcast reaches.
        plan: DeliveryPlan,
        /// The room's context at the commit, as the hooks left it, an edit's
        /// or a delete's target as the commit left it.
        context: Box<RoomContext>,
        /// What an accepted edit or delete changed, for its mutation
        /// trigger (RFC §10.3).
        changed: Option<Box<Changed>>,
    },
    /// Committed blocked, by a hook or by a source that cannot write; an
    /// instruction is blocked without being stored (RFC §10.1.1 rule 4).
    Blocked {
        /// The committed record, or the blocked instruction, with its
        /// `blocked_by`.
        event: Box<RoomEvent>,
        /// Why it was blocked.
        reason: Option<String>,
        /// The room's context at the commit.
        context: Box<RoomContext>,
    },
    /// Refused by the room's status, at step 6 or once the hooks closed the
    /// room: nothing was stored (RFC §5.1, gap G-070).
    Refused {
        /// The status that refused it.
        status: RoomStatus,
    },
    /// Already committed under its idempotency key (RFC §13.4).
    Duplicate {
        /// The event the first delivery committed.
        event: Box<RoomEvent>,
    },
    /// An edit or a delete refused at step 8a: nothing was stored (RFC
    /// §10.1 step 8a).
    EditRefused {
        /// Why.
        refusal: EditRefusal,
    },
}

/// What an accepted edit or delete changed, as its mutation trigger fires
/// with it (RFC §10.3 "Mutation triggers", §9.2, gap G-062).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Changed {
    /// The target, as the commit left it: the trigger's payload.
    pub(super) target: RoomEvent,
    /// The room as the commit left it: its counters past the change, the
    /// target rewritten, the events committed ahead of the change during
    /// its hooks, and the change itself, the newest event of its history.
    pub(super) context: RoomContext,
}

/// Everything a pass produced.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PassReport {
    pub(super) result: PassResult,
    pub(super) injected_events: Vec<InjectedEvent>,
    pub(super) tasks: Vec<Task>,
    pub(super) observations: Vec<Observation>,
    /// The failure of the lock's release, which changes nothing the pass
    /// decided (gap G-025).
    pub(super) release_error: Option<ErrorRecord>,
    /// Whether the event delivered took its place in the room's lane.
    pub(super) placed: bool,
}

/// One event's pass through the locked section.
#[derive(Debug)]
pub(super) struct CommitPass {
    origin: PassOrigin,
    event: RoomEvent,
    state: State,
    /// The edit or the delete step 8a accepted, with its target.
    accepted: Option<Accepted>,
    /// The call's deadline, which the trigger's pass carries up to its
    /// commit (RFC §13.6).
    bound: Bound,
    /// The chain the trigger's off-lock check left, which step 9 goes on
    /// with (RFC §9.5.1).
    checked: Option<Box<HookChain<RoomEvent>>>,
    /// What a direct injection is checked for at the gate (gap G-069).
    direct: Option<DirectCall>,
    injected_events: Vec<InjectedEvent>,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
    release_error: Option<ErrorRecord>,
    /// Whether the event took its place in the room's delivery lane.
    placed: bool,
    /// The hooks of the timer transitions its gate applied, fired once the
    /// lock is released (RFC §5.1 When).
    timer_hooks: TimerHooks,
}

/// What the hooks decided, which the pass applies once it has read the room
/// again.
#[derive(Debug)]
struct Verdict {
    /// The event as the hooks left it, its identity kept (RFC §9.4).
    payload: RoomEvent,
    /// The hook that blocked it, and its reason; `None` when allowed.
    blocked: Option<(String, Option<String>)>,
}

#[derive(Debug)]
enum State {
    Start,
    Locking(EffectId),
    LoadingContext(EffectId),
    ApplyingTimers {
        effect: EffectId,
        pending: Box<Pending>,
    },
    CheckingKey {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    /// Step 8a reads the target of an edit or a delete.
    ReadingTarget {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    RunningHook {
        effect: EffectId,
        run: Box<HookRun>,
        context: Box<RoomContext>,
    },
    /// The hooks are over: the room is read again, as they left it.
    Rereading {
        effect: EffectId,
        verdict: Box<Verdict>,
    },
    /// An accepted edit's or delete's target is read again, as the hooks
    /// left it.
    RereadingTarget {
        effect: EffectId,
        payload: Box<RoomEvent>,
        context: Box<RoomContext>,
    },
    Committing {
        effect: EffectId,
        result: PassResult,
    },
    /// The event delivered takes its place in the room's lane, under the
    /// lock.
    Placing {
        effect: EffectId,
        result: PassResult,
    },
    Releasing {
        effect: EffectId,
        result: Result<PassResult, Abort>,
    },
    Finished(Result<PassResult, Abort>),
    Reported,
}

impl State {
    /// The effect the state waits for.
    fn awaits(&self) -> Option<EffectId> {
        match self {
            Self::Locking(effect)
            | Self::LoadingContext(effect)
            | Self::ApplyingTimers { effect, .. }
            | Self::CheckingKey { effect, .. }
            | Self::ReadingTarget { effect, .. }
            | Self::RunningHook { effect, .. }
            | Self::Rereading { effect, .. }
            | Self::RereadingTarget { effect, .. }
            | Self::Committing { effect, .. }
            | Self::Placing { effect, .. }
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Finished(_) | Self::Reported => None,
        }
    }
}

impl CommitPass {
    /// A pass of `event`, stamped already, from `origin`.
    pub(super) fn new(origin: PassOrigin, event: RoomEvent) -> Self {
        Self {
            origin,
            event,
            state: State::Start,
            accepted: None,
            bound: Bound::Unbounded,
            checked: None,
            direct: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            release_error: None,
            placed: false,
            timer_hooks: TimerHooks::default(),
        }
    }

    /// The trigger's pass of `event`, bounded by the call's `bound` up
    /// to its commit (RFC §13.6), going on with the chain its off-lock check
    /// left, if it ran one (RFC §9.5.1).
    pub(super) fn trigger(
        event: RoomEvent,
        bound: Bound,
        checked: Option<HookChain<RoomEvent>>,
    ) -> Self {
        Self {
            bound,
            checked: checked.map(Box::new),
            ..Self::new(PassOrigin::Trigger, event)
        }
    }

    /// The pass of a direct injection for `direct`, whose room the gate
    /// checks before the hooks (gap G-069); `None` for any other event.
    pub(super) fn checking(self, direct: Option<DirectCall>) -> Self {
        Self { direct, ..self }
    }

    /// Whether the pass waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.state.awaits() == Some(effect)
    }

    /// Takes the room's lock (RFC §10.1 step 6).
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            let room_id = self.event.room_id.clone();
            let request = HoldRequest::AcquireLock { room_id };
            self.state = State::Locking(cx.io.issue_within(request, self.bound));
        }
    }

    /// Asks for `request` under the room's lock, within the call's
    /// deadline: what precedes the commit is bounded by it (RFC §13.6).
    fn issue(&self, cx: &mut Cx<'_>, request: StoreRequest) -> EffectId {
        let locked = Some(self.event.room_id.clone());
        cx.io.issue_under(request, self.bound, locked)
    }

    /// Takes the driver's answer to the pass's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<PassReport, Abort>> {
        if !self.awaits(effect) {
            debug_assert!(false, "pass received {effect:?} in {:?}", self.state);
            return Progress::Waiting;
        }
        if outcome == Outcome::DeadlinePassed {
            self.state = self.expire(cx);
            return self.report();
        }
        self.state = match mem::replace(&mut self.state, State::Reported) {
            State::Locking(_) => self.on_locked(cx, outcome),
            State::LoadingContext(_) => self.on_context(cx, outcome),
            State::ApplyingTimers { pending, .. } => match pending.stored(cx, outcome) {
                Ok(opened) => {
                    self.timer_hooks = opened.hooks;
                    self.on_gate(cx, opened.context)
                }
                Err(error) => self.release(cx, Err(Abort::Store(error))),
            },
            State::CheckingKey { context, .. } => self.on_key(cx, context, outcome),
            State::ReadingTarget { context, .. } => self.on_target(cx, context, outcome),
            State::RunningHook { run, context, .. } => self.on_hook(cx, run, context, outcome),
            State::Rereading { verdict, .. } => self.on_reread(cx, *verdict, outcome),
            State::RereadingTarget {
                payload, context, ..
            } => self.on_target_reread(cx, *payload, context, outcome),
            State::Committing { result, .. } => self.on_committed(cx, result, outcome),
            State::Placing { result, .. } => self.on_placed(cx, result, outcome),
            State::Releasing { result, .. } => {
                let (result, release_error) =
                    ops::after_release(&self.event.room_id, result, outcome.into_released());
                self.release_error = release_error;
                mem::take(&mut self.timer_hooks).fire(cx);
                State::Finished(result)
            }
            state @ (State::Start | State::Finished(_) | State::Reported) => state,
        };
        self.report()
    }

    fn report(&mut self) -> Progress<Result<PassReport, Abort>> {
        if !matches!(self.state, State::Finished(_)) {
            return Progress::Waiting;
        }
        let State::Finished(result) = mem::replace(&mut self.state, State::Reported) else {
            return Progress::Waiting;
        };
        Progress::Done(result.map(|result| PassReport {
            result,
            injected_events: mem::take(&mut self.injected_events),
            tasks: mem::take(&mut self.tasks),
            observations: mem::take(&mut self.observations),
            release_error: self.release_error.take(),
            placed: self.placed,
        }))
    }

    /// The call's deadline passed (RFC §13.6): nothing is stored, and the
    /// lock is released when the pass holds it. What the hooks produced is
    /// dropped with the pass.
    fn expire(&mut self, cx: &mut Cx<'_>) -> State {
        match mem::replace(&mut self.state, State::Reported) {
            State::Locking(_) => State::Finished(Err(Abort::DeadlinePassed(None))),
            State::LoadingContext(_)
            | State::ApplyingTimers { .. }
            | State::CheckingKey { .. }
            | State::ReadingTarget { .. }
            | State::RunningHook { .. }
            | State::Rereading { .. }
            | State::RereadingTarget { .. } => {
                let expired = Abort::DeadlinePassed(Some(self.event.id.clone()));
                self.release(cx, Err(expired))
            }
            // The commit, the place and the release carry no deadline (RFC
            // §13.6): `Io` answers them with no `DeadlinePassed`.
            state @ (State::Start
            | State::Committing { .. }
            | State::Placing { .. }
            | State::Releasing { .. }
            | State::Finished(_)
            | State::Reported) => {
                debug_assert!(false, "a deadline passed for an unbounded request");
                state
            }
        }
    }

    /// Ends the pass with an edit or a delete refused, nothing stored (RFC
    /// §10.1 step 8a).
    fn refuse_edit(&mut self, cx: &mut Cx<'_>, refusal: EditRefusal) -> State {
        super::edits::log_refusal(&self.event, &self.origin, refusal);
        self.release(cx, Ok(PassResult::EditRefused { refusal }))
    }

    /// Releases the room's lock before reporting `result` (RFC §10.1 step 13).
    fn release(&mut self, cx: &mut Cx<'_>, result: Result<PassResult, Abort>) -> State {
        let effect = cx.io.issue(HoldRequest::ReleaseLock {
            room_id: self.event.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}
