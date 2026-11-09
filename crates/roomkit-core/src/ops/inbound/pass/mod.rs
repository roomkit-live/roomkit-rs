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
//! deadline ends it, the lock released, with nothing stored (RFC §13.6); so
//! does the pass of an entry the call's work adopted before its decision,
//! cut with the hook whose code made it (RFC §9.5.1 Reentrance). It also
//! carries the outcome of the trigger's off-lock check, which step 9
//! applies first, running only the hooks that need the lock (RFC §9.5.1).

mod decision;
mod gates;

use std::collections::VecDeque;
use std::mem;

use super::delivery::Placement;
use super::direct::DirectCall;
use super::edits::Accepted;
use super::identity::Participation;
use super::queues::Lineage;
use crate::model::{
    ChannelId, ErrorRecord, InjectedEvent, Observation, RoomContext, RoomEvent, RoomStatus, Task,
    Visibility,
};
use crate::ops::io::Bound;
use crate::ops::lifecycle::gate::{Applied, Pending, TimerHooks};
use crate::ops::protocol::{EffectId, EntryId, HoldRequest, Outcome, StoreRequest};
use crate::ops::rooting::Rooting;
use crate::ops::{self, Abort, Cx, Progress};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::edits::EditRefusal;
use crate::rules::hook_chain::HookChain;
use crate::rules::hook_registry::HookTrigger;
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
        /// The adopted entry whose answer waits for its commit: the entry
        /// whose own pass injected it, or whose answer the pass that did
        /// waits for, as Python's adopted call commits what its hooks
        /// injected before it returns (RFC §9.5.1 Reentrance).
        owed: Option<EntryId>,
    },
}

impl PassOrigin {
    /// The adopted entry whose answer waits for the pass's commit, an
    /// injected event's (`PassOrigin::Injected`).
    pub(super) fn owed(&self) -> Option<EntryId> {
        match self {
            Self::Injected { owed, .. } => *owed,
            Self::Trigger | Self::Adopted(_) | Self::Response { .. } => None,
        }
    }

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
        status: Option<RoomStatus>,
    },
    /// An instruction to a discussion that is over, which gives no turn
    /// (RFC §19.7.5 rule 15): nothing was stored.
    DiscussionOver,
    /// Already committed under its idempotency key (RFC §13.4).
    Duplicate {
        /// The event the first delivery committed; `None` when the store
        /// holds the key and cannot read the event back, a blocked
        /// duplicate (RFC §13.4).
        event: Option<Box<RoomEvent>>,
    },
    /// An edit or a delete refused at step 8a: nothing was stored (RFC
    /// §10.1 step 8a).
    EditRefused {
        /// Why.
        refusal: EditRefusal,
    },
    /// Committed with nothing to deliver from: the greeting of an agent the
    /// room does not bind (RFC §19.1).
    Stored {
        /// The committed event.
        event: Box<RoomEvent>,
        /// The room's context at the commit.
        context: Box<RoomContext>,
    },
}

impl PassResult {
    /// Takes `stored`, the event as the store committed it, in place of the
    /// planned one, in the result and in the context the commit left: the
    /// index the store gave it is the event's (RFC §8.1), as the Python
    /// implementation's pipeline goes on with the event `commit_event`
    /// returns. A delivered event joins its delivery set's context as its
    /// newest event, after the window read before its commit, as Python's
    /// broadcast context holds it (`committed_in`).
    pub(super) fn adopt(&mut self, stored: RoomEvent) {
        match self {
            Self::Delivered {
                event,
                changed,
                context,
                ..
            } => {
                if let Some(changed) = changed {
                    super::commit::adopt(&mut changed.context, &stored);
                }
                super::commit::join_newest(&mut context.recent_events, &stored);
                **event = stored;
            }
            Self::Blocked { event, .. } | Self::Stored { event, .. } => **event = stored,
            Self::Refused { .. }
            | Self::DiscussionOver
            | Self::Duplicate { .. }
            | Self::EditRefused { .. } => {}
        }
    }

    /// The room's context at the commit, for a pass that committed.
    pub(super) fn context(&self) -> Option<&RoomContext> {
        match self {
            Self::Delivered { context, .. }
            | Self::Blocked { context, .. }
            | Self::Stored { context, .. } => Some(context),
            Self::Refused { .. }
            | Self::DiscussionOver
            | Self::Duplicate { .. }
            | Self::EditRefused { .. } => None,
        }
    }
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
    /// What became of the delivered event's place in the room's lane.
    pub(super) placement: Placement,
    /// Whose cascade the pass belongs to.
    pub(super) lineage: Lineage,
    /// Whether the pass was an agent's greeting (RFC §19.1).
    pub(super) greeting: bool,
}

/// The ASYNC hooks of `BEFORE_BROADCAST` observing the event its chain
/// allowed, as the SYNC hooks left it (RFC §9.2, §9.3), fired once the pass
/// is over, its event's place in the room's lane taken: running when the
/// lane was idle, queued behind the sets ahead otherwise. An observer that
/// closes the kit finds the set delivering, given the close's grace, or
/// queued, dropped (RFC §12.10.4), whatever the runtime's threads.
#[derive(Debug)]
pub(super) struct Observed {
    payload: RoomEvent,
    context: RoomContext,
    /// The cascade's budget the pass worked within, which the bodies draw
    /// on (RFC §9.5.1); none, they are work of their own.
    budget: Option<ReentryBudget>,
}

impl Observed {
    /// The hooks observing `payload`, the event its chain allowed, in
    /// `context`, as the work of the cascade the pass works for while they
    /// run (RFC §9.2, §9.5.1; decided in step 3 milestone 2): a call a body
    /// makes into the room draws one of the cascade's hook calls, refused
    /// past them, as an `AFTER_BROADCAST` body's does, so an observer that
    /// posts on every event it observes stops at the cap. A pass always
    /// works for a cascade; none lent, the bodies are work of their own.
    pub(super) fn new(cx: &Cx<'_>, payload: &RoomEvent, context: &RoomContext) -> Self {
        Self {
            payload: payload.clone(),
            context: context.clone(),
            budget: cx.io.work().cloned(),
        }
    }

    /// Fires the hooks.
    pub(super) fn fire(self, cx: &mut Cx<'_>) {
        let trigger = HookTrigger::BeforeBroadcast;
        match &self.budget {
            Some(budget) => {
                cx.fire_as_cascade_work(trigger, &self.payload, &self.context, budget, 0);
            }
            None => cx.fire_observers(trigger, &self.payload, &self.context),
        }
    }
}

/// One event's pass through the locked section.
#[derive(Debug)]
pub(super) struct CommitPass {
    origin: PassOrigin,
    event: RoomEvent,
    /// Whose cascade the pass belongs to: the call's, or an entry's.
    lineage: Lineage,
    state: State,
    /// The edit or the delete step 8a accepted, with its target.
    accepted: Option<Accepted>,
    /// The call's deadline, which the trigger's pass carries up to its
    /// commit, and an entry's adopted before the call's decision (RFC
    /// §13.6).
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
    /// What became of the event's place in the room's delivery lane.
    placement: Placement,
    /// The hooks of the timer transitions its gate applied, fired once the
    /// lock is released (RFC §5.1 When).
    timer_hooks: TimerHooks,
    /// The ASYNC hooks of `BEFORE_BROADCAST` observing the event the chain
    /// allowed, until the pass is over.
    observed: Option<Box<Observed>>,
    /// What the trigger's pass writes of its sender once the status gate
    /// let the event through, as identity resolution left them (RFC §10.1
    /// step 5).
    participation: Option<Box<Participation>>,
    /// Whether the event is an agent's greeting, committed by a path of its
    /// own: no hook, no check of its source's right to write, and a refusal
    /// of its room's status only logged (RFC §7.5 rule 2, §19.1).
    greeting: bool,
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
    /// The sender's record is written, as identity resolution left them,
    /// each write in turn (RFC §10.1 step 5).
    Participating {
        effect: EffectId,
        context: Box<RoomContext>,
        writes: VecDeque<StoreRequest>,
    },
    /// Step 8 reads the event the thread parent names, then the root a
    /// reply names, to root it.
    RootingParent {
        effect: EffectId,
        context: Box<RoomContext>,
        rooting: Box<Rooting>,
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
    /// The event a thread parent a hook set names is read, then the root a
    /// reply names, to root it (RFC §10.1 step 9).
    RootingHookParent {
        effect: EffectId,
        verdict: Box<Verdict>,
        context: Box<RoomContext>,
        reread: bool,
        rooting: Box<Rooting>,
    },
    /// An accepted edit's or delete's target is read again, as the hooks
    /// left it.
    RereadingTarget {
        effect: EffectId,
        payload: Box<RoomEvent>,
        context: Box<RoomContext>,
    },
    /// The room's timeline is read, to rebuild the register of authors of a
    /// room that holds none before the event is committed (RFC §10.1 step
    /// 12, gap G-089).
    ReadingTimeline {
        effect: EffectId,
        pending: Box<decision::Unrecorded>,
    },
    Committing {
        effect: EffectId,
        result: PassResult,
    },
    /// What an event the kit's persistence policy keeps out of the timeline
    /// changes is written (RFC §14.3).
    WritingUnstored {
        effect: EffectId,
        result: PassResult,
    },
    /// The event delivered takes its place in the room's lane, under the
    /// lock.
    Placing {
        effect: EffectId,
        result: PassResult,
    },
    /// The discussion holding the room is told, under the lock (RFC §19.7.5).
    TellingDiscussion {
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
            | Self::Participating { effect, .. }
            | Self::RootingParent { effect, .. }
            | Self::ReadingTarget { effect, .. }
            | Self::RunningHook { effect, .. }
            | Self::Rereading { effect, .. }
            | Self::RootingHookParent { effect, .. }
            | Self::RereadingTarget { effect, .. }
            | Self::ReadingTimeline { effect, .. }
            | Self::Committing { effect, .. }
            | Self::WritingUnstored { effect, .. }
            | Self::Placing { effect, .. }
            | Self::TellingDiscussion { effect, .. }
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Finished(_) | Self::Reported => None,
        }
    }
}

/// `event` without the routing stamps only a hook writes, each dropped
/// with a warning (RFC §19.4).
pub(in crate::ops::inbound) fn without_routing_stamps(mut event: RoomEvent) -> RoomEvent {
    let dropped = crate::rules::permissions::strip_routing_stamps(&mut event);
    if !dropped.is_empty() {
        tracing::warn!(
            target: "roomkit::routing",
            room_id = %event.room_id,
            event_id = %event.id,
            ?dropped,
            "an event arrived with routing stamps only a hook writes: dropped"
        );
    }
    event
}

impl CommitPass {
    /// A pass of `event`, stamped already, from `origin`, without the
    /// routing stamps it arrived with: a check's outcome keeps those its
    /// off-lock hooks wrote (RFC §19.4).
    pub(super) fn new(origin: PassOrigin, event: RoomEvent) -> Self {
        let event = without_routing_stamps(event);
        let lineage = match origin {
            PassOrigin::Adopted(entry) => Lineage::Entry(entry),
            PassOrigin::Trigger | PassOrigin::Response { .. } | PassOrigin::Injected { .. } => {
                Lineage::Call
            }
        };
        Self {
            origin,
            event,
            lineage,
            state: State::Start,
            accepted: None,
            bound: Bound::Unbounded,
            checked: None,
            direct: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            release_error: None,
            placement: Placement::Missing,
            timer_hooks: TimerHooks::default(),
            observed: None,
            participation: None,
            greeting: false,
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

    /// The same pass, bounded by `bound` up to its commit (RFC §13.6): an
    /// entry adopted from the call's work before its decision carries the
    /// call's deadline, as the hook whose code made it does.
    pub(super) fn bounded(self, bound: Bound) -> Self {
        Self { bound, ..self }
    }

    /// The pass of a direct injection for `direct`, whose room the gate
    /// checks before the hooks (gap G-069); `None` for any other event.
    pub(super) fn checking(self, direct: Option<DirectCall>) -> Self {
        Self { direct, ..self }
    }

    /// The trigger's pass, writing its sender's record as `participation`
    /// says once the status gate let the event through (RFC §10.1 step 5).
    pub(super) fn participating(self, participation: Option<Participation>) -> Self {
        Self {
            participation: participation.map(Box::new),
            ..self
        }
    }

    /// The pass of `self`'s event as an agent's greeting (RFC §19.1).
    pub(super) fn greeting(self) -> Self {
        Self {
            greeting: true,
            ..self
        }
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
                Ok(Applied::Opened(opened)) => {
                    self.timer_hooks = opened.hooks;
                    self.on_gate(cx, opened.context)
                }
                Ok(Applied::Writing { effect, pending }) => {
                    State::ApplyingTimers { effect, pending }
                }
                Err(error) => self.release(cx, Err(Abort::Store(error))),
            },
            State::CheckingKey { context, .. } => self.on_key(cx, context, outcome),
            State::Participating {
                context, writes, ..
            } => self.on_participant_written(cx, context, writes, outcome),
            State::RootingParent {
                context, rooting, ..
            } => self.on_parent(cx, context, rooting, outcome),
            State::ReadingTarget { context, .. } => self.on_target(cx, context, outcome),
            State::RunningHook { run, context, .. } => self.on_hook(cx, run, context, outcome),
            State::Rereading { verdict, .. } => self.on_reread(cx, *verdict, outcome),
            State::RootingHookParent {
                verdict,
                context,
                reread,
                rooting,
                ..
            } => self.on_hook_parent(cx, *verdict, context, reread, rooting, outcome),
            State::RereadingTarget {
                payload, context, ..
            } => self.on_target_reread(cx, *payload, context, outcome),
            State::ReadingTimeline { pending, .. } => self.on_timeline(cx, *pending, outcome),
            State::Committing { result, .. } => self.on_committed(cx, result, outcome),
            State::WritingUnstored { result, .. } => self.on_unstored_written(cx, result, outcome),
            State::Placing { result, .. } => self.on_placed(cx, result, outcome),
            State::TellingDiscussion { result, .. } => self.on_told(cx, result, outcome),
            State::Releasing { result, .. } => {
                let released = outcome.into_released();
                let failure = released.as_ref().err().cloned();
                let (result, release_error) =
                    ops::after_release(&self.event.room_id, result, released);
                self.release_error = release_error;
                mem::take(&mut self.timer_hooks).fire(cx);
                State::Finished(result.map_err(|abort| abort.released_with(failure)))
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
            placement: self.placement,
            lineage: self.lineage,
            greeting: self.greeting,
        }))
    }

    /// The same pass, of the cascade `lineage`: a response or an injected
    /// event belongs to the cascade of the event that made it.
    #[must_use]
    pub(super) fn within(mut self, lineage: Lineage) -> Self {
        self.lineage = lineage;
        self
    }

    /// Whose cascade the pass belongs to.
    pub(super) fn lineage(&self) -> Lineage {
        self.lineage
    }

    /// Whether the pass is a response's, which re-enters (RFC §10.1 step
    /// 14).
    pub(super) fn is_response(&self) -> bool {
        matches!(self.origin, PassOrigin::Response { .. })
    }

    /// The call's deadline passed (RFC §13.6): nothing is stored, and the
    /// lock is released when the pass holds it. What the hooks produced is
    /// dropped with the pass.
    fn expire(&mut self, cx: &mut Cx<'_>) -> State {
        match mem::replace(&mut self.state, State::Reported) {
            State::Locking(_) => State::Finished(Err(Abort::expired())),
            State::LoadingContext(_)
            | State::ApplyingTimers { .. }
            | State::CheckingKey { .. }
            | State::RootingParent { .. }
            | State::ReadingTarget { .. }
            | State::RunningHook { .. }
            | State::Rereading { .. }
            | State::RootingHookParent { .. }
            | State::RereadingTarget { .. } => {
                let expired = Abort::DeadlinePassed {
                    locked_event: Some(self.event.id.clone()),
                    release_failure: None,
                };
                self.release(cx, Err(expired))
            }
            // The commit, the place and the release carry no deadline (RFC
            // §13.6): `Io` answers them with no `DeadlinePassed`, nor the
            // writes of the sender's record.
            state @ (State::Start
            | State::Participating { .. }
            | State::ReadingTimeline { .. }
            | State::Committing { .. }
            | State::WritingUnstored { .. }
            | State::Placing { .. }
            | State::TellingDiscussion { .. }
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
    /// The ASYNC hooks of `BEFORE_BROADCAST` observe the event its chain
    /// allowed then, once its place in the room's lane is taken
    /// ([`Observed`]).
    fn release(&mut self, cx: &mut Cx<'_>, result: Result<PassResult, Abort>) -> State {
        if let Some(observed) = self.observed.take() {
            observed.fire(cx);
        }
        let effect = cx.io.issue(HoldRequest::ReleaseLock {
            room_id: self.event.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}
