//! The shape every change of a room shares: take the room's lock, read the
//! room, apply its due timers, write the change, release the lock, then
//! report it.
//!
//! [`LockedSection`] is that shape as a sub-machine: a lifecycle operation
//! runs one as its whole work ([`LockedChange`]), and the inbound pipeline
//! runs one for the attach of its step 2 (RFC §10.1 step 2).

use std::mem;
use std::sync::Arc;

use super::LifecycleError;
use super::change::{Decision, SectionChange};
use super::gate::{self, Gate, Opened, Pending};
use super::report;
use crate::model::{ErrorRecord, RoomContext, RoomId};
use crate::ops::io::{Bound, Io};
use crate::ops::protocol::{EffectId, HoldRequest, Outcome, ProtocolError, Step, StoreRequest};
use crate::ops::{self, Cx, Environment, KitConfig, Progress};
use crate::rules::organizations;
use crate::rules::timers::DueTransition;

/// Why a locked section failed: the room, the store or the lock failed it,
/// or its change refused, with nothing written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SectionError<R> {
    RoomNotFound(RoomId),
    Store(ErrorRecord),
    Lock(ErrorRecord),
    Refused(R),
}

impl SectionError<LifecycleError> {
    /// The failure as a lifecycle operation reports it.
    fn into_lifecycle(self) -> LifecycleError {
        match self {
            Self::RoomNotFound(room_id) => LifecycleError::RoomNotFound(room_id),
            Self::Store(error) => LifecycleError::Store(error),
            Self::Lock(error) => LifecycleError::Lock(error),
            Self::Refused(error) => error,
        }
    }
}

/// What a locked section of the change `C` ends with.
pub(crate) type SectionResult<C> =
    Result<<C as SectionChange>::Done, SectionError<<C as SectionChange>::Refusal>>;

/// One change of a room, made under its lock, as an operation's whole work.
#[derive(Debug)]
pub(super) struct LockedChange<C: SectionChange<Refusal = LifecycleError>> {
    config: Arc<KitConfig>,
    io: Io,
    section: LockedSection<C>,
    result: Option<Result<C::Done, LifecycleError>>,
}

impl<C: SectionChange<Refusal = LifecycleError>> LockedChange<C> {
    /// The change of `room_id`, for a caller acting for `organization_id`:
    /// a room of another organization, or of none, is not found (RFC
    /// §17.2); `None` reads the room unscoped.
    pub(super) fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        change: C,
    ) -> Self {
        let mut section = LockedSection::new(room_id, change);
        section.organization_id = organization_id;
        Self {
            config,
            io: Io::new(),
            section,
            result: None,
        }
    }

    /// A change refused before anything is read: it ends at once with
    /// `error`, taking no lock.
    pub(super) fn refused(
        config: Arc<KitConfig>,
        room_id: RoomId,
        change: C,
        error: LifecycleError,
    ) -> Self {
        let mut section = LockedSection::new(room_id, change);
        section.state = State::Reported;
        Self {
            config,
            io: Io::new(),
            section,
            result: Some(Err(error)),
        }
    }

    /// Advances the change; `output` reads the operation's result from what
    /// the change did and the timer transitions the gate applied.
    pub(super) fn poll<T>(
        &mut self,
        environment: &dyn Environment,
        output: impl Fn(C::Done, &[DueTransition]) -> T,
    ) -> Step<Result<T, LifecycleError>> {
        let Self {
            config,
            io,
            section,
            result,
        } = self;
        let mut cx = Cx {
            config,
            env: environment,
            io,
        };
        section.start(&mut cx);
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            if let Progress::Done(done) = section.accept(&mut cx, effect, outcome) {
                *result = Some(done.map_err(SectionError::into_lifecycle));
            }
        }
        let ready = result
            .take()
            .map(|result| result.map(|done| output(done, section.transitions())));
        self.io.step(ready)
    }

    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}

/// One change of a room under its lock, as a sub-machine: started once, then
/// handed the outcome of each of its effects until it is done.
///
/// Every path that took the lock releases it before the section is done,
/// and a change made is reported after the release (gap G-027).
#[derive(Debug)]
pub(crate) struct LockedSection<C: SectionChange> {
    room_id: RoomId,
    /// The organization the caller acts for, whose rooms only it reaches
    /// (RFC §17.2); `None` reads the room unscoped. Step 2 checked the
    /// room's organization on its own read already.
    organization_id: Option<String>,
    change: C,
    /// What bounds the wait for the lock: the inbound call's bound (RFC
    /// §10.4, §13.6), `Bound::Unbounded` for a lifecycle change.
    bound: Bound,
    state: State<C>,
    /// The timer transitions the gate applied.
    timers: Vec<DueTransition>,
}

#[derive(Debug)]
enum State<C: SectionChange> {
    Start,
    Locking(EffectId),
    Loading(EffectId),
    ApplyingTimers {
        effect: EffectId,
        pending: Box<Pending>,
    },
    Writing {
        effect: EffectId,
        decision: Box<Decision<C::Done>>,
    },
    Releasing {
        effect: EffectId,
        result: SectionResult<C>,
    },
    Finished(SectionResult<C>),
    Reported,
}

impl<C: SectionChange> State<C> {
    fn awaits(&self) -> Option<EffectId> {
        match self {
            Self::Locking(effect)
            | Self::Loading(effect)
            | Self::ApplyingTimers { effect, .. }
            | Self::Writing { effect, .. }
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Finished(_) | Self::Reported => None,
        }
    }
}

impl<C: SectionChange> LockedSection<C> {
    /// The section making `change` in `room_id`.
    pub(crate) fn new(room_id: RoomId, change: C) -> Self {
        Self {
            room_id,
            organization_id: None,
            change,
            bound: Bound::Unbounded,
            state: State::Start,
            timers: Vec::new(),
        }
    }

    /// The section waiting for the lock within `bound` at most: past the
    /// call's deadline, or once its caller is gone, the wait is answered
    /// `DeadlinePassed`, which the section's caller takes before handing it
    /// the outcome, since no lock is held then.
    pub(crate) fn waiting_within(mut self, bound: Bound) -> Self {
        self.bound = bound;
        self
    }

    /// Takes the room's lock, once.
    pub(crate) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            let room_id = self.room_id.clone();
            let request = HoldRequest::AcquireLock { room_id };
            self.state = State::Locking(cx.io.issue_within(request, self.bound));
        }
    }

    /// Whether the section waits for `effect`.
    pub(crate) fn awaits(&self, effect: EffectId) -> bool {
        self.state.awaits() == Some(effect)
    }

    /// The timer transitions the gate applied.
    pub(crate) fn transitions(&self) -> &[DueTransition] {
        &self.timers
    }

    /// Takes the driver's answer to the section's outstanding effect. The
    /// section is done once the lock is released, or could not be taken,
    /// with what the change did.
    pub(crate) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<SectionResult<C>> {
        if !self.awaits(effect) {
            debug_assert!(
                false,
                "lifecycle change received {effect:?} in {:?}",
                self.state
            );
            return Progress::Waiting;
        }
        self.state = match mem::replace(&mut self.state, State::Reported) {
            State::Locking(_) => self.on_locked(cx, outcome),
            State::Loading(_) => self.on_loaded(cx, outcome),
            State::ApplyingTimers { pending, .. } => self.on_timers(cx, *pending, outcome),
            State::Writing { decision, .. } => self.on_written(cx, *decision, outcome),
            State::Releasing { result, .. } => self.on_released(cx, result, outcome),
            state @ (State::Start | State::Finished(_) | State::Reported) => state,
        };
        match mem::replace(&mut self.state, State::Reported) {
            State::Finished(result) => Progress::Done(result),
            state @ (State::Start
            | State::Locking(_)
            | State::Loading(_)
            | State::ApplyingTimers { .. }
            | State::Writing { .. }
            | State::Releasing { .. }
            | State::Reported) => {
                self.state = state;
                Progress::Waiting
            }
        }
    }

    fn on_locked(&self, cx: &mut Cx<'_>, outcome: Outcome) -> State<C> {
        match outcome.into_locked() {
            // The change's hooks receive the room's recent events, as every
            // hook does (gap G-027).
            Ok(()) => State::Loading(cx.io.issue(StoreRequest::LoadContext {
                room_id: self.room_id.clone(),
                recent_events: cx.config.context_window,
            })),
            Err(error) => State::Finished(Err(SectionError::Lock(error))),
        }
    }

    /// The room as the lock found it goes through the timer gate first. A
    /// room the caller's organization does not reach is not found, nothing
    /// written in it, its due timers included (RFC §17.2).
    fn on_loaded(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State<C> {
        let organization_id = self.organization_id.as_deref();
        let context = match outcome.into_context() {
            Ok(Some(context)) if organizations::admits(organization_id, &context.room) => context,
            Ok(_) => {
                let error = SectionError::RoomNotFound(self.room_id.clone());
                return self.release(cx, Err(error));
            }
            Err(error) => return self.release(cx, Err(SectionError::Store(error))),
        };
        match gate::open(cx, context) {
            Gate::Clear(context) => self.decide(cx, context),
            Gate::Writing { effect, pending } => State::ApplyingTimers { effect, pending },
        }
    }

    fn on_timers(&mut self, cx: &mut Cx<'_>, pending: Pending, outcome: Outcome) -> State<C> {
        match pending.stored(cx, outcome) {
            Ok(Opened {
                context,
                transitions,
            }) => {
                self.timers = transitions;
                self.decide(cx, context)
            }
            Err(error) => self.release(cx, Err(SectionError::Store(error))),
        }
    }

    /// Decides the change on the room as the gate left it.
    fn decide(&self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State<C> {
        let now = cx.env.now();
        match self.change.decide(context, now) {
            Ok(decision) => self.write_next(cx, decision),
            Err(refusal) => self.release(cx, Err(SectionError::Refused(refusal))),
        }
    }

    /// Issues the decision's next write, or releases the lock once none is
    /// left.
    fn write_next(&self, cx: &mut Cx<'_>, mut decision: Decision<C::Done>) -> State<C> {
        match decision.writes.pop_front() {
            Some(request) => State::Writing {
                effect: cx.io.issue(request),
                decision: Box::new(decision),
            },
            None => self.release(cx, Ok(decision.done)),
        }
    }

    fn on_written(
        &self,
        cx: &mut Cx<'_>,
        decision: Decision<C::Done>,
        outcome: Outcome,
    ) -> State<C> {
        match outcome.into_stored() {
            Ok(()) => self.write_next(cx, decision),
            Err(error) => self.release(cx, Err(SectionError::Store(error))),
        }
    }

    /// Reports the change once the lock is released. A failed release
    /// changes nothing the change did; a lifecycle change reports no error
    /// beside its result, so the failure is only logged (gap G-025).
    fn on_released(&self, cx: &mut Cx<'_>, result: SectionResult<C>, outcome: Outcome) -> State<C> {
        let (result, _logged) = ops::after_release(&self.room_id, result, outcome.into_released());
        if let Some((reported, context)) = result.as_ref().ok().and_then(C::reported) {
            report::report(cx, reported, context);
        }
        State::Finished(result)
    }

    fn release(&self, cx: &mut Cx<'_>, result: SectionResult<C>) -> State<C> {
        let effect = cx.io.issue(HoldRequest::ReleaseLock {
            room_id: self.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}
