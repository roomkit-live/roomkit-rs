//! The frame every change of a room made under its lock shares (RFC §13.5,
//! gaps G-025, G-027, G-045): take the room's lock, read the room, refuse a
//! room the caller does not reach, apply the room's due timer transitions,
//! do the work, release the lock, then report.
//!
//! [`Section`] is that frame as a sub-machine; what it does under the lock
//! is its [`SectionWork`]: a lifecycle change or step 2's (`lifecycle`), a
//! direct change of a stored event or a record committed outside the
//! pipeline (`events`), a record the inbound pipeline stores refused
//! (`inbound::record`). The inbound pipeline's pass, whose hooks and
//! delivery planning run under the lock, keeps a machine of its own.
//!
//! Every path that took the lock releases it before the section is done; a
//! lock that cannot be taken ends the section at once, with nothing to
//! release. A room the caller does not reach is refused on the room as the
//! lock finds it, before its due timers are applied, so nothing is written
//! in it (RFC §17.2). What the work did is reported once the lock is
//! released (gap G-027), and a failed release changes nothing it did (gap
//! G-025).

use std::fmt::Debug;
use std::mem;

use super::io::Bound;
use super::lifecycle::gate::{self, Applied, Gate, Opened, Pending, TimerHooks};
use super::protocol::{EffectId, HoldRequest, Outcome, StoreRequest};
use super::{Cx, Progress};
use crate::model::{ErrorRecord, Room, RoomContext, RoomId};
use crate::rules::timers::DueTransition;

/// A failure of the frame itself, which its work reports as its own error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The store failed a read or a write of the frame's.
    Store(ErrorRecord),
    /// The room's lock could not be taken.
    Lock(ErrorRecord),
}

/// What a section does under the room's lock.
pub(crate) trait SectionWork: Debug {
    /// What the work did.
    type Output: Debug;
    /// Why it did not, nothing written by it.
    type Error: Debug;

    /// The frame's `failure`, as the work reports it.
    fn failure(failure: Failure) -> Self::Error;

    /// Whether the work reads the room's latest events itself, so that its
    /// context holds at least the floor of them: no work does by default.
    fn reads_history(&self) -> bool {
        false
    }

    /// Whether the room's due timer transitions are applied before the
    /// work begins: every work's but a change the Python implementation
    /// makes under the lock without them (`update_room_metadata`).
    fn applies_timers(&self) -> bool {
        true
    }

    /// What the work gives for `room_id`, a room the store does not hold.
    ///
    /// # Errors
    ///
    /// Returns the work's error when a missing room fails it.
    fn missing(&self, room_id: &RoomId) -> Result<Self::Output, Self::Error>;

    /// What the work gives for `room`, as the lock finds it, when its caller
    /// does not reach it (RFC §17.2): refused before its due timers are
    /// applied. `None` for a room the caller reaches.
    fn refuses(&self, room: &Room) -> Option<Result<Self::Output, Self::Error>>;

    /// Starts on `context`, the room as the timer gate left it.
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
    ) -> Progress<Result<Self::Output, Self::Error>>;

    /// Takes the answer to `effect`, an effect the work waits for.
    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Self::Output, Self::Error>>;

    /// Whether the work waits for `effect`.
    fn awaits(&self, effect: EffectId) -> bool;

    /// Reports what the work did, once the lock is released (gap G-027).
    fn report(&self, cx: &mut Cx<'_>, result: &Result<Self::Output, Self::Error>);

    /// What the work gives when its read of the room, bounded by
    /// [`Section::reading_within`], passed the call's deadline: the lock is
    /// released, nothing done. `None` for a work whose read is never
    /// bounded.
    fn read_expired(&self) -> Option<Result<Self::Output, Self::Error>> {
        None
    }
}

/// What a section ends with.
#[derive(Debug)]
pub(crate) struct Ended<W: SectionWork> {
    /// What the work did, or why it did not.
    pub(crate) result: Result<W::Output, W::Error>,
    /// The failure of the lock's release, handed back when the work
    /// succeeded: it changes nothing the work did (gap G-025).
    pub(crate) release_error: Option<ErrorRecord>,
}

/// One change of a room under its lock, as a sub-machine: started once,
/// then handed the answer to each of its effects until it ends.
#[derive(Debug)]
pub(crate) struct Section<W: SectionWork> {
    room_id: RoomId,
    /// What bounds the wait for the lock: an inbound call's bound (RFC
    /// §10.4, §13.6), `Bound::Unbounded` otherwise.
    bound: Bound,
    /// What bounds the read of the room under the lock, `Bound::Unbounded`
    /// unless [`reading_within`](Self::reading_within) sets it.
    read_bound: Bound,
    work: W,
    state: State<W>,
    /// The timer transitions the gate applied.
    timers: Vec<DueTransition>,
    /// Their hooks, fired once the lock is released (RFC §5.1 When).
    timer_hooks: TimerHooks,
    /// Finish the timer gate, then skip the abandoned caller's work.
    cancelled_after_timers: bool,
}

#[derive(Debug)]
enum State<W: SectionWork> {
    Start,
    Locking(EffectId),
    Loading(EffectId),
    ApplyingTimers {
        effect: EffectId,
        pending: Box<Pending>,
    },
    Working,
    Releasing {
        effect: EffectId,
        result: Result<W::Output, W::Error>,
    },
    Ended,
}

impl<W: SectionWork> State<W> {
    /// The frame's own effect the state waits for.
    fn awaits(&self) -> Option<EffectId> {
        match self {
            Self::Locking(effect)
            | Self::Loading(effect)
            | Self::ApplyingTimers { effect, .. }
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Working | Self::Ended => None,
        }
    }
}

impl<W: SectionWork> Section<W> {
    /// The section doing `work` in `room_id`.
    pub(crate) fn new(room_id: RoomId, work: W) -> Self {
        Self {
            room_id,
            bound: Bound::Unbounded,
            read_bound: Bound::Unbounded,
            work,
            state: State::Start,
            timers: Vec::new(),
            timer_hooks: TimerHooks::default(),
            cancelled_after_timers: false,
        }
    }

    /// A section that ended before it started: it takes no lock.
    pub(crate) fn ended(room_id: RoomId, work: W) -> Self {
        Self {
            state: State::Ended,
            ..Self::new(room_id, work)
        }
    }

    /// The section waiting for the lock within `bound` at most: past the
    /// call's deadline, or, for the call's own bound, once its caller is
    /// gone, the wait is answered `DeadlinePassed`, which the section's
    /// caller takes before handing it the outcome, since no lock is held
    /// then.
    pub(crate) fn waiting_within(mut self, bound: Bound) -> Self {
        self.bound = bound;
        self
    }

    /// The section reading the room under the lock within `bound` too: past
    /// it, the read is answered `DeadlinePassed`, and the section releases
    /// the lock and ends with its work's
    /// [`read_expired`](SectionWork::read_expired).
    pub(crate) fn reading_within(mut self, bound: Bound) -> Self {
        self.read_bound = bound;
        self
    }

    /// Whether the section still waits for the room's lock, holding none.
    pub(crate) fn locking(&self) -> bool {
        matches!(self.state, State::Locking(_))
    }

    /// A direct caller left while its timer gate was writing. Its timers
    /// and their reports finish, but its own change must not begin.
    pub(crate) fn cancel_after_timers(&mut self) {
        self.cancelled_after_timers |= matches!(self.state, State::ApplyingTimers { .. });
    }

    /// The work, before the section starts it.
    pub(crate) fn work_mut(&mut self) -> &mut W {
        &mut self.work
    }

    /// The work, as it stands.
    pub(crate) fn work(&self) -> &W {
        &self.work
    }

    /// The room the section works in.
    pub(crate) fn room_id(&self) -> &RoomId {
        &self.room_id
    }

    /// The timer transitions the gate applied.
    pub(crate) fn transitions(&self) -> &[DueTransition] {
        &self.timers
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
        if matches!(self.state, State::Working) {
            return self.work.awaits(effect);
        }
        self.state.awaits() == Some(effect)
    }

    /// Takes the driver's answer to one of the section's effects. The
    /// section ends once the lock is released, or could not be taken.
    pub(crate) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Ended<W>> {
        if !self.awaits(effect) {
            debug_assert!(false, "a locked section received {effect:?}");
            return Progress::Waiting;
        }
        self.state = match mem::replace(&mut self.state, State::Ended) {
            State::Locking(_) => match outcome.into_locked() {
                // The work's hooks receive the room's recent events, as every
                // hook does (gap G-027).
                Ok(()) => {
                    let mut recent_events = cx.config.recent_events();
                    if self.work.reads_history() {
                        recent_events = recent_events.read_by_the_caller();
                    }
                    let request = StoreRequest::LoadContext {
                        room_id: self.room_id.clone(),
                        recent_events,
                    };
                    State::Loading(cx.io.issue_within(request, self.read_bound))
                }
                Err(error) => {
                    return Progress::Done(Ended {
                        result: Err(W::failure(Failure::Lock(error))),
                        release_error: None,
                    });
                }
            },
            State::Loading(_) if outcome == Outcome::DeadlinePassed => {
                match self.work.read_expired() {
                    Some(expired) => self.release(cx, expired),
                    None => self.on_loaded(cx, outcome),
                }
            }
            State::Loading(_) => self.on_loaded(cx, outcome),
            State::ApplyingTimers { pending, .. } => match pending.stored(cx, outcome) {
                Ok(Applied::Opened(Opened {
                    context,
                    transitions,
                    hooks,
                })) => {
                    self.timers = transitions;
                    self.timer_hooks = hooks;
                    self.after_timers(cx, context)
                }
                Ok(Applied::Writing { effect, pending }) => {
                    State::ApplyingTimers { effect, pending }
                }
                Err(error) => self.release(cx, Err(W::failure(Failure::Store(error)))),
            },
            State::Working => {
                let accepted = self.work.accept(cx, effect, outcome);
                self.working(cx, accepted)
            }
            State::Releasing { result, .. } => {
                let (result, release_error) =
                    super::after_release(&self.room_id, result, outcome.into_released());
                mem::take(&mut self.timer_hooks).fire(cx);
                self.work.report(cx, &result);
                return Progress::Done(Ended {
                    result,
                    release_error,
                });
            }
            state @ (State::Start | State::Ended) => state,
        };
        Progress::Waiting
    }

    /// The room as the lock found it: refused when its caller does not reach
    /// it, else through the timer gate first.
    fn on_loaded(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State<W> {
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => {
                let missing = self.work.missing(&self.room_id);
                return self.release(cx, missing);
            }
            Err(error) => return self.release(cx, Err(W::failure(Failure::Store(error)))),
        };
        if let Some(refused) = self.work.refuses(&context.room) {
            return self.release(cx, refused);
        }
        if !self.work.applies_timers() {
            let begun = self.work.begin(cx, context);
            return self.working(cx, begun);
        }
        match gate::open(cx, context) {
            Gate::Clear(context) => {
                let begun = self.work.begin(cx, context);
                self.working(cx, begun)
            }
            Gate::Writing { effect, pending } => State::ApplyingTimers { effect, pending },
        }
    }

    fn after_timers(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State<W> {
        if self.cancelled_after_timers {
            let error = ErrorRecord::new("CancelledError", "the caller left during its timer gate");
            return self.release(cx, Err(W::failure(Failure::Store(error))));
        }
        let begun = self.work.begin(cx, context);
        self.working(cx, begun)
    }

    /// Goes on with the work, or releases the lock once it is done.
    fn working(
        &mut self,
        cx: &mut Cx<'_>,
        progress: Progress<Result<W::Output, W::Error>>,
    ) -> State<W> {
        match progress {
            Progress::Waiting => State::Working,
            Progress::Done(result) => self.release(cx, result),
        }
    }

    fn release(&self, cx: &mut Cx<'_>, result: Result<W::Output, W::Error>) -> State<W> {
        let effect = cx.io.issue(HoldRequest::ReleaseLock {
            room_id: self.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}
