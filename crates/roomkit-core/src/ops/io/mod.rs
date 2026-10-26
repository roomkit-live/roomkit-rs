//! The bookkeeping every operation shares: issuing effects, checking the
//! outcomes that answer them, collecting notices and the results of the
//! entries it adopted.
//!
//! Two of an operation's sections never hold one room's lock at once
//! (`locks`): the second waits for the first's release. A section nested
//! under the section that holds the lock, an entry adopted from that
//! section's hook (gap G-070), runs within its tenure: its requests for the
//! lock are answered here, and never reach the driver. The requests of a
//! section that stands in a room's admission order (RFC §9.5.1) say so, as
//! those of a section under a lock do.
//!
//! A hook, a parse or a route asked for under a lock or in an admission
//! order carries the reentry budget of the cascade its section works for
//! (`Effect::budget`, gap G-087), which the operation lends.
//!
//! A request within the call's bound, from its start to its decision (RFC
//! §13.6), is given up on past its deadline, by the driver, and when the
//! call expires before it, its caller gone (`expire`), here: it is
//! answered `DeadlinePassed` as the driver would answer it, and so is every
//! one asked for afterwards. A write, a release and a commit are never
//! within it.

mod locks;

use std::collections::{BTreeMap, VecDeque};

use self::locks::RoomLocks;
use super::protocol::{
    ChannelRequest, Decided, Effect, EffectId, EntryId, HoldOutcome, HoldRequest, Next, Notice,
    Outcome, ProtocolError, Request, RequestKind, Settled, Step, TURN_CUT,
};
use super::{InboundError, InboundResult};
use crate::model::{ErrorRecord, RoomId, Timestamp};
use crate::rules::chain_depth::ReentryBudget;

/// What bounds a request in time (RFC §13.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Bound {
    /// Nothing but its own timeouts: a write, a release, a commit, what
    /// completes a room step 2 creates, what follows a call's decision, and
    /// every request of an operation that has no call's bound, an entry
    /// adopted among them.
    Unbounded,
    /// The call's bound, from its start to its decision: the deadline of
    /// its `process_timeout`, `None` when the kit sets none. The request is
    /// given up on past it, and when the call expires before it, its caller
    /// gone ([`Io::expire`]).
    Call(Option<Timestamp>),
}

impl Bound {
    /// The deadline the effect carries (`Effect::deadline`).
    fn deadline(self) -> Option<Timestamp> {
        match self {
            Self::Unbounded => None,
            Self::Call(deadline) => deadline,
        }
    }
}

/// What `Io` keeps of an effect until it is answered.
#[derive(Debug)]
struct Outstanding {
    kind: RequestKind,
    /// Whether it carried a deadline, which the driver may answer
    /// `DeadlinePassed` past.
    bounded: bool,
    /// Whether it is within the call's bound, given up on when the call
    /// expires.
    within_call: bool,
    locked: Option<RoomId>,
    admitted: Option<RoomId>,
}

/// An operation's side of the protocol.
#[derive(Debug)]
pub(crate) struct Io {
    next_id: EffectId,
    /// Each effect not answered yet: its request's family, whether it
    /// carried a deadline, the room whose lock it was made under, and the
    /// room in whose admission order it was made.
    outstanding: BTreeMap<EffectId, Outstanding>,
    issued: Vec<Effect>,
    notices: Vec<Notice>,
    settled: Vec<Settled>,
    /// The call's decision, until the next step publishes it.
    decided: Option<Decided>,
    /// Whether the call's decision is known, published or about to be.
    has_decided: bool,
    inbox: VecDeque<(EffectId, Outcome)>,
    locks: RoomLocks,
    /// The room whose lock the section being advanced runs under, nested
    /// in the section that holds it.
    nested: Option<RoomId>,
    /// The notices a nested section emitted for after its release, held
    /// until the section that holds the room's lock released it.
    held_for_release: Vec<(RoomId, Notice)>,
    /// The room in whose admission order the section being advanced runs
    /// (RFC §9.5.1).
    admitted: Option<RoomId>,
    /// The reentry budget of the cascade the sections being advanced work
    /// for, which the operation lends ([`Io::work_for`]).
    work: Option<ReentryBudget>,
    /// Whether the call expired before its decision, its caller gone: a
    /// request within its bound is answered `DeadlinePassed` at once.
    expired: bool,
}

impl Io {
    pub(crate) fn new() -> Self {
        Self {
            next_id: EffectId::first(),
            outstanding: BTreeMap::new(),
            issued: Vec::new(),
            notices: Vec::new(),
            settled: Vec::new(),
            decided: None,
            has_decided: false,
            inbox: VecDeque::new(),
            locks: RoomLocks::default(),
            nested: None,
            held_for_release: Vec::new(),
            admitted: None,
            work: None,
            expired: false,
        }
    }

    /// Makes the sections advanced from now on work for the cascade whose
    /// budget is `budget`: a hook, a parse or a route they ask for under a
    /// lock or in an admission order carries it (`Effect::budget`, RFC
    /// §10.3, gap G-087), so that a direct change its code makes is that
    /// cascade's work.
    pub(crate) fn work_for(&mut self, budget: &ReentryBudget) {
        self.work = Some(budget.clone());
    }

    /// The budget of the cascade the sections being advanced work for, as
    /// [`Io::work_for`] lent it.
    pub(crate) fn work(&self) -> Option<&ReentryBudget> {
        self.work.as_ref()
    }

    /// The budget `request`, made under a lock when `locked`, carries
    /// unless it names its own: the cascade's its section works for, for
    /// a request that runs the integrator's code under a lock or in an
    /// admission order.
    fn lent(&self, request: &Request, locked: bool) -> Option<ReentryBudget> {
        let runs_code = matches!(
            request,
            Request::Hook(_) | Request::Channel(_) | Request::Route { .. }
        );
        let in_section = locked || self.admitted.is_some();
        if runs_code && in_section {
            self.work.clone()
        } else {
            None
        }
    }

    /// Asks the driver for `request`, which nothing bounds; the effect goes
    /// out on the next step.
    pub(crate) fn issue(&mut self, request: impl Into<Request>) -> EffectId {
        self.issue_within(request, Bound::Unbounded)
    }

    /// Asks the driver for `request`, within `bound` (RFC §13.6).
    pub(crate) fn issue_within(&mut self, request: impl Into<Request>, bound: Bound) -> EffectId {
        self.issue_under(request, bound, None)
    }

    /// Asks the driver for `request`, within `bound`, on behalf of a section
    /// that holds the lock of `locked` (RFC §13.5). A request of a nested
    /// section runs under the lock it is nested in, and its own requests
    /// for that lock are answered at once, without the driver.
    pub(crate) fn issue_under(
        &mut self,
        request: impl Into<Request>,
        bound: Bound,
        locked: Option<RoomId>,
    ) -> EffectId {
        self.issue_effect(request.into(), bound, locked, None)
    }

    /// Asks the driver for `request` on behalf of a section that holds
    /// `room_id`'s lock (`Effect::locked`, RFC §13.5), unbounded: a commit,
    /// or the place its event takes in the room's lane, which nothing
    /// bounds (RFC §13.6).
    pub(crate) fn issue_locked(
        &mut self,
        request: impl Into<Request>,
        room_id: RoomId,
    ) -> EffectId {
        self.issue_under(request, Bound::Unbounded, Some(room_id))
    }

    /// Asks the driver for `request`, a delivery set's `OnEvent` or
    /// `Deliver`, whose code's calls into a room draw on `budget`, the
    /// reentry budget of the cascade that delivers (`Effect::budget`, gap
    /// G-070).
    pub(crate) fn issue_drawing_on(
        &mut self,
        request: ChannelRequest,
        budget: &ReentryBudget,
    ) -> EffectId {
        self.issue_effect(request.into(), Bound::Unbounded, None, Some(budget.clone()))
    }

    fn issue_effect(
        &mut self,
        request: Request,
        bound: Bound,
        locked: Option<RoomId>,
        budget: Option<ReentryBudget>,
    ) -> EffectId {
        let id = self.next_id;
        self.next_id = id.next();
        if let Some(outcome) = self.answer_nested(&request) {
            self.inbox.push_back((id, outcome));
            return id;
        }
        let within_call = matches!(bound, Bound::Call(_));
        if self.expired && within_call {
            self.inbox.push_back((id, Outcome::DeadlinePassed));
            return id;
        }
        let deadline = bound.deadline();
        let locked = locked.or_else(|| self.nested.clone());
        let budget = budget.or_else(|| self.lent(&request, locked.is_some()));
        let outstanding = Outstanding {
            kind: request.kind(),
            bounded: deadline.is_some(),
            within_call,
            locked: locked.clone(),
            admitted: self.admitted.clone(),
        };
        self.outstanding.insert(id, outstanding);
        let effect = Effect {
            id,
            request,
            deadline,
            locked,
            admitted: self.admitted.clone(),
            budget,
        };
        if let Some(effect) = self.locks.admit(effect) {
            self.issued.push(effect);
        }
        id
    }

    /// The answer a nested section's lock request gets at once: the lock is
    /// its outer section's, held throughout.
    fn answer_nested(&self, request: &Request) -> Option<Outcome> {
        let nested = self.nested.as_ref()?;
        let Request::Hold(hold) = request else {
            return None;
        };
        let answer = match hold {
            HoldRequest::AcquireLock { room_id } if room_id == nested => HoldOutcome::Locked,
            HoldRequest::ReleaseLock { room_id } if room_id == nested => HoldOutcome::Released,
            HoldRequest::AcquireLock { .. }
            | HoldRequest::ReleaseLock { .. }
            | HoldRequest::EnterLane { .. }
            | HoldRequest::AwaitLane { .. }
            | HoldRequest::LeaveLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::AwaitTicket { .. }
            | HoldRequest::ReleaseTicket { .. } => return None,
        };
        Some(answer.into())
    }

    /// Makes the sections advanced from now on nested under the section
    /// that holds `room_id`'s lock, or sections of their own for `None`;
    /// returns what it replaces, which its caller puts back.
    pub(crate) fn nest(&mut self, room_id: Option<RoomId>) -> Option<RoomId> {
        std::mem::replace(&mut self.nested, room_id)
    }

    /// Makes the requests issued from now on run in the admission order of
    /// `room_id` (RFC §9.5.1, `Effect::admitted`), or in none for `None`;
    /// returns what it replaces, which its caller puts back.
    pub(crate) fn admit(&mut self, room_id: Option<RoomId>) -> Option<RoomId> {
        std::mem::replace(&mut self.admitted, room_id)
    }

    /// Emits `notice` on the next step.
    pub(crate) fn notify(&mut self, notice: Notice) {
        self.notices.push(notice);
    }

    /// Emits `notice`, which a section owes once it released the room's
    /// lock (RFC §5.1 When): on the next step when the section held a
    /// tenure of its own, released by now; once the section that holds the
    /// lock released it when the section ran nested under it, whose release
    /// `Io` answered without the driver.
    pub(crate) fn notify_after_release(&mut self, notice: Notice) {
        match &self.nested {
            Some(room_id) => self.held_for_release.push((room_id.clone(), notice)),
            None => self.notices.push(notice),
        }
    }

    /// Emits the notices held for `room_id`'s release, now answered.
    fn release_held(&mut self, room_id: &RoomId) {
        let (released, held): (Vec<_>, Vec<_>) = std::mem::take(&mut self.held_for_release)
            .into_iter()
            .partition(|(room, _)| room == room_id);
        self.held_for_release = held;
        self.notices
            .extend(released.into_iter().map(|(_, notice)| notice));
    }

    /// Reports the result of the adopted entry `entry` on the next step.
    pub(crate) fn settle(&mut self, entry: EntryId, result: Result<InboundResult, InboundError>) {
        self.settled.push(Settled { entry, result });
    }

    /// Publishes the call's decision on the next step, once: a later one
    /// is a defect of the operation, and is not published.
    pub(crate) fn decide(&mut self, decided: Decided) {
        debug_assert!(!self.has_decided, "a call is decided once");
        if !self.has_decided {
            self.has_decided = true;
            self.decided = Some(decided);
        }
    }

    /// Whether the call's decision is known.
    pub(crate) fn has_decided(&self) -> bool {
        self.has_decided
    }

    /// Expires the call before its decision, its caller gone (RFC §13.6,
    /// gap G-076): every request within the call's bound not answered yet
    /// is given up on, as the driver gives up on one past its deadline, and
    /// answered `DeadlinePassed` here; so is every one asked for from now
    /// on. An acquire held back here leaves the queue it waits in. Returns
    /// the effects the driver holds among them: it drops them, and never
    /// answers them.
    pub(crate) fn expire(&mut self) -> Vec<EffectId> {
        self.expired = true;
        let given_up: Vec<EffectId> = self
            .outstanding
            .iter()
            .filter(|(_, outstanding)| outstanding.within_call)
            .map(|(id, _)| *id)
            .collect();
        // The acquires held back leave first: the answer to the one the
        // driver holds would otherwise hand the next of them on.
        let held: Vec<EffectId> = given_up
            .iter()
            .copied()
            .filter(|id| self.locks.withdraw(*id))
            .collect();
        for id in &given_up {
            self.outstanding.remove(id);
            let next = self.locks.answered(*id, &Outcome::DeadlinePassed);
            self.issued.extend(next);
            self.inbox.push_back((*id, Outcome::DeadlinePassed));
        }
        given_up
            .into_iter()
            .filter(|id| !held.contains(id))
            .collect()
    }

    /// Gives up on `effects`, turns its operation cut (RFC §10.1 "Explicit
    /// delivery cancellation"): each one outstanding is answered here, as a
    /// turn cut, and returns those the driver holds, which it cuts and never
    /// answers.
    pub(crate) fn cut(&mut self, effects: &[EffectId]) -> Vec<EffectId> {
        let mut cut = Vec::new();
        for effect in effects {
            if self.outstanding.remove(effect).is_some() {
                let error = ErrorRecord::new(TURN_CUT, "the turn's cascade was cancelled");
                self.inbox.push_back((*effect, Outcome::Failed(error)));
                cut.push(*effect);
            }
        }
        cut
    }

    /// Whether the call expired, its caller gone before its decision.
    pub(crate) fn is_expired(&self) -> bool {
        self.expired
    }

    /// Whether `effect` is issued and not answered yet.
    pub(crate) fn is_outstanding(&self, effect: EffectId) -> bool {
        self.outstanding.contains_key(&effect)
    }

    /// The room whose lock the operation holds on behalf of `effect`, an
    /// effect not answered yet (`Effect::locked`).
    pub(crate) fn locked_for(&self, effect: EffectId) -> Option<&RoomId> {
        self.outstanding.get(&effect)?.locked.as_ref()
    }

    /// The room in whose admission order `effect`, an effect not answered
    /// yet, was made (`Effect::admitted`).
    pub(crate) fn admitted_for(&self, effect: EffectId) -> Option<&RoomId> {
        self.outstanding.get(&effect)?.admitted.as_ref()
    }

    /// Accepts the driver's answer to `id`, once and only if it answers the
    /// effect's request.
    pub(crate) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        let outstanding = self
            .outstanding
            .get(&id)
            .ok_or(ProtocolError::UnknownEffect(id))?;
        outstanding.kind.check(id, &outcome, outstanding.bounded)?;
        self.outstanding.remove(&id);
        let released = self.locks.release_of(id);
        let next = self.locks.answered(id, &outcome);
        self.issued.extend(next);
        if let Some(room_id) = released {
            self.release_held(&room_id);
        }
        self.inbox.push_back((id, outcome));
        Ok(())
    }

    /// The next answer to process, in completion order.
    pub(crate) fn next_outcome(&mut self) -> Option<(EffectId, Outcome)> {
        self.inbox.pop_front()
    }

    /// Whether answers wait to be processed.
    pub(crate) fn has_outcomes(&self) -> bool {
        !self.inbox.is_empty()
    }

    /// Hands back an answer held back until now, to be processed again.
    pub(crate) fn requeue(&mut self, effect: EffectId, outcome: Outcome) {
        self.inbox.push_back((effect, outcome));
    }

    /// The step the driver receives: the notices so far, the entries
    /// settled, the call's decision, then the effects issued, a wait, or the
    /// operation's result.
    pub(crate) fn step<T>(&mut self, result: Option<T>) -> Step<T> {
        let notices = std::mem::take(&mut self.notices);
        let settled = std::mem::take(&mut self.settled);
        let decided = self.decided.take();
        let next = match result {
            Some(result) => Next::Ready(result),
            None if !self.issued.is_empty() => Next::Effects(std::mem::take(&mut self.issued)),
            None => Next::Waiting,
        };
        Step {
            notices,
            settled,
            decided,
            next,
        }
    }
}
