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
//! within it. An entry adopted from the call's work before its decision
//! carries the call's deadline up to its own commit (`Bound::Adopted`),
//! given up on past it by the driver alone.

mod locks;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use self::locks::RoomLocks;
use super::protocol::{
    AnswerOf, ChannelRequest, Decided, Delivered, Effect, EffectId, EntryId, HoldOutcome,
    HoldRequest, Next, Notice, Outcome, ProtocolError, Request, RequestKind, Settled, Step,
    TURN_CUT,
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
    /// adopted from its work after its decision among them.
    Unbounded,
    /// The call's bound, from its start to its decision: the deadline of
    /// its `process_timeout`, `None` when the kit sets none or the call takes
    /// none, a greeting's (RFC §19.1). The request is
    /// given up on past it, and when the call expires before it, its caller
    /// gone ([`Io::expire`]).
    Call(Option<Timestamp>),
    /// The bound of the adopted entry `.0`, up to its commit: its own
    /// deadline, `.1`, from its adoption, as every call's runs from its
    /// start (RFC §13.6, §9.5.1 Reentrance), and its caller: a request
    /// within it is given up on past that deadline, and once the entry's
    /// caller is gone ([`Io::expire_entry`]), not when the call's caller
    /// stops waiting.
    Adopted(EntryId, Option<Timestamp>),
}

impl Bound {
    /// Whether the call's deadline has passed at `now`: nothing passes an
    /// unbounded request's.
    pub(crate) fn has_passed(self, now: Timestamp) -> bool {
        self.deadline().is_some_and(|deadline| now >= deadline)
    }

    /// The deadline the effect carries (`Effect::deadline`).
    pub(crate) fn deadline(self) -> Option<Timestamp> {
        match self {
            Self::Unbounded => None,
            Self::Call(deadline) | Self::Adopted(_, deadline) => deadline,
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
    /// The bound it was asked within: the call's, given up on when the
    /// call expires, an adopted entry's, or none.
    bound: Bound,
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
    delivered: Vec<Delivered>,
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
    /// The adopted entries whose caller is gone: a request within an
    /// entry's bound is answered `DeadlinePassed` at once.
    expired_entries: BTreeSet<EntryId>,
    /// The acquire each `HoldRequest::Expiry` the driver holds stands for:
    /// one held back behind another section, whose deadline only that
    /// expiry tells.
    expiries: BTreeMap<EffectId, EffectId>,
}

impl Io {
    pub(crate) fn new() -> Self {
        Self {
            next_id: EffectId::first(),
            outstanding: BTreeMap::new(),
            issued: Vec::new(),
            notices: Vec::new(),
            settled: Vec::new(),
            delivered: Vec::new(),
            decided: None,
            has_decided: false,
            inbox: VecDeque::new(),
            locks: RoomLocks::default(),
            nested: None,
            held_for_release: Vec::new(),
            admitted: None,
            work: None,
            expired: false,
            expired_entries: BTreeSet::new(),
            expiries: BTreeMap::new(),
        }
    }

    /// Numbers the effects issued from now on past each of `taken`, ids the
    /// operation is handed rather than issues: an effect of its own then
    /// never shares an id with one of them.
    pub(crate) fn number_past(&mut self, taken: impl IntoIterator<Item = EffectId>) {
        for id in taken {
            if id >= self.next_id {
                self.next_id = id.next();
            }
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
        let within_gone_entry = matches!(
            bound,
            Bound::Adopted(entry, _) if self.expired_entries.contains(&entry)
        );
        if (self.expired && within_call) || within_gone_entry {
            self.inbox.push_back((id, Outcome::DeadlinePassed));
            return id;
        }
        let deadline = bound.deadline();
        let locked = locked.or_else(|| self.nested.clone());
        let budget = budget.or_else(|| self.lent(&request, locked.is_some()));
        let outstanding = Outstanding {
            kind: request.kind(),
            bounded: deadline.is_some(),
            bound,
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
            decides: None,
            answers: None,
        };
        let held_back_until = if let Request::Hold(
            HoldRequest::AcquireLock { room_id } | HoldRequest::AcquireLockForRecord { room_id },
        ) = &effect.request
        {
            effect.deadline.map(|deadline| (room_id.clone(), deadline))
        } else {
            None
        };
        match self.locks.admit(effect) {
            Some(effect) => self.issued.push(effect),
            None => {
                if let Some((room_id, deadline)) = held_back_until {
                    self.expire_held_back(id, room_id, deadline);
                }
            }
        }
        id
    }

    /// Asks the driver to tell when `deadline` passes for `acquire`, an
    /// acquire of `room_id`'s lock held back behind another section of the
    /// operation: past it, the acquire leaves the queue it waits in and is
    /// answered `DeadlinePassed`, as one the driver waits for is (RFC
    /// §13.6).
    fn expire_held_back(&mut self, acquire: EffectId, room_id: RoomId, deadline: Timestamp) {
        let id = self.next_id;
        self.next_id = id.next();
        self.expiries.insert(id, acquire);
        self.issued.push(Effect {
            id,
            request: HoldRequest::Expiry { room_id }.into(),
            deadline: Some(deadline),
            locked: None,
            admitted: None,
            budget: None,
            decides: None,
            answers: None,
        });
    }

    /// Takes the driver's answer to `expiry`, a `HoldRequest::Expiry`: the
    /// acquire it stands for, still held back, is given up on.
    fn expired_held_back(&mut self, expiry: EffectId) -> bool {
        let Some(acquire) = self.expiries.remove(&expiry) else {
            return false;
        };
        if self.locks.withdraw(acquire) && self.outstanding.remove(&acquire).is_some() {
            let next = self.locks.answered(acquire, &Outcome::DeadlinePassed);
            self.issued.extend(next);
            self.inbox.push_back((acquire, Outcome::DeadlinePassed));
        }
        true
    }

    /// The answer a nested section's lock request gets at once: the lock is
    /// its outer section's, held throughout.
    fn answer_nested(&self, request: &Request) -> Option<Outcome> {
        let nested = self.nested.as_ref()?;
        let Request::Hold(hold) = request else {
            return None;
        };
        let answer = match hold {
            HoldRequest::AcquireLock { room_id }
            | HoldRequest::AcquireLockForRecord { room_id }
                if room_id == nested =>
            {
                HoldOutcome::Locked
            }
            HoldRequest::ReleaseLock { room_id } if room_id == nested => HoldOutcome::Released,
            HoldRequest::AcquireLock { .. }
            | HoldRequest::AcquireLockForRecord { .. }
            | HoldRequest::ReleaseLock { .. }
            | HoldRequest::EnterLane { .. }
            | HoldRequest::AwaitLane { .. }
            | HoldRequest::LeaveLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::AwaitTicket { .. }
            | HoldRequest::ReleaseTicket { .. }
            | HoldRequest::Expiry { .. } => return None,
        };
        Some(answer.into())
    }

    /// Records that the operation holds `room_id`'s lock from its start, the
    /// lock its caller handed it (`Inbound::under_lock`): once it asks for
    /// that lock's release, an acquire of the room waits for its answer.
    pub(crate) fn hold_handed(&mut self, room_id: RoomId) {
        self.locks.hold(room_id);
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

    /// Marks `effect`, a commit issued for the next step, as the one that
    /// decides the call as `result` once it is stored (`Effect::decides`).
    pub(crate) fn decides(&mut self, effect: EffectId, result: InboundResult) {
        let issued = self.issued.iter_mut().find(|issued| issued.id == effect);
        debug_assert!(
            issued.is_some(),
            "a call's deciding commit goes out on the next step"
        );
        if let Some(issued) = issued {
            issued.decides = Some(Box::new(result));
        }
    }

    /// Marks `effect`, a commit or a stream's read issued for the next step,
    /// as storing or reading an answer of `of`'s cascade (`Effect::answers`).
    pub(crate) fn answers(&mut self, effect: EffectId, of: AnswerOf) {
        if let Some(issued) = self.issued.iter_mut().find(|issued| issued.id == effect) {
            issued.answers = Some(of);
        }
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

    /// Reports what the delivery set of the adopted entry `entry`'s event
    /// came to, `result`, on the next step.
    pub(crate) fn delivered(&mut self, entry: EntryId, result: InboundResult) {
        self.delivered.push(Delivered { entry, result });
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
        self.give_up_within(|bound| matches!(bound, Bound::Call(_)))
    }

    /// Expires the adopted entry `entry` before its commit, its caller gone
    /// (RFC §9.5.1 Reentrance, §13.6): every request within its bound not
    /// answered yet is given up on and answered `DeadlinePassed` here, and
    /// so is every one it asks for from now on, as [`Io::expire`] does for
    /// the call's. Returns the effects the driver holds among them.
    pub(crate) fn expire_entry(&mut self, entry: EntryId) -> Vec<EffectId> {
        self.expired_entries.insert(entry);
        self.give_up_within(|bound| matches!(bound, Bound::Adopted(of, _) if of == entry))
    }

    /// Gives up on every effect not answered yet whose bound `within`
    /// holds, answering it `DeadlinePassed`; returns those the driver holds.
    fn give_up_within(&mut self, within: impl Fn(Bound) -> bool) -> Vec<EffectId> {
        let given_up: Vec<EffectId> = self
            .outstanding
            .iter()
            .filter(|(_, outstanding)| within(outstanding.bound))
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

    /// Whether a section of the operation holds `room_id`'s lock, or waits
    /// for it, through the driver: a release of the room's ticket waits for
    /// it (RFC §9.5.1).
    pub(crate) fn holds_lock_of(&self, room_id: &RoomId) -> bool {
        self.locks.holds(room_id)
    }

    /// The bound `effect`, an effect not answered yet, was asked within
    /// (RFC §13.6): the call's for a request of its own before its
    /// decision, an adopted entry's for one of the entry's before its
    /// commit, none for any other.
    pub(crate) fn bound_of(&self, effect: EffectId) -> Bound {
        self.outstanding
            .get(&effect)
            .map_or(Bound::Unbounded, |outstanding| outstanding.bound)
    }

    /// The room in whose admission order `effect`, an effect not answered
    /// yet, was made (`Effect::admitted`).
    pub(crate) fn admitted_for(&self, effect: EffectId) -> Option<&RoomId> {
        self.outstanding.get(&effect)?.admitted.as_ref()
    }

    /// Accepts the driver's answer to `id`, once and only if it answers the
    /// effect's request.
    pub(crate) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        if self.expired_held_back(id) {
            return Ok(());
        }
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
        let delivered = std::mem::take(&mut self.delivered);
        let decided = self.decided.take();
        let next = match result {
            Some(result) => Next::Ready(result),
            None if !self.issued.is_empty() => Next::Effects(std::mem::take(&mut self.issued)),
            None => Next::Waiting,
        };
        Step {
            notices,
            settled,
            delivered,
            decided,
            next,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EventId;
    use crate::ops::protocol::StoreRequest;
    use crate::ops::protocol::{Next, Request, Step};

    // RMK-1472: an acquire held back behind another section of the
    // operation is given up on at its own deadline, which an expiry tells.
    #[test]
    fn an_acquire_held_back_is_given_up_on_at_its_deadline() {
        let room_id = RoomId::new("room_1").unwrap();
        let deadline = Timestamp::from_unix_millis(100).unwrap();
        let mut io = Io::new();
        let first = io.issue(HoldRequest::AcquireLock {
            room_id: room_id.clone(),
        });
        let held_back = io.issue_within(
            HoldRequest::AcquireLock {
                room_id: room_id.clone(),
            },
            Bound::Call(Some(deadline)),
        );
        let Step { next, .. } = io.step::<()>(None);
        let Next::Effects(effects) = next else {
            panic!("no effect issued");
        };
        let ids: Vec<(EffectId, Option<Timestamp>)> = effects
            .iter()
            .map(|effect| (effect.id, effect.deadline))
            .collect();
        let expiry = effects
            .iter()
            .find(|effect| matches!(effect.request, Request::Hold(HoldRequest::Expiry { .. })))
            .map(|effect| effect.id)
            .expect("an expiry for the held-back acquire");
        io.complete(expiry, Outcome::DeadlinePassed).unwrap();

        assert_eq!(
            (ids.len(), io.next_outcome(), first < held_back),
            (2, Some((held_back, Outcome::DeadlinePassed)), true)
        );
    }

    // RMK-1185: a nested operation handed a turn under its caller's id
    // never issues that id itself.
    #[test]
    fn an_operation_numbers_its_effects_past_the_ids_it_is_handed() {
        let mut io = Io::new();
        let handed = (0..3).fold(EffectId::first(), |id, _| id.next());
        io.number_past([handed]);

        let own = io.issue(StoreRequest::GetEvent {
            event_id: EventId::new("evt_1").unwrap(),
        });

        assert!(own > handed, "{own:?} is not past {handed:?}");
    }
}
