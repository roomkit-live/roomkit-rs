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

mod locks;

use std::collections::{BTreeMap, VecDeque};

use self::locks::RoomLocks;
use super::protocol::{
    Decided, Effect, EffectId, EntryId, Next, Notice, Outcome, ProtocolError, Request, RequestKind,
    Settled, Step,
};
use super::{InboundError, InboundResult};
use crate::model::{RoomId, Timestamp};
use crate::rules::chain_depth::ReentryBudget;

/// What `Io` keeps of an effect until it is answered.
#[derive(Debug)]
struct Outstanding {
    kind: RequestKind,
    bounded: bool,
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
    /// The room in whose admission order the section being advanced runs
    /// (RFC §9.5.1).
    admitted: Option<RoomId>,
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
            admitted: None,
        }
    }

    /// Asks the driver for `request`; the effect goes out on the next step.
    pub(crate) fn issue(&mut self, request: Request) -> EffectId {
        self.issue_within(request, None)
    }

    /// Asks the driver for `request`, to be given up on past `deadline`
    /// (RFC §13.6); `None` bounds it by nothing.
    pub(crate) fn issue_within(
        &mut self,
        request: Request,
        deadline: Option<Timestamp>,
    ) -> EffectId {
        self.issue_under(request, deadline, None)
    }

    /// Asks the driver for `request`, within `deadline`, on behalf of a
    /// section that holds the lock of `locked` (RFC §13.5). A request of a
    /// nested section runs under the lock it is nested in, and its own
    /// requests for that lock are answered at once, without the driver.
    pub(crate) fn issue_under(
        &mut self,
        request: Request,
        deadline: Option<Timestamp>,
        locked: Option<RoomId>,
    ) -> EffectId {
        self.issue_effect(request, deadline, locked, None)
    }

    /// Asks the driver for `request` on behalf of a section that holds
    /// `room_id`'s lock (`Effect::locked`, RFC §13.5), with no deadline: a
    /// commit, or the place its event takes in the room's lane, which
    /// nothing bounds (RFC §13.6).
    pub(crate) fn issue_locked(&mut self, request: Request, room_id: RoomId) -> EffectId {
        self.issue_under(request, None, Some(room_id))
    }

    /// Asks the driver for `request`, a delivery set's `OnEvent` or
    /// `Deliver`, whose code's calls into a room draw on `budget`, the
    /// reentry budget of the cascade that delivers (`Effect::budget`, gap
    /// G-070).
    pub(crate) fn issue_drawing_on(
        &mut self,
        request: Request,
        budget: &ReentryBudget,
    ) -> EffectId {
        self.issue_effect(request, None, None, Some(budget.clone()))
    }

    fn issue_effect(
        &mut self,
        request: Request,
        deadline: Option<Timestamp>,
        locked: Option<RoomId>,
        budget: Option<ReentryBudget>,
    ) -> EffectId {
        let id = self.next_id;
        self.next_id = id.next();
        if let Some(outcome) = self.answer_nested(&request) {
            self.inbox.push_back((id, outcome));
            return id;
        }
        let locked = locked.or_else(|| self.nested.clone());
        let outstanding = Outstanding {
            kind: request.kind(),
            bounded: deadline.is_some(),
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
        if let Request::AcquireLock { room_id } = request
            && room_id == nested
        {
            return Some(Outcome::Locked);
        }
        if let Request::ReleaseLock { room_id } = request
            && room_id == nested
        {
            return Some(Outcome::Released);
        }
        None
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
        if let Some(next) = self.locks.answered(id, &outcome) {
            self.issued.push(next);
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
