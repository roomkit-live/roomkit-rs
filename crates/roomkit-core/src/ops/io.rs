//! The bookkeeping every operation shares: issuing effects, checking the
//! outcomes that answer them, collecting notices.

use std::collections::{BTreeMap, VecDeque};

use super::protocol::{
    Effect, EffectId, Next, Notice, Outcome, ProtocolError, Request, RequestKind, Step,
};

/// An operation's side of the protocol.
#[derive(Debug)]
pub(crate) struct Io {
    next_id: EffectId,
    outstanding: BTreeMap<EffectId, RequestKind>,
    issued: Vec<Effect>,
    notices: Vec<Notice>,
    inbox: VecDeque<(EffectId, Outcome)>,
}

impl Io {
    pub(crate) fn new() -> Self {
        Self {
            next_id: EffectId::first(),
            outstanding: BTreeMap::new(),
            issued: Vec::new(),
            notices: Vec::new(),
            inbox: VecDeque::new(),
        }
    }

    /// Asks the driver for `request`; the effect goes out on the next step.
    pub(crate) fn issue(&mut self, request: Request) -> EffectId {
        let id = self.next_id;
        self.next_id = id.next();
        self.outstanding.insert(id, request.kind());
        self.issued.push(Effect { id, request });
        id
    }

    /// Emits `notice` on the next step.
    pub(crate) fn notify(&mut self, notice: Notice) {
        self.notices.push(notice);
    }

    /// Accepts the driver's answer to `id`, once and only if it answers the
    /// effect's request.
    pub(crate) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        let kind = self
            .outstanding
            .get(&id)
            .copied()
            .ok_or(ProtocolError::UnknownEffect(id))?;
        kind.check(id, &outcome)?;
        self.outstanding.remove(&id);
        self.inbox.push_back((id, outcome));
        Ok(())
    }

    /// The next answer to process, in completion order.
    pub(crate) fn next_outcome(&mut self) -> Option<(EffectId, Outcome)> {
        self.inbox.pop_front()
    }

    /// The step the driver receives: the notices so far, then the effects
    /// issued, a wait, or the operation's result.
    pub(crate) fn step<T>(&mut self, result: Option<T>) -> Step<T> {
        let notices = std::mem::take(&mut self.notices);
        let next = match result {
            Some(result) => Next::Ready(result),
            None if !self.issued.is_empty() => Next::Effects(std::mem::take(&mut self.issued)),
            None => Next::Waiting,
        };
        Step { notices, next }
    }
}
