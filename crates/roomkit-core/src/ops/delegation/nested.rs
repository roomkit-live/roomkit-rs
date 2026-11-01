//! An operation run inside another, its effects renamed with the outer
//! operation's ids so that the two never share one.

use std::collections::BTreeMap;

use crate::ops::Environment;
use crate::ops::protocol::{Effect, EffectId, Next, Notice, Operation, Outcome, ProtocolError};

/// An inner operation and the outer ids of its outstanding effects.
#[derive(Debug)]
pub(super) struct Nested<O> {
    op: O,
    /// The inner id of each outstanding effect, by its outer id.
    ids: BTreeMap<EffectId, EffectId>,
}

impl<O: Operation> Nested<O> {
    pub(super) fn new(op: O) -> Self {
        Self {
            op,
            ids: BTreeMap::new(),
        }
    }

    /// Polls the inner operation: its effects, renamed with ids taken from
    /// `next`, go to `effects`, its notices to `notices`; its output once
    /// it is over.
    pub(super) fn poll(
        &mut self,
        environment: &dyn Environment,
        next: &mut EffectId,
        effects: &mut Vec<Effect>,
        notices: &mut Vec<Notice>,
    ) -> Option<O::Output> {
        let step = self.op.poll(environment);
        notices.extend(step.notices);
        match step.next {
            Next::Effects(own) => {
                for mut effect in own {
                    let outer = *next;
                    *next = outer.next();
                    self.ids.insert(outer, effect.id);
                    effect.id = outer;
                    effects.push(effect);
                }
                None
            }
            Next::Waiting => None,
            Next::Ready(output) => Some(output),
        }
    }

    /// Whether `id` names one of the inner operation's effects.
    pub(super) fn owns(&self, id: EffectId) -> bool {
        self.ids.contains_key(&id)
    }

    /// Hands the inner operation the answer to its effect `id`.
    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        let inner = self
            .ids
            .remove(&id)
            .ok_or(ProtocolError::UnknownEffect(id))?;
        self.op.complete(inner, outcome)
    }
}
