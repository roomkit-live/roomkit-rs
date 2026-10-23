//! A pass's gates before its hooks (RFC §10.1 steps 6 to 8a): the room as
//! the lock finds it, its due timers applied, its status, the idempotency
//! key, the provisional index, and an edit's or a delete's target.

use super::{CommitPass, PassResult, State};
use crate::model::RoomContext;
use crate::ops::lifecycle::gate::{self, Gate};
use crate::ops::protocol::{Outcome, Request};
use crate::ops::{Abort, Cx};

impl CommitPass {
    pub(super) fn on_locked(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
        match outcome.into_locked() {
            Ok(()) => State::LoadingContext(self.load_context(cx)),
            Err(error) => State::Finished(Err(Abort::Lock(error))),
        }
    }

    /// Reads the room with its bindings, participants and recent events.
    pub(super) fn load_context(&self, cx: &mut Cx<'_>) -> crate::ops::protocol::EffectId {
        let request = Request::LoadContext {
            room_id: self.event.room_id.clone(),
            recent_events: cx.config.context_window,
        };
        self.issue(cx, request)
    }

    /// The room as the lock found it goes through the timer gate first (gap
    /// G-045).
    pub(super) fn on_context(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return self.release(cx, Err(Abort::RoomNotFound)),
            Err(error) => return self.release(cx, Err(Abort::Store(error))),
        };
        match gate::open(cx, context) {
            Gate::Clear(context) => self.on_gate(cx, context),
            Gate::Writing { effect, pending } => State::ApplyingTimers { effect, pending },
        }
    }

    /// The room's status gate, on the room as its due timers left it, then
    /// the idempotency check of an entry's key (RFC §13.4, gap G-023), step
    /// 8a or the hooks (RFC §10.1 steps 6 to 8). The index given here is
    /// the one the hooks see; the pass takes it again once they are over.
    pub(super) fn on_gate(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        let status = context.room.status;
        if !status.accepts_events() {
            super::super::report::room_refused(cx, &self.event, &self.origin, status);
            return self.release(cx, Ok(PassResult::Refused { status }));
        }
        self.event.index = context.room.event_count;
        match self.event.idempotency_key.clone() {
            Some(key) if self.origin.is_entry() => State::CheckingKey {
                effect: self.issue(
                    cx,
                    Request::FindByIdempotencyKey {
                        room_id: self.event.room_id.clone(),
                        channel_id: self.event.source.channel_id.clone(),
                        key,
                    },
                ),
                context,
            },
            _ => self.check_edit(cx, context),
        }
    }

    pub(super) fn on_key(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        match outcome.into_idempotent() {
            Ok(Some(event)) => self.release(cx, Ok(PassResult::Duplicate { event })),
            Ok(None) => self.check_edit(cx, context),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// Step 8a: an edit or a delete reads its target, under the lock; any
    /// other event goes to the hooks (RFC §10.1 step 8a).
    fn check_edit(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        match super::super::edits::target_of(&self.event) {
            Ok(None) => self.start_hooks(cx, context),
            Ok(Some(event_id)) => State::ReadingTarget {
                effect: self.issue(cx, Request::GetEvent { event_id }),
                context,
            },
            Err(refusal) => self.refuse_edit(cx, refusal),
        }
    }

    /// The target is read: an accepted edit or delete goes to the hooks, a
    /// refused one ends the pass with nothing stored (RFC §10.1 step 8a).
    pub(super) fn on_target(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        let target = match outcome.into_event() {
            Ok(target) => target.map(|target| *target),
            Err(error) => return self.release(cx, Err(Abort::Store(error))),
        };
        let checked =
            super::super::edits::check(&self.event, &self.origin, target, &context.participants);
        match checked {
            Ok(accepted) => {
                self.accepted = Some(accepted);
                self.start_hooks(cx, context)
            }
            Err(refusal) => self.refuse_edit(cx, refusal),
        }
    }
}
