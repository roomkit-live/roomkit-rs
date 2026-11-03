//! A pass's gates before its hooks (RFC §10.1 steps 6 to 8a): the room as
//! the lock finds it, its due timers applied, its status, the idempotency
//! key, the provisional index and the event's thread root, and an edit's
//! or a delete's target.

use std::collections::VecDeque;

use super::{CommitPass, PassOrigin, PassResult, State};
use crate::model::RoomContext;
use crate::ops::inbound::direct::DirectCall;
use crate::ops::inbound::{greeting, identity};
use crate::ops::lifecycle::gate::{self, Gate};
use crate::ops::lifecycle::record;
use crate::ops::protocol::{KeyHeld, Outcome, StoreRequest};
use crate::ops::rooting::{Rooted, Rooting};
use crate::ops::{Abort, Cx};
use crate::rules::{permissions, publications};

impl CommitPass {
    pub(super) fn on_locked(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
        match outcome.into_locked() {
            Ok(()) => State::LoadingContext(self.load_context(cx)),
            Err(error) => State::Finished(Err(Abort::Lock(error))),
        }
    }

    /// Reads the room with its bindings, participants and recent events.
    pub(super) fn load_context(&self, cx: &mut Cx<'_>) -> crate::ops::protocol::EffectId {
        let request = StoreRequest::LoadContext {
            room_id: self.event.room_id.clone(),
            recent_events: cx.config.recent_events(),
        };
        self.issue(cx, request)
    }

    /// The room as the lock found it goes through the timer gate first (gap
    /// G-045), unless it is not the caller's: a direct injection into
    /// another organization's room is refused before its due timers are
    /// applied, nothing written in it (RFC §17.2, gap G-069).
    pub(super) fn on_context(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return self.release(cx, Err(Abort::RoomNotFound)),
            Err(error) => return self.release(cx, Err(Abort::Store(error))),
        };
        let foreign = self
            .direct
            .as_ref()
            .and_then(|direct| direct.foreign(&context.room));
        if let Some(refusal) = foreign {
            return self.release(cx, Err(Abort::Refused(refusal)));
        }
        match gate::open(cx, context) {
            Gate::Clear(context) => self.on_gate(cx, context),
            Gate::Writing { effect, pending } => State::ApplyingTimers { effect, pending },
        }
    }

    /// The room's status gate, on the room as its due timers left it, then
    /// the idempotency check of an entry's key (RFC §13.4, gap G-023), step
    /// 8a or the hooks (RFC §10.1 steps 6 to 8). The index given here is
    /// the one the hooks see; the pass takes it again once they are over.
    /// An event a hook injected is given the visibility of the targets the
    /// room binds, which its own hooks may narrow (gap G-031). A direct
    /// injection into a room that binds no channel it names is refused
    /// first, nothing written (gap G-069).
    pub(super) fn on_gate(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        let refusal = self
            .direct
            .as_ref()
            .and_then(|_| DirectCall::unbound(&self.event, &context));
        if let Some(refusal) = refusal {
            return self.release(cx, Err(Abort::Refused(refusal)));
        }
        let status = context.room.status;
        if !status.accepts_events() {
            if self.greeting {
                greeting::log_refused(&self.event);
            } else {
                super::super::report::room_refused(cx, &self.event, &self.origin, status);
            }
            return self.release(cx, Ok(PassResult::Refused { status }));
        }
        let mut context = context;
        if let Some(participation) = self.participation.take() {
            let writes = identity::writes(cx, *participation, &self.event, &mut context);
            return self.write_participant(cx, context, writes);
        }
        self.after_status(cx, context)
    }

    /// Writes the next of `writes`, the sender's record as identity
    /// resolution left them (RFC §10.1 step 5), under the lock and with no
    /// deadline, as every write; once all are done, the gate goes on.
    fn write_participant(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        mut writes: VecDeque<StoreRequest>,
    ) -> State {
        match writes.pop_front() {
            Some(write) => State::Participating {
                effect: record::issue_write(cx, write),
                context,
                writes,
            },
            None => self.after_status(cx, context),
        }
    }

    /// A write of the sender's record is answered: the next one goes out;
    /// a store that fails it fails the event, nothing of it committed.
    pub(super) fn on_participant_written(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        writes: VecDeque<StoreRequest>,
        outcome: Outcome,
    ) -> State {
        match outcome.into_written() {
            Ok(()) => self.write_participant(cx, context, writes),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// The gate past the room's status: the provisional index, an injected
    /// event's visibility, then the idempotency check of an entry's key or
    /// step 8.
    fn after_status(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        self.event.index = context.room.event_count;
        if let PassOrigin::Injected { targets } = &self.origin {
            self.event.visibility =
                permissions::targets_visibility(targets.as_deref(), &context.bindings);
        }
        match self.event.idempotency_key.clone() {
            Some(key) if self.origin.is_entry() => State::CheckingKey {
                effect: self.issue(
                    cx,
                    StoreRequest::GetEventByIdempotencyKey {
                        room_id: self.event.room_id.clone(),
                        // A publication's key within the room, whatever
                        // channel committed it (RFC §22.1).
                        channel_id: publications::lookup_channel(
                            &key,
                            &self.event.source.channel_id,
                        ),
                        key,
                    },
                ),
                context,
            },
            _ => self.root_parent(cx, context),
        }
    }

    pub(super) fn on_key(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        match outcome.into_idempotent() {
            Ok(KeyHeld::Event(event)) => {
                self.release(cx, Ok(PassResult::Duplicate { event: Some(event) }))
            }
            // A key held whose event the store cannot read back: a message
            // is a blocked duplicate (RFC §13.4); a direct injection gets
            // back the event it injected, as Python's `send_event` returns
            // it. Neither is processed again.
            Ok(KeyHeld::Seen) => {
                let event = self.direct.is_some().then(|| Box::new(self.event.clone()));
                self.release(cx, Ok(PassResult::Duplicate { event }))
            }
            Ok(KeyHeld::Free) => self.root_parent(cx, context),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// Step 8: the event's thread is rooted, whatever its origin: the
    /// event its parent names is read under the lock, then the root a
    /// reply names (RFC §10.1 step 8, `ops::rooting`).
    fn root_parent(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        match Rooting::of(&self.event) {
            Some((rooting, request)) => State::RootingParent {
                effect: self.issue(cx, request),
                context,
                rooting: Box::new(rooting),
            },
            None => self.check_edit(cx, context),
        }
    }

    /// A read of the parent's rooting is answered: the next one goes out,
    /// or the event keeps its thread's root, or no parent at all when the
    /// room holds no such event, then goes to step 8a.
    pub(super) fn on_parent(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        mut rooting: Box<Rooting>,
        outcome: Outcome,
    ) -> State {
        match rooting.on_read(outcome) {
            Ok(Rooted::Read(request)) => State::RootingParent {
                effect: self.issue(cx, *request),
                context,
                rooting,
            },
            Ok(Rooted::Parent(parent)) => {
                self.event.parent_event_id = parent;
                self.check_edit(cx, context)
            }
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// Step 8a: an edit or a delete reads its target, under the lock; any
    /// other event goes to the hooks (RFC §10.1 step 8a).
    fn check_edit(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        match super::super::edits::target_of(&self.event) {
            Ok(None) => self.start_hooks(cx, context),
            Ok(Some(event_id)) => State::ReadingTarget {
                effect: self.issue(cx, StoreRequest::GetEvent { event_id }),
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
