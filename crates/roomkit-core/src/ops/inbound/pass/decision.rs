//! A pass's hooks and what follows them (RFC §10.1 steps 9 to 12): the
//! `BEFORE_BROADCAST` chain, the room read again as the hooks left it, the
//! next index, the source check, the commit and its broadcast plan.
//!
//! A hook runs under the room lock, and code it runs may take the lock
//! again (RFC §13.5): commit an event ahead of this one, close the room,
//! mute the source. So once a chain that ran a hook under the lock is over,
//! the pass reads the room again (gap G-070); with no such hook, the room
//! is as the gate read it, the lock held since. A room that no longer
//! accepts events refuses the event, as step 6 would have, with nothing
//! stored. Otherwise the event takes the room's next index, which its hooks
//! saw provisionally, and its source is checked against the bindings as the
//! hooks left them (RFC §7.5 rule 2).

use super::super::commit::{self, Committing};
use super::super::stamp;
use super::{CommitPass, PassOrigin, PassResult, State, Verdict};
use crate::model::{ChannelBinding, ChannelId, RoomContext, RoomEvent, Visibility};
use crate::ops::protocol::{HoldRequest, Outcome, StoreRequest, TargetUpdate};
use crate::ops::rooting::{Rooted, Rooting};
use crate::ops::{Abort, Cx};
use crate::rules::authors;
use crate::rules::edits::EditRefusal;
use crate::rules::hook_chain::{ChainDecision, ChainVerdict};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::permissions::{self, DeliveryPlan, WriteRefusal};

use super::super::chain::HookRun;

/// An event the pass decided to deliver, before its author is recorded.
#[derive(Debug)]
pub(super) struct Unrecorded {
    event: RoomEvent,
    plan: DeliveryPlan,
    context: Box<RoomContext>,
    written: Option<(TargetUpdate, RoomEvent)>,
}

impl CommitPass {
    /// Step 9: the chain the off-lock check left goes on with the hooks
    /// that need the lock, its outcome applied first (RFC §9.5.1); with no
    /// check, the whole chain runs here.
    pub(super) fn start_hooks(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        let source = &self.event.source;
        let run = match self.checked.take() {
            Some(chain) => HookRun::new(cx.config.hooks.locked_chain(source), *chain),
            None => HookRun::of(
                cx.config.hooks.chain(HookTrigger::BeforeBroadcast, source),
                &self.event,
            ),
        };
        self.next_hook(cx, Box::new(run), context)
    }

    /// Runs the next hook of the chain under the room lock, or concludes it
    /// (RFC §10.1 step 9).
    fn next_hook(
        &mut self,
        cx: &mut Cx<'_>,
        mut run: Box<HookRun>,
        context: Box<RoomContext>,
    ) -> State {
        let locked = Some(&self.event.room_id);
        if let Some(effect) = run.ask_next(cx, &self.event, &context, self.bound, locked) {
            return State::RunningHook {
                effect,
                run,
                context,
            };
        }
        let asked = run.asked();
        self.conclude_chain(cx, run.into_chain().finish(), context, asked)
    }

    pub(super) fn on_hook(
        &mut self,
        cx: &mut Cx<'_>,
        mut run: Box<HookRun>,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        run.apply(cx, outcome, &context.bindings);
        self.next_hook(cx, run, context)
    }

    /// The chain is over: its side effects are kept, the hooks registered
    /// ASYNC on the trigger observe the event it allowed, as the SYNC hooks
    /// left it (RFC §9.2, §9.3), and the room is read again before its
    /// decision applies when a hook ran under the lock, `asked` (gap G-070);
    /// `context`, the room the hooks were handed, is as the gate read it
    /// otherwise.
    fn conclude_chain(
        &mut self,
        cx: &mut Cx<'_>,
        verdict: ChainVerdict<RoomEvent>,
        context: Box<RoomContext>,
        asked: bool,
    ) -> State {
        super::super::report::hook_incidents(cx, &self.event, &verdict.incidents);
        self.injected_events.extend(verdict.injected_events);
        self.tasks.extend(verdict.tasks);
        self.observations.extend(verdict.observations);
        let payload = stamp::keep_identity(&self.event, verdict.payload, &context.bindings);
        let blocked = match verdict.decision {
            ChainDecision::Blocked { by, reason } => Some((by, reason)),
            ChainDecision::Allowed => None,
        };
        if blocked.is_none() {
            Self::observe(cx, &payload, &context);
        }
        let unchanged = (!asked).then_some(context);
        let verdict = Verdict { payload, blocked };
        match unchanged {
            Some(context) => self.apply(cx, verdict, context, false),
            None => State::Rereading {
                effect: self.load_context(cx),
                verdict: Box::new(verdict),
            },
        }
    }

    /// The ASYNC hooks of `BEFORE_BROADCAST` observe `payload`, the event
    /// its chain allowed, as the work of the cascade the pass works for
    /// while they run (RFC §9.2, §9.5.1; decided in step 3 milestone 2): a
    /// call a body makes into the room draws one of the cascade's hook
    /// calls, refused past them, as an `AFTER_BROADCAST` body's does, so an
    /// observer that posts on every event it observes stops at the cap.
    fn observe(cx: &mut Cx<'_>, payload: &RoomEvent, context: &RoomContext) {
        let trigger = HookTrigger::BeforeBroadcast;
        // A pass always works for a cascade; none lent, the bodies are work
        // of their own.
        let Some(budget) = cx.io.work().cloned() else {
            return cx.fire_observers(trigger, payload, context);
        };
        cx.fire_as_cascade_work(trigger, payload, context, &budget, 0);
    }

    /// The room read again, as the hooks left it.
    pub(super) fn on_reread(
        &mut self,
        cx: &mut Cx<'_>,
        verdict: Verdict,
        outcome: Outcome,
    ) -> State {
        match outcome.into_context() {
            Ok(Some(context)) => self.apply(cx, verdict, context, true),
            Ok(None) => self.release(cx, Err(Abort::RoomNotFound)),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// The target of an edit or a delete read again, as the hooks left it:
    /// step 8a checks it once more, so a change committed ahead of this one
    /// (a delete of the target, nested in a hook) is never undone by it
    /// (RFC §10.1 step 8a, gap G-070).
    pub(super) fn on_target_reread(
        &mut self,
        cx: &mut Cx<'_>,
        payload: RoomEvent,
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
                self.decide(cx, payload, context)
            }
            Err(refusal) => self.refuse_edit(cx, refusal),
        }
    }

    /// The hooks' decision on the room as they left it, `reread` when the
    /// pass read it again: its status refuses the event when it no longer
    /// accepts any; otherwise a thread parent a hook set is rooted first
    /// (RFC §10.1 step 9), then the verdict applies.
    fn apply(
        &mut self,
        cx: &mut Cx<'_>,
        verdict: Verdict,
        context: Box<RoomContext>,
        reread: bool,
    ) -> State {
        let status = context.room.status;
        if !status.accepts_events() {
            super::super::report::room_refused(cx, &self.event, &self.origin, status);
            return self.release(cx, Ok(PassResult::Refused { status }));
        }
        let set_by_a_hook = verdict
            .payload
            .parent_event_id
            .clone()
            .filter(|parent| self.event.parent_event_id.as_ref() != Some(parent));
        let rooting = set_by_a_hook.and_then(|_| Rooting::of(&verdict.payload));
        match rooting {
            Some((rooting, request)) => State::RootingHookParent {
                effect: self.issue(cx, request),
                verdict: Box::new(verdict),
                context,
                reread,
                rooting: Box::new(rooting),
            },
            None => self.apply_verdict(cx, verdict, context, reread),
        }
    }

    /// A read of the rooting of the thread parent a hook set is answered:
    /// the next one goes out, or the event keeps its thread's root, or
    /// none, as at step 8 (RFC §10.1 step 9).
    pub(super) fn on_hook_parent(
        &mut self,
        cx: &mut Cx<'_>,
        mut verdict: Verdict,
        context: Box<RoomContext>,
        reread: bool,
        mut rooting: Box<Rooting>,
        outcome: Outcome,
    ) -> State {
        match rooting.on_read(outcome) {
            Ok(Rooted::Read(request)) => State::RootingHookParent {
                effect: self.issue(cx, *request),
                verdict: Box::new(verdict),
                context,
                reread,
                rooting,
            },
            Ok(Rooted::Parent(parent)) => {
                verdict.payload.parent_event_id = parent;
                self.apply_verdict(cx, verdict, context, reread)
            }
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// The verdict on a room that accepts events: the event takes the
    /// room's next index and is committed, blocked or delivered (RFC §10.1
    /// steps 10 to 12). An accepted edit or delete reads its target again
    /// first when the room was read again.
    fn apply_verdict(
        &mut self,
        cx: &mut Cx<'_>,
        verdict: Verdict,
        context: Box<RoomContext>,
        reread: bool,
    ) -> State {
        self.event.index = context.room.event_count;
        let Verdict {
            mut payload,
            blocked,
        } = verdict;
        payload.index = self.event.index;
        let mut payload = self.within_its_scope(payload, &context);
        match blocked {
            Some((by, reason)) => {
                // A blocked record is stored as step 12 would have stored it
                // when its source is bound (gap G-036).
                payload.visibility = self.stored_visibility(&payload, &context);
                let blocked = commit::blocked(cx, payload, by, reason, &context);
                self.committing(cx, blocked)
            }
            None => match self.accepted.as_ref().filter(|_| reread) {
                Some(accepted) => {
                    let event_id = accepted.target_id().clone();
                    State::RereadingTarget {
                        effect: self.issue(cx, StoreRequest::GetEvent { event_id }),
                        payload: Box::new(payload),
                        context,
                    }
                }
                None => self.decide(cx, payload, context),
            },
        }
    }

    /// The source check, then the commit and its broadcast plan (RFC §10.1
    /// steps 11 and 12).
    fn decide(
        &mut self,
        cx: &mut Cx<'_>,
        mut event: RoomEvent,
        context: Box<RoomContext>,
    ) -> State {
        match self.plan(cx, &mut event, &context) {
            Ok(plan) => {
                let written = match self.accepted.take().map(|accepted| accepted.write(&event)) {
                    Some(None) => return self.refuse_edit(cx, EditRefusal::TargetNotFound),
                    Some(Some(written)) => Some(written),
                    None => None,
                };
                let unrecorded = Unrecorded {
                    event,
                    plan,
                    context,
                    written,
                };
                self.record_author(cx, unrecorded, None)
            }
            Err(refusal) => {
                let blocked_by = refusal.blocked_by().to_owned();
                event.visibility = self.stored_visibility(&event, &context);
                let blocked =
                    commit::blocked(cx, event, blocked_by.clone(), Some(blocked_by), &context);
                self.committing(cx, blocked)
            }
        }
    }

    /// Records the event's author, then commits it (RFC §10.1 step 12, gap
    /// G-089); a room that holds no register, and whose timeline its
    /// context's window does not hold whole, has its timeline read first,
    /// under the lock, to rebuild one.
    fn record_author(
        &mut self,
        cx: &mut Cx<'_>,
        unrecorded: Unrecorded,
        timeline: Option<&[RoomEvent]>,
    ) -> State {
        let Unrecorded {
            mut event,
            plan,
            context,
            written,
        } = unrecorded;
        if let Ok(register) = authors::with_author(&mut event, &context, timeline) {
            let delivered = commit::delivered(cx, event, plan, context, written, register);
            return self.committing(cx, delivered);
        }
        // The room holds no register, and its window not its timeline whole.
        let room_id = event.room_id.clone();
        let request = StoreRequest::ListEvents {
            room_id: room_id.clone(),
        };
        State::ReadingTimeline {
            effect: cx.io.issue_locked(request, room_id),
            pending: Box::new(Unrecorded {
                event,
                plan,
                context,
                written,
            }),
        }
    }

    /// The room's timeline is read: the author is recorded with the register
    /// it rebuilds.
    pub(super) fn on_timeline(
        &mut self,
        cx: &mut Cx<'_>,
        unrecorded: Unrecorded,
        outcome: Outcome,
    ) -> State {
        match outcome.into_events() {
            Ok(timeline) => self.record_author(cx, unrecorded, Some(&timeline)),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// `event`, a response as its hooks left it, back within where the
    /// answers of the message its cascade derives from may go (RFC §5.12,
    /// gap G-073): a hook may change its visibility as on any event, never
    /// past that scope. Any other pass's event is left as it is.
    fn within_its_scope(&self, event: RoomEvent, context: &RoomContext) -> RoomEvent {
        match &self.origin {
            PassOrigin::Response { answers_within } => {
                stamp::answer_within(event, answers_within.as_ref(), &context.bindings)
            }
            PassOrigin::Trigger | PassOrigin::Adopted(_) | PassOrigin::Injected { .. } => event,
        }
    }

    /// The visibility a record the pass stores blocked keeps: what step 12
    /// would have given it, its own intersected with its source binding's,
    /// when its source is bound; its own otherwise. An event a hook
    /// injected is no channel's: it keeps the targets the room binds, as
    /// its hooks narrowed them (gaps G-031, G-036).
    fn stored_visibility(&self, event: &RoomEvent, context: &RoomContext) -> Visibility {
        match &self.origin {
            PassOrigin::Injected { targets } => {
                self.injected_visibility(event, targets.as_deref(), &context.bindings)
            }
            PassOrigin::Trigger | PassOrigin::Adopted(_) | PassOrigin::Response { .. } => {
                commit::stored_visibility(event, context)
            }
        }
    }

    /// The visibility of `event`, a hook's injected event for `targets`, as
    /// its hooks left it: within the targets the room bound when they were
    /// given it, and binds now (RFC §9.4, gap G-031).
    fn injected_visibility(
        &self,
        event: &RoomEvent,
        targets: Option<&[ChannelId]>,
        bindings: &[ChannelBinding],
    ) -> Visibility {
        permissions::injected_visibility(
            &event.visibility,
            &self.event.visibility,
            targets,
            bindings,
        )
    }

    /// Whom the event reaches, once its source may write; the event is
    /// committed with the visibility its source binding allows (RFC §7.5
    /// rules 2 and 4, gap G-009), within its target's for an edit or a
    /// delete (gap G-062).
    fn plan(
        &self,
        cx: &Cx<'_>,
        event: &mut RoomEvent,
        context: &RoomContext,
    ) -> Result<DeliveryPlan, WriteRefusal> {
        let bindings = &context.bindings;
        // An injected event is the hook's, not a channel's: it takes no source
        // check and reaches the transport targets its visibility admits only
        // (gaps G-021, G-031).
        if let PassOrigin::Injected { targets } = &self.origin {
            event.visibility = self.injected_visibility(event, targets.as_deref(), bindings);
            if let Some(accepted) = &self.accepted {
                accepted.narrow(event, bindings);
            }
            return Ok(permissions::plan_targets(
                event,
                targets.as_deref(),
                bindings,
            ));
        }
        let source = context
            .binding(&event.source.channel_id)
            .ok_or(WriteRefusal::Unbound)?;
        if let Some(refusal) = permissions::write_refusal(source) {
            return Err(refusal);
        }
        event.visibility = permissions::effective_visibility(&event.visibility, source, bindings);
        if let Some(accepted) = &self.accepted {
            accepted.narrow(event, bindings);
        }
        Ok(permissions::plan_delivery(
            event,
            bindings,
            context.room.agent_response_policy,
            cx.config.max_chain_depth,
        ))
    }

    /// Waits for the commit to be durable; an instruction, which writes
    /// nothing, goes on at once (RFC §10.1.1 rule 5).
    fn committing(&mut self, cx: &mut Cx<'_>, committing: Committing) -> State {
        match committing.effect {
            Some(effect) => State::Committing {
                effect,
                result: committing.result,
            },
            None => self.place(cx, committing.result),
        }
    }

    pub(super) fn on_committed(
        &mut self,
        cx: &mut Cx<'_>,
        result: PassResult,
        outcome: Outcome,
    ) -> State {
        match outcome.into_committed() {
            Ok(()) => self.place(cx, result),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// An event delivered, committed or, for an instruction, decided, takes
    /// its place in the room's delivery lane before the lock is released
    /// (RFC §10.1 step 12), with no deadline: it is the room's already. A
    /// record stored blocked has no delivery set, and no place.
    fn place(&mut self, cx: &mut Cx<'_>, result: PassResult) -> State {
        let PassResult::Delivered { event, .. } = &result else {
            return self.release(cx, Ok(result));
        };
        let room_id = event.room_id.clone();
        let request = HoldRequest::EnterLane {
            room_id: room_id.clone(),
            event_id: event.id.clone(),
        };
        State::Placing {
            effect: cx.io.issue_locked(request, room_id),
            result,
        }
    }

    /// The event's place is taken. A place that could not be taken is
    /// logged, and the event delivered all the same, out of the lane's
    /// order (RFC §13.6: an event stored is delivered).
    pub(super) fn on_placed(
        &mut self,
        cx: &mut Cx<'_>,
        result: PassResult,
        outcome: Outcome,
    ) -> State {
        match outcome.into_entered() {
            Ok(()) => self.placed = true,
            Err(error) => tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = %self.event.room_id,
                event_id = %self.event.id,
                error_type = %error.error_type,
                message = %error.message,
                "taking the event's place in the room's delivery lane failed; it is delivered \
                 all the same"
            ),
        }
        self.release(cx, Ok(result))
    }
}
