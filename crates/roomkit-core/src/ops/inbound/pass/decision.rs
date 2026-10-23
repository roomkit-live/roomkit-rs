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
use crate::model::{ChannelBinding, RoomContext, RoomEvent};
use crate::ops::protocol::{Outcome, Request};
use crate::ops::{Abort, Cx};
use crate::rules::edits::EditRefusal;
use crate::rules::hook_chain::{ChainDecision, ChainVerdict};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::permissions::{self, DeliveryPlan, WriteRefusal};

use super::super::chain::HookRun;

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
        if let Some(effect) = run.ask_next(cx, &self.event, &context, self.deadline, locked) {
            return State::RunningHook {
                effect,
                run,
                context,
            };
        }
        let unchanged = if run.asked() { None } else { Some(context) };
        self.conclude_chain(cx, run.into_chain().finish(), unchanged)
    }

    pub(super) fn on_hook(
        &mut self,
        cx: &mut Cx<'_>,
        mut run: Box<HookRun>,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        run.apply(cx, outcome);
        self.next_hook(cx, run, context)
    }

    /// The chain is over: its side effects are kept, and the room is read
    /// again before its decision applies when a hook ran under the lock
    /// (gap G-070); `unchanged` is the room as the gate read it otherwise.
    fn conclude_chain(
        &mut self,
        cx: &mut Cx<'_>,
        verdict: ChainVerdict<RoomEvent>,
        unchanged: Option<Box<RoomContext>>,
    ) -> State {
        super::super::report::hook_incidents(cx, &self.event, &verdict.incidents);
        self.injected_events.extend(verdict.injected_events);
        self.tasks.extend(verdict.tasks);
        self.observations.extend(verdict.observations);
        let payload = stamp::keep_identity(&self.event, verdict.payload);
        let blocked = match verdict.decision {
            ChainDecision::Blocked { by, reason } => Some((by, reason)),
            ChainDecision::Allowed => None,
        };
        let verdict = Verdict { payload, blocked };
        match unchanged {
            Some(context) => self.apply(cx, verdict, context, false),
            None => State::Rereading {
                effect: self.load_context(cx),
                verdict: Box::new(verdict),
            },
        }
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
    /// accepts any; otherwise the event takes the room's next index and is
    /// committed, blocked or delivered (RFC §10.1 steps 10 to 12). An
    /// accepted edit or delete reads its target again first when the room
    /// was read again.
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
        self.event.index = context.room.event_count;
        let Verdict {
            mut payload,
            blocked,
        } = verdict;
        payload.index = self.event.index;
        match blocked {
            Some((by, reason)) => {
                // A blocked record is stored as step 12 would have stored it
                // when its source is bound (gap G-036).
                payload.visibility = self.stored_visibility(&payload, &context.bindings);
                let blocked = commit::blocked(cx, payload, by, reason, &context);
                self.committing(cx, blocked)
            }
            None => match self.accepted.as_ref().filter(|_| reread) {
                Some(accepted) => {
                    let event_id = accepted.target_id().clone();
                    State::RereadingTarget {
                        effect: self.issue(cx, Request::GetEvent { event_id }),
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
                let delivered = commit::delivered(cx, event, plan, context, written);
                self.committing(cx, delivered)
            }
            Err(refusal) => {
                let blocked_by = refusal.blocked_by().to_owned();
                event.visibility = self.stored_visibility(&event, &context.bindings);
                let blocked =
                    commit::blocked(cx, event, blocked_by.clone(), Some(blocked_by), &context);
                self.committing(cx, blocked)
            }
        }
    }

    /// The visibility a record the pass stores blocked keeps: what step 12
    /// would have given it, its own intersected with its source binding's,
    /// when its source is bound; its own otherwise, and for an event a hook
    /// injected, which is no channel's (gap G-036).
    fn stored_visibility(
        &self,
        event: &RoomEvent,
        bindings: &[ChannelBinding],
    ) -> crate::model::Visibility {
        if matches!(self.origin, PassOrigin::Injected { .. }) {
            return event.visibility.clone();
        }
        commit::stored_visibility(event, bindings)
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
        // check and reaches its transport targets only (gap G-021).
        if let PassOrigin::Injected { targets } = &self.origin {
            let mut plan = permissions::plan_targets(event, targets.as_deref(), bindings);
            if let Some(accepted) = &self.accepted {
                accepted.narrow(event, bindings);
                accepted.narrow_plan(&mut plan);
            }
            return Ok(plan);
        }
        let source = bindings
            .iter()
            .find(|binding| binding.channel_id == event.source.channel_id)
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
    /// nothing, releases the lock at once (RFC §10.1.1 rule 5).
    fn committing(&mut self, cx: &mut Cx<'_>, committing: Committing) -> State {
        match committing.effect {
            Some(effect) => State::Committing {
                effect,
                result: committing.result,
            },
            None => self.release(cx, Ok(committing.result)),
        }
    }

    pub(super) fn on_committed(
        &mut self,
        cx: &mut Cx<'_>,
        result: PassResult,
        outcome: Outcome,
    ) -> State {
        match outcome.into_committed() {
            Ok(()) => self.release(cx, Ok(result)),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }
}
