//! One event's way through the locked section of the pipeline (RFC §10.1
//! steps 6 to 13).
//!
//! Under the room's lock, a pass applies the room's due timer transitions
//! (gap G-045), checks the room still accepts events, answers a duplicate,
//! assigns the index, checks an edit or a delete against its target (step
//! 8a), runs the `BEFORE_BROADCAST` chain, checks the source may write,
//! commits the event with the room's counters, resuming a paused room and
//! updating an edit's or a delete's target, and plans its broadcast. Every
//! path that took the lock releases it.

use std::collections::VecDeque;
use std::mem;

use super::commit::{self, Committing};
use super::edits::{self, Accepted};
use super::stamp;
use crate::model::{
    ChannelBinding, ChannelId, ErrorRecord, InjectedEvent, Observation, RoomContext, RoomEvent,
    RoomStatus, Task,
};
use crate::ops::lifecycle::gate::{self, Gate, Pending};
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::ops::{self, Abort, Cx, FrameworkEvent, Notice, Progress, RefusedOperation};
use crate::rules::edits::EditRefusal;
use crate::rules::hook_chain::{ChainDecision, ChainVerdict, HookChain};
use crate::rules::hook_registry::{HookId, HookTrigger};
use crate::rules::permissions::{self, DeliveryPlan, WriteRefusal};

/// Where the event of a pass comes from.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum PassOrigin {
    /// The inbound message itself.
    Trigger,
    /// A channel's response, re-entering (RFC §10.1 step 14).
    Response,
    /// An event a hook injected, delivered to its targets only (gap G-021).
    Injected {
        /// The channels it is delivered to; `None` stores it only.
        targets: Option<Vec<ChannelId>>,
    },
}

/// What a pass did with its event.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum PassResult {
    /// Committed delivered, with its broadcast planned.
    Delivered {
        /// The committed event.
        event: Box<RoomEvent>,
        /// Whom its broadcast reaches.
        plan: DeliveryPlan,
        /// The room's context at the commit, bindings as the hooks left them
        /// and an edit's or a delete's target as the commit left it.
        context: Box<RoomContext>,
        /// The target of an edit or a delete, as the commit left it (RFC
        /// §10.3), for its mutation trigger.
        target: Option<Box<RoomEvent>>,
    },
    /// Committed blocked, by a hook or by a source that cannot write.
    Blocked {
        /// The committed record, with its `blocked_by`.
        event: Box<RoomEvent>,
        /// Why it was blocked.
        reason: Option<String>,
        /// The room's context its hooks read.
        context: Box<RoomContext>,
    },
    /// Refused by the room's status: nothing was stored (RFC §5.1).
    Refused {
        /// The status that refused it.
        status: RoomStatus,
    },
    /// Already committed under its idempotency key (RFC §13.4).
    Duplicate {
        /// The event the first delivery committed.
        event: Box<RoomEvent>,
    },
    /// An edit or a delete refused at step 8a: nothing was stored (RFC
    /// §10.1 step 8a).
    EditRefused {
        /// Why.
        refusal: EditRefusal,
    },
}

/// Everything a pass produced.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PassReport {
    pub(super) result: PassResult,
    pub(super) injected_events: Vec<InjectedEvent>,
    pub(super) tasks: Vec<Task>,
    pub(super) observations: Vec<Observation>,
    /// The failure of the lock's release, which changes nothing the pass
    /// decided (gap G-025).
    pub(super) release_error: Option<ErrorRecord>,
}

/// One event's pass through the locked section.
#[derive(Debug)]
pub(super) struct CommitPass {
    origin: PassOrigin,
    event: RoomEvent,
    state: State,
    /// The edit or the delete step 8a accepted, with its target.
    accepted: Option<Accepted>,
    injected_events: Vec<InjectedEvent>,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
    release_error: Option<ErrorRecord>,
}

#[derive(Debug)]
enum State {
    Start,
    Locking(EffectId),
    LoadingContext(EffectId),
    ApplyingTimers {
        effect: EffectId,
        pending: Box<Pending>,
    },
    CheckingKey {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    /// Step 8a reads the target of an edit or a delete.
    ReadingTarget {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    RunningHook {
        effect: EffectId,
        hook: HookId,
        rest: VecDeque<HookId>,
        chain: Box<HookChain<RoomEvent>>,
        context: Box<RoomContext>,
    },
    LoadingBindings {
        effect: EffectId,
        payload: Box<RoomEvent>,
        context: Box<RoomContext>,
    },
    Committing {
        effect: EffectId,
        result: PassResult,
    },
    Releasing {
        effect: EffectId,
        result: Result<PassResult, Abort>,
    },
    Finished(Result<PassResult, Abort>),
    Reported,
}

impl State {
    /// The effect the state waits for.
    fn awaits(&self) -> Option<EffectId> {
        match self {
            Self::Locking(effect)
            | Self::LoadingContext(effect)
            | Self::ApplyingTimers { effect, .. }
            | Self::CheckingKey { effect, .. }
            | Self::ReadingTarget { effect, .. }
            | Self::RunningHook { effect, .. }
            | Self::LoadingBindings { effect, .. }
            | Self::Committing { effect, .. }
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Finished(_) | Self::Reported => None,
        }
    }
}

impl CommitPass {
    /// A pass of `event`, stamped already, from `origin`.
    pub(super) fn new(origin: PassOrigin, event: RoomEvent) -> Self {
        Self {
            origin,
            event,
            state: State::Start,
            accepted: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            release_error: None,
        }
    }

    /// Takes the room's lock (RFC §10.1 step 6).
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            let room_id = self.event.room_id.clone();
            self.state = State::Locking(cx.io.issue(Request::AcquireLock { room_id }));
        }
    }

    /// Takes the driver's answer to the pass's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<PassReport, Abort>> {
        if self.state.awaits() != Some(effect) {
            debug_assert!(false, "pass received {effect:?} in {:?}", self.state);
            return Progress::Waiting;
        }
        self.state = match mem::replace(&mut self.state, State::Reported) {
            State::Locking(_) => self.on_locked(cx, outcome),
            State::LoadingContext(_) => self.on_context(cx, outcome),
            State::ApplyingTimers { pending, .. } => match pending.stored(cx, outcome) {
                Ok(opened) => self.on_gate(cx, opened.context),
                Err(error) => self.release(cx, Err(Abort::Store(error))),
            },
            State::CheckingKey { context, .. } => self.on_key(cx, context, outcome),
            State::ReadingTarget { context, .. } => self.on_target(cx, context, outcome),
            State::RunningHook {
                hook,
                rest,
                chain,
                context,
                ..
            } => self.on_hook(cx, hook, rest, *chain, context, outcome),
            State::LoadingBindings {
                payload, context, ..
            } => self.on_bindings(cx, *payload, context, outcome),
            State::Committing { result, .. } => self.on_committed(cx, result, outcome),
            State::Releasing { result, .. } => {
                let (result, release_error) =
                    ops::after_release(&self.event.room_id, result, outcome.into_released());
                self.release_error = release_error;
                State::Finished(result)
            }
            state @ (State::Start | State::Finished(_) | State::Reported) => state,
        };
        self.report()
    }

    fn report(&mut self) -> Progress<Result<PassReport, Abort>> {
        if !matches!(self.state, State::Finished(_)) {
            return Progress::Waiting;
        }
        let State::Finished(result) = mem::replace(&mut self.state, State::Reported) else {
            return Progress::Waiting;
        };
        Progress::Done(result.map(|result| PassReport {
            result,
            injected_events: mem::take(&mut self.injected_events),
            tasks: mem::take(&mut self.tasks),
            observations: mem::take(&mut self.observations),
            release_error: self.release_error.take(),
        }))
    }

    fn on_locked(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
        match outcome.into_locked() {
            Ok(()) => State::LoadingContext(cx.io.issue(Request::LoadContext {
                room_id: self.event.room_id.clone(),
                recent_events: cx.config.context_window,
            })),
            Err(error) => State::Finished(Err(Abort::Lock(error))),
        }
    }

    /// The room as the lock found it goes through the timer gate first (gap
    /// G-045).
    fn on_context(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> State {
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
    /// the idempotency check, step 8a or the hooks (RFC §10.1 steps 6 to 8).
    fn on_gate(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        let status = context.room.status;
        if !status.accepts_events() {
            self.notify_refusal(cx, status);
            return self.release(cx, Ok(PassResult::Refused { status }));
        }
        self.event.index = context.room.event_count;
        match (&self.origin, self.event.idempotency_key.clone()) {
            (PassOrigin::Trigger, Some(key)) => State::CheckingKey {
                effect: cx.io.issue(Request::FindByIdempotencyKey {
                    room_id: self.event.room_id.clone(),
                    channel_id: self.event.source.channel_id.clone(),
                    key,
                }),
                context,
            },
            _ => self.check_edit(cx, context),
        }
    }

    fn on_key(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>, outcome: Outcome) -> State {
        match outcome.into_idempotent() {
            Ok(Some(event)) => self.release(cx, Ok(PassResult::Duplicate { event })),
            Ok(None) => self.check_edit(cx, context),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// Step 8a: an edit or a delete reads its target, under the lock; any
    /// other event goes to the hooks (RFC §10.1 step 8a).
    fn check_edit(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        match edits::target_of(&self.event) {
            Ok(None) => self.start_hooks(cx, context),
            Ok(Some(event_id)) => State::ReadingTarget {
                effect: cx.io.issue(Request::GetEvent { event_id }),
                context,
            },
            Err(refusal) => self.refuse_edit(cx, refusal),
        }
    }

    /// The target is read: an accepted edit or delete goes to the hooks, a
    /// refused one ends the pass with nothing stored (RFC §10.1 step 8a).
    fn on_target(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>, outcome: Outcome) -> State {
        let target = match outcome.into_event() {
            Ok(target) => target.map(|target| *target),
            Err(error) => return self.release(cx, Err(Abort::Store(error))),
        };
        match edits::check(&self.event, &self.origin, target, &context.participants) {
            Ok(accepted) => {
                self.accepted = Some(accepted);
                self.start_hooks(cx, context)
            }
            Err(refusal) => self.refuse_edit(cx, refusal),
        }
    }

    fn refuse_edit(&mut self, cx: &mut Cx<'_>, refusal: EditRefusal) -> State {
        edits::log_refusal(&self.event, &self.origin, refusal);
        self.release(cx, Ok(PassResult::EditRefused { refusal }))
    }

    fn start_hooks(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> State {
        let hooks = cx
            .config
            .hooks
            .chain(HookTrigger::BeforeBroadcast, &self.event.source);
        let chain = HookChain::new(HookTrigger::BeforeBroadcast, self.event.clone());
        self.next_hook(cx, hooks.into(), chain, context)
    }

    /// Runs the next hook of the chain, or concludes it (RFC §10.1 step 9).
    fn next_hook(
        &mut self,
        cx: &mut Cx<'_>,
        mut rest: VecDeque<HookId>,
        chain: HookChain<RoomEvent>,
        context: Box<RoomContext>,
    ) -> State {
        let next = if chain.is_blocked() {
            None
        } else {
            rest.pop_front()
                .and_then(|hook| cx.config.hooks.get(hook).map(|spec| (hook, spec.timeout)))
        };
        let Some((hook, timeout)) = next else {
            return self.conclude_chain(cx, chain.finish(), context);
        };
        let effect = cx.io.issue(Request::RunHook {
            hook,
            trigger: HookTrigger::BeforeBroadcast,
            event: stamp::keep_identity(&self.event, chain.payload().clone()),
            context: context.clone(),
            timeout,
        });
        State::RunningHook {
            effect,
            hook,
            rest,
            chain: Box::new(chain),
            context,
        }
    }

    fn on_hook(
        &mut self,
        cx: &mut Cx<'_>,
        hook: HookId,
        rest: VecDeque<HookId>,
        mut chain: HookChain<RoomEvent>,
        context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        if let Some(spec) = cx.config.hooks.get(hook) {
            chain.apply(spec, outcome.into_hook());
        }
        self.next_hook(cx, rest, chain, context)
    }

    /// Blocked: commits the record (RFC §10.1 step 10). Allowed: reads the
    /// bindings again, as the hooks left them, for the source check (RFC §7.5
    /// rule 2).
    fn conclude_chain(
        &mut self,
        cx: &mut Cx<'_>,
        verdict: ChainVerdict<RoomEvent>,
        context: Box<RoomContext>,
    ) -> State {
        super::report::hook_incidents(cx, &self.event, &verdict.incidents);
        self.injected_events.extend(verdict.injected_events);
        self.tasks.extend(verdict.tasks);
        self.observations.extend(verdict.observations);
        let payload = stamp::keep_identity(&self.event, verdict.payload);
        match verdict.decision {
            ChainDecision::Blocked { by, reason } => {
                committing(commit::blocked(cx, payload, by, reason, &context))
            }
            ChainDecision::Allowed => State::LoadingBindings {
                effect: cx.io.issue(Request::LoadBindings {
                    room_id: self.event.room_id.clone(),
                }),
                payload: Box::new(payload),
                context,
            },
        }
    }

    /// The source check, then the commit and its broadcast plan (RFC §10.1
    /// steps 11 and 12).
    fn on_bindings(
        &mut self,
        cx: &mut Cx<'_>,
        mut event: RoomEvent,
        mut context: Box<RoomContext>,
        outcome: Outcome,
    ) -> State {
        let bindings = match outcome.into_bindings() {
            Ok(bindings) => bindings,
            Err(error) => return self.release(cx, Err(Abort::Store(error))),
        };
        let planned = self.plan(cx, &mut event, &bindings, &context);
        context.bindings = bindings;
        match planned {
            Ok(plan) => {
                let written = match self.accepted.take().map(|accepted| accepted.write(&event)) {
                    Some(None) => return self.refuse_edit(cx, EditRefusal::TargetNotFound),
                    Some(Some(written)) => Some(written),
                    None => None,
                };
                committing(commit::delivered(cx, event, plan, context, written))
            }
            Err(refusal) => {
                let blocked_by = refusal.blocked_by().to_owned();
                let blocked =
                    commit::blocked(cx, event, blocked_by.clone(), Some(blocked_by), &context);
                committing(blocked)
            }
        }
    }

    /// Whom the event reaches, once its source may write; the event is
    /// committed with the visibility its source binding allows (RFC §7.5
    /// rules 2 and 4, gap G-009), within its target's for an edit or a
    /// delete (gap G-062).
    fn plan(
        &self,
        cx: &Cx<'_>,
        event: &mut RoomEvent,
        bindings: &[ChannelBinding],
        context: &RoomContext,
    ) -> Result<DeliveryPlan, WriteRefusal> {
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

    fn on_committed(&mut self, cx: &mut Cx<'_>, result: PassResult, outcome: Outcome) -> State {
        match outcome.into_committed() {
            Ok(()) => self.release(cx, Ok(result)),
            Err(error) => self.release(cx, Err(Abort::Store(error))),
        }
    }

    /// Releases the room's lock before reporting `result` (RFC §10.1 step 13).
    fn release(&mut self, cx: &mut Cx<'_>, result: Result<PassResult, Abort>) -> State {
        let effect = cx.io.issue(Request::ReleaseLock {
            room_id: self.event.room_id.clone(),
        });
        State::Releasing { effect, result }
    }

    /// A refused event is observable (RFC §5.1): an inbound one as
    /// `inbound`, a response or a hook's injected event as `reentry`, the
    /// framework's own re-injection into the room (gap G-043).
    fn notify_refusal(&self, cx: &mut Cx<'_>, status: RoomStatus) {
        let operation = match self.origin {
            PassOrigin::Trigger => RefusedOperation::Inbound,
            PassOrigin::Response | PassOrigin::Injected { .. } => RefusedOperation::Reentry,
        };
        cx.io
            .notify(Notice::FrameworkEvent(FrameworkEvent::RoomRefusedEvent {
                room_id: self.event.room_id.clone(),
                event_id: self.event.id.clone(),
                status,
                operation,
                event_type: self.event.event_type,
            }));
    }
}

/// The state that waits for a commit to be durable.
fn committing(Committing { effect, result }: Committing) -> State {
    State::Committing { effect, result }
}
