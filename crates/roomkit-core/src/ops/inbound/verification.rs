//! Step 5b of the inbound pipeline: the sender's verification (RFC §10.1
//! step 5b, §11.7.3, §11.7.4), as the Python implementation's
//! `VerificationMixin._apply_verification`.
//!
//! It runs for a kit built with a verification policy, once step 5 let
//! the message through, when its sender is identified: the identity step 5
//! resolved, or the room's IDENTIFIED participant the event names
//! (`rules::verification::sender_identity`). The sender's verification is
//! read through the driver (`IdentityRequest::Verification`), in the room's
//! organization. Then:
//!
//! - a message whose whole text is a code, from a sender with a request
//!   open, is attempted against it (`IdentityRequest::TryCode`); when it was
//!   a reply to an open code request of the room, the message is consumed:
//!   refused `verification_code`, its text never stored nor delivered;
//! - any other message is labelled with the verification's status
//!   (`source.verification`); a VERIFIED sender's goes on;
//! - an unverified sender's fires `ON_VERIFICATION_REQUIRED` (SYNC), whose
//!   hooks may block it (refused with their reason, `verification_required`
//!   by default) or pass it ([`HookResult::allow_unverified`]); one no hook
//!   passed is held from the room's agents when the policy holds
//!   (`rules::verification::hold`), an edit or a delete refused
//!   `verification_required` instead.
//!
//! A refusal stores nothing of the message. Every read and every hook
//! carries the call's deadline (RFC §13.6); the attempt, a write, carries
//! none.
//!
//! [`HookResult::allow_unverified`]: crate::model::HookResult::allow_unverified

use std::collections::VecDeque;

use super::deadline::Stopped;
use super::{InboundError, InboundResult};
use crate::model::hook::{VERIFICATION_DECISION_KEY, VERIFICATION_PASS};
use crate::model::{
    EventType, Identity, IdentityId, RoomContext, RoomEvent, Verification, VerificationStatus,
};
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, HookRequest, IdentityRequest, Outcome};
use crate::ops::{Cx, FrameworkEvent, Notice, Progress};
use crate::rules::hook_chain::{ChainDecision, HookChain, HookIncident, HookProblem};
use crate::rules::hook_registry::{HookId, HookTrigger};
use crate::rules::verification;

/// The reason a code reply is refused with: it was consumed (RFC §11.7.4).
const VERIFICATION_CODE: &str = "verification_code";
/// The reason an unverified sender's message is refused with by default
/// (RFC §11.7.3).
const VERIFICATION_REQUIRED: &str = "verification_required";

/// The trigger step 5b fires.
const TRIGGER: HookTrigger = HookTrigger::OnVerificationRequired;

/// The event and the room's context as step 5b left them, or why it
/// stopped the message.
pub(super) type Verified = Progress<Result<(RoomEvent, Box<RoomContext>), Stopped>>;

/// Whose verification labels `event` when the kit verifies its senders:
/// the identity and the room's organization, `None` when step 5b does not
/// run (no policy, or a sender who is not identified).
pub(super) fn sender(
    cx: &Cx<'_>,
    event: &RoomEvent,
    context: &RoomContext,
    resolved: Option<&Identity>,
) -> Option<(IdentityId, Option<String>)> {
    cx.config.verification.as_ref()?;
    verification::sender_identity(event, context, resolved)
}

/// Step 5b for one message.
#[derive(Debug)]
pub(super) struct Verifying {
    event: RoomEvent,
    context: Box<RoomContext>,
    identity_id: IdentityId,
    organization_id: Option<String>,
    /// The call's deadline (RFC §13.6).
    bound: Bound,
    state: State,
}

#[derive(Debug)]
enum State {
    /// The sender's verification is read.
    Reading(EffectId),
    /// The message's code is attempted; `.1` the verification it replied to.
    Trying(EffectId, Box<Verification>),
    /// `ON_VERIFICATION_REQUIRED`'s hooks run.
    Hooking(EffectId, Box<Hooks>),
    Done,
}

/// The hooks of `ON_VERIFICATION_REQUIRED` left to run, the one running,
/// and their chain.
#[derive(Debug)]
struct Hooks {
    rest: VecDeque<HookId>,
    running: Option<HookId>,
    chain: HookChain<RoomEvent>,
}

impl Verifying {
    /// Reads the verification of `sender`, `event`'s identity and its
    /// room's organization, within the call's `bound`.
    pub(super) fn start(
        cx: &mut Cx<'_>,
        event: RoomEvent,
        context: Box<RoomContext>,
        (identity_id, organization_id): (IdentityId, Option<String>),
        bound: Bound,
    ) -> Self {
        let request = IdentityRequest::Verification {
            identity_id: identity_id.clone(),
            organization_id: organization_id.clone(),
        };
        let effect = cx.io.issue_within(request, bound);
        Self {
            event,
            context,
            identity_id,
            organization_id,
            bound,
            state: State::Reading(effect),
        }
    }

    /// Whether step 5b waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            State::Reading(awaited) | State::Trying(awaited, _) | State::Hooking(awaited, _) => {
                *awaited == effect
            }
            State::Done => false,
        }
    }

    /// Takes the driver's answer to step 5b's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Verified {
        match std::mem::replace(&mut self.state, State::Done) {
            State::Reading(expected) if expected == effect => self.on_read(cx, outcome),
            State::Trying(expected, state) if expected == effect => {
                self.on_tried(cx, &state, outcome)
            }
            State::Hooking(expected, hooks) if expected == effect => {
                self.on_hook(cx, *hooks, outcome)
            }
            state @ (State::Reading(_) | State::Trying(..) | State::Hooking(..) | State::Done) => {
                debug_assert!(false, "step 5b received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    /// The sender's verification is read: a code reply is attempted, any
    /// other message labelled (Python's `_consume_code`, before the label).
    fn on_read(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Verified {
        let state = match outcome.into_verification() {
            Ok(state) => state,
            Err(error) => return failed(error),
        };
        let code_length = cx
            .config
            .verification
            .as_ref()
            .map_or(0, |policy| policy.code_length);
        let reply = verification::code_reply(&self.event, code_length);
        if let (Some(request_id), Some(code)) = (state.request_id.clone(), reply) {
            let request = IdentityRequest::TryCode {
                request_id,
                room_id: self.event.room_id.clone(),
                code,
            };
            self.state = State::Trying(cx.io.issue(request), state);
            return Progress::Waiting;
        }
        self.label(cx, &state)
    }

    /// The code was attempted: a reply to the request is consumed, its text
    /// never stored nor broadcast, and no agent reads it (RFC §11.7.4); a
    /// message the request did not ask for is labelled as any other.
    fn on_tried(&mut self, cx: &mut Cx<'_>, state: &Verification, outcome: Outcome) -> Verified {
        match outcome.into_code_tried() {
            Ok(true) => refused(VERIFICATION_CODE.to_owned()),
            Ok(false) => self.label(cx, state),
            Err(error) => failed(error),
        }
    }

    /// Labels the event with `state`'s status (RFC §11.7.3): a VERIFIED
    /// sender's goes on, any other's fires `ON_VERIFICATION_REQUIRED`.
    fn label(&mut self, cx: &mut Cx<'_>, state: &Verification) -> Verified {
        self.event.source.verification = Some(state.status);
        if state.status == VerificationStatus::Verified {
            return self.done();
        }
        let hooks = Hooks {
            rest: cx.config.hooks.chain(TRIGGER, &self.event).into(),
            running: None,
            chain: HookChain::new(TRIGGER, self.event.clone()),
        };
        self.next_hook(cx, hooks)
    }

    /// Runs the next hook on the payload as the previous ones left it,
    /// within its timeout and the call's deadline; decides once a hook
    /// blocked or none is left.
    fn next_hook(&mut self, cx: &mut Cx<'_>, mut hooks: Hooks) -> Verified {
        hooks.running = None;
        let next = if hooks.chain.is_blocked() {
            None
        } else {
            hooks
                .rest
                .pop_front()
                .and_then(|hook| cx.config.hooks.get(hook).map(|spec| (hook, spec.timeout)))
        };
        let Some((hook, timeout)) = next else {
            return self.decide(cx, hooks.chain);
        };
        hooks.running = Some(hook);
        let request = HookRequest::RunHook {
            hook,
            trigger: TRIGGER,
            event: Box::new(hooks.chain.payload().clone()),
            context: self.context.clone(),
            timeout,
        };
        let effect = cx.io.issue_within(request, self.bound);
        self.state = State::Hooking(effect, Box::new(hooks));
        Progress::Waiting
    }

    /// A hook answered: what it produced is applied as the Python
    /// implementation's `run_sync_hooks` applies it, a modify's payload
    /// replacing the event as it is, and a timeout reported.
    fn on_hook(&mut self, cx: &mut Cx<'_>, mut hooks: Hooks, outcome: Outcome) -> Verified {
        if let Some(spec) = hooks.running.and_then(|hook| cx.config.hooks.get(hook)) {
            hooks
                .chain
                .apply_kept(spec, outcome.into_hook(), |payload| payload);
        }
        let incidents = hooks.chain.take_incidents();
        report(cx, &self.event, &incidents);
        self.next_hook(cx, hooks)
    }

    /// What the hooks decided (RFC §11.7.3): a block refuses the message; a
    /// pass, or a policy that does not hold, lets it go on as the hooks
    /// left it; otherwise an edit or a delete is refused, and any other
    /// message held. The trigger's ASYNC hooks observe a message the chain
    /// allowed.
    fn decide(&mut self, cx: &mut Cx<'_>, chain: HookChain<RoomEvent>) -> Verified {
        let verdict = chain.finish();
        if let ChainDecision::Blocked { reason, .. } = verdict.decision {
            let reason = reason
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| VERIFICATION_REQUIRED.to_owned());
            return refused(reason);
        }
        cx.fire_observers(TRIGGER, &verdict.payload, &self.context);
        self.event = verdict.payload;
        let passed = verdict
            .metadata
            .get(VERIFICATION_DECISION_KEY)
            .and_then(serde_json::Value::as_str)
            == Some(VERIFICATION_PASS);
        let holds = cx
            .config
            .verification
            .as_ref()
            .is_some_and(|policy| policy.hold_unverified);
        if passed || !holds {
            return self.done();
        }
        if matches!(self.event.r#type, EventType::Edit | EventType::Delete) {
            // An edit or a deletion changes a message the agents may
            // already read; held, it would still rewrite it.
            return refused(VERIFICATION_REQUIRED.to_owned());
        }
        verification::hold(
            &mut self.event,
            &self.context,
            &self.identity_id,
            self.organization_id.as_deref(),
        );
        self.done()
    }

    fn done(&mut self) -> Verified {
        self.state = State::Done;
        let room = self.context.room.clone();
        let context = std::mem::replace(&mut self.context, Box::new(RoomContext::new(room)));
        Progress::Done(Ok((self.event.clone(), context)))
    }
}

/// The message is refused with `reason` before anything of it is stored.
fn refused(reason: String) -> Verified {
    Progress::Done(Err(Stopped::Refused(Box::new(InboundResult {
        blocked: true,
        reason: Some(reason),
        refused_before_the_lock: true,
        ..InboundResult::default()
    }))))
}

/// The driver could not read or attempt: the message fails, with nothing
/// written.
fn failed(error: crate::model::ErrorRecord) -> Verified {
    Progress::Done(Err(Stopped::Failed(InboundError::Store(error))))
}

/// Reports the hooks that produced no usable result, as Python's
/// `run_sync_hooks` does for a trigger other than `BEFORE_BROADCAST`: a
/// timeout emits `hook_timeout` (RFC §8.2), any other problem is logged.
fn report(cx: &mut Cx<'_>, event: &RoomEvent, incidents: &[HookIncident]) {
    for incident in incidents {
        tracing::warn!(
            target: "roomkit::core::hooks",
            room_id = %event.room_id,
            hook = %incident.hook,
            blocked = incident.blocked,
            problem = ?incident.problem,
            "an ON_VERIFICATION_REQUIRED hook produced no usable result"
        );
        if matches!(incident.problem, HookProblem::TimedOut) {
            let timeout = FrameworkEvent::HookTimeout {
                room_id: event.room_id.clone(),
                hook_name: incident.hook.clone(),
                trigger: TRIGGER,
                timeout: incident.timeout.as_secs_f64(),
            };
            cx.io.notify(Notice::FrameworkEvent(timeout));
        }
    }
}
