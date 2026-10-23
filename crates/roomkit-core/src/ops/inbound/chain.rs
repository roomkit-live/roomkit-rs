//! A `BEFORE_BROADCAST` chain run hook by hook (RFC §9.3, §9.5), as step 5a
//! runs its off-lock hooks before the room lock and step 9 the others under
//! it. Both phases feed one [`HookChain`], so the off-lock outcome is applied
//! first at step 9 and the side effects of both phases are collected once
//! (RFC §9.5.1).

use std::collections::VecDeque;

use super::stamp;
use crate::model::{RoomContext, RoomEvent, RoomId};
use crate::ops::Cx;
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::rules::hook_chain::HookChain;
use crate::rules::hook_registry::{HookId, HookTrigger};

/// The hooks left to run on a chain, and the hook running.
#[derive(Debug)]
pub(super) struct HookRun {
    rest: VecDeque<HookId>,
    chain: HookChain<RoomEvent>,
    running: Option<HookId>,
    /// Whether the run asked for a hook.
    asked: bool,
}

impl HookRun {
    /// A run of `hooks`, in their order, on `chain`.
    pub(super) fn new(hooks: Vec<HookId>, chain: HookChain<RoomEvent>) -> Self {
        Self {
            rest: hooks.into(),
            chain,
            running: None,
            asked: false,
        }
    }

    /// Whether the run asked for a hook: under the room lock, one may have
    /// changed the room (RFC §13.5).
    pub(super) fn asked(&self) -> bool {
        self.asked
    }

    /// A run of `hooks` on a new chain of `event`.
    pub(super) fn of(hooks: Vec<HookId>, event: &RoomEvent) -> Self {
        Self::new(
            hooks,
            HookChain::new(HookTrigger::BeforeBroadcast, event.clone()),
        )
    }

    /// Asks for the next hook on the payload as the previous hooks left
    /// it, with `original`'s identity (RFC §9.4), within its own timeout and
    /// `deadline`, under the lock of `locked` when its caller holds it (RFC
    /// §13.5); `None` once a hook blocked or none is left.
    pub(super) fn ask_next(
        &mut self,
        cx: &mut Cx<'_>,
        original: &RoomEvent,
        context: &RoomContext,
        bound: Bound,
        locked: Option<&RoomId>,
    ) -> Option<EffectId> {
        self.running = None;
        if self.chain.is_blocked() {
            return None;
        }
        let (hook, timeout) = self
            .rest
            .pop_front()
            .and_then(|hook| cx.config.hooks.get(hook).map(|spec| (hook, spec.timeout)))?;
        self.running = Some(hook);
        self.asked = true;
        let request = Request::RunHook {
            hook,
            trigger: HookTrigger::BeforeBroadcast,
            event: stamp::keep_identity(original, self.chain.payload().clone()),
            context: Box::new(context.clone()),
            timeout,
        };
        Some(cx.io.issue_under(request, bound, locked.cloned()))
    }

    /// Applies what the running hook produced (RFC §9.3).
    pub(super) fn apply(&mut self, cx: &Cx<'_>, outcome: Outcome) {
        let Some(spec) = self
            .running
            .take()
            .and_then(|hook| cx.config.hooks.get(hook))
        else {
            return;
        };
        self.chain.apply(spec, outcome.into_hook());
    }

    /// The chain so far.
    pub(super) fn chain_mut(&mut self) -> &mut HookChain<RoomEvent> {
        &mut self.chain
    }

    /// The chain, once no hook is left to run.
    pub(super) fn into_chain(self) -> HookChain<RoomEvent> {
        self.chain
    }
}
