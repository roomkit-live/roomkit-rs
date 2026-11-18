//! What a turn's host does for the triggers of an AI channel's loop (RFC
//! §6.4, §9.2): the kit's hooks on `AFTER_TOOL_ROUND`,
//! `BEFORE_AI_GENERATION` and `ON_AI_RESPONSE`, whose payloads roomkit-ai
//! defines, fired on the turn's executor.

mod collect;
use collect::collect_effects;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{HookAction, Metadata};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec};

use super::{TURN, TurnHost};
use crate::context::ExecutionContext;
use crate::hooks::{self, HookHandler, TurnPayload};
use crate::turns::SyncPipelineResult;

type TurnHooks<P> = Vec<(HookSpec, Arc<dyn HookHandler<P>>)>;

/// What the SYNC chain of a turn payload's hooks came to (RFC §9.3).
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TurnChain<P> {
    /// The payload as the hooks left it: each read it as the one before
    /// left it, a modify's event replacing it.
    pub payload: P,
    /// The hook that ended the chain, by its name, and its reason, when
    /// one did: a block, or a hook that failed closed.
    pub blocked: Option<(String, Option<String>)>,
    /// The block came from a hook failure, not its explicit decision.
    pub failed_closed: bool,
}

impl TurnHost {
    /// Runs the kit's hooks on the trigger of `payload` for this turn's
    /// agent and room, as the Python implementation's `run_sync_hooks` runs
    /// them (RFC §9.3): its SYNC chain in chain order, each hook reading the
    /// payload as the one before left it, a modify's event replacing it; a
    /// block ends the chain with the hook's name and its reason. A hook
    /// that raises, runs past its timeout, or returns nothing or a modify
    /// with no event is skipped, unless it fails closed: it then ends the
    /// chain, its failure the reason. Once the chain let the payload
    /// through, its ASYNC hooks observe the payload it left, each in the
    /// background within its timeout.
    ///
    /// For roomkit-ai's tool loop, which fires each trigger once where the
    /// Python implementation's channel fires it.
    #[doc(hidden)]
    pub async fn run_turn_hooks<P: TurnPayload>(&self, payload: P) -> TurnChain<P> {
        crate::turns::ChannelTurnHooks::from(self.clone())
            .run(payload)
            .await
    }

    /// Runs only the SYNC decision chain. The caller reports the final standing
    /// decision separately, including a silence left by a block.
    #[doc(hidden)]
    pub async fn decide_turn<P: TurnPayload>(&self, payload: P) -> TurnChain<P> {
        crate::turns::ChannelTurnHooks::from(self.clone())
            .decide(payload)
            .await
    }

    /// Starts the kit's hooks on the trigger of `payload`, an observed one,
    /// for this turn's agent and room, each in the background within its
    /// timeout, then runs `then` once every one ended, at once when none is
    /// registered: Python's `_fire_async_hooks(..., then=...)`, which
    /// `ON_AI_RESPONSE` fires (RFC §9.2).
    #[doc(hidden)]
    pub fn observe_turn<P: TurnPayload>(&self, payload: &P, then: impl FnOnce() + Send + 'static) {
        crate::turns::ChannelTurnHooks::from(self.clone()).observe(payload, then);
    }

    /// Emits the framework event `event_type` with `data`, about the turn's
    /// room and agent (RFC §8.2): `before_ai_generation` and `ai_response`,
    /// as the Python implementation's executor emits them.
    #[doc(hidden)]
    pub fn emit_turn_event(&self, event_type: &str, data: Metadata) {
        self.emit(event_type, data);
    }
}

impl<P> TurnChain<P> {
    fn unchanged(payload: P) -> Self {
        Self {
            payload,
            blocked: None,
            failed_closed: false,
        }
    }
}

impl crate::turns::ChannelTurnHooks {
    fn hooks<P: TurnPayload>(&self) -> TurnHooks<P> {
        let (kit, room, channel, kind) = self.parts();
        kit.turn_hooks(P::TRIGGER, (room, channel), kind)
            .into_iter()
            .filter_map(|(spec, hook)| Some((spec, hook.handler::<P>()?)))
            .collect()
    }

    /// Run the synchronous chain and then its observers if the chain was allowed.
    pub async fn run<P: TurnPayload>(&self, payload: P) -> TurnChain<P> {
        self.run_if_reachable(payload)
            .await
            .unwrap_or_else(TurnChain::unchanged)
    }

    /// Run the hooks or return the unchanged payload when their room cannot
    /// be read. A generation skipped for this reason emits no hook result.
    ///
    /// # Errors
    /// Returns the original payload when a registered hook cannot read its room.
    pub async fn run_if_reachable<P: TurnPayload>(&self, payload: P) -> Result<TurnChain<P>, P> {
        self.run_reachable(payload, None).await
    }

    /// Return the synchronous result exposed by the executor's callback.
    pub async fn sync_pipeline<P: TurnPayload>(&self, payload: P) -> SyncPipelineResult<P> {
        let mut result = SyncPipelineResult::default();
        if let Ok(chain) = self.run_reachable(payload, Some(&mut result)).await {
            result.event = Some(chain.payload);
            if let Some((name, reason)) = chain.blocked {
                result.allowed = false;
                result.blocked_by = Some(name);
                result.reason = result.reason.or(reason);
                result.failed_closed = chain.failed_closed;
            }
        }
        result
    }

    async fn run_reachable<P: TurnPayload>(
        &self,
        payload: P,
        effects: Option<&mut SyncPipelineResult<P>>,
    ) -> Result<TurnChain<P>, P> {
        let hooks = self.hooks::<P>();
        if hooks.is_empty() {
            return Ok(TurnChain::unchanged(payload));
        }
        let Some(context) = self.context(P::TRIGGER).await else {
            return Err(payload);
        };
        let chain = self.chain(&hooks, payload, &context, effects).await;
        if chain.blocked.is_none() {
            self.observe_with(&hooks, &chain.payload, &context, None);
        }
        Ok(chain)
    }

    /// Run the synchronous decision chain without starting observers.
    pub async fn decide<P: TurnPayload>(&self, payload: P) -> TurnChain<P> {
        self.decision_in_context(payload).await.0
    }

    /// Run the decision and schedule its observers on the same room snapshot.
    /// They see the standing payload, including a silence left by a block.
    pub async fn decide_and_observe<P: TurnPayload>(
        &self,
        payload: P,
        standing: impl FnOnce(TurnChain<P>) -> P,
    ) -> P {
        let (decision, selection) = self.decision_in_context(payload).await;
        let payload = standing(decision);
        if let Some((hooks, context)) = selection {
            self.observe_with(&hooks, &payload, &context, None);
        }
        payload
    }

    async fn decision_in_context<P: TurnPayload>(
        &self,
        payload: P,
    ) -> (
        TurnChain<P>,
        Option<(TurnHooks<P>, crate::model::RoomContext)>,
    ) {
        let hooks = self.hooks::<P>();
        if hooks.is_empty() {
            return (TurnChain::unchanged(payload), None);
        }
        let Some(context) = self.context(P::TRIGGER).await else {
            let mut decision = TurnChain::unchanged(payload);
            if let Some((spec, _)) = hooks.iter().find(|(spec, _)| {
                spec.execution == HookExecution::Sync
                    && (spec.fail_closed || spec.trigger.fails_closed())
            }) {
                decision.failed_closed = true;
                decision.blocked = Some((spec.name.clone(), None));
            }
            return (decision, None);
        };
        let decision = self.chain(&hooks, payload, &context, None).await;
        (decision, Some((hooks, context)))
    }

    /// Whether this executor has a matching hook for the payload.
    pub fn has_turn_hooks<P: TurnPayload>(&self) -> bool {
        !self.hooks::<P>().is_empty()
    }

    /// Read the room before scheduling observers. The observers' work runs independently.
    pub async fn observe_after_context<P: TurnPayload>(&self, payload: &P) {
        self.observe_after_context_then(payload, || {}).await;
    }

    /// Start observers after reading the room, with an action once all have finished.
    pub async fn observe_after_context_then<P: TurnPayload>(
        &self,
        payload: &P,
        then: impl FnOnce() + Send + 'static,
    ) {
        let hooks = self.hooks::<P>();
        if hooks.is_empty() {
            then();
        } else if let Some(context) = self.context(P::TRIGGER).await {
            self.observe_with(&hooks, payload, &context, Some(Box::new(then)));
        }
    }

    /// Schedule every observer, then the completion action once all finish.
    pub fn observe<P: TurnPayload>(&self, payload: &P, then: impl FnOnce() + Send + 'static) {
        let hooks = self.hooks::<P>();
        if hooks.is_empty() {
            then();
            return;
        }
        let done: Box<dyn FnOnce() + Send> = Box::new(then);
        if let Some(host) = self.host() {
            self.observe_with(&hooks, payload, host.initial_context(), Some(done));
        } else {
            let bound = self.clone();
            let payload = payload.clone();
            self.spawn(Box::pin(async move {
                if let Some(context) = bound.context(P::TRIGGER).await {
                    bound.observe_with(&hooks, &payload, &context, Some(done));
                }
            }));
        }
    }

    async fn chain<P: TurnPayload>(
        &self,
        hooks: &TurnHooks<P>,
        payload: P,
        context: &crate::model::RoomContext,
        mut effects: Option<&mut SyncPipelineResult<P>>,
    ) -> TurnChain<P> {
        let mut chain = TurnChain::unchanged(payload);
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let mut ran = hooks::run_traced(
                spec,
                Arc::clone(handler),
                chain.payload.clone(),
                context.clone(),
                |_| None,
            )
            .await;
            self.timeout(spec, &ran);
            if let Some(result) = effects.as_deref_mut() {
                collect_effects(spec, &mut ran, result);
            }
            let failure = match ran {
                HookOutcome::Returned(result) => match (result.action, result.event) {
                    (HookAction::Block, _) => {
                        chain.blocked = Some((spec.name.clone(), result.reason));
                        return chain;
                    }
                    (HookAction::Modify, Some(modified)) => {
                        chain.payload = modified;
                        continue;
                    }
                    (HookAction::Modify, None) => {
                        format!("hook {} returned a modify with no event", spec.name)
                    }
                    (HookAction::Allow, _) => continue,
                },
                HookOutcome::Raised(error) => {
                    format!("hook {} failed: {}", spec.name, error.message)
                }
                HookOutcome::TimedOut => format!(
                    "hook {} timed out after {:?}s",
                    spec.name,
                    spec.timeout.as_secs_f64()
                ),
                HookOutcome::Invalid { what, .. } => {
                    format!("hook {} returned an unusable result: {what}", spec.name)
                }
            };
            tracing::warn!(target: "roomkit::hooks", hook = %spec.name, trigger = ?spec.trigger, %failure, "a turn's hook gave no usable result");
            if spec.fail_closed || spec.trigger.fails_closed() {
                chain.failed_closed = true;
                chain.blocked = Some((spec.name.clone(), Some(failure)));
                return chain;
            }
        }
        chain
    }

    /// Runs the ASYNC hooks of `hooks` on `payload` in the background, each
    /// within its timeout, as the turn's observers; `then` once every one
    /// ended, at once when there is none.
    fn observe_with<P: TurnPayload>(
        &self,
        hooks: &TurnHooks<P>,
        payload: &P,
        context: &crate::model::RoomContext,
        then: Option<Box<dyn FnOnce() + Send>>,
    ) {
        let observers: Vec<_> = hooks
            .iter()
            .filter(|(spec, _)| {
                spec.execution == HookExecution::Async
                    || spec.trigger.execution() == HookExecution::Async
            })
            .collect();
        if observers.is_empty() {
            if let Some(then) = then {
                then();
            }
            return;
        }
        let ended = Arc::new(Ended {
            remaining: AtomicUsize::new(observers.len()),
            then: Mutex::new(then),
        });
        for (spec, handler) in observers {
            let observing = hooks::observe_traced(
                spec.clone(),
                Arc::clone(handler),
                payload.clone(),
                context.clone(),
            );
            // Boxed at once: each layer below moves its future by value, and
            // a debug build copies this one at every move, past a 2 MB stack
            // in a task's nested turn.
            let observing = Box::pin(ExecutionContext::current().observing(0, observing));
            let (host, spec, done) = (self.clone(), spec.clone(), Arc::clone(&ended));
            let bounded = async move {
                let observed = observing.await;
                host.timeout(&spec, &observed);
                if let HookOutcome::Raised(error) = observed {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "a turn's observer raised");
                }
                done.one();
            };
            // Under the turn's record, as Python's task copies its context.
            let bounded = crate::turns::DirectPrompt::keep(bounded);
            let bounded = crate::turn_record::within_current_turn_record(bounded);
            let bounded = crate::tool_call_scope::within_current_tool_call(bounded);
            let task = ExecutionContext::current().scope(TURN.scope(self.host().cloned(), bounded));
            if !self.spawn(Box::pin(task)) {
                tracing::debug!(target: "roomkit::hooks", "the kit is shut down: a turn's observer is not run");
                ended.one();
            }
        }
    }
}

/// The observers of one firing still running, and what runs once they all
/// ended.
struct Ended {
    remaining: AtomicUsize,
    then: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Ended {
    /// One observer ended: the last runs `then`.
    fn one(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let then = self
            .then
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(then) = then {
            then();
        }
    }
}
