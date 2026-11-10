//! What a turn's host does for the triggers of an AI channel's loop (RFC
//! §6.4, §9.2): the kit's hooks on `AFTER_TOOL_ROUND`,
//! `BEFORE_AI_GENERATION` and `ON_AI_RESPONSE`, whose payloads roomkit-ai
//! defines, fired on the turn's executor.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{HookAction, Metadata};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec};

use super::{TURN, TurnHost};
use crate::context::ExecutionContext;
use crate::hooks::{self, HookHandler, TurnPayload};

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
        let hooks = self.turn_hooks::<P>();
        let chain = self.turn_chain(&hooks, payload).await;
        if chain.blocked.is_none() {
            self.observe_turn_with(&hooks, &chain.payload, None);
        }
        chain
    }

    /// Starts the kit's hooks on the trigger of `payload`, an observed one,
    /// for this turn's agent and room, each in the background within its
    /// timeout, then runs `then` once every one ended, at once when none is
    /// registered: Python's `_fire_async_hooks(..., then=...)`, which
    /// `ON_AI_RESPONSE` fires (RFC §9.2).
    #[doc(hidden)]
    pub fn observe_turn<P: TurnPayload>(&self, payload: &P, then: impl FnOnce() + Send + 'static) {
        let hooks = self.turn_hooks::<P>();
        let then: Box<dyn FnOnce() + Send> = Box::new(then);
        self.observe_turn_with(&hooks, payload, Some(then));
    }

    /// Emits the framework event `event_type` with `data`, about the turn's
    /// room and agent (RFC §8.2): `before_ai_generation` and `ai_response`,
    /// as the Python implementation's executor emits them.
    #[doc(hidden)]
    pub fn emit_turn_event(&self, event_type: &str, data: Metadata) {
        self.emit(event_type, data);
    }

    /// The hooks on the trigger of `P` for the turn's room and agent.
    fn turn_hooks<P: TurnPayload>(&self) -> TurnHooks<P> {
        let agent = (&self.shared.room_id, &self.shared.channel_id);
        self.shared
            .kit
            .turn_hooks(P::TRIGGER, agent, &self.shared.channel_type)
            .into_iter()
            .filter_map(|(spec, hook)| Some((spec, hook.handler::<P>()?)))
            .collect()
    }

    /// The SYNC chain of `hooks` on `payload`.
    async fn turn_chain<P: TurnPayload>(&self, hooks: &TurnHooks<P>, payload: P) -> TurnChain<P> {
        let mut chain = TurnChain {
            payload,
            blocked: None,
        };
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run_traced(
                spec,
                Arc::clone(handler),
                chain.payload.clone(),
                self.shared.context.clone(),
                |_| None,
            )
            .await;
            self.report_timeout(spec, &ran);
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
                chain.blocked = Some((spec.name.clone(), Some(failure)));
                return chain;
            }
        }
        chain
    }

    /// Runs the ASYNC hooks of `hooks` on `payload` in the background, each
    /// within its timeout, as the turn's observers; `then` once every one
    /// ended, at once when there is none.
    fn observe_turn_with<P: TurnPayload>(
        &self,
        hooks: &TurnHooks<P>,
        payload: &P,
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
                self.shared.context.clone(),
            );
            let observing = ExecutionContext::current().observing(0, observing);
            let (host, spec, done) = (self.clone(), spec.clone(), Arc::clone(&ended));
            let bounded = async move {
                let observed = observing.await;
                host.report_timeout(&spec, &observed);
                if let HookOutcome::Raised(error) = observed {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "a turn's observer raised");
                }
                done.one();
            };
            // Under the turn's record, as Python's task copies its context.
            let bounded = crate::turn_record::within_current_turn_record(bounded);
            let task = ExecutionContext::current().scope(TURN.scope(self.clone(), bounded));
            if !self.shared.kit.spawn_observer(Box::pin(task)) {
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
