//! What a turn's host does for the human-input requests its turn opens
//! (RFC §9.3, §10.6.5): their way to the room's owner, and the kit's
//! `ON_USER_INPUT_REQUIRED` hooks, which fire on the turn's executor.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, HookAction, Metadata, RoomId};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec};
use serde_json::Value;
use tokio::task::AbortHandle;

use super::{TURN, TurnHost};
use crate::channel::BoxFuture;
use crate::hooks::{self, HookHandler};
use crate::human_input::PendingInputEvent;
use crate::turns::ExecutorInputs;

type InputHooks = Vec<(HookSpec, Arc<dyn HookHandler<PendingInputEvent>>)>;

impl TurnHost {
    /// The turn's open human-input requests, which its owner answers.
    pub(crate) fn inputs(&self) -> Arc<ExecutorInputs> {
        self.shared.link.inputs()
    }

    /// Runs `task` in the background as the turn's work, tracked by the
    /// kit; `None` once the kit is shut down.
    pub(crate) fn spawn(&self, task: BoxFuture<'static, ()>) -> Option<AbortHandle> {
        self.shared.kit.spawn_tracked(task)
    }

    /// Runs the kit's `ON_USER_INPUT_REQUIRED` hooks on `event`, the
    /// request of a tool of this turn's agent, as the Python
    /// implementation's callback runs them (RFC §9.3): its SYNC chain in
    /// order, each hook reading the request as the one before left it, a
    /// block rejecting it; a hook that raises or runs past its timeout lets
    /// it through (the trigger fails open). Its ASYNC hooks observe a
    /// request the chain let through. Emits `user_input_required` with the
    /// chain's verdict (RFC §8.2). Whether the request stands; a request
    /// naming no room fires nothing and stands.
    pub(crate) async fn user_input_required(&self, event: PendingInputEvent) -> bool {
        if event.room_id.is_empty() {
            return true;
        }
        let hooks = self.input_hooks(&event);
        let (current, blocked) = self.input_chain(&hooks, event).await;
        let allowed = blocked.is_none();
        if allowed {
            self.observe_input(&hooks, &current);
        }
        let mut data = Metadata::new();
        data.insert(
            "pending_id".to_owned(),
            Value::from(current.pending_id.as_str()),
        );
        data.insert(
            "tool_name".to_owned(),
            Value::from(current.tool_name.as_str()),
        );
        data.insert(
            "tool_call_id".to_owned(),
            Value::from(current.tool_call_id.as_str()),
        );
        data.insert("allowed".to_owned(), Value::Bool(allowed));
        data.insert(
            "reason".to_owned(),
            blocked.flatten().map_or(Value::Null, Value::from),
        );
        self.emit("user_input_required", data);
        allowed
    }

    /// The hooks on the request's trigger for its room and agent, as its
    /// event names them, else the turn's.
    fn input_hooks(&self, event: &PendingInputEvent) -> InputHooks {
        let room = RoomId::new(&event.room_id).unwrap_or_else(|_| self.shared.room_id.clone());
        let channel =
            ChannelId::new(&event.channel_id).unwrap_or_else(|_| self.shared.channel_id.clone());
        self.shared
            .kit
            .input_hooks((&room, &channel), &event.channel_type)
    }

    /// The SYNC chain of `hooks` on `event`: the request as the last hook
    /// left it, and the reason of the block that ended it, if one did.
    async fn input_chain(
        &self,
        hooks: &InputHooks,
        mut event: PendingInputEvent,
    ) -> (PendingInputEvent, Option<Option<String>>) {
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run(
                Arc::clone(handler),
                event.clone(),
                self.shared.context.clone(),
                spec.timeout,
                |_| None,
            )
            .await;
            self.report_timeout(spec, &ran);
            match ran {
                HookOutcome::Returned(result) => match result.action {
                    HookAction::Block => return (event, Some(result.reason)),
                    HookAction::Modify => {
                        if let Some(modified) = result.event {
                            event = modified;
                        }
                    }
                    HookAction::Allow => {}
                },
                HookOutcome::Raised(error) => {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "an ON_USER_INPUT_REQUIRED hook raised: the request stands");
                }
                HookOutcome::TimedOut | HookOutcome::Invalid { .. } => {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, "an ON_USER_INPUT_REQUIRED hook gave no usable result: the request stands");
                }
            }
        }
        (event, None)
    }

    /// Runs the ASYNC hooks of `hooks` on `event` in the background, each
    /// within its timeout, as the turn's observers.
    fn observe_input(&self, hooks: &InputHooks, event: &PendingInputEvent) {
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Async)
        {
            let observing = hooks::run(
                Arc::clone(handler),
                event.clone(),
                self.shared.context.clone(),
                spec.timeout,
                |_| None,
            );
            let (host, spec) = (self.clone(), spec.clone());
            let bounded = async move {
                let observed = observing.await;
                host.report_timeout(&spec, &observed);
                if let HookOutcome::Raised(error) = observed {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "a human-input request's observer raised");
                }
            };
            let task = crate::context::ExecutionContext::current()
                .scope(TURN.scope(self.clone(), bounded));
            if !self.shared.kit.spawn_observer(Box::pin(task)) {
                tracing::debug!(target: "roomkit::hooks", "the kit is shut down: a human-input request's observer is not run");
            }
        }
    }
}
