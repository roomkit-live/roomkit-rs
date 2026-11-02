//! What a turn's host does for the human-input requests its turn opens
//! (RFC §9.3, §10.6.5): their way to the room's owner, and the kit's
//! `ON_USER_INPUT_REQUIRED` hooks, which fire on the turn's executor, or
//! on the kit for a request made outside its turns.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, HookAction, Metadata, RoomContext, RoomId};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec};
use serde_json::Value;
use tokio::task::AbortHandle;

use super::{TURN, TurnHost, TurnKit};
use crate::channel::BoxFuture;
use crate::context::ExecutionContext;
use crate::hooks::{self, HookHandler};
use crate::human_input::PendingInputEvent;
use crate::turns::ExecutorInputs;

/// The `ON_USER_INPUT_REQUIRED` hooks of a request: its SYNC chain in
/// chain order, then its observers.
pub(crate) type InputHooks = Vec<(HookSpec, Arc<dyn HookHandler<PendingInputEvent>>)>;

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
    /// request of a tool of this turn's agent, as
    /// [`InputNotification::run`] says, about the turn's room and agent,
    /// its observers within the turn. Whether the request stands; a
    /// request naming no room fires nothing and stands.
    pub(crate) async fn user_input_required(&self, event: PendingInputEvent) -> bool {
        if event.room_id.is_empty() {
            return true;
        }
        let notification = InputNotification {
            kit: Arc::clone(&self.shared.kit),
            ids: (self.shared.room_id.clone(), self.shared.channel_id.clone()),
            hooks: Some((self.input_hooks(&event), self.shared.context.clone())),
            turn: Some(self.clone()),
        };
        notification.run(event).await
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
}

/// A human-input request's `ON_USER_INPUT_REQUIRED` hooks as they run (RFC
/// §9.3): through the turn the request was made in, or through the kit
/// that registered its channel, for one made outside a turn.
pub(crate) struct InputNotification {
    /// The kit whose hooks run.
    pub(crate) kit: Arc<dyn TurnKit>,
    /// The room and the agent its framework events name.
    pub(crate) ids: (RoomId, ChannelId),
    /// The hooks, with the room's context they read: `None` when no hook
    /// runs.
    pub(crate) hooks: Option<(InputHooks, RoomContext)>,
    /// The turn its observers run within, if any.
    pub(crate) turn: Option<TurnHost>,
}

impl InputNotification {
    /// Runs the hooks on `event` as the Python implementation's callback
    /// runs them (RFC §9.3): the SYNC chain in order, each hook reading the
    /// request as the one before left it, a block rejecting it; a hook that
    /// raises or runs past its timeout lets it through (the trigger fails
    /// open). The ASYNC hooks observe a request the chain let through, as
    /// the chain left it. Emits `user_input_required` with the chain's
    /// verdict, naming the request as the tool asked it, whatever a hook's
    /// modify made of it (RFC §8.2, Python's
    /// `build_on_user_input_required_hook`). Whether the request stands.
    pub(crate) async fn run(self, event: PendingInputEvent) -> bool {
        let (current, blocked) = self.chain(event.clone()).await;
        let allowed = blocked.is_none();
        if allowed {
            self.observe(&current);
        }
        let mut data = Metadata::new();
        data.insert(
            "pending_id".to_owned(),
            Value::from(event.pending_id.as_str()),
        );
        data.insert(
            "tool_name".to_owned(),
            Value::from(event.tool_name.as_str()),
        );
        data.insert(
            "tool_call_id".to_owned(),
            Value::from(event.tool_call_id.as_str()),
        );
        data.insert("allowed".to_owned(), Value::Bool(allowed));
        data.insert(
            "reason".to_owned(),
            blocked.flatten().map_or(Value::Null, Value::from),
        );
        emit(&self.kit, &self.ids, "user_input_required", data);
        allowed
    }

    /// The SYNC chain on `event`: the request as the last hook left it, and
    /// the reason of the block that ended it, if one did.
    async fn chain(
        &self,
        mut event: PendingInputEvent,
    ) -> (PendingInputEvent, Option<Option<String>>) {
        let Some((hooks, context)) = &self.hooks else {
            return (event, None);
        };
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run(
                Arc::clone(handler),
                event.clone(),
                context.clone(),
                spec.timeout,
                |_| None,
            )
            .await;
            report_timeout(&self.kit, &self.ids, spec, &ran);
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

    /// Runs the ASYNC hooks on `event` in the background, each within its
    /// timeout, as the kit's observers, within the turn when there is one.
    fn observe(&self, event: &PendingInputEvent) {
        let Some((hooks, context)) = &self.hooks else {
            return;
        };
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Async)
        {
            let observing = hooks::run(
                Arc::clone(handler),
                event.clone(),
                context.clone(),
                spec.timeout,
                |_| None,
            );
            let (kit, ids, spec) = (Arc::clone(&self.kit), self.ids.clone(), spec.clone());
            let bounded = async move {
                let observed = observing.await;
                report_timeout(&kit, &ids, &spec, &observed);
                if let HookOutcome::Raised(error) = observed {
                    tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "a human-input request's observer raised");
                }
            };
            let task: BoxFuture<'static, ()> = match &self.turn {
                Some(turn) => {
                    Box::pin(ExecutionContext::current().scope(TURN.scope(turn.clone(), bounded)))
                }
                None => Box::pin(ExecutionContext::current().scope(bounded)),
            };
            if !self.kit.spawn_observer(task) {
                tracing::debug!(target: "roomkit::hooks", "the kit is shut down: a human-input request's observer is not run");
            }
        }
    }
}

/// Emits the framework event `event_type` about the room and agent `ids`.
fn emit(
    kit: &Arc<dyn TurnKit>,
    (room_id, channel_id): &(RoomId, ChannelId),
    event_type: &str,
    data: Metadata,
) {
    let ids = (Some(room_id.clone()), Some(channel_id.clone()), None);
    kit.emit(event_type.to_owned(), ids, data);
}

/// Emits the `hook_timeout` framework event of the hook `spec` when it ran
/// past its timeout, SYNC or ASYNC (RFC §8.2, §9.1), about the room and
/// agent `ids`, as `TurnHost` reports a tool hook's.
fn report_timeout<P>(
    kit: &Arc<dyn TurnKit>,
    ids: &(RoomId, ChannelId),
    spec: &HookSpec,
    ran: &HookOutcome<P>,
) {
    if !matches!(ran, HookOutcome::TimedOut) {
        return;
    }
    let mut data = Metadata::new();
    data.insert("hook_name".to_owned(), Value::from(spec.name.as_str()));
    data.insert(
        "trigger".to_owned(),
        serde_json::to_value(spec.trigger).unwrap_or_default(),
    );
    data.insert(
        "timeout".to_owned(),
        Value::from(spec.timeout.as_secs_f64()),
    );
    emit(kit, ids, "hook_timeout", data);
}
