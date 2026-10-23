//! The calls the driver runs as tasks, one handler per family of request:
//! the store's, a channel's, hooks' and the router's, each within its
//! execution context; and the notices it performs without answering.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent};
use roomkit_core::ops::{
    CHANNEL_NOT_REGISTERED, ChannelOutcome, ChannelRequest, HookAnswer, HookRequest, Notice,
    Outcome, StoreOutcome, StoreRequest,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookId, HookTrigger};
use roomkit_core::rules::routing::RouteQuery;
use tokio::task::JoinSet;

use crate::channel::{ChannelError, EventDraft};
use crate::context::ExecutionContext;
use crate::hooks;
use crate::kit::Inner;
use crate::store::ConversationStore;

/// Carries out a request on the store (RFC §14).
pub(super) async fn store<S: ConversationStore>(store: &S, request: StoreRequest) -> Outcome {
    let answer = match request {
        StoreRequest::LoadContext {
            room_id,
            recent_events,
        } => {
            return match store.load_context(&room_id, recent_events).await {
                Ok(Some(context)) => StoreOutcome::Context(Box::new(context)).into(),
                Ok(None) => StoreOutcome::RoomNotFound.into(),
                Err(error) => Outcome::Failed(error.to_record()),
            };
        }
        StoreRequest::FindByIdempotencyKey {
            room_id,
            channel_id,
            key,
        } => store
            .find_by_idempotency_key(&room_id, &channel_id, &key)
            .await
            .map(|event| StoreOutcome::Idempotent(event.map(Box::new))),
        StoreRequest::GetEvent { event_id } => store
            .get_event(&event_id)
            .await
            .map(|event| StoreOutcome::Event(event.map(Box::new))),
        StoreRequest::Commit {
            event,
            counters,
            target,
        } => store
            .commit(*event, counters, target)
            .await
            .map(|()| StoreOutcome::Committed),
        StoreRequest::UpdateEvent { event_id, updates } => store
            .update_event(&event_id, updates)
            .await
            .map(|_| StoreOutcome::Stored),
        StoreRequest::DeleteEvent {
            room_id,
            event_id,
            cascade_replies,
        } => store
            .delete_event(&room_id, &event_id, cascade_replies)
            .await
            .map(StoreOutcome::Deleted),
        StoreRequest::PersistSideEffects {
            room_id,
            tasks,
            observations,
        } => store
            .persist_side_effects(&room_id, tasks, observations)
            .await
            .map(|()| StoreOutcome::Persisted),
        StoreRequest::CreateRoom { room } => {
            store.create_room(room).await.map(|()| StoreOutcome::Stored)
        }
        StoreRequest::UpdateRoom { room_id, updates } => store
            .update_room(&room_id, updates)
            .await
            .map(|_| StoreOutcome::Stored),
        StoreRequest::StoreBinding { binding } => store
            .store_binding(binding)
            .await
            .map(|()| StoreOutcome::Stored),
        StoreRequest::DeleteBinding {
            room_id,
            channel_id,
        } => store
            .delete_binding(&room_id, &channel_id)
            .await
            .map(|()| StoreOutcome::Stored),
        StoreRequest::ResolveIdentity {
            channel_type,
            address,
            organization_id,
        } => store
            .resolve_identity(channel_type, &address, organization_id.as_deref())
            .await
            .map(|identity| StoreOutcome::Resolved(identity.map(|identity| identity.id))),
    };
    answer.map_or_else(|error| Outcome::Failed(error.to_record()), Outcome::from)
}

/// Calls a channel (RFC §6.1): a parse, a turn or a delivery.
pub(super) async fn channel<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    request: ChannelRequest,
) -> Outcome {
    match request {
        ChannelRequest::HandleInbound {
            channel_id,
            message,
            context,
            id,
        } => {
            let Some(channel) = inner.channel(&channel_id) else {
                return unknown_channel(&channel_id);
            };
            let draft = EventDraft {
                id,
                room_id: context.room.id.clone(),
                now: inner.environment().now(),
            };
            match channel.handle_inbound(message, *context, draft).await {
                Ok(event) => ChannelOutcome::Parsed(Box::new(event)).into(),
                Err(error) => Outcome::Failed(ErrorRecord::new("ChannelError", error.to_string())),
            }
        }
        ChannelRequest::OnEvent {
            binding,
            event,
            context,
        } => {
            let Some(channel) = inner.channel(&binding.channel_id) else {
                return unregistered_channel(&binding.channel_id);
            };
            channel_output(channel.on_event(event, binding, *context).await)
        }
        ChannelRequest::Deliver {
            binding,
            event,
            context,
        } => {
            let Some(channel) = inner.channel(&binding.channel_id) else {
                return unregistered_channel(&binding.channel_id);
            };
            channel_output(channel.deliver(event, binding, *context).await)
        }
    }
}

/// Runs hooks (RFC §9.3): one SYNC hook within its timeout, or observers,
/// awaited.
pub(super) async fn hook<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    request: HookRequest,
) -> Outcome {
    match request {
        HookRequest::RunHook {
            hook,
            event,
            context,
            timeout,
            ..
        } => {
            let outcome = match inner.hook(hook) {
                Some(handler) => hooks::run(handler, *event, *context, timeout).await,
                None => {
                    HookOutcome::Raised(ErrorRecord::new("HookError", "the hook has no handler"))
                }
            };
            HookAnswer::Ran(Box::new(outcome)).into()
        }
        HookRequest::RunObservers {
            trigger,
            hooks,
            event,
            context,
        } => {
            run_observers(inner, trigger, hooks, *event, *context).await;
            HookAnswer::Observed.into()
        }
    }
}

/// Asks the kit's router which room a message that names none belongs to
/// (RFC §10.4).
pub(super) async fn route<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    query: &RouteQuery,
) -> Outcome {
    let router = inner.router();
    match router.route(&inner.store, query).await {
        Ok(room_id) => Outcome::Routed(room_id),
        Err(error) => Outcome::Failed(ErrorRecord::new("RouterError", error.to_string())),
    }
}

/// Performs a notice: publishes a framework event, or runs observer hooks in
/// the background, as tasks the kit tracks, logging what they raise (RFC
/// §9.3). A background observer runs outside every execution context: it
/// runs after the fact, and nothing awaits it as its own. Once the kit is
/// shut down, none runs.
pub(super) fn perform<S: ConversationStore>(inner: &Arc<Inner<S>>, notice: Notice) {
    match notice {
        Notice::FrameworkEvent(event) => inner.publish(event),
        Notice::AsyncHooks {
            trigger,
            hooks,
            event,
            context,
        } => {
            for hook in hooks {
                let Some(observing) = observe(inner, trigger, hook, &event, &context) else {
                    continue;
                };
                if inner.tasks.spawn(observing).is_none() {
                    tracing::debug!(
                        target: "roomkit::hooks",
                        ?trigger,
                        "the kit is shut down: an observer hook is not run"
                    );
                }
            }
        }
    }
}

/// Runs observer hooks concurrently and returns once every one has
/// finished or timed out, what they raise logged (RFC §9.3, gap G-050).
/// Awaited, each runs in the context of the call that awaits it.
async fn run_observers<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    trigger: HookTrigger,
    hooks: Vec<HookId>,
    event: RoomEvent,
    context: RoomContext,
) {
    let execution = ExecutionContext::current();
    let mut observers = JoinSet::new();
    for hook in hooks {
        if let Some(observing) = observe(inner, trigger, hook, &event, &context) {
            observers.spawn(execution.clone().scope(observing));
        }
    }
    while let Some(joined) = observers.join_next().await {
        if let Err(failure) = joined {
            tracing::warn!(
                target: "roomkit::hooks",
                ?trigger,
                panicked = failure.is_panic(),
                "an awaited observer hook did not finish"
            );
        }
    }
}

/// One observer's run on `event`, within its timeout, reporting what it
/// raised; `None` for a hook with no handler.
fn observe<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    trigger: HookTrigger,
    hook: HookId,
    event: &RoomEvent,
    context: &RoomContext,
) -> Option<impl Future<Output = ()> + Send + 'static> {
    let (handler, spec) = inner.hook_with_spec(hook)?;
    let inner = Arc::clone(inner);
    let (event, context) = (event.clone(), context.clone());
    Some(async move {
        let room_id = event.room_id.clone();
        let outcome = hooks::run(handler, event, context, spec.timeout).await;
        inner.report_observer(&spec, trigger, room_id, &outcome);
    })
}

fn channel_output(result: Result<ChannelOutput, ChannelError>) -> Outcome {
    match result {
        Ok(output) => ChannelOutcome::Output(Box::new(output)).into(),
        Err(error) => Outcome::Failed(ErrorRecord::new("ChannelError", error.to_string())),
    }
}

/// A channel the kit no longer holds, asked to parse a message.
fn unknown_channel(channel_id: &ChannelId) -> Outcome {
    Outcome::Failed(ErrorRecord::new(
        "UnknownChannel",
        format!("no channel {channel_id} is registered"),
    ))
}

/// A channel the kit no longer holds, unregistered since the operation
/// started, asked to take or deliver an event: the delivery set skips a
/// transport and reports an intelligence channel (gap G-048).
fn unregistered_channel(channel_id: &ChannelId) -> Outcome {
    Outcome::Failed(ErrorRecord::new(
        CHANNEL_NOT_REGISTERED,
        format!("no channel {channel_id} is registered"),
    ))
}
