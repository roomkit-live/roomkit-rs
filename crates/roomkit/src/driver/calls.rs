//! The calls the driver runs as tasks: a request on the store, a channel,
//! a hook or the router, each within its execution context; and the
//! notices it performs without answering.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent};
use roomkit_core::ops::{Notice, Outcome, Request};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookId, HookTrigger};
use tokio::task::JoinSet;

use crate::channel::{ChannelError, EventDraft};
use crate::context::ExecutionContext;
use crate::hooks;
use crate::kit::Inner;
use crate::store::ConversationStore;

/// Carries out a request on the store, a channel, a hook or the router.
pub(super) async fn call<S: ConversationStore>(inner: &Arc<Inner<S>>, request: Request) -> Outcome {
    match request {
        Request::HandleInbound {
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
                Ok(event) => Outcome::Parsed(Box::new(event)),
                Err(error) => Outcome::Failed(ErrorRecord::new("ChannelError", error.to_string())),
            }
        }
        Request::OnEvent {
            binding,
            event,
            context,
        } => {
            let Some(channel) = inner.channel(&binding.channel_id) else {
                return unknown_channel(&binding.channel_id);
            };
            channel_output(channel.on_event(event, binding, *context).await)
        }
        Request::Deliver {
            binding,
            event,
            context,
        } => {
            let Some(channel) = inner.channel(&binding.channel_id) else {
                return unknown_channel(&binding.channel_id);
            };
            channel_output(channel.deliver(event, binding, *context).await)
        }
        Request::RunHook {
            hook,
            event,
            context,
            timeout,
            ..
        } => {
            let outcome = match inner.hook(hook) {
                Some(handler) => hooks::run(handler, event, *context, timeout).await,
                None => {
                    HookOutcome::Raised(ErrorRecord::new("HookError", "the hook has no handler"))
                }
            };
            Outcome::Hook(Box::new(outcome))
        }
        Request::RunObservers {
            trigger,
            hooks,
            event,
            context,
        } => {
            run_observers(inner, trigger, hooks, *event, *context).await;
            Outcome::Observed
        }
        Request::Route { query } => {
            let router = inner.router();
            match router.route(&inner.store, &query).await {
                Ok(room_id) => Outcome::Routed(room_id),
                Err(error) => Outcome::Failed(ErrorRecord::new("RouterError", error.to_string())),
            }
        }
        Request::LoadContext { .. }
        | Request::FindByIdempotencyKey { .. }
        | Request::GetEvent { .. }
        | Request::UpdateEvent { .. }
        | Request::DeleteEvent { .. }
        | Request::Commit { .. }
        | Request::PersistSideEffects { .. }
        | Request::CreateRoom { .. }
        | Request::UpdateRoom { .. }
        | Request::StoreBinding { .. }
        | Request::DeleteBinding { .. }
        | Request::ResolveIdentity { .. } => store_call(&inner.store, request).await,
        Request::AcquireLock { .. }
        | Request::ReleaseLock { .. }
        | Request::EnterLane { .. }
        | Request::LeaveLane { .. }
        | Request::TakeTicket { .. }
        | Request::AwaitTicket { .. }
        | Request::ReleaseTicket { .. } => Outcome::Failed(ErrorRecord::new(
            "Internal",
            "a lock, lane or ticket request reached the call executor",
        )),
    }
}

/// Carries out a request on the store.
async fn store_call<S: ConversationStore>(store: &S, request: Request) -> Outcome {
    let answer = match request {
        Request::LoadContext {
            room_id,
            recent_events,
        } => {
            return match store.load_context(&room_id, recent_events).await {
                Ok(Some(context)) => Outcome::Context(Box::new(context)),
                Ok(None) => Outcome::RoomNotFound,
                Err(error) => Outcome::Failed(error.to_record()),
            };
        }
        Request::FindByIdempotencyKey {
            room_id,
            channel_id,
            key,
        } => store
            .find_by_idempotency_key(&room_id, &channel_id, &key)
            .await
            .map(|event| Outcome::Idempotent(event.map(Box::new))),
        Request::GetEvent { event_id } => store
            .get_event(&event_id)
            .await
            .map(|event| Outcome::Event(event.map(Box::new))),
        Request::Commit {
            event,
            counters,
            target,
        } => store
            .commit(event, counters, target)
            .await
            .map(|()| Outcome::Committed),
        Request::UpdateEvent { event_id, updates } => store
            .update_event(&event_id, updates)
            .await
            .map(|_| Outcome::Stored),
        Request::DeleteEvent {
            room_id,
            event_id,
            cascade_replies,
        } => store
            .delete_event(&room_id, &event_id, cascade_replies)
            .await
            .map(Outcome::Deleted),
        Request::PersistSideEffects {
            room_id,
            tasks,
            observations,
        } => store
            .persist_side_effects(&room_id, tasks, observations)
            .await
            .map(|()| Outcome::Persisted),
        Request::CreateRoom { room } => store.create_room(room).await.map(|()| Outcome::Stored),
        Request::UpdateRoom { room_id, updates } => store
            .update_room(&room_id, updates)
            .await
            .map(|_| Outcome::Stored),
        Request::StoreBinding { binding } => {
            store.store_binding(binding).await.map(|()| Outcome::Stored)
        }
        Request::DeleteBinding {
            room_id,
            channel_id,
        } => store
            .delete_binding(&room_id, &channel_id)
            .await
            .map(|()| Outcome::Stored),
        Request::ResolveIdentity {
            channel_type,
            address,
            organization_id,
        } => store
            .resolve_identity(channel_type, &address, organization_id.as_deref())
            .await
            .map(|identity| Outcome::Resolved(identity.map(|identity| identity.id))),
        Request::AcquireLock { .. }
        | Request::ReleaseLock { .. }
        | Request::EnterLane { .. }
        | Request::LeaveLane { .. }
        | Request::TakeTicket { .. }
        | Request::AwaitTicket { .. }
        | Request::ReleaseTicket { .. }
        | Request::HandleInbound { .. }
        | Request::OnEvent { .. }
        | Request::Deliver { .. }
        | Request::RunHook { .. }
        | Request::RunObservers { .. }
        | Request::Route { .. } => {
            return Outcome::Failed(ErrorRecord::new(
                "Internal",
                "a request reached the store that is not the store's",
            ));
        }
    };
    answer.unwrap_or_else(|error| Outcome::Failed(error.to_record()))
}

/// Performs a notice: publishes a framework event, or runs observer hooks in
/// the background, logging what they raise (RFC §9.3). A background
/// observer runs outside every execution context: it runs after the fact,
/// and nothing awaits it as its own.
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
                tokio::spawn(observing);
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
        Ok(output) => Outcome::Output(Box::new(output)),
        Err(error) => Outcome::Failed(ErrorRecord::new("ChannelError", error.to_string())),
    }
}

fn unknown_channel(channel_id: &ChannelId) -> Outcome {
    Outcome::Failed(ErrorRecord::new(
        "UnknownChannel",
        format!("no channel {channel_id} is registered"),
    ))
}
