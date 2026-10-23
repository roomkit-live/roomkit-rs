//! The driver: runs any operation of the core to its end, executing the
//! effects it asks for and performing its notices.

use std::collections::HashMap;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, ChannelOutput, ErrorRecord, RoomId};
use roomkit_core::ops::{Effect, EffectId, Next, Notice, Operation, Outcome, Request};
use roomkit_core::rules::hook_chain::HookOutcome;
use tokio::task::JoinSet;

use crate::channel::{ChannelError, EventDraft};
use crate::error::KitError;
use crate::hooks;
use crate::kit::Inner;
use crate::locks::RoomGuard;
use crate::store::ConversationStore;

/// Runs `operation` until it is ready.
///
/// The room locks it takes are held here and released when it asks, or when
/// the run ends, whatever ends it.
pub(crate) async fn run<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    mut operation: O,
) -> Result<O::Output, KitError> {
    let mut locks: HashMap<RoomId, RoomGuard<'_>> = HashMap::new();
    loop {
        let step = {
            let mut environment = inner.environment();
            operation.poll(&mut **environment)
        };
        for notice in step.notices {
            perform(inner, notice);
        }
        let effects = match step.next {
            Next::Ready(output) => return Ok(output),
            // Every effect is completed before the next poll.
            Next::Waiting => return Err(KitError::Stalled),
            Next::Effects(effects) => effects,
        };
        for (id, outcome) in execute(inner, &mut locks, effects).await {
            operation.complete(id, outcome)?;
        }
    }
}

/// Executes one step's effects. The room lock is taken and released in
/// place; the other calls run concurrently, and their outcomes come back in
/// the order they finish.
async fn execute<'kit, S: ConversationStore>(
    inner: &'kit Arc<Inner<S>>,
    locks: &mut HashMap<RoomId, RoomGuard<'kit>>,
    effects: Vec<Effect>,
) -> Vec<(EffectId, Outcome)> {
    let mut outcomes = Vec::with_capacity(effects.len());
    let mut calls = JoinSet::new();
    let mut effect_of_task = HashMap::new();
    for Effect { id, request } in effects {
        match request {
            Request::AcquireLock { room_id } => {
                let guard = inner.locks.lock(&room_id).await;
                locks.insert(room_id, guard);
                outcomes.push((id, Outcome::Locked));
            }
            Request::ReleaseLock { room_id } => {
                locks.remove(&room_id);
                outcomes.push((id, Outcome::Released));
            }
            request @ (Request::LoadContext { .. }
            | Request::LoadBindings { .. }
            | Request::FindByIdempotencyKey { .. }
            | Request::Commit { .. }
            | Request::PersistSideEffects { .. }
            | Request::CreateRoom { .. }
            | Request::UpdateRoom { .. }
            | Request::StoreBinding { .. }
            | Request::DeleteBinding { .. }
            | Request::HandleInbound { .. }
            | Request::OnEvent { .. }
            | Request::Deliver { .. }
            | Request::RunHook { .. }) => {
                let inner = Arc::clone(inner);
                let task = calls.spawn(async move { call(&inner, request).await });
                effect_of_task.insert(task.id(), id);
            }
        }
    }
    while let Some(joined) = calls.join_next_with_id().await {
        let (task, outcome) = match joined {
            Ok(finished) => finished,
            Err(failure) => {
                let error = if failure.is_panic() {
                    ErrorRecord::new("Panic", "the call panicked")
                } else {
                    ErrorRecord::new("Cancelled", "the call was cancelled")
                };
                (failure.id(), Outcome::Failed(error))
            }
        };
        if let Some(id) = effect_of_task.get(&task) {
            outcomes.push((*id, outcome));
        } else {
            tracing::error!(
                target: "roomkit::driver",
                ?task,
                "a finished call matches no effect; the operation will not hear of it"
            );
        }
    }
    outcomes
}

/// Carries out a request on the store, a channel or a hook.
async fn call<S: ConversationStore>(inner: &Inner<S>, request: Request) -> Outcome {
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
        Request::LoadContext { .. }
        | Request::LoadBindings { .. }
        | Request::FindByIdempotencyKey { .. }
        | Request::Commit { .. }
        | Request::PersistSideEffects { .. }
        | Request::CreateRoom { .. }
        | Request::UpdateRoom { .. }
        | Request::StoreBinding { .. }
        | Request::DeleteBinding { .. } => store_call(&inner.store, request).await,
        Request::AcquireLock { .. } | Request::ReleaseLock { .. } => Outcome::Failed(
            ErrorRecord::new("Internal", "a lock request reached the call executor"),
        ),
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
        Request::LoadBindings { room_id } => {
            store.list_bindings(&room_id).await.map(Outcome::Bindings)
        }
        Request::FindByIdempotencyKey {
            room_id,
            channel_id,
            key,
        } => store
            .find_by_idempotency_key(&room_id, &channel_id, &key)
            .await
            .map(|event| Outcome::Idempotent(event.map(Box::new))),
        Request::Commit { event, counters } => store
            .commit(event, counters)
            .await
            .map(|()| Outcome::Committed),
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
        Request::AcquireLock { .. }
        | Request::ReleaseLock { .. }
        | Request::HandleInbound { .. }
        | Request::OnEvent { .. }
        | Request::Deliver { .. }
        | Request::RunHook { .. } => {
            return Outcome::Failed(ErrorRecord::new(
                "Internal",
                "a request reached the store that is not the store's",
            ));
        }
    };
    answer.unwrap_or_else(|error| Outcome::Failed(error.to_record()))
}

/// Performs a notice: publishes a framework event, or runs observer hooks in
/// the background, logging what they raise (RFC §9.3).
fn perform<S: ConversationStore>(inner: &Arc<Inner<S>>, notice: Notice) {
    match notice {
        Notice::FrameworkEvent(event) => inner.publish(event),
        Notice::AsyncHooks {
            trigger,
            hooks,
            event,
            context,
        } => {
            for hook in hooks {
                let Some((handler, spec)) = inner.hook_with_spec(hook) else {
                    continue;
                };
                let inner = Arc::clone(inner);
                let (event, context) = ((*event).clone(), (*context).clone());
                tokio::spawn(async move {
                    let room_id = event.room_id.clone();
                    let outcome = hooks::run(handler, event, context, spec.timeout).await;
                    inner.report_observer(&spec, trigger, room_id, &outcome);
                });
            }
        }
    }
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
