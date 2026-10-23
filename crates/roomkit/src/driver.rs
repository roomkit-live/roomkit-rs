//! The driver: runs any operation of the core to its end, executing the
//! effects it asks for and performing its notices.

use std::collections::HashMap;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, ChannelOutput, ErrorRecord, RoomContext, RoomEvent, RoomId};
use roomkit_core::ops::{Effect, EffectId, Next, Notice, Operation, Outcome, Request};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookId, HookTrigger};
use tokio::task::JoinSet;

use crate::channel::{ChannelError, EventDraft};
use crate::error::KitError;
use crate::hooks;
use crate::kit::Inner;
use crate::locks::RoomGuard;
use crate::store::ConversationStore;

/// The room locks and lanes a run holds, released when the operation asks,
/// or when the run ends, whatever ends it.
#[derive(Default)]
struct Held<'kit> {
    locks: HashMap<RoomId, RoomGuard<'kit>>,
    lanes: HashMap<RoomId, RoomGuard<'kit>>,
}

/// Runs `operation` until it is ready.
pub(crate) async fn run<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    mut operation: O,
) -> Result<O::Output, KitError> {
    let mut held = Held::default();
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
        for (id, outcome) in execute(inner, &mut held, effects).await {
            operation.complete(id, outcome)?;
        }
    }
}

/// Executes one step's effects. A room's lock and lane are taken and let go
/// in place; the other calls run concurrently, and their outcomes come back
/// in the order they finish.
async fn execute<'kit, S: ConversationStore>(
    inner: &'kit Arc<Inner<S>>,
    held: &mut Held<'kit>,
    effects: Vec<Effect>,
) -> Vec<(EffectId, Outcome)> {
    let mut outcomes = Vec::with_capacity(effects.len());
    let mut calls = JoinSet::new();
    let mut effect_of_task = HashMap::new();
    for Effect { id, request } in effects {
        match request {
            Request::AcquireLock { room_id } => {
                let guard = inner.locks.lock(&room_id).await;
                held.locks.insert(room_id, guard);
                outcomes.push((id, Outcome::Locked));
            }
            Request::ReleaseLock { room_id } => {
                held.locks.remove(&room_id);
                outcomes.push((id, Outcome::Released));
            }
            Request::EnterLane { room_id } => {
                let guard = inner.lanes.lock(&room_id).await;
                held.lanes.insert(room_id, guard);
                outcomes.push((id, Outcome::Entered));
            }
            Request::LeaveLane { room_id } => {
                held.lanes.remove(&room_id);
                outcomes.push((id, Outcome::Left));
            }
            request @ (Request::LoadContext { .. }
            | Request::LoadBindings { .. }
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
            | Request::HandleInbound { .. }
            | Request::OnEvent { .. }
            | Request::Deliver { .. }
            | Request::RunHook { .. }
            | Request::RunObservers { .. }
            | Request::ResolveIdentity { .. }
            | Request::Route { .. }) => {
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
async fn call<S: ConversationStore>(inner: &Arc<Inner<S>>, request: Request) -> Outcome {
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
        | Request::LoadBindings { .. }
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
        | Request::LeaveLane { .. } => Outcome::Failed(ErrorRecord::new(
            "Internal",
            "a lock or lane request reached the call executor",
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
async fn run_observers<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    trigger: HookTrigger,
    hooks: Vec<HookId>,
    event: RoomEvent,
    context: RoomContext,
) {
    let mut observers = JoinSet::new();
    for hook in hooks {
        if let Some(observing) = observe(inner, trigger, hook, &event, &context) {
            observers.spawn(observing);
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
