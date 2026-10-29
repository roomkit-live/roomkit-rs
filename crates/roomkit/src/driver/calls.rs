//! The calls the driver runs as tasks, one handler per family of request:
//! the store's, a channel's, hooks' and the router's, each within its
//! execution context; and the notices it performs without answering.

use std::convert::Infallible;
use std::sync::Arc;

use roomkit_core::model::{
    ChannelCategory, ChannelId, ChannelOutput, ErrorRecord, ModelError, RoomContext, RoomEvent,
    RoomId,
};
use roomkit_core::ops::{
    AfterDelivery, CHANNEL_NOT_REGISTERED, ChannelOutcome, ChannelRequest, EffectId, Environment,
    HookAnswer, HookRequest, Notice, Outcome, StoreOutcome, StoreRequest,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookTrigger};
use roomkit_core::rules::routing::RouteQuery;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;

use crate::channel::{ChannelError, EventDraft};
use crate::context::{ExecutionContext, HookBody, RunShared};
use crate::hooks;
use crate::kit::Inner;
use crate::router;
use crate::store::ConversationStore;
use crate::stream::{self, KeptStream, Stops};
use crate::tasks::{Kind, Marks};
use crate::turn::TurnKit;
use crate::turns::{self, Answer};
use crate::unwind::unwinding;

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
        StoreRequest::GetEventByIdempotencyKey {
            room_id,
            channel_id,
            key,
        } => store
            .get_event_by_idempotency_key(&room_id, channel_id.as_ref(), &key)
            .await
            .map(|event| StoreOutcome::Idempotent(event.map(Box::new))),
        StoreRequest::GetEvent { event_id } => store
            .get_event(&event_id)
            .await
            .map(|event| StoreOutcome::Event(event.map(Box::new))),
        StoreRequest::ListEvents { room_id } => crate::store::whole_timeline(store, &room_id)
            .await
            .map(StoreOutcome::Events),
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
            .map(StoreOutcome::Resolved),
    };
    answer.map_or_else(|error| Outcome::Failed(error.to_record()), Outcome::from)
}

/// Calls a channel (RFC §6.1): a parse, a turn, a delivery or the read of
/// a turn's stream; a parse drafts its event at `environment`'s instant,
/// the run's. A turn that answers with a stream leaves it with its run,
/// `shared`, by its effect, `effect`, until it is read. Work a close spared
/// calls no channel once the kit is sealed, its channels closed, the call
/// refused with an error naming the shutdown (RFC §12.10.4): a turn with
/// its link's `backend_closed` (RFC §10.6.8), any other call as a write is.
pub(super) async fn channel<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    environment: &(dyn Environment + Send + Sync),
    shared: &RunShared,
    effect: EffectId,
    request: ChannelRequest,
) -> Outcome {
    if inner.tasks.refuses(&Marks::current()) {
        return Outcome::Failed(refused_once_sealed(&request));
    }
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
                now: environment.now(),
            };
            match channel.handle_inbound(message, *context, draft).await {
                Ok(event) => match event.check() {
                    Ok(()) => ChannelOutcome::Parsed(Box::new(event)).into(),
                    Err(refusal) => refused_content(&refusal),
                },
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
            if binding.category != ChannelCategory::Intelligence {
                return channel_output(channel.on_event(event, binding, *context).await);
            }
            // A turn: it runs through the turn backend (RFC §10.6), on the
            // room's conversation and its channel's tool rows (RFC §6.4).
            let context = crate::history::turn_context(&*inner.store, &binding, &event, *context);
            let context = context.await;
            let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(inner));
            let cut = |turn| shared.turn_submitted(effect, turn);
            let answered = inner.turns.run(&kit, &binding, event, context, cut).await;
            let outcome = match answered {
                Ok(Answer::Output(output)) => checked_output(output),
                Ok(Answer::Streamed(output, turn)) => {
                    shared.keep_stream(effect, KeptStream { turn });
                    ChannelOutcome::Streamed(output).into()
                }
                Err(error) => Outcome::Failed(error),
            };
            // Its stream kept first, the turn stops being one the call
            // awaits: an abort between the two reads the stream kept, never
            // a delivery cut running.
            shared.turn_answered(effect);
            outcome
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
        ChannelRequest::ReadStream {
            turn,
            responder,
            answers,
            target,
        } => {
            let kept = shared.take_stream(turn);
            let taken = kept.is_some();
            let unit = kept.as_ref().map(|kept| kept.turn.unit());
            let _reading = kept.as_ref().map(|kept| inner.turns.reading(&kept.turn));
            let stops = Stops {
                read: shared.stop_signal(effect),
                kit: inner.turns.cutting(),
                turn: unit,
            };
            let read = Box::pin(stream::read(
                inner, kept, stops, responder, *answers, target,
            ))
            .await;
            // A read the run's abort drops never returns: its stream stays
            // open, as the abort reads it. One that returns as the kit's
            // close drains stays open too, until the run ends: an abort
            // before that end reads the turn's end, which the run had not
            // reported yet.
            if taken && !inner.tasks.is_draining() {
                shared.read_returned();
            }
            read
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
    let options = router::options_of(query);
    let routed = inner
        .router()
        .route(&query.channel_id, &query.channel_type, options)
        .await;
    match routed {
        Ok(room_id) => Outcome::Routed(room_id),
        Err(error) => Outcome::Failed(ErrorRecord::new("RouterError", error.to_string())),
    }
}

/// The kind of the failure of a write the kit refused, sealed: the work a
/// close spared asked it once the close sealed the kit (RFC §12.10.4).
pub(super) const SHUT_DOWN: &str = "ShutDown";

/// The refusal of `request`, a channel's call work a close spared made
/// once the kit was sealed: a turn's, its submission or the read of its
/// stream, its link's `backend_closed`, the turn still open as its owner
/// closed (RFC §10.6.8); any other, the seal's.
fn refused_once_sealed(request: &ChannelRequest) -> ErrorRecord {
    match request {
        ChannelRequest::OnEvent { binding, .. }
            if binding.category == ChannelCategory::Intelligence =>
        {
            turns::closed()
        }
        ChannelRequest::ReadStream { .. } => turns::closed(),
        ChannelRequest::HandleInbound { .. }
        | ChannelRequest::OnEvent { .. }
        | ChannelRequest::Deliver { .. } => ErrorRecord::new(SHUT_DOWN, "the kit is shut down"),
    }
}

/// What a write of the store came to, once it ended; nothing while it runs.
pub(crate) use crate::context::Written;

/// The run's hold on a write's task once the write ended: never sent,
/// dropped once the run went on from the write's outcome.
pub(super) type WentOn = oneshot::Sender<Infallible>;

/// Carries out `request`, a write of the store, as a task of its own the
/// kit tracks, which the run's abort does not end: a write decided runs to
/// its end, whatever becomes of its run, as the Python implementation runs
/// its decided writes (RFC §13.6). The task ends once the run went on from
/// the write's outcome too, when the run keeps its hold: the kit's close,
/// which waits for the writes, sees an inbound run's commit take its place
/// in the room's lane before it seals the lanes, as Python's decided write
/// covers the commit and the lane's enqueue (`_write_decision`). Returns
/// the run's call, which answers the write's outcome, where anyone else
/// reads it, and the run's hold.
pub(super) fn write<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    request: StoreRequest,
) -> (
    impl Future<Output = Outcome> + Send + 'static,
    Written,
    WentOn,
) {
    let (ended, written) = watch::channel(None);
    let (went_on, run_going_on) = oneshot::channel();
    let writer = Arc::clone(inner);
    let writing = async move {
        let storing = writer.tasks.storing();
        let outcome = unwinding(store(&*writer.store, request))
            .await
            .unwrap_or_else(|_panic| Outcome::Failed(panicked()));
        if let Outcome::Failed(error) = &outcome
            && ended.is_closed()
        {
            // Nobody reads it: its run was aborted, its caller gone.
            log_left_failed(error);
        }
        ended.send_replace(Some(outcome));
        drop(storing);
        let Err(_went_on) = run_going_on.await;
    };
    // A write is spawned whatever the kit's state, but once sealed for
    // work the close spared.
    let spawned = inner.tasks.spawn(Kind::Write, writing).is_some();
    let mut read = written.clone();
    let call = async move {
        if !spawned {
            return Outcome::Failed(ErrorRecord::new(SHUT_DOWN, "the kit is shut down"));
        }
        outcome_of(&mut read).await
    };
    (call, written, went_on)
}

/// Logs `error`, a write's that failed once its caller was gone, as
/// Python's `run_to_its_end` logs it: nobody else learns of it.
pub(super) fn log_left_failed(error: &ErrorRecord) {
    tracing::error!(
        target: "roomkit::driver",
        error_type = %error.error_type,
        message = %error.message,
        "the writes a gone caller left failed"
    );
}

/// The failure of a call that panicked.
pub(super) fn panicked() -> ErrorRecord {
    ErrorRecord::new("Panic", "the call panicked")
}

/// What the write `written` tells came to, once it ended: a failure for one
/// that never ended, its task dropped with the runtime.
pub(crate) async fn outcome_of(written: &mut Written) -> Outcome {
    match written.wait_for(Option::is_some).await {
        Ok(outcome) => outcome.clone().unwrap_or(Outcome::DeadlinePassed),
        Err(_dropped) => Outcome::Failed(ErrorRecord::new(
            "Cancelled",
            "the store's write did not end",
        )),
    }
}

/// Performs a notice: publishes a framework event, or runs observer hooks in
/// the background, as tasks the kit tracks, logging what they raise (RFC
/// §9.3). A background observer runs outside every call's execution
/// context: it runs after the fact, and nothing awaits it as its own. The
/// body of one a cascade fires once an event's delivery set has run, of an
/// ASYNC hook of `BEFORE_BROADCAST` observing the event its chain allowed,
/// or of one a direct change fires, is that work while it runs (a
/// [`HookBody`]): its
/// calls into the room draw on the work's hook calls, at least one deeper
/// than its event (RFC §8.3, §9.5.1, §10.1 step 16, gap G-087). Once the
/// kit is shut down, none runs.
pub(super) fn perform<S: ConversationStore>(inner: &Arc<Inner<S>>, notice: Notice) {
    match notice {
        Notice::FrameworkEvent(event) => inner.publish(event),
        // The run's own: the driver stops its read (`Driver::notice`).
        Notice::StopReading { .. } => {}
        Notice::AsyncHooks {
            trigger,
            hooks,
            event,
            context,
            after_delivery,
        } => {
            for hook in hooks {
                let Some(observing) = observe(inner, trigger, hook, &event, &context) else {
                    continue;
                };
                let observing =
                    in_its_work(trigger, &event.room_id, after_delivery.as_ref(), observing);
                // The observers a delivery fires are its cascade's work,
                // drained and cut with it, as the Python implementation's
                // lane runs them; the others run after the fact. Nothing
                // waits for either: a close one calls spares it alone.
                let kind = if trigger == HookTrigger::AfterBroadcast {
                    Kind::Work
                } else {
                    Kind::Observer
                };
                if inner.tasks.spawn_alone(kind, observing).is_none() {
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

/// `observing`, a hook's body on `trigger`, run as the work
/// `after_delivery` names, in `room_id`, until it is over; as it is outside
/// every work otherwise. A call it makes into the room returns at its
/// decision when `trigger` is an ASYNC trigger's (`AFTER_BROADCAST`, a
/// change's), and once its delivery set has run when it is a SYNC
/// trigger's, `BEFORE_BROADCAST`, as the Python implementation's calls do.
fn in_its_work(
    trigger: HookTrigger,
    room_id: &RoomId,
    after_delivery: Option<&AfterDelivery>,
    observing: impl Future<Output = ()> + Send + 'static,
) -> impl Future<Output = ()> + Send + 'static {
    let detaches = trigger.execution() == HookExecution::Async;
    let body = after_delivery
        .map(|work| HookBody::new(room_id.clone(), work.budget.clone(), work.floor, detaches));
    async move {
        let Some(body) = body else {
            return observing.await;
        };
        let _running = body.running();
        ExecutionContext::of_hook_body(body).scope(observing).await;
    }
}

/// Runs observer hooks concurrently and returns once every one has
/// finished or timed out, what they raise logged (RFC §9.3, gap G-050).
/// Awaited, each runs in the context and the marks of the call that awaits
/// it.
async fn run_observers<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    trigger: HookTrigger,
    hooks: Vec<HookId>,
    event: RoomEvent,
    context: RoomContext,
) {
    let execution = ExecutionContext::current();
    let marks = Marks::current();
    let mut observers = JoinSet::new();
    for hook in hooks {
        if let Some(observing) = observe(inner, trigger, hook, &event, &context) {
            observers.spawn(marks.clone().scoped(execution.clone().scope(observing)));
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
        Ok(output) => checked_output(Box::new(output)),
        Err(error) => Outcome::Failed(ErrorRecord::new("ChannelError", error.to_string())),
    }
}

/// A channel's answer, unless a response it carries holds a content its
/// model refuses, built by struct literal past the model's check: the call
/// then reads as the channel raising the `ValueError` Python's model raises
/// when built, none of its responses kept.
fn checked_output(output: Box<ChannelOutput>) -> Outcome {
    match output
        .response_events
        .iter()
        .find_map(|event| event.check().err())
    {
        Some(refusal) => refused_content(&refusal),
        None => ChannelOutcome::Output(output).into(),
    }
}

/// A channel's call that produced a content its model refuses.
fn refused_content(refusal: &ModelError) -> Outcome {
    Outcome::Failed(ErrorRecord::new("ChannelError", refusal.to_string()))
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
