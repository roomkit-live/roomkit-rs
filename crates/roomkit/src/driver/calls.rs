//! The calls the driver runs as tasks, one handler per family of request:
//! the store's, a channel's, hooks' and the router's, each within its
//! execution context; and the notices it performs without answering.

use std::convert::Infallible;
use std::mem;
use std::sync::Arc;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ErrorRecord, EventId, EventUpdates,
    Metadata, ModelError, Observation, RoomContext, RoomEvent, RoomId, RoomUpdates, Task,
    Timestamp,
};
use roomkit_core::ops::{
    AfterDelivery, CHANNEL_NOT_REGISTERED, ChannelOutcome, ChannelRequest, EffectId, Environment,
    FrameworkEvent as CoreEvent, HookAnswer, HookRequest, KIT_CLOSED, Notice, Outcome,
    RecentEvents, StoreOutcome, StoreRequest,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookTrigger};
use roomkit_core::rules::routing::RouteQuery;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;

use crate::channel::ChannelError;
use crate::context::{ExecutionContext, HookBody, RunShared};
use crate::error::RoomKitError;
use crate::events::Handled;
use crate::hooks;
use crate::kit::Inner;
use crate::router;
use crate::sends::CIRCUIT_OPEN;
use crate::store::{
    CommitEventOptions, ConversationStore, DeleteEventOptions, GetConversationOptions,
    PatchRoomMetadataOptions, ResolveIdentityOptions, StoreError, written,
};
use crate::stream::{self, KeptStream, Stops};
use crate::tasks::{Kind, Marks};
use crate::turn::TurnKit;
use crate::turns::{self, Answer};
use crate::unwind::unwinding;

/// The key of a room's private state holding its register of authors (RFC
/// §10.1 step 12, §14.1), Python's `AUTHOR_REGISTER`.
const AUTHOR_REGISTER: &str = "author_register";

/// Carries out a request on the store (RFC §14), through Python's store
/// calls.
pub(super) async fn store<S: ConversationStore>(store: &S, request: StoreRequest) -> Outcome {
    let answer = match request {
        StoreRequest::LoadContext {
            room_id,
            recent_events,
        } => {
            return match load_context(store, &room_id, recent_events).await {
                Ok(Some(context)) => StoreOutcome::Context(Box::new(context)).into(),
                Ok(None) => StoreOutcome::RoomNotFound.into(),
                Err(error) => Outcome::Failed(error.to_record()),
            };
        }
        StoreRequest::GetEventByIdempotencyKey {
            room_id,
            channel_id,
            key,
        } => key_held(store, &room_id, &key, channel_id.as_ref()).await,
        StoreRequest::GetEvent { event_id } => store
            .get_event(&event_id)
            .await
            .map(|event| StoreOutcome::Event(event.map(Box::new))),
        StoreRequest::ListEvents { room_id } => crate::store::whole_timeline(store, &room_id)
            .await
            .map(StoreOutcome::Events),
        StoreRequest::Commit {
            event,
            target,
            activity,
            author_register,
        } => commit(
            store,
            *event,
            target.map(|target| *target),
            activity,
            author_register,
        )
        .await
        .map(|stored| StoreOutcome::Committed(Box::new(stored))),
        StoreRequest::UpdateEvent { event_id, updates } => update_event(store, &event_id, updates)
            .await
            .map(|()| StoreOutcome::Stored),
        StoreRequest::DeleteEvent {
            room_id,
            event_id,
            cascade_replies,
        } => store
            .delete_event(&room_id, &event_id, DeleteEventOptions { cascade_replies })
            .await
            .map(StoreOutcome::Deleted),
        StoreRequest::PersistSideEffects {
            tasks,
            observations,
            ..
        } => persist_side_effects(store, tasks, observations)
            .await
            .map(|()| StoreOutcome::Persisted),
        StoreRequest::CreateRoom { room } => {
            store.create_room(room).await.map(|_| StoreOutcome::Stored)
        }
        StoreRequest::UpdateRoom { room_id, updates } => update_room(store, &room_id, updates)
            .await
            .map(|()| StoreOutcome::Stored),
        StoreRequest::StoreBinding { binding } => store_binding(store, binding)
            .await
            .map(|()| StoreOutcome::Stored),
        StoreRequest::DeleteBinding {
            room_id,
            channel_id,
        } => store
            .remove_binding(&room_id, &channel_id)
            .await
            .map(|_| StoreOutcome::Stored),
        StoreRequest::ResolveIdentity {
            channel_type,
            address,
            organization_id,
        } => store
            .resolve_identity(
                &channel_type,
                &address,
                ResolveIdentityOptions { organization_id },
            )
            .await
            .map(StoreOutcome::Resolved),
    };
    answer.map_or_else(|error| Outcome::Failed(error.to_record()), Outcome::from)
}

/// The room's context (RFC §10.1 step 3), as the Python implementation's
/// kit builds it: the room, its bindings and participants, its latest
/// messages, as many as `recent_events` settles for its bindings, a blocked
/// one included (`get_conversation`),
/// and its register of authors, its private state (gap G-089); `None` for
/// a room the store lacks.
async fn load_context<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    recent_events: RecentEvents,
) -> Result<Option<RoomContext>, StoreError> {
    let (room, bindings, participants) = store.load_room_context(room_id).await?;
    let Some(room) = room else {
        return Ok(None);
    };
    // Nothing bound reads history and no hook is registered: no read.
    let limit = recent_events.limit(&bindings);
    let recent_events = if limit == 0 {
        Vec::new()
    } else {
        let options = GetConversationOptions {
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
            after_index: None,
        };
        store.get_conversation(room_id, options).await?
    };
    let author_register = store.get_private_state(room_id, AUTHOR_REGISTER).await?;
    Ok(Some(RoomContext {
        room,
        bindings,
        participants,
        recent_events,
        author_register,
    }))
}

/// What the room holds under `key`, from `channel_id` when it names one, as
/// Python's pipeline reads it (RFC §13.4): `check_idempotency`, then the
/// event the key committed; a key held whose event the store cannot read
/// back is answered `KeySeen`, which the pipeline reads as a blocked
/// duplicate, never processed again.
async fn key_held<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    key: &str,
    channel_id: Option<&ChannelId>,
) -> Result<StoreOutcome, StoreError> {
    if !store.check_idempotency(room_id, key, channel_id).await? {
        return Ok(StoreOutcome::Idempotent(None));
    }
    let event = store
        .get_event_by_idempotency_key(room_id, key, channel_id)
        .await?;
    Ok(event.map_or(StoreOutcome::KeySeen, |event| {
        StoreOutcome::Idempotent(Some(Box::new(event)))
    }))
}

/// Commits `event` (RFC §10.1 step 12): the room's register of authors its
/// record changed written first, as the Python implementation writes it,
/// then the commit, the room's activity at `activity` when it is one.
async fn commit<S: ConversationStore>(
    store: &S,
    event: RoomEvent,
    target: Option<RoomEvent>,
    activity: Option<Timestamp>,
    author_register: Option<Metadata>,
) -> Result<RoomEvent, StoreError> {
    let room_id = event.room_id.clone();
    if let Some(register) = author_register {
        store
            .set_private_state(&room_id, AUTHOR_REGISTER, Some(register))
            .await?;
    }
    let options = CommitEventOptions {
        target,
        activity: activity.is_some(),
    };
    let committed = store.commit_event(&room_id, event, options);
    match activity {
        Some(at) => written::at(at, committed).await,
        None => committed.await,
    }
}

/// Writes `updates` on the room (RFC §14.1): its private state through
/// `set_private_state`; a write of its metadata alone through
/// `patch_room_metadata`, the partial write a commit cannot lose, as the
/// Python implementation writes step 15's; any other through a fresh read
/// and `update_room`, which its caller runs under the room's lock.
async fn update_room<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    mut updates: RoomUpdates,
) -> Result<(), StoreError> {
    for (key, value) in mem::take(&mut updates.private_state) {
        store.set_private_state(room_id, &key, value).await?;
    }
    if updates.is_empty() {
        return Ok(());
    }
    let missing = || StoreError::RoomNotFound(room_id.clone());
    let lifecycle = updates.status.is_some()
        || updates.closed_at.is_some()
        || updates.paused_at.is_some()
        || updates.last_activity_at.is_some();
    if !lifecycle {
        let options = PatchRoomMetadataOptions {
            unset: mem::take(&mut updates.removed_metadata),
        };
        let patch = store.patch_room_metadata(room_id, mem::take(&mut updates.metadata), options);
        let patched = match updates.updated_at {
            Some(at) => written::at(at, patch).await?,
            None => patch.await?,
        };
        return patched.map(drop).ok_or_else(missing);
    }
    let mut room = store.get_room(room_id).await?.ok_or_else(missing)?;
    updates.apply(&mut room);
    store.update_room(room).await?;
    Ok(())
}

/// Stores `binding` (RFC §14.1): `add_binding` for a channel the room does
/// not bind yet, `update_binding` for one it binds, as the Python
/// implementation's kit calls them.
async fn store_binding<S: ConversationStore>(
    store: &S,
    binding: ChannelBinding,
) -> Result<(), StoreError> {
    let bound = store
        .get_binding(&binding.room_id, &binding.channel_id)
        .await?;
    match bound {
        Some(_) => store.update_binding(binding).await?,
        None => store.add_binding(binding).await?,
    };
    Ok(())
}

/// Writes `updates` on the stored event `event_id` (RFC §14.1): a read,
/// then `update_event` of the event whole, its delivery results kept by
/// the store (RFC §14.3).
async fn update_event<S: ConversationStore>(
    store: &S,
    event_id: &EventId,
    updates: EventUpdates,
) -> Result<(), StoreError> {
    let mut event = store
        .get_event(event_id)
        .await?
        .ok_or_else(|| StoreError::EventNotFound(event_id.clone()))?;
    updates.apply(&mut event);
    store.update_event(event).await?;
    Ok(())
}

/// Stores a pass's side effects one by one (RFC §10.1 step 15), as
/// Python's `_persist_side_effects` does.
async fn persist_side_effects<S: ConversationStore>(
    store: &S,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
) -> Result<(), StoreError> {
    for task in tasks {
        store.add_task(task).await?;
    }
    for observation in observations {
        store.add_observation(observation).await?;
    }
    Ok(())
}

/// Calls a channel (RFC §6.1): a parse, a turn, a delivery or the read of
/// a turn's stream; a parse drafts its event at `environment`'s instant,
/// the run's. A turn that answers with a stream leaves it with its run,
/// `shared`, by its effect, `effect`, until it is read. Work a close spared
/// calls no channel once the kit is sealed, its channels closed, the call
/// refused (RFC §12.10.4): a turn with its link's `backend_closed` (RFC
/// §10.6.8), any other call `kit_closed`, as a write is.
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
            let room_id = context.room.id.clone();
            match channel.handle_inbound(message, *context).await {
                Ok(mut event) => match event.check() {
                    Ok(()) => {
                        // The pipeline's own (RFC §8.1, §10.1 step 4): the id
                        // the core gives, the room, the instant.
                        event.id = id;
                        event.room_id = room_id;
                        event.created_at = environment.now();
                        ChannelOutcome::Parsed(Box::new(event)).into()
                    }
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
            let window = inner.config().recent_events_limit(&context.bindings);
            let context =
                crate::history::turn_context(&*inner.store, &binding, &event, *context, window);
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
            // A breaker's transition is published without waiting for its
            // handlers, as Python schedules it: a slow one cannot stall the
            // send that tripped it.
            let emit = |notice| drop(inner.emit(notice));
            let sent = inner
                .sends
                .deliver(&*channel, binding, event, *context, emit);
            if let Ok(answered) = sent.await {
                return channel_output(answered);
            }
            let (code, message) = CIRCUIT_OPEN;
            Outcome::Failed(ErrorRecord::new(code, message))
        }
        ChannelRequest::ReadStream {
            turn,
            responder,
            answers,
            target,
        } => {
            let kept = shared.take_stream(turn);
            // The kit's close waits, within its grace, for the read of a turn
            // it cut to store what the turn said.
            let _reading = kept.as_ref().map(|_| inner.turns.reading());
            let stops = Stops {
                read: shared.stop_signal(effect),
                kit: inner.turns.cutting(),
            };
            Box::pin(stream::read(
                inner, kept, stops, responder, *answers, target,
            ))
            .await
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
            let outcome = match inner.kept_hook(hook) {
                Some(kept) => hooks::run_kept(kept, *event, *context, timeout).await,
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

/// The refusal of `request`, a channel's call work a close spared made
/// once the kit was sealed: a turn's, its submission or the read of its
/// stream, its link's `backend_closed`, the turn still open as its owner
/// closed (RFC §10.6.8); any other, the close's, `kit_closed` (RFC
/// §12.10.4).
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
        | ChannelRequest::Deliver { .. } => cut_by_close(),
    }
}

/// The failure of a call the kit's close cut or refused (RFC §12.10.4): a
/// send so read `failed`, code `kit_closed`, retryable.
pub(crate) fn cut_by_close() -> ErrorRecord {
    ErrorRecord::new(KIT_CLOSED, "the kit closed before this call ended")
}

/// What a write of the store came to, once it ended; nothing while it runs.
pub(crate) use crate::context::Written;

/// The run's hold on a write's task once the write ended: never sent,
/// dropped once the run went on from the write's outcome.
pub(super) type WentOn = oneshot::Sender<Infallible>;

/// Carries out `request`, a write of the store, as a task of its own the
/// kit tracks, which the run's abort does not end: a write decided runs to
/// its end, whatever becomes of its run, as the Python implementation runs
/// its decided writes (RFC §13.6), but the kit's close, which gives it its
/// grace, then cuts it (RFC §12.10.4). The task ends once the run went on
/// from the write's outcome too, when the run keeps its hold, so that an
/// inbound run's commit takes its place in the room's lane before its task
/// ends. Returns the run's call, which answers the write's outcome, where
/// anyone else reads it, and the run's hold.
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
        let Err(_went_on) = run_going_on.await;
    };
    // Refused once the close's grace ran out, but for work the close
    // spared, and once the kit is sealed.
    let spawned = inner.tasks.spawn(Kind::Write, writing).is_some();
    let mut read = written.clone();
    let call = async move {
        if !spawned {
            return Outcome::Failed(cut_by_close());
        }
        outcome_of(&mut read).await
    };
    (call, written, went_on)
}

/// Logs `error`, a write's that failed once its caller was gone, as
/// Python's `run_to_its_end` logs it: nobody else learns of it. One the
/// kit's close caused, a write it refused, is expected, and noted at debug
/// (RFC §12.10.4).
pub(super) fn log_left_failed(error: &ErrorRecord) {
    if error.error_type == KIT_CLOSED {
        tracing::debug!(
            target: "roomkit::driver",
            error_type = %error.error_type,
            message = %error.message,
            "the writes a gone caller left ended as the kit closed"
        );
        return;
    }
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
pub(super) fn perform<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    notice: Notice,
) -> Option<Handled> {
    match notice {
        Notice::FrameworkEvent(event) => return inner.emit(event),
        // The run's own: the driver stops its read (`Driver::notice`).
        Notice::StopReading { .. } => {}
        Notice::AsyncHooks {
            trigger,
            hooks,
            event,
            context,
            after_delivery,
            then,
        } => {
            let observers: Vec<_> = hooks
                .into_iter()
                .filter_map(|hook| observe(inner, trigger, hook, &event, &context))
                .collect();
            let countdown = then.map(|then| Countdown::new(inner, observers.len(), then));
            for observing in observers {
                let ticket = countdown.as_ref().map(Countdown::ticket);
                let observing =
                    in_its_work(trigger, &event.room_id, after_delivery.as_ref(), observing);
                let observing = async move {
                    observing.await;
                    if let Some(handled) = ticket.and_then(Ticket::finish) {
                        handled.wait().await;
                    }
                };
                // Nothing waits for an observer: a close one calls spares it
                // alone; the kit's close gives it its grace, then cuts it.
                if inner.tasks.spawn_alone(Kind::Observer, observing).is_none() {
                    tracing::debug!(
                        target: "roomkit::hooks",
                        ?trigger,
                        "the kit is shut down: an observer hook is not run"
                    );
                }
            }
            if let Some(countdown) = countdown {
                // No observer could run: the event follows at once.
                return countdown.ticket().finish();
            }
        }
    }
    None
}

/// The observers a framework event follows, as the Python implementation's
/// `then` follows its hooks (RFC §8.2 `event_processed`): the last of them
/// to end, finished, failed or cut, publishes it. No task of its own waits
/// for them, which a close called from one of them would wait for in turn.
struct Countdown {
    shared: Arc<CountdownState>,
}

struct CountdownState {
    left: std::sync::atomic::AtomicUsize,
    then: std::sync::Mutex<Option<CoreEvent>>,
    publish: Box<dyn Fn(CoreEvent) -> Option<Handled> + Send + Sync>,
}

/// One observer's place in a [`Countdown`]: its end counts, however it
/// ends.
struct Ticket {
    shared: Option<Arc<CountdownState>>,
}

impl Countdown {
    /// A countdown over `observers` observers, then `then`, the event
    /// published by `inner`; one more ticket stands for the firing itself,
    /// which [`perform`] ends once every observer is spawned.
    fn new<S: ConversationStore>(inner: &Arc<Inner<S>>, observers: usize, then: CoreEvent) -> Self {
        let kit = Arc::downgrade(inner);
        Self {
            shared: Arc::new(CountdownState {
                left: std::sync::atomic::AtomicUsize::new(observers.saturating_add(1)),
                then: std::sync::Mutex::new(Some(then)),
                publish: Box::new(move |event| kit.upgrade().and_then(|inner| inner.emit(event))),
            }),
        }
    }

    fn ticket(&self) -> Ticket {
        Ticket {
            shared: Some(Arc::clone(&self.shared)),
        }
    }
}

impl Ticket {
    /// Counts this end: the event's handlers to wait for when it was the
    /// last.
    fn finish(mut self) -> Option<Handled> {
        let shared = self.shared.take()?;
        shared.count()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        // An observer that panicked or was cut ends all the same.
        if let Some(shared) = self.shared.take() {
            drop(shared.count());
        }
    }
}

impl CountdownState {
    fn count(&self) -> Option<Handled> {
        let previous = self.left.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        if previous != 1 {
            return None;
        }
        let then = self
            .then
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()?;
        (self.publish)(then)
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
            if inner.tasks.is_closing() && failure.is_cancelled() {
                tracing::debug!(target: "roomkit::hooks", ?trigger, "an awaited observer hook was cut as the kit closed");
                continue;
            }
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
    let (kept, spec) = inner.hook_with_spec(hook)?;
    let inner = Arc::clone(inner);
    let (event, context) = (event.clone(), context.clone());
    Some(async move {
        let room_id = event.room_id.clone();
        let outcome = hooks::run_kept(kept, event, context, spec.timeout).await;
        inner.report_observer(&spec, trigger, room_id, &outcome);
    })
}

fn channel_output(result: Result<ChannelOutput, ChannelError>) -> Outcome {
    match result {
        Ok(output) => checked_output(Box::new(output)),
        Err(error) => channel_failed(&error),
    }
}

/// What a channel's `on_event()` or `deliver()` failing with `error` reads
/// as: the provider's refusal it carries, when it is the kit's
/// `ProviderDelivery`, Python's `ProviderDeliveryError`; a kit's error, a
/// failure of the code its class carries, `kit_closed`, or else of its
/// Python class's name, as Python's delivery reads an error's `code` or
/// its type's name (RFC §5.13); any other a failure of type
/// `ChannelError`, a boxed error having no class name.
fn channel_failed(error: &ChannelError) -> Outcome {
    let kit_error = error.downcast_ref::<RoomKitError>();
    let code = match kit_error {
        Some(RoomKitError::ProviderDelivery { result }) => {
            return ChannelOutcome::ProviderDelivery(Box::new(result.clone())).into();
        }
        Some(kit_error) => kit_error.code().unwrap_or_else(|| kit_error.class_name()),
        None => "ChannelError".to_owned(),
    };
    Outcome::Failed(ErrorRecord::new(code, error.to_string()))
}

/// A channel's answer, unless a response it carries holds a content its
/// model refuses, built by struct literal past the model's check: the call
/// then reads as the channel raising the `ValueError` Python's model raises
/// when built, none of its responses kept. A response kept is written as
/// its constructor writes it.
fn checked_output(mut output: Box<ChannelOutput>) -> Outcome {
    match output
        .response_events
        .iter_mut()
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
