//! The calls the driver runs as tasks, one handler per family of request:
//! the store's, a channel's, hooks' and the router's, each within its
//! execution context; and the notices it performs without answering.

use std::convert::Infallible;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ErrorRecord, EventId, EventUpdates,
    IdentificationStatus, IdentityResult, InboundMessage, Metadata, ModelError, Observation,
    RoomContext, RoomEvent, RoomId, RoomUpdates, Task, Timestamp,
};
use roomkit_core::ops::{
    AfterDelivery, CHANNEL_NOT_REGISTERED, ChannelOutcome, ChannelRequest, DiscussionOutcome,
    DiscussionRequest, EffectId, FrameworkEvent as CoreEvent, HookAnswer, HookRequest,
    IdentityAnswer, IdentityRequest, KIT_CLOSED, NextTurn, Notice, Outcome, RecentEvents,
    StoreOutcome, StoreRequest,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookTrigger};
use roomkit_core::rules::routing::RouteQuery;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;

use crate::channel::{ChannelError, DynChannel};
use crate::classed::ClassedError;
use crate::context::{ExecutionContext, HookBody, RunShared};
use crate::error::RoomKitError;
use crate::events::Handled;
use crate::hooks;
use crate::kit::{
    Inner, end_identity_span, named_correspondent, run_identity_hooks,
    verification_inbound as verification,
};
use crate::room_turns::{self, Queued, Started};
use crate::router;
use crate::sends::CIRCUIT_OPEN;
use crate::store::{
    CommitEventOptions, ConversationStore, DeleteEventOptions, GetConversationOptions,
    PatchRoomMetadataOptions, ResolveIdentityOptions, StoreError, written,
};
use crate::stream::{self, KeptStream, Stops};
use crate::tasks::{Kind, Marks, TaskCut};
use crate::telemetry::{SpanKind, end_hook_span, end_observer_span};
use crate::turn::TurnKit;
use crate::turn_runners::{Answering, FirstPass, Next, Passes, Taken, TurnRunner};
use crate::turns::{self, Answer};
use crate::unwind::{self, unwinding};

/// The key of a room's private state holding its register of authors (RFC
/// §10.1 step 12, §14.1), Python's `AUTHOR_REGISTER`.
const AUTHOR_REGISTER: &str = "author_register";

/// Carries out a request on the store (RFC §14), through Python's store
/// calls.
#[expect(
    clippy::too_many_lines,
    reason = "a dispatch: one arm per store request, each a call of the store"
)]
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
        StoreRequest::ListEventsPage {
            room_id,
            channel_id,
            offset,
            limit,
        } => store
            .list_events(&room_id, router::received_page(&channel_id, offset, limit))
            .await
            .map(StoreOutcome::Events),
        StoreRequest::RecordDeliveryResults {
            room_id,
            event_id,
            results,
        } => store
            .record_delivery_results(&room_id, &event_id, results)
            .await
            .map(|()| StoreOutcome::Stored),
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
        // A room the pipeline creates for its router's id is refused one
        // holding a NUL character, as `create_room` refuses it (RFC §13.5).
        StoreRequest::CreateRoom { room } if room.id.as_str().contains('\0') => Err(
            StoreError::Value("A room id holds no NUL character".to_owned()),
        ),
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
        StoreRequest::AddParticipant { participant } => store
            .add_participant(*participant)
            .await
            .map(|_| StoreOutcome::Stored),
        StoreRequest::UpdateParticipant { participant } => store
            .update_participant(*participant)
            .await
            .map(|_| StoreOutcome::Stored),
    };
    answer.map_or_else(|error| Outcome::Failed(error.to_record()), Outcome::from)
}

/// Carries out identity resolution's request (RFC §10.1 step 5, §11): the
/// kit's resolver asked within its timeout, or its identity hooks run.
pub(super) async fn identity<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    request: IdentityRequest,
) -> Outcome {
    match request {
        IdentityRequest::Resolve {
            message,
            context,
            timeout,
        } => resolve_sender(inner, &message, &context, timeout).await,
        IdentityRequest::RunHooks {
            trigger,
            event,
            context,
            result,
        } => {
            let answer = run_identity_hooks(inner, trigger, &event, &context, &result).await;
            IdentityAnswer::Hooked(answer.map(Box::new)).into()
        }
        IdentityRequest::Verification {
            identity_id,
            organization_id,
        } => verification::read(inner, &identity_id, organization_id).await,
        IdentityRequest::TryCode {
            request_id,
            room_id,
            code,
        } => verification::try_code(inner, &request_id, &room_id, &code).await,
    }
}

/// Asks the kit's resolver who sent `message` (RFC §11.1), within
/// `timeout`: past it, the answer is `TimedOut` (RFC §11.5); what the
/// resolver raised, or a panic in it, fails the message.
async fn resolve_sender<S>(
    inner: &Inner<S>,
    message: &InboundMessage,
    context: &RoomContext,
    timeout: Duration,
) -> Outcome {
    let Some(resolver) = inner.identity_resolver.clone() else {
        // The core asks only a kit with a resolver.
        let unknown = IdentityResult::new(IdentificationStatus::Unknown);
        return IdentityAnswer::Resolved(Box::new(unknown)).into();
    };
    let span = inner.identity_span(&context.room.id, &message.channel_id);
    let answered = unwinding(resolver.resolve(message, context));
    match tokio::time::timeout(timeout, answered).await {
        Err(_) => {
            span.fail("timeout", Metadata::new());
            IdentityAnswer::TimedOut.into()
        }
        Ok(Ok(Ok(result))) => {
            end_identity_span(span, &result);
            IdentityAnswer::Resolved(Box::new(result)).into()
        }
        Ok(Ok(Err(error))) => {
            span.fail_error(IDENTITY_ERROR, error.to_string(), Metadata::new());
            Outcome::Failed(ErrorRecord::new(IDENTITY_ERROR, error.to_string()))
        }
        Ok(Err(panic)) => {
            let message = unwind::message(&*panic);
            span.fail_error(IDENTITY_ERROR, message.clone(), Metadata::new());
            Outcome::Failed(ErrorRecord::new(IDENTITY_ERROR, message))
        }
    }
}

/// The class name a resolver's failure carries, as a router's carries
/// `RouterError`: a boxed Rust error has none of its own.
const IDENTITY_ERROR: &str = "IdentityError";

/// The room's context (RFC §10.1 step 3), as the Python implementation's
/// kit builds it: the room, its bindings and participants, its latest
/// messages, as many as `recent_events` settles for its bindings, a blocked
/// one included (`get_conversation`),
/// and its register of authors, its private state (gap G-089); `None` for
/// a room the store lacks.
pub(crate) async fn load_context<S: ConversationStore>(
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
/// a turn's stream. A turn that answers with a stream leaves it with its run,
/// `shared`, by its effect, `effect`, until it is read. Work a close spared
/// calls no channel once the kit is sealed, its channels closed, the call
/// refused (RFC §12.10.4): a turn with its link's `backend_closed` (RFC
/// §10.6.8), any other call `kit_closed`, as a write is.
pub(super) async fn channel<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    request: ChannelRequest,
) -> Outcome {
    if inner.tasks.refuses(&Marks::current()) {
        return Outcome::Failed(refused_once_sealed(&request));
    }
    match request {
        ChannelRequest::StreamsIn {
            room_id,
            channel_ids,
        } => streams_in(inner, &room_id, channel_ids),
        ChannelRequest::RoomAttached { binding } => room_attached(inner, binding).await,
        ChannelRequest::RoomDetached { binding } => room_detached(inner, binding).await,
        ChannelRequest::NamedCorrespondent { binding } => named_binding(inner, binding),
        ChannelRequest::RoutedRecipient { binding } => routed_recipient(inner, &binding),
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
                        // the core gives and the room. The instant is the
                        // parse's, as a channel may read it off its wire.
                        event.id = id;
                        event.room_id = room_id;
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
            let window = inner.config().recent_events_limit(&context.bindings);
            if let Some(runner) = inner
                .turn_runners
                .get(&context.room.id, &binding.channel_id)
            {
                let asked = (binding, event, *context);
                return strategy_turn(inner, shared, effect, &*runner, asked, window).await;
            }
            if serializes(&*channel, &binding) {
                let asked = (&*channel, binding);
                return ask_in_line(inner, shared, effect, asked, event, *context, window).await;
            }
            take_turn(inner, shared, effect, binding, event, *context, window).await
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
            correlation_id,
            as_trace,
        } => {
            let read = stream::StreamRead {
                responder,
                answers: *answers,
                target,
                correlation_id,
                as_trace,
            };
            Box::pin(read_stream(inner, shared, (effect, turn), read)).await
        }
    }
}

/// Reads the stream the turn of `turn` answered with, for the read
/// `effect`: a turn that waited its place starts now, and its stream holds it
/// until what it said is stored (RFC §6.4).
async fn read_stream<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    (effect, turn): (EffectId, EffectId),
    read: stream::StreamRead,
) -> Outcome {
    // The cut of the held task whose work the read runs within: the work
    // is not dropped while the read stores what its turn said.
    let task_cut = inner.tasks.task_cuts.of(&Marks::current());
    let _task_read = task_cut.as_ref().map(TaskCut::reading);
    let mut stops = Stops {
        read: shared.stop_signal(effect),
        kit: inner.turns.cutting(),
        task: task_cut.as_ref().map(|cut| cut.signal()),
        read_turns: shared.read_turns(),
    };
    let kept = match shared.take_queued(turn) {
        Some(queued) => match queued.start(inner, &mut stops).await {
            Started::Turn(kept) => Some(kept),
            Started::Answered(outcome) => return outcome,
        },
        None => shared.take_stream(turn),
    };
    // The kit's close waits, within its grace, for the read of a turn it
    // cut to store what the turn said.
    let _reading = kept.as_ref().map(|_| inner.turns.reading());
    stream::read(inner, kept, stops, read).await
}

/// The turn of `binding`'s intelligence channel on `event`, for the call
/// `effect`: it runs through the turn backend (RFC §10.6), on the room's
/// conversation and its channel's tool rows (RFC §6.4).
async fn take_turn<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    binding: ChannelBinding,
    event: RoomEvent,
    context: RoomContext,
    window: usize,
) -> Outcome {
    let context = crate::history::turn_context(&*inner.store, &binding, &event, context, window);
    let context = context.await;
    run_turn(inner, shared, effect, binding, event, context).await
}

/// The channel's own turn on an event standing in for the one asked, for
/// the call `effect`: its history the room's as it was when that event
/// came (Python's turn handed the context loaded for it), its own tool
/// rows the latest (RFC §19.7.3).
async fn answer_in_place<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    answering: Answering,
    window: usize,
) -> Outcome {
    let Answering {
        event,
        binding,
        context,
        asked,
    } = answering;
    let context = crate::history::turn_context(&*inner.store, &binding, &asked, context, window);
    let context = context.await;
    run_turn(inner, shared, effect, binding, event, context).await
}

/// The turn of `binding`'s channel on `event` in `context`, its history
/// read, for the call `effect`.
async fn run_turn<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    binding: ChannelBinding,
    event: RoomEvent,
    context: RoomContext,
) -> Outcome {
    let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(inner));
    let cut = |turn| shared.turn_submitted(effect, turn);
    let answered = inner.turns.run(&kit, &binding, event, context, cut).await;
    let outcome = match answered {
        Ok(Answer::Output(output)) => checked_output(output),
        // Muting silences the voice, a streaming one included: its stream is
        // closed unread, so its reply is never generated (RFC §7.5, §10.6.4).
        Ok(Answer::Streamed(output, mut turn)) if binding.muted => {
            turn.close_stream().await;
            checked_output(stream::muted_answer(output))
        }
        Ok(Answer::Streamed(output, turn)) => {
            shared.keep_stream(
                effect,
                KeptStream {
                    turn,
                    then: None,
                    place: None,
                },
            );
            ChannelOutcome::Streamed(output).into()
        }
        Err(failure) => failure.into_outcome(),
    };
    // Its stream kept first, the turn stops being one the call awaits: an
    // abort between the two reads the stream kept, never a delivery cut
    // running.
    shared.turn_answered(effect);
    outcome
}

/// The turn of `binding`'s channel on `event` that a strategy's runner
/// takes in the room (RFC §10.6.1, §19.7): its answer, or the channel's own
/// turn, which never waits its place in the room, as the strategy orders
/// its turns itself (Python's `_serializes`).
async fn strategy_turn<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    runner: &dyn TurnRunner<S>,
    (binding, event, context): (ChannelBinding, RoomEvent, RoomContext),
    window: usize,
) -> Outcome {
    let kit = crate::kit::RoomKit {
        inner: Arc::clone(inner),
    };
    match runner.take(kit, event, binding, context) {
        Taken::Answered(answer) => channel_output(answer.await),
        Taken::Own(own) => {
            let (event, binding, context) = *own;
            take_turn(inner, shared, effect, binding, event, context, window).await
        }
        Taken::Passes(passes) => first_pass(inner, shared, effect, passes, window).await,
        Taken::Later(work) => match work.await {
            Ok(Next::Own(answering)) => {
                answer_in_place(inner, shared, effect, *answering, window).await
            }
            Ok(Next::FirstStands | Next::Fallback(..)) => {
                ChannelOutcome::Output(Box::default()).into()
            }
            Err(error) => channel_output(Err(error.into())),
        },
    }
}

/// A strategy's first pass of its channel's own turn, for the call
/// `effect` (RFC §19.7.3): a pass that streams is read by the operation's
/// `ReadStream`, its text kept out of the room, then what follows it
/// ([`stream::read`]); one that answers at once is handed on here.
async fn first_pass<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    Passes { first, then }: Passes,
    window: usize,
) -> Outcome {
    let (event, binding, context) = *first;
    let context = crate::history::turn_context(&*inner.store, &binding, &event, context, window);
    let context = context.await;
    let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(inner));
    let cut = |turn| shared.turn_submitted(effect, turn);
    let answered = inner.turns.run(&kit, &binding, event, context, cut).await;
    let output = match answered {
        Ok(Answer::Streamed(output, turn)) => {
            shared.keep_stream(
                effect,
                KeptStream {
                    turn,
                    then: Some(then),
                    place: None,
                },
            );
            shared.turn_answered(effect);
            return ChannelOutcome::Streamed(output).into();
        }
        Ok(Answer::Output(output)) => output,
        Err(failure) => {
            shared.turn_answered(effect);
            return failure.into_outcome();
        }
    };
    shared.turn_answered(effect);
    if output.error.is_some() {
        return checked_output(output);
    }
    match (then.0)(FirstPass::answered(&output)).await {
        Ok(Next::Own(answering)) => {
            answer_in_place(inner, shared, effect, *answering, window).await
        }
        Ok(Next::FirstStands | Next::Fallback(..)) => checked_output(output),
        Err(error) => channel_output(Err(error.into())),
    }
}

/// Whether `channel`'s turn waits its place in the room (RFC §6.4): an
/// intelligence channel that takes a room's turns one at a time, unless
/// muted, which reacts to each event as it comes, its answer not
/// delivered (RFC §7.2).
fn serializes(channel: &dyn DynChannel, binding: &ChannelBinding) -> bool {
    channel.serializes_room_turns() && !binding.muted
}

/// The turn of a channel that takes a room's turns one at a time (RFC
/// §6.4): for one that steers them, `event` handed to the turn its line
/// in the room holds alone, no turn of its own; otherwise a turn that takes
/// its place in the line now and starts once the turns asked before it
/// are over, as its stream is read.
async fn ask_in_line<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    shared: &RunShared,
    effect: EffectId,
    (channel, binding): (&dyn DynChannel, ChannelBinding),
    event: RoomEvent,
    context: RoomContext,
    window: usize,
) -> Outcome {
    if channel.steers_room_turns() && room_turns::steered(inner, &binding, &event, &context).await {
        return ChannelOutcome::Output(Box::default()).into();
    }
    let place = inner.room_turns.join(&event.room_id, &binding.channel_id);
    shared.keep_queued(effect, Queued::new(place, binding, event, context, window));
    ChannelOutcome::Streamed(Box::default()).into()
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
            trigger,
            event,
            context,
            timeout,
        } => {
            let span = inner.hook_with_spec(hook).and_then(|(_, spec)| {
                let room_id = &event.room_id;
                inner
                    .telemetry
                    .hook_span(SpanKind::HookSync, &spec.name, trigger, room_id)
            });
            let outcome = match inner.kept_hook(hook) {
                Some(kept) => hooks::run_kept(kept, *event, *context, timeout).await,
                None => {
                    HookOutcome::Raised(ErrorRecord::new("HookError", "the hook has no handler"))
                }
            };
            end_hook_span(span, &outcome);
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

/// Those of `channel_ids` whose channel takes a streamed response in
/// `room_id`, as each says (RFC §6.1 `supports_streaming_delivery_for`);
/// a channel no longer registered takes none.
fn streams_in<S>(inner: &Inner<S>, room_id: &RoomId, channel_ids: Vec<ChannelId>) -> Outcome {
    let streams = |channel_id: &ChannelId| {
        inner
            .channel(channel_id)
            .is_some_and(|channel| channel.supports_streaming_delivery_for(room_id))
    };
    ChannelOutcome::StreamsIn(channel_ids.into_iter().filter(streams).collect()).into()
}

/// `binding`, its addresses written by its channel as an attach writes them
/// (RFC §5.7, §10.4); as given for a channel no longer registered.
fn named_binding<S>(inner: &Inner<S>, binding: ChannelBinding) -> Outcome {
    let named = match (
        inner.channel(&binding.channel_id),
        inner.registered(&binding.channel_id),
    ) {
        (Some(channel), Some(registered)) => {
            named_correspondent(binding, &*channel, registered.recipient_key())
        }
        (Some(_) | None, _) => binding,
    };
    ChannelOutcome::Named(Box::new(named)).into()
}

/// Who `binding` delivers to, as routing reads it (RFC §5.7, §10.4); `None`
/// for a channel no longer registered, whose declared key the core reads.
fn routed_recipient<S>(inner: &Inner<S>, binding: &ChannelBinding) -> Outcome {
    let recipient = inner
        .registered(&binding.channel_id)
        .and_then(|channel| channel.routed_recipient(binding));
    ChannelOutcome::Recipient(recipient).into()
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
        | ChannelRequest::Deliver { .. }
        | ChannelRequest::StreamsIn { .. }
        | ChannelRequest::RoomAttached { .. }
        | ChannelRequest::RoomDetached { .. }
        | ChannelRequest::NamedCorrespondent { .. }
        | ChannelRequest::RoutedRecipient { .. } => cut_by_close(),
    }
}

/// Tells `binding`'s channel of its attach (RFC §6.1 `on_room_attached`):
/// an intelligence channel through the turn backend, whose executor logs
/// what it raises (RFC §10.6.3); any other directly, what it raises
/// refusing the attach.
async fn room_attached<S>(inner: &Inner<S>, binding: ChannelBinding) -> Outcome {
    let channel = inner.channel(&binding.channel_id);
    if binding.category == ChannelCategory::Intelligence {
        inner.turns.attached(&binding, channel).await;
        return ChannelOutcome::Told.into();
    }
    let Some(channel) = channel else {
        return ChannelOutcome::Told.into();
    };
    told(
        channel
            .on_room_attached(binding.room_id.clone(), binding)
            .await,
    )
}

/// Tells `binding`'s channel of its detach (RFC §6.1 `on_room_detached`):
/// an intelligence channel through the turn backend, whose executor logs
/// what it raises (RFC §10.6.3); any other directly, what it raises
/// failing the detach once announced. A channel no longer registered is
/// not told.
async fn room_detached<S>(inner: &Inner<S>, binding: ChannelBinding) -> Outcome {
    let channel = inner.channel(&binding.channel_id);
    if binding.category == ChannelCategory::Intelligence {
        let (channel_id, room_id) = (&binding.channel_id, &binding.room_id);
        inner.turns.detached(channel_id, room_id, channel).await;
        return ChannelOutcome::Told.into();
    }
    let Some(channel) = channel else {
        return ChannelOutcome::Told.into();
    };
    told(channel.on_room_detached(binding.room_id).await)
}

/// A room callback's answer: told, or failed with what the channel raised.
fn told(answered: Result<(), ChannelError>) -> Outcome {
    match answered {
        Ok(()) => ChannelOutcome::Told.into(),
        Err(error) => Outcome::Failed(ErrorRecord::new("ChannelError", error.to_string())),
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
        Notice::StopReading { .. }
        | Notice::CutReadOver { .. }
        | Notice::Span(_)
        | Notice::Answered(_)
        | Notice::StreamFailed { .. } => {}
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
        let span = inner
            .telemetry
            .hook_span(SpanKind::HookAsync, &spec.name, trigger, &room_id);
        let outcome = hooks::run_kept(kept, event, context, spec.timeout).await;
        end_observer_span(span, &outcome);
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
/// its type's name (RFC §5.13); any other a failure of the class a
/// [`ClassedError`] names, its attributes kept, else of type
/// `ChannelError`, a boxed error having no class name.
fn channel_failed(error: &ChannelError) -> Outcome {
    let kit_error = error.downcast_ref::<RoomKitError>();
    let classed = error.downcast_ref::<ClassedError>();
    let code = match kit_error {
        Some(RoomKitError::ProviderDelivery { result }) => {
            return ChannelOutcome::ProviderDelivery(Box::new(result.clone())).into();
        }
        Some(kit_error) => kit_error.code().unwrap_or_else(|| kit_error.class_name()),
        None => classed
            .map_or("ChannelError", ClassedError::class)
            .to_owned(),
    };
    Outcome::Failed(ErrorRecord {
        attributes: Box::new(
            classed
                .map(ClassedError::attributes)
                .cloned()
                .unwrap_or_default(),
        ),
        ..ErrorRecord::new(code, error.to_string())
    })
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

/// What the pipeline asks of the discussion this process runs for the room,
/// under the room's lock (RFC §19.7.5); a room it runs none for has nothing
/// to answer, and no turn to give. What the discussion raises is answered
/// as a failure: logged for a committed message, the call's for the rest.
pub(super) async fn discussion<S: ConversationStore>(
    inner: &Inner<S>,
    request: DiscussionRequest,
) -> Outcome {
    let room_id = match &request {
        DiscussionRequest::Committed { room_id, .. }
        | DiscussionRequest::Instruction { room_id, .. }
        | DiscussionRequest::Turn { room_id, .. } => room_id,
    };
    let Some(seam) = inner.discussions.get(room_id) else {
        tracing::warn!(
            target: "roomkit::discussion",
            %room_id,
            "no discussion of the room runs in this process: it is told nothing"
        );
        return match request {
            DiscussionRequest::Turn { .. } => DiscussionOutcome::Turn(NextTurn::default()).into(),
            DiscussionRequest::Committed { .. } | DiscussionRequest::Instruction { .. } => {
                DiscussionOutcome::Done.into()
            }
        };
    };
    let answered = match request {
        DiscussionRequest::Committed {
            event,
            reach,
            context,
            ..
        } => seam
            .on_committed(*event, reach, *context)
            .await
            .map(|()| DiscussionOutcome::Done),
        DiscussionRequest::Instruction { event, .. } => seam
            .queue_instruction(*event)
            .await
            .map(|()| DiscussionOutcome::Done),
        DiscussionRequest::Turn { context, .. } => {
            seam.next_turn(*context).await.map(DiscussionOutcome::Turn)
        }
    };
    match answered {
        Ok(outcome) => outcome.into(),
        Err(error) => Outcome::Failed(ErrorRecord::new("RoomKitError", error.to_string())),
    }
}
