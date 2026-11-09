//! The read of a strategy's passes (RFC §19.7.3): a first pass of an
//! agent's own turn, its calls the room's rows and its text kept out of
//! the room, then what the strategy makes of it, all within the one
//! `ReadStream` of the turn the operation asked: Python's supervisor
//! reading its first pass through `persist_tool_calls`, then answering
//! with its own turn on a stand-in for the event, or with its fallback.
//!
//! Each pass's rows are the room's as any streamed turn's: they commit
//! through the operation, `BEFORE_BROADCAST` once. Each pass is a turn of
//! its own (RFC §10.6.1): the first pass's rows carry a `correlation_id` of
//! their own, which the read's report of its failure carries, and the
//! second pass's carry the read's, which the operation's report of its
//! failure carries (Python's `_pass1_task`, then the room turn's stream).
//! What follows the first pass raising fails the turn as an `on_event()`
//! that raises ([`ChannelOutcome::Raised`]): Python runs it within the
//! supervisor's `on_event()`.

use std::sync::Arc;

use roomkit_core::model::{
    ChannelId, ChannelOutput, DeliveryResult, ErrorRecord, EventDirection, EventSource, Metadata,
    RoomEvent,
};
use roomkit_core::ops::{ChannelOutcome, Entry, Outcome, Raised};

use super::Stops;
use super::reader::{Failing, KeptStream, StreamRead, read_kept};
use super::segments::Segments;
use crate::channel::ChannelError;
use crate::context;
use crate::error::RoomKitError;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::turn::TurnKit;
use crate::turn_runners::{Answering, FirstPass, Next, Then};
use crate::turns::Answer;

/// Reads `kept`, a strategy's first pass, then what `then` makes of it:
/// the answer of the operation's `ReadStream`. A first pass that failed is
/// reported here, once, and answers with its failure, which the operation
/// hands on unreported (RFC §19.7.3).
pub(super) async fn read<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    kept: KeptStream,
    then: Then,
    stops: Stops,
    read: StreamRead,
) -> Outcome {
    let (first, pass) = match read_first(inner, kept, &stops, &read).await {
        Ok(read) => read,
        Err(answer) => return ChannelOutcome::Output(answer).into(),
    };
    // A read stopped meanwhile (the operation's stop, the kit's close, the
    // held task's cut) cuts what follows the pass, its workers with it, as
    // Python's cancelled turn cancels its delegation; nothing more answers.
    let mut watching = stops.clone();
    let next = tokio::select! {
        // A stop already given wins over the work it would start.
        biased;
        reason = watching.stopped() => {
            tracing::debug!(
                target: "roomkit::driver",
                room_id = %read.answers.room_id,
                reason,
                "a strategy's passes were stopped after the first"
            );
            return ChannelOutcome::Output(Box::new(first)).into();
        }
        next = (then.0)(pass) => next,
    };
    match next {
        // Cancelling the strategy stops its answer, without turning the
        // first pass's committed side effects into a delivery failure.
        Ok(Next::FirstStands) | Err(RoomKitError::Cancelled) => {
            ChannelOutcome::Output(Box::new(first)).into()
        }
        Ok(Next::Fallback(text, record)) => {
            commit_fallback(&read, &text, record).await;
            ChannelOutcome::Output(Box::new(first)).into()
        }
        Ok(Next::Own(answering)) => second_pass(inner, *answering, stops, read).await,
        Err(error) => raised(&read.responder.channel_id, error),
    }
}

/// Reads the first pass, `kept`, for `read`: a turn of its own, its rows a
/// correlation id of their own, its text kept out of the room. A pass that
/// failed is reported here, once, and its output, its error marked
/// reported, answers the read, as one with nothing to hand on does.
async fn read_first<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    kept: KeptStream,
    stops: &Stops,
    read: &StreamRead,
) -> Result<(ChannelOutput, FirstPass), Box<ChannelOutput>> {
    let first_read = StreamRead {
        responder: read.responder.clone(),
        answers: read.answers.clone(),
        target: None,
        // A turn of its own: its rows carry an id of their own, as every id
        // the kit gives (RFC §10.6.1).
        correlation_id: inner.environment().new_event_id().to_string(),
        // A first pass's text is kept out of the room (RFC §19.7.3): it is
        // no trace's, whoever reads the turn after it.
        as_trace: false,
    };
    let failing = Failing::of(&first_read);
    let (mut first, pass) = Box::pin(read_kept(inner, kept, stops.clone(), first_read, true)).await;
    if let Some(error) = first.error.take() {
        failing.report_first_pass(inner, &error).await;
        first.error = Some(error.marked_reported());
        return Err(Box::new(first));
    }
    match pass {
        Some(pass) => Ok((first, pass)),
        None => Err(Box::new(first)),
    }
}

/// What follows the first pass raised `error` (its workers): the turn
/// failed as an `on_event()` that raises, its error read as a channel's,
/// with the failed delivery it declares (RFC §19.7.3, §5.13).
fn raised(responder: &ChannelId, error: RoomKitError) -> Outcome {
    let (error, failed) = match crate::driver::channel_failed(&ChannelError::from(error)) {
        Outcome::Channel(ChannelOutcome::ProviderDelivery(result)) => {
            let failed = DeliveryResult::refused(responder.clone(), *result);
            let message = failed.error.as_ref().map(|error| error.message.clone());
            let error = ErrorRecord::new("ProviderDeliveryError", message.unwrap_or_default());
            (error, failed)
        }
        Outcome::Failed(error) => {
            let failed = DeliveryResult::raised(responder.clone(), &error);
            (error, failed)
        }
        // A channel's failure reads as one of the two above.
        other @ (Outcome::Hold(_)
        | Outcome::Store(_)
        | Outcome::Channel(_)
        | Outcome::Hook(_)
        | Outcome::Routed(_)
        | Outcome::DeadlinePassed
        | Outcome::Identity(_)
        | Outcome::Discussion(_)) => return other,
    };
    let Some(delivery) = failed.error else {
        return Outcome::Failed(error);
    };
    ChannelOutcome::Raised(Box::new(Raised { error, delivery })).into()
}

/// The agent's own turn on the event the strategy hands it, read as the
/// operation's turn is: its rows the room's, answering the operation's
/// event, through its streaming target; its history the room's as it was
/// when that event came, its own tool rows the latest (the first pass's
/// among them).
async fn second_pass<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    answering: Answering,
    stops: Stops,
    read: StreamRead,
) -> Outcome {
    let Answering {
        event,
        binding,
        context,
        asked,
    } = answering;
    let window = inner.config().recent_events_limit(&context.bindings);
    let context = crate::history::turn_context(&*inner.store, &binding, &asked, context, window);
    let context = context.await;
    let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(inner));
    let read = StreamRead {
        responder: binding.clone(),
        ..read
    };
    let delivery = stops.delivery.clone();
    let submitted = move |turn| delivery.map(|delivery| delivery.follows(&turn));
    match inner
        .turns
        .run(&kit, &binding, event, context, submitted)
        .await
    {
        // Mute closes the answer unread unless read-only takes precedence (RFC §7.5).
        Ok(Answer::Streamed(output, mut turn)) if binding.muted && binding.access.writes() => {
            turn.close_stream().await;
            ChannelOutcome::Output(super::muted_answer(output)).into()
        }
        Ok(Answer::Streamed(_output, turn)) => {
            let kept = KeptStream {
                turn,
                then: None,
                place: None,
            };
            let (output, _) = Box::pin(read_kept(inner, kept, stops, read, false)).await;
            ChannelOutcome::Output(Box::new(output)).into()
        }
        Ok(Answer::Output(mut output)) => {
            for row in std::mem::take(&mut output.response_events) {
                commit(row).await;
            }
            ChannelOutcome::Output(output).into()
        }
        Err(failure) => failure.into_outcome(),
    }
}

/// Commits the agent's fallback `text`, carrying `record`, as the message
/// its turn ends on (Python's `_pass1_answer`).
async fn commit_fallback(read: &StreamRead, text: &str, record: Metadata) {
    let responder = &read.responder;
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(responder.channel_id.clone(), responder.channel_type.clone())
    };
    let mut segments = Segments::new(source, read.answers.clone(), read.correlation_id.clone());
    segments.text(text);
    segments.end_turn(record);
    if let Some(mut row) = segments.last_text(false) {
        // The fallback is no row of the pass's response (Python's has none).
        row.correlation_id = None;
        commit(row).await;
    }
}

/// Hands `row` to the operation, which commits it as the turn's.
async fn commit(row: RoomEvent) {
    let entry = Entry::Row {
        room_id: row.room_id.clone(),
        event: Box::new(row),
        streamed: false,
    };
    drop(context::adopt_by_own_run(entry).await);
}
