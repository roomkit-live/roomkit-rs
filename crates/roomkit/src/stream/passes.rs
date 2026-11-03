//! The read of a strategy's passes (RFC §19.7.3): a first pass of an
//! agent's own turn, its calls the room's rows and its text kept out of
//! the room, then what the strategy makes of it, all within the one
//! `ReadStream` of the turn the operation asked: Python's supervisor
//! reading its first pass through `persist_tool_calls`, then answering
//! with its own turn on a stand-in for the event, or with its fallback.
//!
//! Each pass's rows are the room's as any streamed turn's: they commit
//! through the operation, `BEFORE_BROADCAST` once, and carry the read's
//! `correlation_id`, which the operation's report of either pass's failure
//! carries too.

use std::sync::Arc;

use roomkit_core::model::{
    ChannelBinding, ErrorRecord, EventDirection, EventSource, Metadata, RoomContext, RoomEvent,
};
use roomkit_core::ops::{ChannelOutcome, Entry, Outcome};

use super::Stops;
use super::reader::{KeptStream, StreamRead, read_kept};
use super::segments::Segments;
use crate::context;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::turn::TurnKit;
use crate::turn_runners::{Next, Then};
use crate::turns::Answer;

/// Reads `kept`, a strategy's first pass, then what `then` makes of it:
/// the answer of the operation's `ReadStream`. A first pass that failed
/// answers with its failure, which the operation reports once.
pub(super) async fn read<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    kept: KeptStream,
    then: Then,
    stops: Stops,
    read: StreamRead,
) -> Outcome {
    let first_read = StreamRead {
        responder: read.responder.clone(),
        answers: read.answers.clone(),
        target: None,
        correlation_id: read.correlation_id.clone(),
    };
    let (mut first, pass) = Box::pin(read_kept(inner, kept, stops.clone(), first_read, true)).await;
    let Some(pass) = pass.filter(|_| first.error.is_none()) else {
        return ChannelOutcome::Output(Box::new(first)).into();
    };
    // A read stopped meanwhile (the operation's stop, the kit's close, the
    // held task's cut) cuts what follows the pass, its workers with it, as
    // Python's cancelled turn cancels its delegation; nothing more answers.
    let mut watching = stops.clone();
    let next = tokio::select! {
        next = (then.0)(pass) => next,
        reason = watching.stopped() => {
            tracing::debug!(
                target: "roomkit::driver",
                room_id = %read.answers.room_id,
                reason,
                "a strategy's passes were stopped after the first"
            );
            return ChannelOutcome::Output(Box::new(first)).into();
        }
    };
    match next {
        Ok(Next::FirstStands) => ChannelOutcome::Output(Box::new(first)).into(),
        Ok(Next::Fallback(text, record)) => {
            commit_fallback(&read, &text, record).await;
            ChannelOutcome::Output(Box::new(first)).into()
        }
        Ok(Next::Own(own)) => second_pass(inner, *own, stops, read).await,
        Err(error) => {
            first.error = Some(ErrorRecord::new(error.class_name(), error.to_string()));
            ChannelOutcome::Output(Box::new(first)).into()
        }
    }
}

/// The agent's own turn on the event the strategy hands it, read as the
/// operation's turn is: its rows the room's, answering the operation's
/// event, through its streaming target.
async fn second_pass<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    (event, binding, context): (RoomEvent, ChannelBinding, RoomContext),
    stops: Stops,
    read: StreamRead,
) -> Outcome {
    let window = inner.config().recent_events_limit(&context.bindings);
    let context = crate::history::turn_context(&*inner.store, &binding, &event, context, window);
    let context = context.await;
    let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(inner));
    let read = StreamRead {
        responder: binding.clone(),
        ..read
    };
    match inner.turns.run(&kit, &binding, event, context, drop).await {
        Ok(Answer::Streamed(_output, turn)) => {
            let kept = KeptStream { turn, then: None };
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
    if let Some(row) = segments.last_text(false) {
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
