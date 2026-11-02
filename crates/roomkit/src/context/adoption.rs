//! A call into a room handed to the run whose work under the room's lock or
//! in its admission order made it, which adopts it or declines it (RFC
//! §13.5, §9.5.1 Reentrance, §10.1 step 18).

use std::sync::Arc;

use roomkit_core::ops::{EffectId, Entry, InboundError, InboundResult};
use tokio::sync::{oneshot, watch};

use super::{ExecutionContext, Frame};
use crate::delivery_keys::CancelRequest;
use crate::driver::StoredAnswers;

/// What an adopted entry's caller receives (RFC §10.1 step 18).
pub(crate) type EntryResult = Result<InboundResult, InboundError>;

/// Where an adopted entry's caller reads its delivery's end: what the
/// adopting run's operation reported of the entry's delivery set, once it
/// ran, closed, with nothing, once that run is over without it; and the
/// answers the entry's cascade committed, which the caller reads then.
#[derive(Clone, Debug)]
pub(crate) struct Completion {
    /// The report of the entry's delivery set.
    pub(crate) delivered: watch::Receiver<Option<InboundResult>>,
    /// The answers its cascade committed.
    pub(crate) answers: Arc<StoredAnswers>,
    /// The cut of its turns its caller asks for: its delivery cancelled
    /// (RFC §10.1 "Explicit delivery cancellation").
    pub(crate) cancel: Arc<CancelRequest>,
}

/// A call into a room, handed to the run whose work made it.
#[derive(Debug)]
pub(crate) struct Adoption {
    /// The call.
    pub(crate) entry: Entry,
    /// The effect whose call made it, in that run.
    pub(crate) origin: EffectId,
    /// Where its answer goes.
    pub(crate) reply: oneshot::Sender<Reply>,
    /// Where its delivery's end goes, once its set ran.
    pub(crate) delivered: watch::Sender<Option<InboundResult>>,
    /// Where its cascade's answers go, as they are committed.
    pub(crate) answers: Arc<StoredAnswers>,
    /// The cut of its turns its caller asks for, which the run carries out
    /// once asked.
    pub(crate) cancel: Arc<CancelRequest>,
}

/// The answer of the run a call into its room was handed to.
#[derive(Debug)]
pub(crate) enum Reply {
    /// The run's operation adopted the call and decided it.
    Settled(Box<EntryResult>),
    /// The run did not adopt the call: its origin was answered already, or
    /// the run is over. The call runs apart.
    Declined(Box<Entry>),
}

/// What became of a call handed to the run whose work made it.
#[derive(Debug)]
pub(crate) enum Adopting {
    /// No frame of the caller's context works in the call's room, or its
    /// run declined the call: the call runs apart, as any call does.
    Apart(Entry),
    /// The run's operation adopted the call and decided it; the
    /// completion tells its delivery's end.
    Settled(Box<EntryResult>, Completion),
    /// The run adopted the call, then was dropped before deciding it.
    Abandoned,
}

/// Hands `entry` to the run of the current task's context whose work under
/// its room's lock or in its admission order made it, if any: the call is
/// adopted by that run's operation, or declined (RFC §13.5, §9.5.1
/// Reentrance). A call whose context hands it its room's lock runs apart,
/// nested under that lock ([`Hand`](super::Hand)).
pub(crate) async fn adopt(entry: Entry) -> Adopting {
    let context = ExecutionContext::current();
    if context.hands(entry.room_id()) {
        return Adopting::Apart(entry);
    }
    let Some(frame) = context.taker(entry.room_id()).cloned() else {
        return Adopting::Apart(entry);
    };
    hand(&frame, entry).await
}

/// Hands `entry` to the run whose call the current task runs, the innermost
/// frame's: a row of the stream that call reads, which its operation commits
/// (RFC §12.2 step 13s).
pub(crate) async fn adopt_by_own_run(entry: Entry) -> Adopting {
    let context = ExecutionContext::current();
    let Some(frame) = context.frames.last().cloned() else {
        return Adopting::Apart(entry);
    };
    hand(&frame, entry).await
}

/// Hands `entry` to `frame`'s run, and waits for its decision.
async fn hand(frame: &Frame, entry: Entry) -> Adopting {
    let (reply, answer) = oneshot::channel();
    let (delivered, told) = watch::channel(None);
    let answers = Arc::new(StoredAnswers::default());
    let cancel = Arc::new(CancelRequest::default());
    let completion = Completion {
        delivered: told,
        answers: Arc::clone(&answers),
        cancel: Arc::clone(&cancel),
    };
    let adoption = Adoption {
        entry,
        origin: frame.effect,
        reply,
        delivered,
        answers,
        cancel,
    };
    if let Err(unsent) = frame.run.hand(adoption) {
        // The run is over: it holds nothing of the room any more.
        return Adopting::Apart(unsent.entry);
    }
    match answer.await {
        Ok(Reply::Settled(result)) => Adopting::Settled(result, completion),
        Ok(Reply::Declined(entry)) => Adopting::Apart(*entry),
        Err(_dropped) => Adopting::Abandoned,
    }
}
