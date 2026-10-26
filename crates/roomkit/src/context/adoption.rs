//! A call into a room handed to the run whose work under the room's lock or
//! in its admission order made it, which adopts it or declines it (RFC
//! §13.5, §9.5.1 Reentrance, §10.1 step 18).

use roomkit_core::ops::{EffectId, Entry, InboundError, InboundResult};
use tokio::sync::oneshot;

use super::{ExecutionContext, Frame};

/// What an adopted entry's caller receives (RFC §10.1 step 18).
pub(crate) type EntryResult = Result<InboundResult, InboundError>;

/// A call into a room, handed to the run whose work made it.
#[derive(Debug)]
pub(crate) struct Adoption {
    /// The call.
    pub(crate) entry: Entry,
    /// The effect whose call made it, in that run.
    pub(crate) origin: EffectId,
    /// Where its answer goes.
    pub(crate) reply: oneshot::Sender<Reply>,
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
    /// The run's operation adopted the call and decided it.
    Settled(Box<EntryResult>),
    /// The run adopted the call, then was dropped before deciding it.
    Abandoned,
}

/// Hands `entry` to the run of the current task's context whose work under
/// its room's lock or in its admission order made it, if any: the call is
/// adopted by that run's operation, or declined (RFC §13.5, §9.5.1
/// Reentrance).
pub(crate) async fn adopt(entry: Entry) -> Adopting {
    let context = ExecutionContext::current();
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
    let adoption = Adoption {
        entry,
        origin: frame.effect,
        reply,
    };
    if let Err(unsent) = frame.run.hand(adoption) {
        // The run is over: it holds nothing of the room any more.
        return Adopting::Apart(unsent.entry);
    }
    match answer.await {
        Ok(Reply::Settled(result)) => Adopting::Settled(result),
        Ok(Reply::Declined(entry)) => Adopting::Apart(*entry),
        Err(_dropped) => Adopting::Abandoned,
    }
}
