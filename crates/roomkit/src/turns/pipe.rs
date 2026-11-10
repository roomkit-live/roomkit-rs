//! One direction of a turn's link in memory (RFC §10.6.1, §10.6.3): each
//! message serialized in its numbered envelope, queued, and parsed anew on
//! the other side, as any backend carries it.
//!
//! - A message is serialized before it is numbered: one that cannot cross,
//!   or is larger than [`MAX_MESSAGE_BYTES`], takes no number and is never a
//!   lost frame.
//! - The receiving side admits the next number only: a duplicate is
//!   dropped, a gap fails the link with `frame_lost`.
//! - A message is checked against its bound before it is parsed.
//! - A pipe shut (its turn over for the owner, or the backend closed) drops
//!   whatever is put on it, keeping nothing: a frame of a turn that is over
//!   allocates nothing that lasts.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use super::observe::{Carried, Observers};

/// The largest message a link carries, checked as it is written and before
/// it is parsed: a request without its events, a frame, a control message.
pub(crate) const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Why a link stopped carrying messages (RFC §10.6.8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LinkFailure {
    /// The backend closed, or the other side let go of the turn.
    Closed,
    /// A message was lost: the next one carried another number.
    Lost { expected: u64, got: u64 },
    /// A message could not be read.
    Invalid(String),
}

impl LinkFailure {
    /// The reason RFC §10.6.8 names the failure by.
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Closed => "backend_closed",
            Self::Lost { .. } => "frame_lost",
            Self::Invalid(_) => "frame_invalid",
        }
    }

    /// What the failure says.
    pub(crate) fn message(&self) -> String {
        match self {
            // Python's `TurnLinkError` words (RMK-1106).
            Self::Closed => "The turn ended: its turn backend closed".to_owned(),
            Self::Lost { expected, .. } => format!("Message {expected} of the turn was lost"),
            Self::Invalid(why) => format!("The turn's frames could not be read: {why}"),
        }
    }
}

/// Why a message could not be sent: it cannot cross.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Uncarried(pub(crate) String);

/// The receiving end's rule: numbers from 1, each exactly once and in order.
#[derive(Debug, Default)]
pub(crate) struct MessageOrder {
    last: u64,
}

impl MessageOrder {
    /// Whether `seq` is the next message: `false` for a duplicate, a
    /// failure for a gap.
    pub(crate) fn admit(&mut self, seq: u64) -> Result<bool, LinkFailure> {
        if seq <= self.last {
            return Ok(false);
        }
        let expected = self.last.saturating_add(1);
        if seq != expected {
            return Err(LinkFailure::Lost { expected, got: seq });
        }
        self.last = seq;
        Ok(true)
    }
}

/// What a pipe queues: a serialized envelope, or the word that it shut.
#[derive(Debug)]
enum Packet {
    Message(String),
    Shut,
}

/// The writing end of one direction of a turn's link.
#[derive(Debug)]
pub(crate) struct Pipe<M> {
    turn_id: Arc<str>,
    carried: Carried,
    /// The last number given, held while a message is numbered and queued,
    /// so numbers and the queue's order agree.
    sent: Mutex<u64>,
    open: AtomicBool,
    queue: mpsc::UnboundedSender<Packet>,
    observers: Observers,
    message: PhantomData<fn(M)>,
}

/// The reading end of one direction of a turn's link.
#[derive(Debug)]
pub(crate) struct PipeReceiver<M> {
    turn_id: Arc<str>,
    queue: mpsc::UnboundedReceiver<Packet>,
    order: MessageOrder,
    message: PhantomData<fn() -> M>,
}

/// An envelope as it is written.
#[derive(Serialize)]
struct Outgoing<'a, M> {
    turn_id: &'a str,
    seq: u64,
    message: &'a M,
}

/// An envelope as it is read.
#[derive(serde::Deserialize)]
#[serde(bound = "M: DeserializeOwned")]
struct Incoming<M> {
    turn_id: String,
    seq: u64,
    message: M,
}

/// A new pipe of `turn_id`'s link, carrying `carried` messages.
pub(crate) fn pipe<M>(
    turn_id: Arc<str>,
    carried: Carried,
    observers: Observers,
) -> (Pipe<M>, PipeReceiver<M>) {
    let (queue, queued) = mpsc::unbounded_channel();
    let pipe = Pipe {
        turn_id: Arc::clone(&turn_id),
        carried,
        sent: Mutex::new(0),
        open: AtomicBool::new(true),
        queue,
        observers,
        message: PhantomData,
    };
    let receiver = PipeReceiver {
        turn_id,
        queue: queued,
        order: MessageOrder::default(),
        message: PhantomData,
    };
    (pipe, receiver)
}

impl<M: Serialize> Pipe<M> {
    /// Queues `message`, numbered; its number. A shut pipe drops it and
    /// answers the last number given.
    pub(crate) fn put(&self, message: &M) -> Result<u64, Uncarried> {
        let mut sent = self.sent.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.is_open() {
            return Ok(*sent);
        }
        let seq = sent.saturating_add(1);
        let raw = serde_json::to_string(&Outgoing {
            turn_id: &self.turn_id,
            seq,
            message,
        })
        .map_err(|error| Uncarried(error.to_string()))?;
        if raw.len() > MAX_MESSAGE_BYTES {
            return Err(Uncarried(format!(
                "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
                raw.len()
            )));
        }
        let fate = self.observers.carry(self.carried, &raw);
        #[cfg(test)]
        if fate.refused {
            return Err(Uncarried("the link refused the frame".to_owned()));
        }
        if fate.unsent {
            return Ok(*sent);
        }
        *sent = seq;
        if fate.lost {
            return Ok(seq);
        }
        if fate.duplicated {
            drop(self.queue.send(Packet::Message(raw.clone())));
        }
        drop(self.queue.send(Packet::Message(raw)));
        Ok(seq)
    }
}

impl<M> Pipe<M> {
    /// Whether the pipe still carries messages.
    pub(crate) fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Stops the pipe: its reader learns it at once, and later messages are
    /// dropped.
    pub(crate) fn shut(&self) {
        if self.open.swap(false, Ordering::AcqRel) {
            drop(self.queue.send(Packet::Shut));
        }
    }
}

impl<M: DeserializeOwned> PipeReceiver<M> {
    /// The next message in order, parsed anew, and its number.
    pub(crate) async fn get(&mut self) -> Result<(u64, M), LinkFailure> {
        loop {
            let raw = match self.queue.recv().await {
                Some(Packet::Message(raw)) => raw,
                Some(Packet::Shut) | None => return Err(LinkFailure::Closed),
            };
            if let Some(admitted) = self.admit(&raw)? {
                return Ok(admitted);
            }
        }
    }

    /// The message `raw`, read within its bound, when it is the next one;
    /// `None` for a duplicate.
    fn admit(&mut self, raw: &str) -> Result<Option<(u64, M)>, LinkFailure> {
        if raw.len() > MAX_MESSAGE_BYTES {
            return Err(LinkFailure::Invalid(format!(
                "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
                raw.len()
            )));
        }
        let envelope: Incoming<M> =
            serde_json::from_str(raw).map_err(|error| LinkFailure::Invalid(error.to_string()))?;
        if *envelope.turn_id != *self.turn_id {
            return Err(LinkFailure::Invalid(format!(
                "a message of turn {} on the link of turn {}",
                envelope.turn_id, self.turn_id
            )));
        }
        if !self.order.admit(envelope.seq)? {
            return Ok(None);
        }
        Ok(Some((envelope.seq, envelope.message)))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::turns::wire::{Control, Frame};

    fn frames() -> (Pipe<Frame>, PipeReceiver<Frame>) {
        pipe(Arc::from("turn-1"), Carried::Frame, Observers::default())
    }

    #[test]
    fn a_duplicate_is_dropped_and_a_gap_fails_frame_lost() {
        let mut order = MessageOrder::default();
        assert_eq!(
            (order.admit(1), order.admit(1), order.admit(3)),
            (
                Ok(true),
                Ok(false),
                Err(LinkFailure::Lost {
                    expected: 2,
                    got: 3
                })
            )
        );
    }

    #[tokio::test]
    async fn a_message_crosses_numbered_and_parsed_anew() {
        let (pipe, mut receiver) = frames();
        let sent = (pipe.put(&Frame::Heartbeat), pipe.put(&Frame::End));
        let got = (receiver.get().await, receiver.get().await);
        assert_eq!(
            (sent, got),
            (
                (Ok(1), Ok(2)),
                (Ok((1, Frame::Heartbeat)), Ok((2, Frame::End)))
            )
        );
    }

    #[tokio::test]
    async fn a_shut_pipe_drops_what_is_put_and_its_reader_learns_it() {
        let (pipe, mut receiver) = frames();
        pipe.shut();
        let late = pipe.put(&Frame::Heartbeat);
        assert_eq!(
            (late, receiver.get().await, receiver.queue.is_empty()),
            (Ok(0), Err(LinkFailure::Closed), true)
        );
    }

    /// RFC §10.6.3: a message is checked against its bound before it is
    /// parsed, and one past it is never written either.
    #[tokio::test]
    async fn a_message_past_its_bound_is_neither_written_nor_parsed() {
        let (pipe, mut receiver) =
            pipe::<Control>(Arc::from("turn-1"), Carried::Control, Observers::default());
        let reason = "x".repeat(MAX_MESSAGE_BYTES);
        let refused = pipe.put(&Control::Cancel { reason });
        let oversized = format!("{{\"pad\":\"{}\"}}", "x".repeat(MAX_MESSAGE_BYTES));
        let read = receiver
            .admit(&oversized)
            .map(|admitted| admitted.is_some());
        assert_eq!(
            (
                refused.is_err(),
                matches!(read, Err(LinkFailure::Invalid(_)))
            ),
            (true, true)
        );
    }
}
