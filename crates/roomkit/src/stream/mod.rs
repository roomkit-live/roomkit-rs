//! Streamed responses (RFC §6.1 `response_stream` and `deliver_stream`,
//! §12.2 steps 13s and 14s).
//!
//! An intelligence channel answers a turn with a stream of
//! [`ResponseItem`]s, which it hands its turn's host
//! ([`TurnHost::respond_with_stream`](crate::TurnHost::respond_with_stream)):
//! text as it is written, each tool call's start and end, the turn's end.
//! The kit reads it once the event's delivery set is over, and stores each
//! row as it ends (a text segment, a call's start, a call's end), the
//! record of the turn on its last message. The stream is advanced on the
//! kit's turn executor, its items crossing the turn backend as frames, a
//! window of them at a time, as the kit's reader pulls them (RFC §10.6.4).
//!
//! The kit's reader is a transport that streams in the room
//! ([`supports_streaming_delivery_for`](crate::Channel::supports_streaming_delivery_for)):
//! its [`deliver_stream`](crate::Channel::deliver_stream) reads a
//! [`DeliveryStream`], one stream of [`StreamItem`]s, the text deltas and
//! each row once the store holds it, in index order (step 4, decision 9). It
//! sets the pace: a model round and a tool call run only as it reads, and
//! one that stops reading leaves no call started after that point, the text
//! it was handed stored marked `cancelled`. The other targets receive each
//! row through `deliver()` as it is stored, while the turn goes on.
//!
//! A streaming target shows text `BEFORE_BROADCAST` has not judged: the
//! hooks run on each row as it is stored (RFC §9.5), so a hook's change
//! reaches the stored row and the other targets, never the deltas already
//! shown (RFC §6.4: a streamed segment, once delivered, is not taken back).

mod loop_end;
mod passes;
mod reader;
mod segments;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use roomkit_core::model::{ChannelOutput, ResponseMetadata, RoomEvent, ToolCallContent};
use tokio::sync::{mpsc, oneshot};

pub(crate) use self::loop_end::read_record;
pub use self::loop_end::{LoopEndMarker, LoopEndReason};
pub(crate) use self::reader::{CALLER_CANCELLED, KIT_CLOSED, KeptStream, Stops, StreamRead, read};
use crate::channel::ChannelError;

/// One item of a turn's response, as an intelligence channel writes it (RFC
/// §6.4 "One loop, streamed or not").
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ResponseItem {
    /// Text, as it is written.
    Text(String),
    /// The turn's loop reached its end (RFC §6.4 "The turn's record"):
    /// Python's `LoopEndMarker`, whose record the kit writes on the turn's
    /// last message.
    LoopEnd(LoopEndMarker),
    /// A tool call starts: its row, its arguments as the model wrote them,
    /// pending. It ends the text before it, a segment of its own. Python's
    /// private `ToolCallStartMarker`, which roomkit-ai writes.
    #[doc(hidden)]
    ToolCallStart(ToolCallContent),
    /// A tool call ended: its row, how it ended and what the model read.
    /// Python's private `ToolCallEndMarker`, which roomkit-ai writes.
    #[doc(hidden)]
    ToolCallEnd(ToolCallContent),
    /// The loop asks the model again within the turn (a message steered in
    /// once it answered, RFC §21.3): the text before it is a segment of its
    /// own, a MESSAGE of its own (RFC §6.4). Python's private
    /// `SegmentBreakMarker`, which roomkit-ai writes.
    #[doc(hidden)]
    SegmentBreak,
    /// A piece of the model's reasoning, as it is written: Python's
    /// `ThinkingDeltaMarker` (RFC §6.1 `deliver_stream`, §6.7). Never stored.
    Thinking(String),
}

/// A turn's response as its channel writes it: a stream ended by its last
/// item, or by an error, the response's failure (RFC §12.2 step 13s). It is
/// pulled: nothing of it runs but while it is read.
pub type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<ResponseItem, ChannelError>> + Send + 'static>>;

/// One item a streaming channel reads (RFC §6.1 `deliver_stream`).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum StreamItem {
    /// Text of the response, as it is written: shown before the store holds
    /// it, and before `BEFORE_BROADCAST` judged it.
    Text(String),
    /// A row of the response, as the store holds it, with its id and its
    /// index: the text segment the deltas before it join to, a call's start
    /// or end.
    Row(Box<RoomEvent>),
    /// A piece of the model's reasoning, in its order among the text and
    /// the rows: Python's `ThinkingDeltaMarker`. Never stored; the default
    /// `deliver_stream` skips it.
    Thinking(String),
}

/// What a muted channel's turn leaves of its output, its stream closed
/// unread: its side effects, always allowed (RFC §7.5), and no answer nor
/// record, since no reply was generated.
pub(crate) fn muted_answer(output: Box<ChannelOutput>) -> Box<ChannelOutput> {
    Box::new(ChannelOutput {
        responded: false,
        response_events: Vec::new(),
        response_metadata: ResponseMetadata::default(),
        ..*output
    })
}

/// The next item of a turn's response, as its executor reads it; `None` at
/// its end.
pub(crate) async fn next_item(
    stream: &mut ResponseStream,
) -> Option<Result<ResponseItem, ChannelError>> {
    std::future::poll_fn(|context| stream.as_mut().poll_next(context)).await
}

/// What a pull of a [`DeliveryStream`] is answered with: the next item, an
/// error ending the stream, or its end.
type Pulled = Option<Result<StreamItem, ChannelError>>;

/// The stream a streaming channel's `deliver_stream` reads (RFC §6.1,
/// step 4 decision 9): the response's text as it is written, each piece of
/// the model's reasoning, and each row once the store holds it, in index
/// order; the response's failure, if it
/// fails, as its last item. Each read pulls the response one item further:
/// nothing runs past what was read. Dropping it, or returning from
/// `deliver_stream` before its end, stops the response: the text it was
/// handed is stored marked `cancelled`, a call that runs is let finish, and
/// none starts any more (RFC §12.2 step 13s).
///
/// A row whose delivery reaches the channel is handed here only: the
/// channel is never delivered it again.
pub struct DeliveryStream {
    pulls: mpsc::UnboundedSender<oneshot::Sender<Pulled>>,
    waiting: Option<oneshot::Receiver<Pulled>>,
    ended: bool,
    /// Whether its end is no end of the response, its read stopped from
    /// outside: Python's reader reads a cancel there.
    cut: bool,
}

impl DeliveryStream {
    /// The stream whose pulls go to `pulls`.
    fn new(pulls: mpsc::UnboundedSender<oneshot::Sender<Pulled>>) -> Self {
        Self {
            pulls,
            waiting: None,
            ended: false,
            cut: false,
        }
    }

    /// Whether its read was stopped from outside: the end it reads is no end
    /// of the response.
    pub(crate) fn cut(&self) -> bool {
        self.cut
    }

    /// The next item: text, a row, or the response's failure, its last;
    /// `None` once the response is over or its read was stopped from outside.
    /// It reads the response one item further, as polling the stream does.
    pub async fn next(&mut self) -> Option<Result<StreamItem, ChannelError>> {
        std::future::poll_fn(|context| Pin::new(&mut *self).poll_next(context)).await
    }
}

impl std::fmt::Debug for DeliveryStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeliveryStream")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl Stream for DeliveryStream {
    type Item = Result<StreamItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let waiting = if let Some(waiting) = this.waiting.as_mut() {
            waiting
        } else {
            let (pull, answer) = oneshot::channel();
            if this.pulls.send(pull).is_err() {
                this.ended = true;
                this.cut = true;
                return Poll::Ready(None);
            }
            this.waiting.insert(answer)
        };
        let Poll::Ready(answered) = Pin::new(waiting).poll(context) else {
            return Poll::Pending;
        };
        this.waiting = None;
        match answered {
            Ok(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Ok(Some(Err(error))) => {
                this.ended = true;
                Poll::Ready(Some(Err(error)))
            }
            Ok(None) => {
                this.ended = true;
                Poll::Ready(None)
            }
            Err(_) => {
                this.ended = true;
                this.cut = true;
                Poll::Ready(None)
            }
        }
    }
}
