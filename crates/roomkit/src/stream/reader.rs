//! The reading of a turn's streamed response, the driver's side of
//! `ReadStream` (RFC §10.6.4, §12.2 steps 13s and 14s).
//!
//! The reader, the streaming target's `deliver_stream` or the driver's own
//! drain with no target, pulls a [`DeliveryStream`]; each pull is answered
//! here, one item at a time, and the response is pulled only then: a model
//! round or a tool call runs only as it is read, so one that stops leaves
//! no call started after that point. The response is the turn's on its
//! owner's side (`turns::OwnerTurn`): each read pulls its executor across
//! the turn backend (RFC §10.6.4). A text delta reaches the reader at
//! once; a row the response ends is handed to the operation first
//! ([`Entry::Row`]), which commits it, and the reader is handed it as the
//! store holds it.
//!
//! A read in flight when the reader stops is let finish: a round of calls
//! that runs ends, its ends stored with their real results, and the
//! response is closed before its next round. A call announced but not
//! running never runs, its end stored `failed`. The text the reader was
//! handed since the last row is stored marked `cancelled`, the turn's
//! record `cancelled`. A read stopped from outside, by the operation or the
//! kit's close, cuts the turn first: `cancel` crosses, its executor cuts
//! the call it runs, and the turn's end is awaited within its cancel bound
//! before the calls left open are ended `failed`, their error `cancelled`,
//! or `kit_closed` for the kit's close (RFC §12.10.4). A response that fails
//! stores the text it produced,
//! hands its reader that row, then the failure; a streaming channel that
//! fails leaves the text to every target. The turn's record, when no row
//! carried it, is written on its last message once the response is over,
//! a direct update (`ON_EVENT_UPDATED` fires) made in the room's work.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use roomkit_core::model::{
    ChannelBinding, ChannelOutput, ErrorRecord, EventDirection, EventId, EventSource, EventUpdates,
    Metadata, RoomEvent, RoomId,
};
use roomkit_core::ops::{ChannelOutcome, Entry, Outcome, ROW_HIDDEN, ROW_WITHHELD, StreamTarget};
use tokio::sync::{mpsc, oneshot, watch};

use super::segments::Segments;
use super::{DeliveryStream, Pulled, ResponseItem, StreamItem};
use crate::RoomKit;
use crate::channel::ChannelError;
use crate::context::{self, Adopting};
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::turns::{OwnerTurn, RemoteItem, record_of};

/// The error of a call a stream's reader stopped before it ended.
const CANCELLED: &str = "cancelled";

/// The error of a call open when its response, or its streaming channel,
/// failed.
const TURN_FAILED: &str = "turn failed";

/// The error of a call whose round failed after its reader stopped.
const ROUND_FAILED: &str = "tool round failed";

/// A turn's stream, kept by the run from its turn's output until it is
/// read: the owner's side of the turn, whose reads pull its executor
/// (RFC §10.6.4). Dropped unread, it closes the turn's stream.
#[derive(Debug)]
pub(crate) struct KeptStream {
    pub(crate) turn: OwnerTurn,
}

/// What a read of the turn's response gave.
type Read = Option<Result<RemoteItem, ChannelError>>;

/// The reason a read the operation stops cuts its turn with (RFC §10.1
/// "Explicit delivery cancellation").
const CALLER_CANCELLED: &str = "caller_cancelled";

/// The reason the kit's shutdown cuts a turn with (RFC §10.6.8).
const KIT_CLOSED: &str = "kit_closed";

/// Who reads a stream's rows in the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reads {
    /// The driver: no streaming target.
    Nobody,
    /// The streaming target, every row of the response, as the Python
    /// implementation streams them.
    Target,
    /// The target failed, or may not read a row of the response: it reads
    /// nothing more in the stream, the rows written since reaching every
    /// target their plans name.
    Lost,
}

/// How the response ended, as its rows were stored.
#[derive(Debug)]
enum Over {
    /// It reached its end.
    End,
    /// It failed.
    Failed(ErrorRecord),
}

/// Reads `kept`, the stream of `responder`'s turn on `answers`, to its end
/// or until `target`, its streaming channel, stops reading it; the driver
/// reads it with no target. Answers the operation's `ReadStream`: the
/// target's output, its `error` the response's failure or the target's.
pub(crate) async fn read<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    kept: Option<KeptStream>,
    stops: Stops,
    responder: ChannelBinding,
    answers: RoomEvent,
    target: Option<StreamTarget>,
) -> Outcome {
    let Some(kept) = kept else {
        let error = ErrorRecord::new("ChannelError", "the turn's stream is gone");
        return Outcome::Failed(error);
    };
    let (pulls, pulled) = mpsc::unbounded_channel();
    let delivery = DeliveryStream::new(pulls);
    let channel = target
        .as_ref()
        .and_then(|target| Some((inner.channel(&target.binding.channel_id)?, target.clone())));
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(responder.channel_id.clone(), responder.channel_type.clone())
    };
    let mut producer = Producer {
        room_id: answers.room_id.clone(),
        segments: Segments::new(source, answers),
        stream: Some(kept.turn),
        cut: None,
        unanswered: None,
        reads: channel.as_ref().map_or(Reads::Nobody, |_| Reads::Target),
        ready: VecDeque::new(),
        over: None,
        read_through: false,
        in_flight: None,
    };
    let placeholder = producer.segments.placeholder();
    let (done, reader_done) = oneshot::channel::<()>();
    let reader = async move {
        let read = match channel {
            Some((channel, target)) => {
                channel
                    .deliver_stream(delivery, placeholder, target.binding, *target.context)
                    .await
            }
            None => drain(delivery).await,
        };
        // The producer stops at its next wait; one gone already needs no word.
        let _signalled = done.send(());
        read
    };
    let read = producer.read_by(reader, pulled, reader_done, stops).await;
    let output = producer.finish(read).await;
    if let Some((event_id, metadata)) = producer.segments.owed_record() {
        write_record(inner, &producer.room_id, &event_id, metadata).await;
    }
    ChannelOutcome::Output(Box::new(output)).into()
}

/// The driver's reader of a stream no channel streams: read to its end.
async fn drain(mut stream: DeliveryStream) -> Result<ChannelOutput, ChannelError> {
    while let Some(item) = stream.next().await {
        item?;
    }
    Ok(ChannelOutput::default())
}

/// What stops a read from outside: the operation's stop of this read, and
/// the kit's close, which cuts every turn as it starts.
#[derive(Debug)]
pub(crate) struct Stops {
    /// `true` once the operation stops the read.
    pub(crate) read: watch::Receiver<bool>,
    /// `true` once the kit's close started.
    pub(crate) kit: watch::Receiver<bool>,
}

impl Stops {
    /// Returns once the read is stopped, with the reason its turn is cut
    /// with.
    async fn stopped(&mut self) -> &'static str {
        let read = async { self.read.wait_for(|stopped| *stopped).await.is_ok() };
        let kit = async { self.kit.wait_for(|cut| *cut).await.is_ok() };
        tokio::select! {
            true = read => CALLER_CANCELLED,
            true = kit => KIT_CLOSED,
            else => std::future::pending().await,
        }
    }
}

/// The answering side of a stream's pulls.
struct Producer {
    room_id: RoomId,
    segments: Segments,
    /// The turn's response, until it is closed.
    stream: Option<OwnerTurn>,
    /// Why the read was stopped from outside, once it was: the turn is cut.
    cut: Option<&'static str>,
    /// The reader's pull the read stopped on, unanswered: kept open until
    /// the reader is dropped, so that it never reads an end there.
    unanswered: Option<oneshot::Sender<Pulled>>,
    /// Who reads the stream's rows in it.
    reads: Reads,
    /// What the reader is handed next, in order.
    ready: VecDeque<Result<StreamItem, ChannelError>>,
    /// How the response ended, once it did.
    over: Option<Over>,
    /// Whether the reader was handed the stream's end, or its failure.
    read_through: bool,
    /// What a read in flight when the reader stopped gave.
    in_flight: Option<Read>,
}

impl Producer {
    /// Answers the pulls of `reader`, the stream's target or the driver's
    /// drain, until both are done: what the reader returned. The reader is
    /// polled first, so that what was handed it is taken before the
    /// producer's end is seen. The kit's close cuts the reader with the
    /// turn, as the Python implementation cancels its stream readers: a
    /// streaming target that reads on no more holds nothing, its
    /// `deliver_stream()` dropped once polled one last time, so that what
    /// it was handed it took, and that text is stored (RFC §12.2 step
    /// 13s).
    async fn read_by(
        &mut self,
        reader: impl Future<Output = Result<ChannelOutput, ChannelError>>,
        pulls: mpsc::UnboundedReceiver<oneshot::Sender<Pulled>>,
        reader_done: oneshot::Receiver<()>,
        stops: Stops,
    ) -> Result<ChannelOutput, ChannelError> {
        let mut reader = Box::pin(reader);
        let mut pulls = pulls;
        {
            let mut serving = pin!(self.serve(&mut pulls, reader_done, stops));
            tokio::select! {
                biased;
                read = &mut reader => {
                    serving.await;
                    return read;
                }
                () = &mut serving => {}
            }
        }
        if self.cut != Some(KIT_CLOSED) {
            // The reader reads the stream's end.
            drop(self.unanswered.take());
            drop(pulls);
            return reader.await;
        }
        // The pulls and the one left unanswered stay open while the reader
        // is polled one last time: it is dropped where it waits, never
        // reading an end the stream did not have.
        let last = poll_fn(|cx| Poll::Ready(reader.as_mut().poll(cx))).await;
        drop(reader);
        drop(self.unanswered.take());
        drop(pulls);
        match last {
            Poll::Ready(read) => read,
            Poll::Pending => Ok(ChannelOutput::default()),
        }
    }

    /// Answers the reader's pulls, until it stops or is handed the end, or
    /// `stop` stops the read, the turn cut from outside: the reader then
    /// reads the stream's end, and the response is settled as a stopped
    /// one's (RFC §12.2 step 13s).
    async fn serve(
        &mut self,
        pulls: &mut mpsc::UnboundedReceiver<oneshot::Sender<Pulled>>,
        mut reader_done: oneshot::Receiver<()>,
        mut stops: Stops,
    ) {
        loop {
            let pull = tokio::select! {
                biased;
                _ = &mut reader_done => return,
                reason = stops.stopped() => {
                    self.cut = Some(reason);
                    return;
                }
                pull = pulls.recv() => match pull {
                    Some(pull) => pull,
                    None => return,
                },
            };
            if !self.answer(pull, &mut stops).await {
                return;
            }
        }
    }

    /// Answers one pull: the next item, the response read further when
    /// none is ready; whether the reader goes on. A read `stop` stops while
    /// the response is read is abandoned, a running call cut with it, as a
    /// turn cancelled from outside is (RFC §12.2 step 13s).
    async fn answer(&mut self, pull: oneshot::Sender<Pulled>, stops: &mut Stops) -> bool {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return self.hand(pull, Some(item));
            }
            if self.over.is_some() {
                return self.hand(pull, None);
            }
            let read = tokio::select! {
                biased;
                reason = stops.stopped() => {
                    self.cut = Some(reason);
                    self.unanswered = Some(pull);
                    return false;
                }
                read = self.pull_turn() => read,
            };
            if pull.is_closed() {
                // The reader stopped while the read was in flight.
                self.in_flight = Some(read);
                return false;
            }
            self.take(read).await;
        }
    }

    /// Hands `item` to the reader; whether it goes on. Text counts as read
    /// once handed.
    fn hand(&mut self, pull: oneshot::Sender<Pulled>, item: Pulled) -> bool {
        let text = match &item {
            Some(Ok(StreamItem::Text(text))) => Some(text.clone()),
            Some(Ok(StreamItem::Row(_)) | Err(_)) | None => None,
        };
        let ends = matches!(item, None | Some(Err(_)));
        if pull.send(item).is_err() {
            return false;
        }
        self.segments.handed();
        if let Some(text) = text {
            self.segments.text(&text);
        }
        if ends {
            self.read_through = true;
        }
        !ends
    }

    /// Reads the turn's response one item further: a pull its executor
    /// reads on (RFC §10.6.4).
    async fn pull_turn(&mut self) -> Read {
        self.stream.as_mut()?.next_item().await
    }

    /// Takes what a read gave: text for the reader, the rows a call's start
    /// or end writes, the turn's record, the response's end or failure.
    async fn take(&mut self, read: Read) {
        match read {
            None => {
                if let Some(row) = self.segments.last_text(false) {
                    self.commit(row).await;
                }
                self.over = Some(Over::End);
            }
            // The target reads the text as it comes; nobody else does.
            Some(Ok(RemoteItem::Item(ResponseItem::Text(delta)))) => match self.reads {
                Reads::Target => self.ready.push_back(Ok(StreamItem::Text(delta))),
                // Nobody is handed the text: the turn began all the same, and
                // a cut ends it `cancelled` (RFC §6.4).
                Reads::Nobody | Reads::Lost => {
                    self.segments.handed();
                    self.segments.text(&delta);
                }
            },
            Some(Ok(RemoteItem::Item(ResponseItem::ToolCallStart(start)))) => {
                let rows = self.segments.start(start);
                self.commit_all(rows).await;
            }
            Some(Ok(RemoteItem::Item(ResponseItem::ToolCallEnd(end)))) => {
                let rows = self.segments.end(end);
                self.commit_all(rows).await;
            }
            Some(Ok(RemoteItem::Item(ResponseItem::LoopEnd(marker)))) => {
                self.segments.end_turn(marker.record());
            }
            // The loop goes on after a round without a call: its text is a
            // segment of its own (RFC §6.4).
            Some(Ok(RemoteItem::SegmentBreak)) => {
                if let Some(row) = self.segments.last_text(false) {
                    self.commit(row).await;
                }
            }
            Some(Err(error)) => {
                if let Some(row) = self.segments.last_text(false) {
                    self.commit(row).await;
                }
                let record = record_of(&*error);
                self.ready.push_back(Err(error));
                self.over = Some(Over::Failed(record));
            }
        }
    }

    /// Settles the response once its reader is done, read `read` returned
    /// with: read through, it ended or failed; otherwise it stopped (RFC
    /// §12.2 step 13s), unless the target failed. Returns what answers the
    /// operation's read.
    async fn finish(&mut self, read: Result<ChannelOutput, ChannelError>) -> ChannelOutput {
        let (mut output, target_error) = match read {
            Ok(output) => (output, None),
            Err(error) => {
                let record = ErrorRecord::new("ChannelError", error.to_string());
                (ChannelOutput::default(), Some(record))
            }
        };
        match self.over.take() {
            Some(Over::Failed(error)) => {
                if let Some(target_error) = &target_error {
                    tracing::warn!(
                        target: "roomkit::driver",
                        room_id = %self.room_id,
                        error = %target_error.message,
                        "the streaming channel failed on a response that failed"
                    );
                }
                self.close_open(TURN_FAILED).await;
                output.error = Some(error);
            }
            Some(Over::End) if self.read_through && target_error.is_none() => {}
            over @ (Some(Over::End) | None) => {
                if target_error.is_some() {
                    self.target_failed().await;
                    output.error = target_error;
                } else {
                    self.stopped(over.is_some()).await;
                }
            }
        }
        output
    }

    /// The reader stopped before the end (RFC §12.2 step 13s): the round
    /// in flight is let finish, the calls it did not run are closed
    /// `failed`, the response is closed before its next round, and the
    /// text the reader was handed is stored marked `cancelled`, the turn
    /// recording `cancelled`. `ended` says the response had reached its end.
    async fn stopped(&mut self, ended: bool) {
        self.cut_turn().await;
        let error = match self.settle_in_flight().await {
            // A call the kit's close cut ends `kit_closed` (RFC §12.10.4).
            CANCELLED if self.cut == Some(KIT_CLOSED) => KIT_CLOSED,
            error => error,
        };
        self.close_open(error).await;
        self.stream = None;
        if !ended {
            self.segments.stopped();
        }
        if let Some(row) = self.segments.last_text(true) {
            self.commit(row).await;
        }
    }

    /// Cuts the turn a stop from outside stopped (RFC §10.6.8): `cancel`
    /// crosses, its executor cuts the call it runs, and the turn's end is
    /// awaited within its cancel bound before its rows are settled.
    async fn cut_turn(&mut self) {
        let (Some(reason), Some(turn)) = (self.cut, self.stream.as_mut()) else {
            return;
        };
        turn.cancel(reason);
        turn.ended().await;
    }

    /// The streaming channel failed, the response did not: what it was
    /// handed may not all have been shown, so what is left goes to every
    /// target. A round of calls running is let finish, its ends stored with
    /// their real results, and the calls left open are closed `failed`
    /// (RFC §12.2 step 13s).
    async fn target_failed(&mut self) {
        self.reads = Reads::Lost;
        let error = match self.settle_in_flight().await {
            ROUND_FAILED => ROUND_FAILED,
            _ => TURN_FAILED,
        };
        self.close_open(error).await;
        self.stream = None;
        if let Some(row) = self.segments.last_text(false) {
            self.commit(row).await;
        }
    }

    /// Lets the read in flight when the reader stopped finish: a round of
    /// calls that runs ends, each end stored, and the response is read no
    /// further once none is open. The error the calls left open close with.
    async fn settle_in_flight(&mut self) -> &'static str {
        let mut read = self.in_flight.take();
        loop {
            match read {
                Some(Some(Ok(RemoteItem::Item(ResponseItem::ToolCallEnd(end))))) => {
                    // The stream no longer carries rows: to every target,
                    // the streaming one included (RFC §12.2 step 13s).
                    for row in self.segments.end(end) {
                        self.commit_as(row, false).await;
                    }
                    if !self.segments.has_open_calls() {
                        return CANCELLED;
                    }
                    read = Some(self.pull_turn().await);
                }
                // A start: the round had not begun to run.
                Some(Some(Ok(RemoteItem::Item(ResponseItem::ToolCallStart(start))))) => {
                    self.segments.opened_unwritten(start);
                    return CANCELLED;
                }
                Some(Some(Err(error))) => {
                    tracing::warn!(
                        target: "roomkit::driver",
                        room_id = %self.room_id,
                        %error,
                        "a running tool round failed after its reader stopped"
                    );
                    return ROUND_FAILED;
                }
                Some(
                    Some(Ok(
                        RemoteItem::Item(ResponseItem::Text(_) | ResponseItem::LoopEnd(_))
                        | RemoteItem::SegmentBreak,
                    ))
                    | None,
                )
                | None => {
                    return CANCELLED;
                }
            }
        }
    }

    /// Closes the calls still open, `failed` with `error`, to every target,
    /// the streaming one included: the stream no longer carries rows (RFC
    /// §12.2 step 13s).
    async fn close_open(&mut self, error: &str) {
        let stream = self.stream.as_ref();
        let ran_with = |tool_id: &str| stream.and_then(|turn| turn.ran_with(tool_id));
        for row in self.segments.close_open(error, ran_with) {
            self.commit_as(row, false).await;
        }
    }

    async fn commit_all(&mut self, rows: Vec<RoomEvent>) {
        for row in rows {
            self.commit(row).await;
        }
    }

    /// Hands `row` to the operation, which commits it; the target reads it
    /// in the stream, while it reads the stream.
    async fn commit(&mut self, row: RoomEvent) {
        let streamed = self.reads == Reads::Target;
        self.commit_as(row, streamed).await;
    }

    /// Hands `row` to the operation, which commits it; `streamed`, its
    /// delivery skips the target, which is handed it once stored, if it
    /// still reads.
    async fn commit_as(&mut self, row: RoomEvent, streamed: bool) {
        let entry = Entry::Row {
            room_id: self.room_id.clone(),
            event: Box::new(row),
            streamed,
        };
        match settled(context::adopt_by_own_run(entry).await) {
            Settled::Stored(stored) => {
                self.segments.stored(&stored);
                if streamed {
                    self.ready.push_back(Ok(StreamItem::Row(stored)));
                }
            }
            Settled::Hidden(stored) => self.segments.stored(&stored),
            Settled::Withheld(stored) => {
                if let Some(stored) = stored {
                    self.segments.stored(&stored);
                }
                // The target reads nothing more of the response in the
                // stream, neither text nor rows, the rest delivered as each
                // row's plan says.
                self.reads = Reads::Lost;
            }
            Settled::Nothing => {}
        }
    }
}

/// What a row's settled entry tells its reader.
enum Settled {
    /// The store holds the row, which the target reads in the stream when
    /// it reads the stream's rows of its kind.
    Stored(Box<RoomEvent>),
    /// The store holds the tool-call row, which the target may not read; it
    /// reads the rest of the stream ([`ROW_HIDDEN`]).
    Hidden(Box<RoomEvent>),
    /// The target may read nothing more of the response in the stream
    /// ([`ROW_WITHHELD`]); the row, when the store holds it unblocked.
    Withheld(Option<Box<RoomEvent>>),
    /// Refused or stored blocked: no reader is handed it.
    Nothing,
}

/// Writes the turn's record, `metadata`, on its last message, `event_id`,
/// when no row carried it (RFC §6.4 "The turn's record"): a direct update,
/// `ON_EVENT_UPDATED` firing as for any; best effort, a failure logged.
async fn write_record<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    room_id: &RoomId,
    event_id: &EventId,
    metadata: Metadata,
) {
    let kit = RoomKit {
        inner: Arc::clone(inner),
    };
    let updates = EventUpdates {
        metadata: Some(metadata),
        ..EventUpdates::default()
    };
    if let Err(error) = kit.update_own(room_id, event_id, updates).await {
        if inner.tasks.is_closing() {
            tracing::debug!(
                target: "roomkit::driver",
                room_id = %room_id,
                event_id = %event_id,
                %error,
                "the turn's record was not written on its last message: the kit closed"
            );
            return;
        }
        tracing::warn!(
            target: "roomkit::driver",
            room_id = %room_id,
            event_id = %event_id,
            %error,
            "the turn's record could not be written on its last message"
        );
    }
}

/// What a row's settled entry tells its reader: a row settled
/// [`ROW_WITHHELD`] withholds the rest of the stream, blocked or refused as
/// it may be.
fn settled(adopting: Adopting) -> Settled {
    let Adopting::Settled(result, _completion) = adopting else {
        return Settled::Nothing;
    };
    let Ok(result) = *result else {
        return Settled::Nothing;
    };
    let stored = result.event.filter(|_| !result.blocked).map(Box::new);
    match (result.reason.as_deref(), stored) {
        (Some(ROW_WITHHELD), stored) => Settled::Withheld(stored),
        (Some(ROW_HIDDEN), Some(stored)) => Settled::Hidden(stored),
        (_, Some(stored)) => Settled::Stored(stored),
        (_, None) => Settled::Nothing,
    }
}
