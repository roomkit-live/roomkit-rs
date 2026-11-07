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
//! or `kit_closed` for the kit's close (RFC §12.10.4). That stop stores what
//! the turn leaves at once, and only the read's caller waits for a reader
//! still working past it. A response that fails stores the text it produced,
//! hands its reader that row, then the failure; a streaming channel that
//! fails leaves the text to every target. The turn's record, when no row
//! carried it, is written on its last message, else its last row, as the store
//! holds it, once the response is over, a direct update (`ON_EVENT_UPDATED`
//! fires) made in the room's work; the read's answer holds that row.

use std::collections::VecDeque;
use std::error::Error;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Poll;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, EventContent, EventDirection, EventId,
    EventSource, EventType, EventUpdates, Metadata, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::failure_log::{self, Log};
use roomkit_core::ops::{
    ChannelOutcome, Entry, Notice, Outcome, ROW_HIDDEN, ROW_WITHHELD, StreamFailure, StreamTarget,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use super::segments::Segments;
use super::silence::Hold;
use super::{DeliveryStream, Pulled, ResponseItem, StreamItem, UsageMarker};
use crate::RoomKit;
use crate::channel::ChannelError;
use crate::context::{self, Adopting, ExecutionContext, ReadTurns};
use crate::error::RoomKitError;
use crate::kit::Inner;
use crate::room_turns::Place;
use crate::store::ConversationStore;
use crate::tasks::{Kind, Marks, Unit};
use crate::turn_runners::{FirstPass, Then};
use crate::turns::{OwnerTurn, RemoteError, RemoteItem, TurnError, record_of};

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
    /// What follows the turn when it is a strategy's first pass, read with
    /// its text kept out of the room ([`Taken::Passes`](crate::turn_runners::Taken::Passes)).
    pub(crate) then: Option<Then>,
    /// The turn's place in its channel's line in the room, given up once what
    /// it said is stored (RFC §6.4), when it waited one.
    pub(crate) place: Option<Place>,
}

/// What a read of the turn's response gave.
type Read = Option<Result<RemoteItem, ChannelError>>;

/// The reason a read the operation stops cuts its turn with (RFC §10.1
/// "Explicit delivery cancellation").
pub(crate) const CALLER_CANCELLED: &str = "caller_cancelled";

/// The key of a caller's record under which each turn's end is (RFC §6.4).
const TURNS: &str = "turns";

/// The reason the kit's shutdown cuts a turn with (RFC §10.6.8).
pub(crate) const KIT_CLOSED: &str = "kit_closed";

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

/// What the operation's `ReadStream` asks of a stream's read.
#[derive(Debug)]
pub(crate) struct StreamRead {
    /// The intelligence channel whose response it is.
    pub(crate) responder: ChannelBinding,
    /// The event the response answers.
    pub(crate) answers: RoomEvent,
    /// The channel that reads the stream as it is written, if one does.
    pub(crate) target: Option<StreamTarget>,
    /// The response's own id, which each of its rows carries.
    pub(crate) correlation_id: String,
    /// Whether its reader commits the turn as a delegated task's trace: only
    /// such a turn's answer text is published (RFC §6.4).
    pub(crate) as_trace: bool,
}

/// Reads `kept`, the stream of the read's responder's turn on its event,
/// to its end or until its target, its streaming channel, stops reading
/// it; the driver reads it with no target. Answers the operation's
/// `ReadStream`: the target's output, its `error` the response's failure
/// or the target's.
pub(crate) async fn read<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    kept: Option<KeptStream>,
    stops: Stops,
    read: StreamRead,
) -> Outcome {
    let Some(mut kept) = kept else {
        let error = ErrorRecord::new("ChannelError", "the turn's stream is gone");
        return Outcome::Failed(error);
    };
    if let Some(then) = kept.then.take() {
        return super::passes::read(inner, kept, then, stops, read).await;
    }
    let (output, _) = Box::pin(read_kept(inner, kept, stops, read, false)).await;
    ChannelOutcome::Output(Box::new(output)).into()
}

/// Reads `kept` as [`read`] does: its output, and, for a strategy's first
/// pass (`first_pass`), what the pass gave. A first pass has no target,
/// its text is kept out of the room, its calls stored as any turn's, and
/// its record is written on no message (Python's `persist_tool_calls`,
/// RFC §19.7.3).
pub(super) async fn read_kept<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    kept: KeptStream,
    stops: Stops,
    read: StreamRead,
    first_pass: bool,
) -> (ChannelOutput, Option<FirstPass>) {
    let failing = Failing::of(&read);
    let StreamRead {
        responder,
        answers,
        target,
        correlation_id,
        as_trace,
    } = read;
    if as_trace {
        kept.turn.read_as_trace();
    }
    // A strategy's first pass is read as Python reads it, under a cascade of
    // its own: its turn's end is not its caller's (RFC §10.6.8, §19.7.3).
    if !first_pass {
        stops.read_turns.reads(kept.turn.canceller().owed_end());
    }
    let target = target.filter(|_| !first_pass);
    let (pulls, pulled) = mpsc::unbounded_channel();
    let delivery = DeliveryStream::new(pulls);
    let channel = target
        .as_ref()
        .and_then(|target| Some((inner.channel(&target.binding.channel_id)?, target.clone())));
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(responder.channel_id.clone(), responder.channel_type.clone())
    };
    // Python's `_discussion_silence`: a stream of one of the agents of the
    // discussion the room holds.
    let hold = channel
        .as_ref()
        .and_then(|_| inner.discussions.get(&answers.room_id))
        .and_then(|seam| seam.silence_for(&responder.channel_id))
        .map(Hold::new);
    let live = kept.turn.record();
    let mut producer = Producer {
        room_id: answers.room_id.clone(),
        failing,
        segments: Segments::new(source, answers, correlation_id).reading(live.clone()),
        stream: Some(kept.turn),
        cut: None,
        place: kept.place,
        settled: false,
        settled_on: None,
        rewritten: None,
        reader_unit: Unit::new(),
        reads: channel.as_ref().map_or(Reads::Nobody, |_| Reads::Target),
        ready: VecDeque::new(),
        over: None,
        failed_at: None,
        read_through: false,
        in_flight: None,
        kept_out: first_pass.then(String::new),
        hold,
        precounted: false,
    };
    let placeholder = producer.segments.placeholder();
    let reader_marks = Marks::current().with(producer.reader_unit);
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
    let reader = reader_marks.scoped(reader);
    let read = producer
        .read_by(reader, pulled, reader_done, inner, stops)
        .await;
    let room_id = producer.room_id.clone();
    let handling = || {
        let target = target.as_ref().map(|target| &target.binding.channel_id);
        let target = target.map(ToString::to_string).unwrap_or_default();
        let source = &responder.channel_id;
        format!(
            "streaming channel {target} handling the failed response of {source} in room {room_id}"
        )
    };
    let mut output = producer.finish(read, handling, inner).await;
    // The turn's record reaches the operation once its stream is read, how
    // it ended under the caller's `turns`, as Python's `_add_turn` puts it
    // there (RFC §6.4); a `turns` the turn wrote is not its caller's, nor
    // anything of the target's own output.
    let mut record = live.to_metadata();
    record.shift_remove(TURNS);
    if let Some(entry) = producer.segments.turn_entry() {
        add_turn(&mut record, &responder.channel_id, entry);
    }
    output.response_metadata = record.into();
    if let Some(answer) = producer.kept_out.take() {
        let pass = first_pass_of(
            answer,
            producer.segments.record(),
            &responder,
            &producer.room_id,
        );
        return (output, Some(pass));
    }
    producer.write_owed_record(inner).await;
    output.response_events = producer.rewritten.take().into_iter().collect();
    (output, None)
}

/// What a first pass gave: its answer only when its turn completed, the
/// text it ended on; a turn its round cap, deadline or budget cut short
/// has none, its narration no answer, which is logged unless someone
/// stopped it (Python's `_handed_on`, RFC §6.4).
fn first_pass_of(
    answer: String,
    record: Metadata,
    responder: &ChannelBinding,
    room_id: &RoomId,
) -> FirstPass {
    let end = record
        .get("loop_end_reason")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let completed = end.as_deref().is_none_or(|end| end == "completed");
    if !completed && end.as_deref() != Some(CANCELLED) {
        tracing::warn!(
            target: "roomkit::tasks",
            channel_id = %responder.channel_id,
            room_id = %room_id,
            "Turn of {} in room {room_id} ended {}: no answer to hand on",
            responder.channel_id,
            end.as_deref().unwrap_or_default()
        );
    }
    FirstPass {
        answer: if completed { answer } else { String::new() },
        end,
        record,
    }
}

/// Puts `channel_id`'s turn `entry` under `record`'s `turns`, beside the
/// other channels' (Python's `add_turn_entry`).
fn add_turn(record: &mut Metadata, channel_id: &ChannelId, entry: Metadata) {
    let turns = record
        .entry(TURNS.to_owned())
        .or_insert_with(|| Value::Object(Metadata::new()));
    if !turns.is_object() {
        *turns = Value::Object(Metadata::new());
    }
    if let Value::Object(turns) = turns {
        turns.insert(channel_id.to_string(), Value::Object(entry));
    }
}

/// Where `error` lives, which tells the very error handed to a reader when
/// it raises it again, as Python's `is` does.
fn address_of(error: &ChannelError) -> usize {
    std::ptr::from_ref::<dyn Error + Send + Sync>(&**error)
        .cast::<()>()
        .addr()
}

/// The driver's reader of a stream no channel streams: read to its end.
async fn drain(mut stream: DeliveryStream) -> Result<ChannelOutput, ChannelError> {
    while let Some(item) = stream.next().await {
        item?;
    }
    Ok(ChannelOutput::default())
}

/// What `reader` gives when polled one last time, then dropped where it
/// waits: nothing more than a default output if it is not done.
async fn last_poll<R>(mut reader: Pin<Box<R>>) -> Result<ChannelOutput, ChannelError>
where
    R: Future<Output = Result<ChannelOutput, ChannelError>> + ?Sized,
{
    match poll_fn(|cx| Poll::Ready(reader.as_mut().poll(cx))).await {
        Poll::Ready(read) => read,
        Poll::Pending => Ok(ChannelOutput::default()),
    }
}

/// What stops a read from outside: the operation's stop of this read, the
/// kit's close, which cuts every turn as it starts, and the cut of the
/// held task whose work the read runs within (RFC §23.3).
#[derive(Clone, Debug)]
pub(crate) struct Stops {
    /// `true` once the operation stops the read.
    pub(crate) read: watch::Receiver<bool>,
    /// `true` once the kit's close started.
    pub(crate) kit: watch::Receiver<bool>,
    /// The reason the task the read runs within is cut with, once it is.
    pub(crate) task: Option<watch::Receiver<Option<&'static str>>>,
    /// Where the read tells whether its turn owes its end (RFC §10.6.8).
    pub(crate) read_turns: ReadTurns,
}

impl Stops {
    /// Returns once what ends the wait for a reader a stop found reading
    /// comes: the kit's close, which gives it its grace, or the cut of the
    /// held task the read runs within, whose caller has no timeout to bound
    /// its wait (RMK-1326).
    async fn until_reader_is_left(&mut self) {
        let kit = async { self.kit.wait_for(|cut| *cut).await.is_ok() };
        let task = async {
            let task = self.task.as_mut()?;
            task.wait_for(Option::is_some).await.ok().map(|_| ())
        };
        tokio::select! {
            true = kit => {}
            Some(()) = task => {}
            else => std::future::pending().await,
        }
    }

    /// Returns once the read is stopped, with the reason its turn is cut
    /// with.
    pub(crate) async fn stopped(&mut self) -> &'static str {
        let read = async { self.read.wait_for(|stopped| *stopped).await.is_ok() };
        let kit = async { self.kit.wait_for(|cut| *cut).await.is_ok() };
        let task = async {
            let task = self.task.as_mut()?;
            task.wait_for(Option::is_some)
                .await
                .ok()
                .and_then(|cut| *cut)
        };
        // The kit's close first: a cut it races ends `kit_closed`.
        tokio::select! {
            biased;
            true = kit => KIT_CLOSED,
            true = read => CALLER_CANCELLED,
            Some(reason) = task => reason,
            else => std::future::pending().await,
        }
    }
}

/// What a target's failure no operation reports is reported as (RFC §12.2
/// step 13s): one of a target left to end alone, or of a read whose work, a
/// held task's that was cut, may be dropped before it takes the read's
/// answer.
#[derive(Clone, Debug)]
struct Failing {
    responder: ChannelBinding,
    answers: RoomEvent,
    correlation_id: String,
    target: Option<ChannelId>,
    /// The cut of the held task the read runs within, if any.
    task: Option<watch::Receiver<Option<&'static str>>>,
}

impl Failing {
    fn of(read: &StreamRead) -> Self {
        Self {
            responder: read.responder.clone(),
            answers: read.answers.clone(),
            correlation_id: read.correlation_id.clone(),
            target: read
                .target
                .as_ref()
                .map(|target| target.binding.channel_id.clone()),
            task: None,
        }
    }

    /// Whether the held task the read runs within was cut: its work is
    /// dropped once none of its reads runs, maybe before it takes this
    /// read's answer (`tasks::cut`).
    fn task_cut(&self) -> bool {
        self.task
            .as_ref()
            .is_some_and(|task| task.borrow().is_some())
    }

    /// Logs `error`, the target's, and fires `ON_ERROR` for it as the core
    /// reports a read's (RFC §9.2, §12.2 step 13s), with the room as it
    /// stands; a failure that is the kit's own close is only noted, as
    /// Python's `_report_stream_failure` (RFC §12.10.4).
    async fn report<S: ConversationStore>(&self, inner: &Arc<Inner<S>>, error: &ChannelError) {
        if ended_by_close(inner, error) {
            tracing::debug!(target: "roomkit::driver", %error, "{} ended as the kit closed", self.what());
            return;
        }
        let error = target_record(error);
        failure_log::log_failure(Log::Framework, &error, &self.what(), false);
        let failure = StreamFailure {
            responder: &self.responder,
            answers: &self.answers,
            correlation_id: Some(&self.correlation_id),
            error: &error,
        };
        let payload = failure.payload(&*inner.environment());
        let hooks = inner
            .config()
            .hooks
            .observers(HookTrigger::OnError, &payload);
        if hooks.is_empty() {
            return;
        }
        let Some(room) = self.room(inner).await else {
            return;
        };
        let notice = Notice::AsyncHooks {
            trigger: HookTrigger::OnError,
            hooks,
            event: Box::new(payload),
            context: Box::new(failure.context(&room)),
            after_delivery: None,
            then: None,
        };
        drop(crate::driver::perform(inner, notice));
    }

    /// What failed, as the log names it.
    fn what(&self) -> String {
        let source = &self.responder.channel_id;
        let room_id = &self.answers.room_id;
        match &self.target {
            Some(target) => {
                format!("streaming delivery of {source} to {target} for room {room_id}")
            }
            None => format!("stream consumption (no targets) of {source} for room {room_id}"),
        }
    }

    /// The room as it stands, its observers' context; none for a room the
    /// store lacks or fails to read.
    async fn room<S: ConversationStore>(&self, inner: &Arc<Inner<S>>) -> Option<RoomContext> {
        let room_id = &self.answers.room_id;
        let recent_events = inner.config().recent_events().read_by_the_caller();
        match crate::driver::load_context(&*inner.store, room_id, recent_events).await {
            Ok(context) => context,
            Err(error) => {
                tracing::debug!(target: "roomkit::hooks", %room_id, %error, "no context for ON_ERROR");
                None
            }
        }
    }
}

/// Whether `error`, a target's, is the kit's close ending the work that met
/// it, as Python's `ended_by_close` tells it: once the close started, a call
/// into the kit it refused, or a turn link it closed (RFC §12.10.4).
fn ended_by_close<S>(inner: &Inner<S>, error: &ChannelError) -> bool {
    let refused = matches!(
        error.downcast_ref::<RoomKitError>(),
        Some(RoomKitError::KitClosed)
    );
    let link_closed = error
        .downcast_ref::<TurnError>()
        .is_some_and(|error| error.kind == "backend_closed");
    inner.tasks.is_closing() && (refused || link_closed)
}

/// A streaming target's own failure as its record reads: a boxed Rust error
/// has no class name.
fn target_record(error: &ChannelError) -> ErrorRecord {
    ErrorRecord::new("ChannelError", error.to_string())
}

/// The answering side of a stream's pulls.
struct Producer {
    room_id: RoomId,
    /// What reports a target's failure no operation takes.
    failing: Failing,
    segments: Segments,
    /// The turn's response, until it is closed.
    stream: Option<OwnerTurn>,
    /// Why the read was stopped from outside, once it was: the turn is cut.
    cut: Option<&'static str>,
    /// The turn's place in its channel's line in the room, while it holds it.
    place: Option<Place>,
    /// Whether the response's rows were settled at a cut, its reader still
    /// working.
    settled: bool,
    /// The failure they settled on, if any.
    settled_on: Option<ErrorRecord>,
    /// The row the turn's record was written on, as stored.
    rewritten: Option<RoomEvent>,
    /// The unit of the kit's work the reader runs in, beside the run's, which
    /// a close it calls spares.
    reader_unit: Unit,
    /// Who reads the stream's rows in it.
    reads: Reads,
    /// What the reader is handed next, in order.
    ready: VecDeque<Result<StreamItem, ChannelError>>,
    /// How the response ended, once it did.
    over: Option<Over>,
    /// Where the failure handed to the reader lives, once it was: the
    /// reader that raises it again raises no error of its own.
    failed_at: Option<usize>,
    /// Whether the reader was handed the stream's end, or its failure.
    read_through: bool,
    /// What a read in flight when the reader stopped gave.
    in_flight: Option<Read>,
    /// For a strategy's first pass, whose text is kept out of the room: the
    /// last text segment it wrote (Python's `_ToolRowsOnly`).
    kept_out: Option<String>,
    /// For a discussion's agent, the start of each segment held back from
    /// the target while it may become the silence token (RFC §19.7.5 rule
    /// 13).
    hold: Option<Hold>,
    /// Whether the next text the reader is handed was counted as read when
    /// it was held.
    precounted: bool,
}

impl Producer {
    /// Answers the pulls of `reader`, the stream's target or the driver's
    /// drain, until both are done: what the reader returned. The reader is
    /// polled first, so that what was handed it is taken before the
    /// producer's end is seen.
    ///
    /// A stop of the read from outside is Python's cancel of its reader, and
    /// a future has no `finally` to run once dropped (RMK-1326). A reader the
    /// stop finds reading is handed the stream's end, and what it does past
    /// it stands for that `finally`: it runs to its end, however long, the
    /// read waiting for it, a caller's own timeout bounding that caller only.
    /// The read does not wait for it to store what it has and give its place
    /// up: only its caller does, so a target stuck there holds no room's line
    /// (RMK-1343). The kit's close and the cut of a held task stop that wait:
    /// the reader ends on a task of the kit's, within the close's grace. A
    /// reader the stop finds past the stream, its end read or its stream
    /// dropped, has no such work left: it is dropped where it waits, once
    /// polled, as Python's cancel lands there.
    async fn read_by<S: ConversationStore>(
        &mut self,
        reader: impl Future<Output = Result<ChannelOutput, ChannelError>> + Send + 'static,
        pulls: mpsc::UnboundedReceiver<oneshot::Sender<Pulled>>,
        reader_done: oneshot::Receiver<()>,
        inner: &Arc<Inner<S>>,
        mut stops: Stops,
    ) -> Result<ChannelOutput, ChannelError> {
        self.failing.task.clone_from(&stops.task);
        let mut reader = Box::pin(reader);
        let mut pulls = pulls;
        {
            let mut serving = pin!(self.serve(&mut pulls, reader_done, &mut stops));
            tokio::select! {
                biased;
                read = &mut reader => {
                    serving.await;
                    return read;
                }
                () = &mut serving => {}
            }
        }
        drop(pulls);
        if self.cut.is_some() {
            // A reader that ends with its stream's end leaves nothing to wait
            // for: its read settles as any reader's that ended.
            tokio::select! {
                biased;
                read = &mut reader => return read,
                () = std::future::ready(()) => {}
            }
            if let Some(read) = self.settle_past(&mut reader, inner).await {
                return read;
            }
        }
        let leaves_it = tokio::select! {
            biased;
            read = &mut reader => return read,
            leaves_it = self.reader_past_the_stream_stopped(&mut stops) => leaves_it,
        };
        if !leaves_it {
            return last_poll(reader).await;
        }
        self.leave_to_end_alone(reader, inner);
        Ok(ChannelOutput::default())
    }

    /// Stores what a cut turn leaves, `reader` working on meanwhile: what it
    /// returned, if it did by then.
    async fn settle_past<R, S>(
        &mut self,
        reader: &mut Pin<Box<R>>,
        inner: &Arc<Inner<S>>,
    ) -> Option<Result<ChannelOutput, ChannelError>>
    where
        R: Future<Output = Result<ChannelOutput, ChannelError>> + ?Sized,
        S: ConversationStore,
    {
        let mut read = None;
        let mut settling = pin!(self.settle_at_the_cut(inner));
        loop {
            tokio::select! {
                biased;
                done = reader.as_mut(), if read.is_none() => read = Some(done),
                () = &mut settling => return read,
            }
        }
    }

    /// Stores what a cut turn leaves, its record with it, and gives its place
    /// in the room's line up, whatever its reader does from here.
    async fn settle_at_the_cut<S: ConversationStore>(&mut self, inner: &Arc<Inner<S>>) {
        self.settled_on = self.settle(false).await;
        self.settled = true;
        self.write_owed_record(inner).await;
        drop(self.place.take());
    }

    /// Writes the turn's record on its last message or else its last row,
    /// when no row carried it. A strategy's first pass, whose record is handed
    /// on (RFC §19.7.3), comes nowhere near: its reader ends with its stream,
    /// and its read returns before this.
    async fn write_owed_record<S: ConversationStore>(&mut self, inner: &Arc<Inner<S>>) {
        if let Some((event_id, record)) = self.segments.take_owed_record() {
            self.rewritten = write_record(inner, &self.room_id, &event_id, record).await;
        }
    }

    /// Returns once what ends the wait for a reader past the stream comes:
    /// for a reader a stop found reading, what [`Stops::until_reader_is_left`]
    /// waits for, whether it is left to end alone; for one already past the
    /// stream, any stop, which cuts the turn and drops it.
    async fn reader_past_the_stream_stopped(&mut self, stops: &mut Stops) -> bool {
        if self.cut.is_some() {
            stops.until_reader_is_left().await;
            return true;
        }
        let reason = stops.stopped().await;
        self.stop(reason);
        false
    }

    /// Lets `reader` end on a task of the kit's, in the current context, in
    /// its own unit and no other: the close waits for it within its grace,
    /// then drops it, even a close called from the turn whose run the read
    /// works for, which spares that run; a close `reader` called itself spares
    /// that unit, and returns to it. A failure of its own fires `ON_ERROR`,
    /// as the read's would have (RFC §12.2 step 13s).
    fn leave_to_end_alone<R, S>(&self, reader: Pin<Box<R>>, inner: &Arc<Inner<S>>)
    where
        R: Future<Output = Result<ChannelOutput, ChannelError>> + Send + 'static,
        S: ConversationStore,
    {
        let failing = self.failing.clone();
        let kit = Arc::clone(inner);
        let work = ExecutionContext::current().scope(async move {
            if let Err(error) = reader.await {
                failing.report(&kit, &error).await;
            }
        });
        drop(
            inner
                .tasks
                .spawn_in_unit(Kind::Turn, self.reader_unit, work),
        );
    }

    /// The read is stopped from outside, for `reason`: `cancel` crosses at
    /// once, so that the turn's cut waits for nothing the reader does (RFC
    /// §10.6.8).
    fn stop(&mut self, reason: &'static str) {
        self.cut = Some(reason);
        if let Some(turn) = self.stream.as_mut() {
            turn.cancel(reason);
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
        stops: &mut Stops,
    ) {
        loop {
            let pull = tokio::select! {
                biased;
                _ = &mut reader_done => return,
                reason = stops.stopped() => {
                    self.stop(reason);
                    return;
                }
                pull = pulls.recv() => match pull {
                    Some(pull) => pull,
                    None => return,
                },
            };
            if !self.answer(pull, stops).await {
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
                    self.stop(reason);
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
            Some(
                Ok(StreamItem::Row(_) | StreamItem::Thinking(_) | StreamItem::Usage(_)) | Err(_),
            )
            | None => None,
        };
        let ends = matches!(item, None | Some(Err(_)));
        if pull.send(item).is_err() {
            return false;
        }
        self.segments.handed();
        if let Some(text) = text
            && !std::mem::take(&mut self.precounted)
        {
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
        let marks_a_segment = matches!(
            read,
            None | Some(Ok(RemoteItem::SegmentBreak
                | RemoteItem::Item(
                    ResponseItem::ToolCallStart(_)
                        | ResponseItem::ToolCallEnd(_)
                        | ResponseItem::LoopEnd(_)
                        | ResponseItem::SegmentBreak
                )))
        );
        if marks_a_segment {
            self.end_held();
        }
        self.take_read(read).await;
        if marks_a_segment
            && self.segments.segment_text().is_empty()
            && let Some(hold) = self.hold.as_mut()
        {
            hold.next_segment();
        }
    }

    /// The held start of a segment the read ends, handed to the target
    /// unless it is the silence token (Python's `_end_held`).
    fn end_held(&mut self) {
        let Some(hold) = self.hold.as_mut() else {
            return;
        };
        let start = hold.end(self.segments.segment_text());
        if let Some(start) = start.filter(|_| self.reads == Reads::Target) {
            self.ready.push_back(Ok(StreamItem::Text(start)));
            self.precounted = true;
        }
    }

    /// [`take`](Self::take), past what holds a discussion's silence back.
    async fn take_read(&mut self, read: Read) {
        match read {
            None => {
                if let Some(row) = self.segments.last_text(false) {
                    self.commit(row).await;
                }
                self.over = Some(Over::End);
            }
            // The target reads the text as it comes; nobody else does. A
            // discussion's agent's segment is read as it comes and handed
            // once it cannot become the silence token.
            Some(Ok(RemoteItem::Item(ResponseItem::Text(delta)))) => match self.reads {
                Reads::Target => match self.hold.as_mut().filter(|hold| !hold.is_released()) {
                    Some(hold) => {
                        self.segments.handed();
                        self.segments.text(&delta);
                        if let Some(start) = hold.release(&delta, self.segments.segment_text()) {
                            self.ready.push_back(Ok(StreamItem::Text(start)));
                            self.precounted = true;
                        }
                    }
                    None => self.ready.push_back(Ok(StreamItem::Text(delta))),
                },
                // Nobody is handed the text: the turn began all the same, and
                // a cut ends it `cancelled` (RFC §6.4).
                Reads::Nobody | Reads::Lost => {
                    self.segments.handed();
                    self.segments.text(&delta);
                }
            },
            // The target reads the reasoning as it comes; the store holds
            // none of it, and a turn nobody reads began all the same.
            Some(Ok(RemoteItem::Item(ResponseItem::Thinking(delta)))) => match self.reads {
                Reads::Target => self.ready.push_back(Ok(StreamItem::Thinking(delta))),
                Reads::Nobody | Reads::Lost => self.segments.handed(),
            },
            Some(Ok(RemoteItem::Item(ResponseItem::Usage(marker)))) => self.take_usage(marker),
            Some(Ok(RemoteItem::Item(ResponseItem::ToolCallStart(start)))) => {
                self.segments.call_started();
                let rows = self.segments.start(start);
                self.commit_all(rows).await;
            }
            Some(Ok(RemoteItem::Item(ResponseItem::ToolCallEnd(end)))) => {
                self.segments.call_ended();
                let rows = self.segments.end(end);
                self.commit_all(rows).await;
            }
            Some(Ok(RemoteItem::Item(ResponseItem::LoopEnd(marker)))) => {
                self.segments.end_turn(marker.record());
            }
            // The loop goes on after a round without a call: its text is a
            // segment of its own (RFC §6.4).
            Some(Ok(RemoteItem::SegmentBreak | RemoteItem::Item(ResponseItem::SegmentBreak))) => {
                self.segments.segment_break();
                if let Some(row) = self.segments.last_text(false) {
                    self.commit(row).await;
                }
            }
            Some(Err(error)) => {
                if let Some(row) = self.segments.last_text(false) {
                    self.commit(row).await;
                }
                let record = record_of(&*error);
                self.failed_at = Some(address_of(&error));
                self.ready.push_back(Err(error));
                self.over = Some(Over::Failed(record));
            }
        }
    }

    /// Takes what a generation used: the record of a turn cut before its end
    /// holds it; the target reads it as it comes, the store never.
    fn take_usage(&mut self, marker: UsageMarker) {
        self.segments.told(&marker);
        match self.reads {
            Reads::Target => self.ready.push_back(Ok(StreamItem::Usage(marker))),
            Reads::Nobody | Reads::Lost => self.segments.handed(),
        }
    }

    /// Whether `error` is the very failure the reader was handed, raised
    /// again, as Python's `is` tells it: the response's failure, which only
    /// the kit makes (a turn's error that crossed, or the turn's link's),
    /// at the address the reader was handed it at. The address alone could
    /// be another error's, made by the reader where it freed the one it was
    /// handed.
    fn handed_back(&self, error: &ChannelError) -> bool {
        let the_kit_s = error.is::<RemoteError>() || error.is::<TurnError>();
        the_kit_s && self.failed_at == Some(address_of(error))
    }

    /// Settles the response once its reader is done, read `read` returned
    /// with, unless a cut did already. Returns what answers the operation's
    /// read, its `error` as [`read_error`](Self::read_error) chooses it. The
    /// target, `handling` the response when it raises another error than
    /// the response's failure, has that error logged (Python's
    /// `_stream_to_target`): the turn's failure stays the response's.
    async fn finish<S: ConversationStore>(
        &mut self,
        read: Result<ChannelOutput, ChannelError>,
        handling: impl FnOnce() -> String,
        inner: &Arc<Inner<S>>,
    ) -> ChannelOutput {
        let (mut output, target_error) = match read {
            Ok(output) => (output, None),
            // The response's own failure, raised again: no error of the
            // reader's.
            Err(error) if self.handed_back(&error) => (ChannelOutput::default(), None),
            Err(error) => (ChannelOutput::default(), Some(error)),
        };
        let failure = if self.settled {
            self.settled_on.take()
        } else {
            self.settle(target_error.is_some()).await
        };
        let error = self
            .read_error(inner, (failure, target_error), handling)
            .await;
        if error.is_some() || self.failing.task_cut() {
            output.error = error;
        }
        output
    }

    /// The error the read answers with, of the response's `failure` and the
    /// `target_error`: the response's failure, else the target's. A read of
    /// a held task's work that was cut answers with none, the cut winning
    /// over the response's failure, as Python's, and reports its target's
    /// failure itself: the operation, cut, may never take the answer
    /// (`tasks::cut`). A target's failure that is the kit's own close is no
    /// error of the call's, nor `ON_ERROR`'s, as Python's `ended_by_close`
    /// (RFC §12.10.4).
    async fn read_error<S: ConversationStore>(
        &self,
        inner: &Arc<Inner<S>>,
        (failure, target_error): (Option<ErrorRecord>, Option<ChannelError>),
        handling: impl FnOnce() -> String,
    ) -> Option<ErrorRecord> {
        let task_cut = self.failing.task_cut();
        match (failure, target_error) {
            (Some(failure), target_error) => {
                // Only a streaming target raises an error of its own:
                // the driver's drain raises what it is handed.
                if let Some(target_error) = &target_error
                    && self.reads != Reads::Nobody
                {
                    let target_error = target_record(target_error);
                    failure_log::log_failure(Log::Framework, &target_error, &handling(), false);
                }
                (!task_cut).then_some(failure)
            }
            (None, Some(target_error)) if task_cut || ended_by_close(inner, &target_error) => {
                self.failing.report(inner, &target_error).await;
                None
            }
            (None, Some(target_error)) => Some(target_record(&target_error)),
            (None, None) => None,
        }
    }

    /// Stores what the response leaves: read through, it ended or failed;
    /// otherwise it stopped (RFC §12.2 step 13s), unless `target_failed`.
    /// Returns its failure, if it failed.
    async fn settle(&mut self, target_failed: bool) -> Option<ErrorRecord> {
        match self.over.take() {
            Some(Over::Failed(error)) => {
                self.close_open(TURN_FAILED).await;
                return Some(error);
            }
            Some(Over::End) if self.read_through && !target_failed => {}
            over @ (Some(Over::End) | None) => {
                if target_failed {
                    self.target_failed().await;
                } else {
                    self.stopped(over.is_some()).await;
                }
            }
        }
        None
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
        self.close_stream().await;
        if !ended {
            self.segments.stopped();
        }
        if let Some(row) = self.segments.last_text(true) {
            self.commit(row).await;
        }
    }

    /// Closes the turn's response and waits for its end, the turn's record
    /// its last (RFC §10.6.4).
    async fn close_stream(&mut self) {
        if let Some(mut turn) = self.stream.take() {
            turn.close_stream().await;
        }
    }

    /// Waits for the turn a stop from outside cut (RFC §10.6.8): its executor
    /// cuts the call it runs, and the turn's end is awaited within its cancel
    /// bound before its rows are settled.
    async fn cut_turn(&mut self) {
        if let (Some(_), Some(turn)) = (self.cut, self.stream.as_ref()) {
            turn.ended().await;
        }
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
        self.close_stream().await;
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
                        RemoteItem::Item(
                            ResponseItem::Text(_)
                            | ResponseItem::Thinking(_)
                            | ResponseItem::Usage(_)
                            | ResponseItem::LoopEnd(_)
                            | ResponseItem::SegmentBreak,
                        )
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
        let ended = |tool_id: &str, nth| stream.and_then(|turn| turn.ended_early(tool_id, nth));
        for row in self.segments.close_open(error, ran_with, ended) {
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
        if let Some(kept_out) = self.kept_out.as_mut()
            && row.r#type == EventType::Message
        {
            // The pass hands on its last text (Python's `_last_text`).
            if let EventContent::Text(text) = &row.content {
                text.body.clone_into(kept_out);
            }
            return;
        }
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

/// Writes the turn's `record` on its last message or else its last row,
/// `event_id`, when no row carried it (RFC §6.4 "The turn's record"), over the
/// row as the store holds it: a direct update, `ON_EVENT_UPDATED` firing as for
/// any; best effort, a failure logged. The row as stored, once written.
async fn write_record<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    room_id: &RoomId,
    event_id: &EventId,
    record: Metadata,
) -> Option<RoomEvent> {
    let written = rewrite_with_record(inner, room_id, event_id, record).await;
    match written {
        Ok(row) => row,
        Err(error) if inner.tasks.is_closing() => {
            tracing::debug!(
                target: "roomkit::driver",
                room_id = %room_id,
                event_id = %event_id,
                %error,
                "the turn's record was not written on its row: the kit closed"
            );
            None
        }
        Err(error) => {
            tracing::warn!(
                target: "roomkit::driver",
                room_id = %room_id,
                event_id = %event_id,
                %error,
                "the turn's record could not be written on its row"
            );
            None
        }
    }
}

/// The update of [`write_record`]: nothing for a row the store no longer holds.
async fn rewrite_with_record<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    room_id: &RoomId,
    event_id: &EventId,
    record: Metadata,
) -> Result<Option<RoomEvent>, RoomKitError> {
    let Some(stored) = inner.store.get_event(event_id).await? else {
        return Ok(None);
    };
    let mut metadata = stored.metadata;
    metadata.extend(record);
    let updates = EventUpdates {
        metadata: Some(metadata),
        ..EventUpdates::default()
    };
    let kit = RoomKit {
        inner: Arc::clone(inner),
    };
    kit.update_own(room_id, event_id, updates).await
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
