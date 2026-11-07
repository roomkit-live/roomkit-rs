//! An inbound call's run (RFC §10.1, §10.5): its operation is driven on a
//! task of its own, which owns the kit and runs within the caller's
//! execution context, so that the frames of the caller's own work are read
//! by the run as they would be by a call the caller drove itself (RFC
//! §13.5).
//!
//! The caller waits for the run's end, unless its context works in the room
//! the call came into, under its lock, in its admission order or
//! delivering in it: the call's delivery may then wait in the room's lane
//! behind the caller's own set, so the caller is answered at the call's
//! decision (RFC §10.1 step 18, detached completion), and the run goes on
//! detached, its end logged.
//!
//! A caller that stops waiting, its future dropped (a client gone, a
//! `tokio::time::timeout` that expired), detaches the run, which is never
//! dropped for its caller (decision 1 of step 2b, gap G-076). Once the call
//! is decided, its event committed or its instruction decided, the run
//! goes on, delivers the event to every target but the agents its cascade
//! would ask, whose turns it cuts (`Operation::cancel_turns`, RFC §10.1
//! "Explicit delivery cancellation", decision 2 of step 3): a turn not
//! started never starts, a running one is aborted and joined, and no
//! answer of the cascade re-enters. It logs its end: an event stored is
//! delivered (RFC §13.6). Before the decision the call
//! expires as one past its `process_timeout` does (`Operation::expire`):
//! its requests within the call's bound are given up on, nothing more of
//! its trigger is written, what it holds is let go, and every event it
//! committed is delivered all the same, an adopted entry's, or its own when
//! its commit was in flight. A room its step 2 created is completed first,
//! its creation never within the call's bound.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use roomkit_core::model::{ErrorRecord, EventId, EventStatus, Metadata, RoomEvent, RoomId};
use roomkit_core::ops::{
    Decided, Inbound, InboundError, InboundResult, KIT_CLOSED, Outcome, SpanNote, StoreOutcome,
};
use serde_json::Value;
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinHandle};

use super::calls::{self, Written};
use super::in_flight::InFlight;
use super::{Committed, Driven, Leases, Stop};
use crate::context::{ExecutionContext, Lease, ReadTurns};
use crate::delivery_keys::{CancelRequest, Named, NamedGuard};
use crate::error::{RoomKitError, with_causes};
use crate::kit::Inner;
use crate::locks::HeldLock;
use crate::store::ConversationStore;
use crate::stream::KeptStream;
use crate::tasks::{Kind, Marks, Tasks};
use crate::telemetry::{KitTelemetry, OpenSpan, SpanKind, StartSpanOptions};
use crate::turns::Canceller;
use crate::turns::TURN_LINK_ERROR;

/// Why a call's delivery is cancelled when its caller asks it without a
/// reason: its handle's `cancel` by default, Python's
/// `reason="caller_cancelled"`.
pub(crate) const CALLER_CANCELLED: &str = "caller_cancelled";

/// What an inbound call's run comes to: the operation's output, or why the
/// kit's call failed.
pub(crate) type InboundOutput = Result<Result<InboundResult, InboundError>, RoomKitError>;

/// What an inbound call's run gave its caller: its output, and, for a call
/// answered at its decision because it deferred its delivery, the run going
/// on, which the call's [`DeliveryHandle`](crate::DeliveryHandle) waits for.
#[derive(Debug)]
pub(crate) struct Ran {
    /// The call's output: at its end, or at its decision.
    pub(crate) output: InboundOutput,
    /// The run of a call deferred at its decision, with its record; `None`
    /// once it ended.
    pub(crate) delivery: Option<(JoinHandle<InboundOutput>, Arc<Record>)>,
}

impl Ran {
    /// The output of a call whose run ended, or never started.
    fn ended(output: InboundOutput) -> Self {
        Self {
            output,
            delivery: None,
        }
    }
}

/// What an inbound call's run leaves its caller beside its output: the
/// call's decision, and the answers its cascade committed, so that a run
/// the kit's close aborted, its unwind run out, still reports the call it
/// decided (RFC §10.1 step 18, §12.10.4); the cut of its turns its caller
/// asks for, and what it reports once they are cut (RFC §10.1 "Explicit
/// delivery cancellation").
#[derive(Debug)]
pub(crate) struct Record {
    /// The cut of the call's turns, once asked for.
    cancel: Arc<CancelRequest>,
    /// The name its delivery key gives the delivery, located once the
    /// call's room is read.
    named: Option<Arc<Named>>,
    /// The call as its cancellation ends it, once its turns are cut and
    /// over: what its caller reads without waiting for the delivery going
    /// on.
    cancelled: watch::Sender<Option<InboundResult>>,
    decided: Mutex<Option<InboundResult>>,
    /// Whether the read of the call's own stream its cut stopped is over; no
    /// read waited for reads as over.
    cut_read_over: watch::Sender<bool>,
    /// What that read left the call.
    cut_read_left: Mutex<Option<CutReadLeft>>,
    /// The commits of the call's own answers: what the call reports as its
    /// `response_events` once the kit's close aborted its run.
    answers: StoredAnswers,
    /// The commit that decides the call, once it started.
    in_flight: InFlight,
    /// What the caller, answered at the decision, did not wait for.
    tail: Mutex<Option<Tail>>,
    /// Set once a turn whose stream the call's run read is over for its
    /// owner without its executor's end: a cut call's cleanup is not known
    /// to be done (RFC §10.6.8).
    cleanup_owed: AtomicBool,
    /// Set once the run ended while the kit's close was under way: its
    /// cascade cut by the close, as Python's close cancels every cascade in
    /// flight (RFC §12.10.4).
    closed_under_way: AtomicBool,
    /// Whether the closing of the streams the call's cut left unread is
    /// over; nothing to close reads as over.
    unread_closed: watch::Sender<bool>,
}

/// What the read a call's cut stopped left the call.
#[derive(Debug)]
struct CutReadLeft {
    record: Metadata,
    error: Option<ErrorRecord>,
    /// The rows it wrote the turn's record on, as stored.
    rewritten: Vec<RoomEvent>,
}

impl CutReadLeft {
    /// `result`, the call as its cut ends it, with what the read left: its
    /// failure its caller's error, but one the kit's close caused (RFC §12.2
    /// step 13s, §12.10.4).
    fn apply(&self, result: &mut InboundResult) {
        result.response_metadata.clone_from(&self.record);
        if result.error.is_none() {
            result.error = self.error.clone().filter(|error| !by_close(error));
        }
        for row in &self.rewritten {
            let answers = result.response_events.iter_mut();
            if let Some(answer) = answers.into_iter().find(|answer| answer.id == row.id) {
                answer.clone_from(row);
            }
        }
    }
}

/// The answers a call's cascade commits, each with what its write came to:
/// what the call reports as its `response_events` when the run that
/// delivers it ends without telling them, the kit's close aborting it, as
/// Python's caller reads its cascade's.
#[derive(Debug, Default)]
pub(crate) struct StoredAnswers(Mutex<Vec<(RoomEvent, Written)>>);

impl StoredAnswers {
    /// The commit of `answer` runs, its outcome told by `written`.
    pub(crate) fn commits(&self, answer: RoomEvent, written: Written) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((answer, written));
    }

    /// The answers whose commit stored them, as stored, in order; a commit
    /// still running counts as none.
    pub(crate) fn stored(&self) -> Vec<RoomEvent> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(_, written)| match &*written.borrow() {
                Some(Outcome::Store(StoreOutcome::Committed(stored))) => Some((**stored).clone()),
                _ => None,
            })
            .collect()
    }
}

/// What the cut of an entry the room's work adopted leaves its handle to wait
/// for: the closing of the streams it left unread, each one's end, and
/// whether a turn among them is over without its executor's end (RFC §10.1
/// "Explicit delivery cancellation", §10.6.8), as Python's adopted call's own
/// cascade cleanup.
#[derive(Debug)]
pub(crate) struct EntryCleanup {
    /// Whether the streams the cut left unread are closed, or there were
    /// none.
    closed: watch::Sender<bool>,
    /// Whether a turn among them is over without its executor's end.
    owed: AtomicBool,
}

impl Default for EntryCleanup {
    fn default() -> Self {
        Self {
            closed: watch::Sender::new(true),
            owed: AtomicBool::new(false),
        }
    }
}

impl EntryCleanup {
    /// Closes `unread`, the streams the entry's cut left unread, one after
    /// the other: told before it starts, over however the closing ends.
    pub(super) fn close(self: Arc<Self>, unread: Vec<KeptStream>) -> impl Future<Output = ()> {
        self.closed.send_replace(false);
        let closing = EntryClosing(self);
        async move {
            if close_unread(unread).await {
                closing.0.owed.store(true, Ordering::Release);
            }
            drop(closing);
        }
    }

    /// Whether the streams the entry's cut left unread are closed.
    pub(crate) fn are_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Returns once the streams the entry's cut left unread are closed:
    /// whether its cleanup is then incomplete, a turn among them over
    /// without its executor's end.
    pub(crate) async fn incomplete_once_closed(&self) -> bool {
        let mut closed = self.closed.subscribe();
        // The cleanup holds the sender: never closed.
        drop(closed.wait_for(|closed| *closed).await);
        self.owed.load(Ordering::Acquire)
    }
}

/// The closing of the streams an entry's cut left unread: over once dropped.
struct EntryClosing(Arc<EntryCleanup>);

impl Drop for EntryClosing {
    fn drop(&mut self) {
        self.0.closed.send_replace(true);
    }
}

impl Record {
    /// The record of a call whose delivery `named` names, if any: its cut
    /// is the one the key asks for, until the run is over.
    fn new(named: Option<&mut NamedGuard>) -> Self {
        let cancel = Arc::new(CancelRequest::default());
        let named = named.map(|named| {
            named.attach(Arc::clone(&cancel));
            Arc::clone(named.named())
        });
        Self {
            cancel,
            named,
            cancelled: watch::Sender::new(None),
            decided: Mutex::new(None),
            cut_read_over: watch::Sender::new(true),
            cut_read_left: Mutex::new(None),
            answers: StoredAnswers::default(),
            in_flight: InFlight::default(),
            tail: Mutex::new(None),
            cleanup_owed: AtomicBool::new(false),
            closed_under_way: AtomicBool::new(false),
            unread_closed: watch::Sender::new(true),
        }
    }

    /// The call's tail, to change or to end.
    fn tail(&self) -> MutexGuard<'_, Option<Tail>> {
        self.tail.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Asks for the cut of the call's turns, for `reason`: whether this
    /// request was the first (RFC §10.1 "Explicit delivery cancellation").
    pub(crate) fn request_cancel(&self, reason: &str) -> bool {
        self.cancel.request(reason)
    }

    /// Why the call's turns are cut, once asked.
    fn cancel_reason(&self) -> Option<String> {
        self.cancel.reason()
    }

    /// A turn whose stream the call's run read is over for its owner
    /// without its executor's end (RFC §10.6.8).
    pub(super) fn owe_cleanup(&self) {
        self.cleanup_owed.store(true, Ordering::Release);
    }

    /// The call's run ended while the kit's close was under way.
    fn closed_under_way(&self) {
        self.closed_under_way.store(true, Ordering::Release);
    }

    /// The streams the call's cut left unread are being closed: over once
    /// what this returns is dropped, however the closing ends.
    pub(super) fn closes_unread(self: &Arc<Self>) -> UnreadClosing {
        self.unread_closed.send_replace(false);
        UnreadClosing(Arc::clone(self))
    }

    /// Whether the streams the call's cut left unread are closed, or there
    /// were none.
    pub(crate) fn unread_are_closed(&self) -> bool {
        *self.unread_closed.borrow()
    }

    /// Returns once the streams the call's cut left unread are closed, each
    /// one's end come, as Python's cleanup closes them before it reports
    /// (RFC §10.1 "Explicit delivery cancellation", §10.6.8).
    pub(crate) async fn unread_closed(&self) {
        let mut closed = self.unread_closed.subscribe();
        // The record holds the sender: never closed.
        drop(closed.wait_for(|closed| *closed).await);
    }

    /// What a deferred call's handle reads in place of its result: Python's
    /// `TimeoutError`, its cleanup incomplete, once the call was cut, by its
    /// caller or by the kit's close, and a turn whose stream it read, or
    /// whose unread stream the cut closed, owes its end (RFC §10.6.8); a
    /// call not cut reads its result. An awaited call reads its result, as
    /// Python's caller does.
    pub(crate) fn cleanup_incomplete(&self) -> Option<RoomKitError> {
        let owed = self.cleanup_owed.load(Ordering::Acquire);
        let cut = self.cancel_reason().is_some() || self.closed_under_way.load(Ordering::Acquire);
        (owed && cut).then_some(RoomKitError::Timeout)
    }

    /// The call's room is read, of `organization_id`: its delivery's key,
    /// if any, names a delivery of that organization from now on.
    pub(super) fn locate(&self, organization_id: Option<&str>) {
        if let Some(named) = &self.named {
            named.locate(organization_id);
        }
    }

    /// Reports the call as its cancellation ends it, its turns cut and
    /// over: decided, its committed answers, `cancellation_reason` the
    /// reason asked; nothing for a call refused before the room's lock,
    /// which reports its refusal (RFC §10.1 "Explicit delivery
    /// cancellation").
    fn end_cancelled(&self) {
        let Some(reason) = self.cancel_reason() else {
            return;
        };
        let decided = self
            .decided
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(mut result) = decided.filter(|result| !result.refused_before_the_lock) else {
            return;
        };
        result.response_events = self.answers.stored();
        result.cancellation_reason = Some(reason);
        self.apply_the_cut_read(&mut result);
        // What the caller did not wait for is cut with its turns, a send
        // still under way no part of it, as Python's reader is.
        let tail = self.tail().take();
        drop(tail);
        self.cancelled.send_replace(Some(result));
    }

    /// `result` with what the read its cut stopped left it, once over.
    fn apply_the_cut_read(&self, result: &mut InboundResult) {
        let left = self
            .cut_read_left
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(left) = left.as_ref() {
            left.apply(result);
        }
    }

    /// The call's cut stops a read of its own stream under way: its end is
    /// to be waited for, the turn's record its caller reads (RFC §6.7).
    pub(super) fn await_cut_read(&self) {
        self.cut_read_over.send_replace(false);
    }

    /// The read the cut stopped is over, the call's record and first error
    /// as it left them, and the rows it wrote the turn's record on.
    pub(super) fn cut_read_over(
        &self,
        record: Metadata,
        error: Option<ErrorRecord>,
        rewritten: Vec<RoomEvent>,
    ) {
        {
            let mut left = self
                .cut_read_left
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut all = left.take().map_or_else(Vec::new, |left| left.rewritten);
            all.extend(rewritten);
            *left = Some(CutReadLeft {
                record,
                error,
                rewritten: all,
            });
        }
        self.cut_read_over.send_replace(true);
    }

    /// What ends the call's tail however the run ends, its task aborted
    /// included: its handle may keep this record long after.
    fn tail_over_with_the_run(self: &Arc<Self>) -> impl Drop + Send + use<> {
        struct RunOver(Arc<Record>);
        impl Drop for RunOver {
            fn drop(&mut self) {
                let tail = self.0.tail().take();
                drop(tail);
            }
        }
        RunOver(Arc::clone(self))
    }

    /// What ends the wait for the read the cut stopped however the run ends:
    /// a cut seen in the step that ends the run tells nothing, nothing polling
    /// the operation after it.
    fn read_over_with_the_run(self: &Arc<Self>) -> impl Drop + Send + use<> {
        struct RunOver(Arc<Record>);
        impl Drop for RunOver {
            fn drop(&mut self) {
                self.0.cut_read_over.send_replace(true);
            }
        }
        RunOver(Arc::clone(self))
    }

    fn decide(&self, result: &InboundResult) {
        // A held event keeps its delivery named until its release (RFC
        // §11.7.6), decided before the room's lock is released.
        if let Some(named) = &self.named {
            named.decided(result);
        }
        *self.decided.lock().unwrap_or_else(PoisonError::into_inner) = Some(result.clone());
    }

    /// The commit of `answer`, an answer of the call's own cascade, runs,
    /// its outcome told by `written`.
    fn answers(&self, answer: RoomEvent, written: Written) {
        self.answers.commits(answer, written);
    }

    /// What a run the kit's close aborted reports, its unwind run out (RFC
    /// §12.10.4): the call as it was decided, with the answers its cascade
    /// stored, `kit_closed`; a call not decided yet, as one the close cut
    /// before its commit, nothing of it stored, a commit it had in flight
    /// warned of as a read-back that failed.
    pub(crate) fn aborted(&self) -> InboundResult {
        let decided = self
            .decided
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(mut result) = decided else {
            self.in_flight.aborted();
            return cut_before_its_commit();
        };
        result.response_events = self.answers.stored();
        self.apply_the_cut_read(&mut result);
        // Its caller's cancellation came first, the kit's close after.
        let reason = self.cancel_reason();
        result.cancellation_reason = Some(reason.unwrap_or_else(|| KIT_CLOSED.to_owned()));
        result
    }
}

/// The call `record` records as its cancellation ends it, once its turns
/// are cut and over ([`Record::end_cancelled`]); never, for a call not
/// cancelled.
pub(crate) async fn cancelled_end(record: &Record) -> InboundResult {
    let mut cancelled = record.cancelled.subscribe();
    let ended = cancelled
        .wait_for(Option::is_some)
        .await
        .map(|result| result.clone());
    match ended {
        Ok(result) => result.unwrap_or_default(),
        // The record outlives every wait on it: never closed.
        Err(_closed) => std::future::pending().await,
    }
}

/// What a message the kit's close cut before its commit reads (RFC
/// §12.10.4): blocked, its `reason` and `cancellation_reason` `kit_closed`,
/// nothing of it stored; Python's `cut_by_close()`.
pub(crate) fn cut_before_its_commit() -> InboundResult {
    InboundResult {
        blocked: true,
        reason: Some(KIT_CLOSED.to_owned()),
        cancellation_reason: Some(KIT_CLOSED.to_owned()),
        // No deferred tail, and its caller's cancellation does not name it.
        refused_before_the_lock: true,
        ..InboundResult::default()
    }
}

/// The closing of the streams a call's cut left unread: over once dropped,
/// the task that closes them ended or aborted ([`Record::closes_unread`]).
#[derive(Debug)]
pub(super) struct UnreadClosing(Arc<Record>);

impl Drop for UnreadClosing {
    fn drop(&mut self) {
        self.0.unread_closed.send_replace(true);
    }
}

/// What a call the kit's close cut as its commit had landed reads, its
/// event read back, `committed` (RFC §12.10.4): the call as that commit
/// decided it, `kit_closed`, a block's record keeping its hooks' reason;
/// delivered to no one.
fn read_back(committed: Committed) -> InboundResult {
    let Committed { event, decides } = committed;
    let mut result = decides.map(|decided| *decided).unwrap_or_default();
    result.blocked = event.status == EventStatus::Blocked;
    if result.blocked {
        result.reason = result.reason.or_else(|| event.blocked_by.clone());
    } else {
        result.reason = None;
    }
    result.event = Some(event);
    result.cancellation_reason = Some(KIT_CLOSED.to_owned());
    result
}

/// The caller of an inbound call's run, as the run sees it: where the
/// call's decision goes, whether the call is decided, and whether its
/// caller still waits (decision 1 of step 2b).
#[derive(Debug)]
pub(super) struct Caller {
    /// Where the call's answer goes at its decision, when the caller's
    /// context detaches from the call's room or the call defers its
    /// delivery; `None` once it went.
    answer: Option<oneshot::Sender<InboundOutput>>,
    /// Whether the call returns at its decision whatever its context: its
    /// delivery deferred (RFC §10.1 step 18, `defer_delivery`).
    defer: bool,
    /// Whether the call is decided (`Step::decided`).
    decided: bool,
    /// Whether the run saw that its caller stopped waiting.
    gone: bool,
    /// What the run did once its caller was gone or cancelled the call's
    /// delivery.
    departure: Departure,
    /// What the run leaves its caller beside its output.
    record: Arc<Record>,
    /// The room's lock the run holds from its start, its call nested under
    /// it (`Inbound::under_lock`).
    pub(super) hand: Option<Under>,
    telemetry: KitTelemetry,
    /// The name the call's delivery key gives its delivery, until the run
    /// is over or the caller answered before the entries the call adopted
    /// (RFC §10.1 "Explicit delivery cancellation", §9.5.1 Reentrance).
    named: Option<NamedGuard>,
}

/// A call's `framework.detached`, Python's span of what a caller did not wait
/// for: open from the instant the call is answered at its decision to the
/// run's end, a child of the call's span and not current, so that it
/// measures without re-parenting. Dropped with its run, the kit's close having
/// aborted it, it ends `cancelled`, as Python's reads a consumer cut.
struct Tail {
    span: Option<OpenSpan>,
    /// How many of the call's turns answered with a stream.
    streams: usize,
    /// The first failure of the call's own cascade past its decision: a turn
    /// of its own set, a stream, a pass or a record that could not end,
    /// Python's `cascade.error`. A failed release, on the call's result, is
    /// not the tail's.
    failure: Option<ErrorRecord>,
}

impl fmt::Debug for Tail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Tail").finish_non_exhaustive()
    }
}

/// What a tail cut reads, whatever cut it, as Python's reads its task
/// cancelled: never a cancellation's reason, the caller's own text.
const CANCELLED: &str = "cancelled";

impl Tail {
    fn open(telemetry: &KitTelemetry, room_id: Option<&RoomId>) -> Self {
        let span = OpenSpan::start(
            telemetry,
            SpanKind::InboundPipeline,
            "framework.detached",
            StartSpanOptions {
                room_id: room_id.map(ToString::to_string),
                ..StartSpanOptions::default()
            },
        );
        Self {
            span: Some(span),
            streams: 0,
            failure: None,
        }
    }

    /// An agent of the call failed with `error`, if any: the first one stands.
    fn failed(&mut self, error: Option<&ErrorRecord>) {
        if self.failure.is_none() {
            self.failure = error.cloned();
        }
    }

    /// What its end reads beside its status.
    fn attributes(&self) -> Metadata {
        let mut attributes = Metadata::new();
        attributes.insert("streams".to_owned(), Value::from(self.streams));
        attributes
    }

    /// Ends it as `output`, the run's at its end, reads: `cut`, the kit's
    /// close ended the run.
    fn end(mut self, output: &InboundOutput, cut: bool) {
        let attributes = self.attributes();
        let Some(span) = self.span.take() else {
            return;
        };
        let result = match output {
            Err(error) => {
                return span.fail_error(error.class_name(), error.to_string(), attributes);
            }
            Ok(Err(error)) => {
                let error = RoomKitError::from(error.clone());
                return span.fail_error(error.class_name(), error.to_string(), attributes);
            }
            Ok(Ok(result)) => result,
        };
        if cut || result.cancellation_reason.is_some() {
            span.fail(CANCELLED, attributes);
        } else if let Some(error) = &self.failure {
            span.fail_error(&error.error_type, &error.message, attributes);
        } else {
            span.end(attributes);
        }
    }
}

impl Drop for Tail {
    fn drop(&mut self) {
        let attributes = self.attributes();
        if let Some(span) = self.span.take() {
            span.fail(CANCELLED, attributes);
        }
    }
}

/// The room's lock an inbound call's run holds from its start, its call
/// nested under it (`Inbound::under_lock`).
#[derive(Debug)]
pub(super) enum Under {
    /// The lock the caller's code handed the call: a keyed publication's
    /// section (RFC §22.1).
    Hand(RoomId, HeldLock),
    /// The lock held by a run that did not adopt the call, granted on the
    /// lease of the code under it that made the call: a channel told of a
    /// lifecycle change under the room's lock (RFC §6.1, §9.5.1), as the
    /// Python implementation's call nests under the lock its caller holds.
    Lease(Lease),
}

impl Under {
    fn room_id(&self) -> &RoomId {
        match self {
            Self::Hand(room_id, _) => room_id,
            Self::Lease(lease) => lease.room_id(),
        }
    }
}

/// What a run did once its caller was gone, or cancelled the call's
/// delivery: each at most once.
#[derive(Clone, Copy, Debug, Default)]
struct Departure {
    /// The run expired its operation, its caller gone before the decision.
    expired: bool,
    /// The run cut its call's turns: its caller gone once the call was
    /// decided, or its delivery cancelled.
    turns_cut: bool,
    /// The run reports the call as its cancellation ends it, once its cut
    /// turns are over ([`Record::end_cancelled`]).
    ending: bool,
}

impl Caller {
    /// The caller answered on `answer` at the decision, when its context
    /// detaches from the call's room or `defer` is set.
    fn new(
        answer: oneshot::Sender<InboundOutput>,
        defer: bool,
        record: Arc<Record>,
        telemetry: KitTelemetry,
    ) -> Self {
        Self {
            answer: Some(answer),
            defer,
            decided: false,
            gone: false,
            departure: Departure::default(),
            record,
            hand: None,
            telemetry,
            named: None,
        }
    }

    /// Reads what the operation told its spans of the call's own cascade.
    pub(super) fn noted(&self, note: &SpanNote) {
        let mut tail = self.record.tail();
        let Some(tail) = tail.as_mut() else {
            return;
        };
        if let SpanNote::SetOver {
            streams,
            error,
            own: true,
            ..
        } = note
        {
            tail.streams += streams;
            tail.failed(error.as_ref());
        }
        if let SpanNote::CascadeFailed { error } = note {
            tail.failed(Some(error));
        }
    }

    /// The caller is answered at the decision: what it does not wait for is
    /// measured from now, unless its call was refused before the room's lock
    /// and has nothing left.
    fn open_tail(&mut self, room_id: Option<&RoomId>, answered: &InboundOutput) {
        if let Ok(Ok(result)) = answered
            && !result.refused_before_the_lock
        {
            *self.record.tail() = Some(Tail::open(&self.telemetry, room_id));
        }
    }

    /// The run is over, as `output` reads, `cut` by the kit's close or not.
    fn end_tail(&self, output: &InboundOutput, cut: bool) {
        let tail = self.record.tail().take();
        if let Some(tail) = tail {
            tail.end(output, cut);
        }
    }

    /// The commit of `answer`, an answer of the call's own cascade, runs,
    /// its outcome told by `written`.
    pub(super) fn answers(&self, answer: RoomEvent, written: Written) {
        self.record.answers(answer, written);
    }

    /// The run of `call` started the commit of `event_id`, which decides the
    /// call.
    pub(super) fn commits(&self, call: &'static str, event_id: &EventId) {
        self.record.in_flight.commits(call, event_id);
    }

    /// The run's read-back of the commit that decides the call is over.
    pub(super) fn read_back_over(&self) {
        self.record.in_flight.settled();
    }

    /// Whether the call is decided: its event committed, or its
    /// instruction decided.
    pub(super) fn is_decided(&self) -> bool {
        self.decided
    }

    /// Takes the call's decision, `decided`: the caller is answered with
    /// it when its `context` detaches from the call's room; a wait of the
    /// run refused in `refused` fails the call instead.
    pub(super) fn offer(
        &mut self,
        context: &ExecutionContext,
        call: &'static str,
        decided: Decided,
        refused: Option<&RoomId>,
    ) {
        self.decided = true;
        if let Ok(result) = &decided.result {
            self.record.decide(result);
        }
        let detaches = self.defer
            || decided
                .room_id
                .as_ref()
                .is_some_and(|room_id| context.detaches(room_id));
        if !detaches {
            return;
        }
        let Some(answer) = self.answer.take() else {
            return;
        };
        let answered = match refused {
            Some(room_id) => Err(RoomKitError::ReentrantCall {
                room_id: room_id.clone(),
            }),
            None => Ok(decided.result),
        };
        self.open_tail(decided.room_id.as_ref(), &answered);
        if answer.send(answered).is_err() {
            tracing::debug!(
                target: "roomkit::driver",
                call,
                "the caller of a call stopped waiting; the call goes on detached"
            );
        }
    }

    /// Answers the caller now with `outcome`, the call's own work over, the
    /// run going on for the entries the call adopted (RFC §9.5.1
    /// Reentrance, §13.6); a caller answered already reads nothing more.
    pub(super) fn answer_now(&mut self, outcome: Result<InboundResult, InboundError>) {
        // The call returned: its key names no delivery any more, as
        // Python's name is released once its call returned.
        drop(self.named.take());
        let Some(answer) = self.answer.take() else {
            return;
        };
        if answer.send(Ok(outcome)).is_err() {
            tracing::debug!(
                target: "roomkit::driver",
                "the caller of a call stopped waiting; its adopted entries go on"
            );
        }
    }

    /// Ready once the run sees that its caller stopped waiting, once: never
    /// for a caller answered at the decision already.
    pub(super) fn poll_gone(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if !self.departure.turns_cut && self.record.cancel.poll(cx) {
            return Poll::Ready(());
        }
        let Some(answer) = self.answer.as_mut().filter(|_| !self.gone) else {
            return Poll::Pending;
        };
        if answer.poll_closed(cx).is_pending() {
            return Poll::Pending;
        }
        self.gone = true;
        Poll::Ready(())
    }

    /// Whether the run expires its operation now: its caller stopped
    /// waiting before the call's decision, and it did not yet.
    pub(super) fn expires(&mut self) -> bool {
        let expires = self.gone && !self.decided && !self.departure.expired;
        if expires {
            self.departure.expired = true;
        }
        expires
    }

    /// Whether the run cuts its call's turns now, and did not yet: its
    /// caller stopped waiting and the call is decided, before or after its
    /// caller left (RFC §10.1: the awaited path propagates its caller's
    /// cancellation, decision 2 of step 3), or its delivery is cancelled,
    /// decided or not: a call cancelled before its commit is committed and
    /// delivered all the same, and none of its turns starts.
    pub(super) fn cuts(&mut self) -> bool {
        let asked = self.record.cancel_reason().is_some();
        let cuts = !self.departure.turns_cut && (asked || self.gone && self.decided);
        if cuts {
            self.departure.turns_cut = true;
        }
        cuts
    }

    /// Whether the run now reports the call as its cancellation ends it,
    /// once its cut turns are over, and did not yet: its delivery
    /// cancelled, and the call decided.
    pub(super) fn ends_cancelled(&mut self) -> bool {
        let Departure {
            turns_cut, ending, ..
        } = self.departure;
        let ends = !ending && turns_cut && self.decided && self.record.cancel_reason().is_some();
        if ends {
            self.departure.ending = true;
        }
        ends
    }

    /// The record the run reports the call's cancelled end on.
    pub(super) fn record(&self) -> &Arc<Record> {
        &self.record
    }

    /// Whether the caller still reads the run's output, its error included:
    /// it was not answered at the decision and still waits.
    pub(super) fn reads(&self) -> bool {
        !self.unread()
    }

    /// Whether nobody reads the run's output: its caller was answered at
    /// the decision, or stopped waiting.
    fn unread(&self) -> bool {
        self.gone || self.answer.as_ref().is_none_or(oneshot::Sender::is_closed)
    }

    /// Logs the end of a run nobody reads, as `output` says it ended; one
    /// the kit's close ended, `closing`, at debug (RFC §12.10.4).
    fn log_unread_end(&self, call: &'static str, output: &InboundOutput, closing: bool) {
        if !self.unread() {
            return;
        }
        match output {
            Err(RoomKitError::KitClosed) if closing => tracing::debug!(
                target: "roomkit::driver",
                call,
                "a call nobody waits for any more ended as the kit closed"
            ),
            Ok(Ok(result)) => tracing::debug!(
                target: "roomkit::driver",
                call,
                error = ?result.error,
                "a call nobody waits for any more is over"
            ),
            // Its commit failed: the writes its caller left (RFC §13.6).
            Ok(Err(InboundError::Store(error))) if self.gone => calls::log_left_failed(error),
            Ok(Err(error)) => tracing::warn!(
                target: "roomkit::driver",
                call,
                %error,
                "a call nobody waits for any more failed"
            ),
            Err(error) => tracing::warn!(
                target: "roomkit::driver",
                call,
                error = %with_causes(&error),
                "a call nobody waits for any more failed"
            ),
        }
    }
}

/// Runs `inbound`, made by the kit's call `call` into `room_id` (`None` for
/// a message the router places), on a task of its own,
/// within the caller's execution context, and waits for its end or, for a
/// caller whose context detaches from the call's room, its decision. A lock
/// its context holds a lease on is refused to it: a call nested under the
/// lock is adopted by the section that holds it, and one that reaches the
/// lock's wait would commit within that section's tenure; a call into the
/// room the run holding the lock did not adopt runs nested on the lease
/// ([`declined_under`]). A call made within a
/// delivery set's code, a turn or a transport's delivery, into the room the
/// set delivers in draws on the reentry budget of the cascade that
/// delivers while that code runs, a routed message once step 2 kept it
/// there, and a message that creates a room; one into another existing room
/// has a budget of its own (gap G-070). A caller that
/// stops waiting detaches the run, which expires its call when it is not
/// decided yet (gap G-076). A call that `defer`s its delivery is answered at
/// its decision, its run going on, handed back in [`Ran::delivery`].
///
/// The call's delivery is named by `named`, a key's name of it, for as
/// long as the run goes on (RFC §10.1 "Explicit delivery cancellation"): a
/// cancellation by the key cuts the run's turns.
///
/// # Errors
///
/// As [`run`](super::run), and [`RoomKitError::ReentrantCall`] for such a lock, or
/// the lock of a room its context stands in the admission order of;
/// [`RoomKitError::KitClosed`] when the kit's shutdown ended the run, and
/// [`RoomKitError::Cancelled`] when the runtime cancelled it, as it does when it
/// shuts down.
pub(crate) async fn run_inbound<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    room_id: Option<&RoomId>,
    inbound: Inbound,
    defer: bool,
    named: Option<NamedGuard>,
) -> Ran {
    // Past the call's door, which its caller kept: a close that started since
    // cuts the call, as it cuts one at its room's lock (RFC §12.10.4).
    if inner.open().is_err() {
        return Ran::ended(Ok(Ok(cut_before_its_commit())));
    }
    run_leased(inner, call, room_id, inbound, defer, named, Leases::Refused).await
}

/// Runs `rerun`, the room's agents run again on a stored event (RFC
/// §11.7.6), as [`run_inbound`] runs a call, its delivery named by `named`;
/// but the room's lock its execution context holds a lease on is granted
/// to it at once, as to any separate operation (RFC §13.5): a rerun is no
/// call the section holding the lock adopts. Called under the lock, its
/// caller is answered once its set is planned, the set running in the
/// room's lane after that section.
///
/// # Errors
///
/// As [`run_inbound`], but for the lock its context holds.
pub(crate) async fn run_rerun<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    room_id: &RoomId,
    rerun: Inbound,
    named: Option<NamedGuard>,
) -> Ran {
    run_leased(
        inner,
        call,
        Some(room_id),
        rerun,
        false,
        named,
        Leases::Granted,
    )
    .await
}

/// [`run_inbound`], a lock its context holds a lease on treated as
/// `leases` says.
async fn run_leased<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    room_id: Option<&RoomId>,
    inbound: Inbound,
    defer: bool,
    named: Option<NamedGuard>,
    leases: Leases,
) -> Ran {
    if let Err(error) = inner.open() {
        return Ran::ended(Err(error));
    }
    let (inbound, context, hand) = placed(inbound, room_id, ExecutionContext::current(), leases);
    // Nested on the lease of a run that did not adopt it, the call's later
    // sections are granted the lock while that lease lives, as a separate
    // operation's are (RFC §13.5).
    let leases = match hand {
        Some(Under::Lease(_)) => Leases::Granted,
        _ => leases,
    };
    let (answer, decided) = oneshot::channel();
    let mut named = named;
    let record = Arc::new(Record::new(named.as_mut()));
    let kit = Arc::clone(inner);
    let mut caller = Caller::new(answer, defer, Arc::clone(&record), kit.telemetry.clone());
    caller.hand = hand;
    // The key names the delivery until the run is over, or until the call's
    // caller is answered before the entries it adopted.
    caller.named = named;
    let read_over = record.read_over_with_the_run();
    let tail_over = record.tail_over_with_the_run();
    let run = async move {
        let _read_over = read_over;
        let _tail_over = tail_over;
        // A room activity: the kit's subscription to its bus first.
        kit.subscribed().await;
        // A deferred delivery goes on apart from its caller, as Python's
        // task of its own: the cut of a held task its caller works for
        // does not reach its turns (RFC §23.3).
        let _apart = defer.then(|| kit.tasks.task_cuts.apart(&Marks::current()));
        let environment = kit.environment();
        let output = super::drive(
            &kit,
            environment,
            call,
            inbound,
            leases,
            Driven::Inbound(&mut caller),
        )
        .await;
        let closing = kit.tasks.is_closing();
        if closing {
            caller.record.closed_under_way();
        }
        let cut = matches!(output, Err(Stop::Cut(_)));
        let mut output = closed_at_its_end(output, closing);
        if caller.departure.turns_cut
            && let Some(reason) = caller.record.cancel_reason()
            && let Ok(Ok(result)) = &mut output
            && !result.refused_before_the_lock
        {
            // Its caller's cancellation came first, the kit's close after.
            result.cancellation_reason = Some(reason);
        }
        caller.end_tail(&output, cut);
        caller.log_unread_end(call, &output, closing);
        output
    };
    // The kit tracks the run. Dropped, the handle detaches it: the run
    // itself stops when its caller is gone before the call's decision.
    let Some(mut task) = inner.tasks.spawn(Kind::Run, context.scope(run)) else {
        return Ran::ended(Err(RoomKitError::KitClosed));
    };
    let cancelled = Arc::clone(&record);
    let ran = tokio::select! {
        biased;
        joined = &mut task => Ran::ended(ended(joined, &record, &inner.tasks)),
        Ok(answer) = decided => {
            return Ran {
                output: answer,
                delivery: defer.then_some((task, record)),
            };
        }
        // Its delivery cancelled, the call returns once its turns are over:
        // the delivery going on, or its set still waiting in the room's
        // lane, is not its turns' work (RFC §10.1 "Explicit delivery
        // cancellation"). The run goes on, detached.
        result = cancelled_end(&cancelled) => Ran {
            output: Ok(Ok(result)),
            delivery: defer.then_some((task, record)),
        },
    };
    // An awaited call its cut ended returns once the streams the cut left
    // unread are closed, each one's end come, as Python's reader closes them
    // before the call returns; a deferred call's handle waits for them.
    if !defer {
        cancelled.unread_closed().await;
    }
    ran
}

/// Reports `record`'s call as its cancellation ends it once each of `turns`,
/// the turns cut, is over for its owner (RFC §10.1 "Explicit delivery
/// cancellation": the cancellation joins the turns it cuts) and it was told
/// what they left, at the cut or once the read the cut stopped is over,
/// however long that takes: a waiter's own timeout bounds that waiter only,
/// and what it reads then is the whole record. A turn whose stream the run
/// read, `read_turns` says, over without its executor's end leaves the
/// call's cleanup incomplete; a turn cut before its output does not, as
/// Python's cleanup reads only its cascade's streams (RFC §10.6.8). Then the
/// streams the cut left `unread` are closed, each waiting for its end, one
/// owed leaving the cleanup incomplete too; `closing` is dropped once they
/// are, which a deferred call's handle and an awaited call both wait for, as
/// Python's reader closes every stream of its cascade before it returns.
pub(super) async fn end_once_over(
    record: Arc<Record>,
    turns: Vec<Canceller>,
    read_turns: ReadTurns,
    (unread, closing): (Vec<KeptStream>, Option<UnreadClosing>),
) {
    for turn in &turns {
        turn.ended().await;
    }
    let mut read_over = record.cut_read_over.subscribe();
    // The record holds the sender: never closed.
    drop(read_over.wait_for(|over| *over).await);
    if read_turns.any_owed() {
        record.owe_cleanup();
    }
    record.end_cancelled();
    if close_unread(unread).await {
        record.owe_cleanup();
    }
    drop(closing);
}

/// Closes `unread`, the streams a cut left that no read took, one after the
/// other, each waiting for its end with no bound of its own, as Python's
/// cascade cleanup closes them (`close_stream`): whether one of their turns
/// is over without its executor's end, its cleanup not known to be done
/// (RFC §10.6.8).
pub(super) async fn close_unread(unread: Vec<KeptStream>) -> bool {
    let mut owed = false;
    for mut kept in unread {
        let turn = kept.turn.canceller();
        kept.turn.close_stream().await;
        turn.ended().await;
        owed |= turn.owed_end().is_owed();
    }
    owed
}

/// `output`, a run's at its end, as the kit's close leaves it, once it
/// started, `closing` (RFC §12.10.4): a run the close cut before its
/// commit reads `cut_before_its_commit()`, and one cut as its commit had
/// landed reads its event, read back; a call whose own commit the sealed
/// kit refused, a run a close spared resuming once it returned, reads as
/// one the close cut before its commit. A call under way as the close
/// started reads `kit_closed`, but one blocked before its delivery, and
/// never a failure the close caused as its error: a send it cut, a turn it
/// cut or refused.
fn closed_at_its_end(
    output: Result<Result<InboundResult, InboundError>, Stop>,
    closing: bool,
) -> InboundOutput {
    let mut result = match output {
        Err(Stop::Cut(None)) => return Ok(Ok(cut_before_its_commit())),
        Err(Stop::Cut(Some(committed))) => return Ok(Ok(read_back(*committed))),
        Err(Stop::Failed(error)) => return Err(error),
        Ok(Err(InboundError::Store(error))) if closing && by_close(&error) => {
            return Ok(Ok(cut_before_its_commit()));
        }
        Ok(Err(error)) => return Ok(Err(error)),
        Ok(Ok(result)) => result,
    };
    if closing {
        if result.error.as_ref().is_some_and(by_close) {
            result.error = None;
        }
        if !result.blocked {
            result
                .cancellation_reason
                .get_or_insert_with(|| KIT_CLOSED.to_owned());
        }
    }
    Ok(Ok(result))
}

/// Whether `error` is the kit's close ending the work that met it: a call
/// it cut or refused, `kit_closed`, an agent's turn that met it, crossed
/// under its class, `KitClosedError` (RMK-1128), or a turn it cut or
/// refused, the turn link's.
fn by_close(error: &ErrorRecord) -> bool {
    error.error_type == KIT_CLOSED
        || error.error_type == TURN_LINK_ERROR
        || error.error_type == RoomKitError::KitClosed.class_name()
}

/// `inbound`, a call into `room_id` or routed, as `context` places it, the
/// context its run runs in, and the room's lock the run holds from its
/// start. Nested under the room's lock its code hands the run (a keyed
/// publication's section, RFC §22.1), taken from the hand, or under the
/// lock of a run that did not adopt the call, on the lease of the code that
/// made it ([`declined_under`]), it draws on the budget of the room's work
/// that code runs in, if any, as the Python implementation's NESTED call
/// does (RFC §9.5.1 Reentrance): a pass of the cascade an admission order
/// or a delivery works for, or a hook call of the cascade whose hook body
/// it runs within; and its run stands in no admission order of the room
/// ([`ExecutionContext::nested_run`]). Otherwise [`charged`], in `context`.
fn placed(
    inbound: Inbound,
    room_id: Option<&RoomId>,
    context: ExecutionContext,
    leases: Leases,
) -> (Inbound, ExecutionContext, Option<Under>) {
    let hand = room_id
        .filter(|room_id| context.hands(room_id))
        .and_then(|_| context.take_hand())
        .map(|(room_id, guard)| Under::Hand(room_id, guard));
    let Some(under) = hand.or_else(|| declined_under(room_id, &context, leases)) else {
        return (charged(inbound, room_id, &context), context, None);
    };
    let room_id = under.room_id().clone();
    let inbound = inbound.under_lock(room_id.clone());
    let inbound = match &under {
        Under::Hand(..) => match context.room_work(&room_id) {
            Some((draw, _depth)) => inbound.drawing_on(draw),
            None => inbound,
        },
        Under::Lease(_) => drawing_under_lease(inbound, &room_id, &context),
    };
    let context = context.nested_run(&room_id);
    (inbound, context, Some(under))
}

/// `inbound`, nested on a lease of `room_id`'s lock, charged to the budget
/// of the room's work the code that made it runs within: the innermost
/// frame of that work that carries one (a delivery, a pass whose hook
/// attached), past the lifecycle change that told its channel, which
/// carries none; else the hook body's; else a delivery's in another room.
/// The Python implementation charges such a call to the innermost frame of
/// the room's work (RFC §9.5.1, §8.3).
fn drawing_under_lease(inbound: Inbound, room_id: &RoomId, context: &ExecutionContext) -> Inbound {
    if let Some(draw) = context.work_budget(room_id) {
        return inbound.drawing_on(draw);
    }
    match context.delivery() {
        Some((delivering, draw)) => inbound.drawing_on_from_delivery(delivering, draw),
        None => inbound,
    }
}

/// The lease `context` holds on `room_id`'s lock, for an inbound call into
/// the room its run would refuse (`leases`): the call reaches the driver
/// though code under the lock made it, so the run holding the lock did not
/// adopt it, as a lifecycle change telling a channel of it adopts nothing
/// (RFC §6.1 `on_room_attached`). Refused, the call could only fail; the
/// Python implementation nests it under the lock its caller holds, and so
/// does this run.
fn declined_under(
    room_id: Option<&RoomId>,
    context: &ExecutionContext,
    leases: Leases,
) -> Option<Under> {
    if leases != Leases::Refused {
        return None;
    }
    context.lease(room_id?).map(Under::Lease)
}

/// `inbound`, a call into `room_id` or routed, charged to the reentry
/// budget it draws on in `context`: the budget of the room a running
/// delivery delivers in, or, routed or into another room, the delivering
/// cascade's once step 2 knows its final room, the delivering room or one
/// it creates (gap G-070).
fn charged(inbound: Inbound, room_id: Option<&RoomId>, context: &ExecutionContext) -> Inbound {
    if let Some(draw) = room_id.and_then(|room_id| context.budget(room_id)) {
        return inbound.drawing_on(draw);
    }
    match context.delivery() {
        Some((delivering, draw)) => inbound.drawing_on_from_delivery(delivering, draw),
        None => inbound,
    }
}

/// The output of a run's task that ended: its own; for one the kit's close
/// aborted, its unwind run out, as `tasks` tell, what its `record` reports
/// ([`Record::aborted`]); for one the runtime cancelled,
/// [`RoomKitError::Cancelled`].
pub(crate) fn ended(
    joined: Result<InboundOutput, JoinError>,
    record: &Record,
    tasks: &Tasks,
) -> InboundOutput {
    match joined {
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(aborted) if tasks.ended_by_close(aborted.id()) => Ok(Ok(record.aborted())),
        Err(_cancelled) => Err(RoomKitError::Cancelled),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A run the close aborted, past a cancel: Python's `wait()` reads the
    /// first reason (`DeliveryCascade.cancel` keeps it), and what its cut read
    /// left of the record.
    #[test]
    fn a_run_aborted_past_a_cancel_keeps_its_reason_and_its_record() {
        let record = Record::new(None);
        record.decide(&InboundResult::default());
        assert_eq!(
            record.aborted().cancellation_reason.as_deref(),
            Some(KIT_CLOSED)
        );
        record.request_cancel("stopped");
        let mut left = Metadata::new();
        left.insert("cited".to_owned(), 3.into());
        record.cut_read_over(left.clone(), None, Vec::new());
        let aborted = record.aborted();
        assert_eq!(aborted.cancellation_reason.as_deref(), Some("stopped"));
        assert_eq!(aborted.response_metadata, left);
    }

    /// A failure the read a cut stopped left is its caller's error, but one the
    /// kit's close caused, which ends the work it met.
    #[test]
    fn a_cut_read_s_failure_is_its_caller_s_unless_the_close_caused_it() {
        for (error_type, read) in [("ValueError", true), (KIT_CLOSED, false)] {
            let record = Record::new(None);
            record.decide(&InboundResult::default());
            record.request_cancel("stopped");
            let failure = ErrorRecord::new(error_type, "m");
            record.cut_read_over(Metadata::new(), Some(failure), Vec::new());
            assert_eq!(record.aborted().error.is_some(), read, "{error_type}");
        }
    }

    /// An agent's turn that met the kit's close crosses under its class,
    /// `KitClosedError`: the close ended it, as a `kit_closed` call and a
    /// cut turn's link, and the caller reads no error (RMK-1128's review;
    /// Python's cascade drops its errors once the close cut it).
    #[test]
    fn a_turn_that_met_the_close_is_the_close_s() {
        let ended = [KIT_CLOSED, "KitClosedError", TURN_LINK_ERROR, "ValueError"]
            .map(|error_type| by_close(&ErrorRecord::new(error_type, "m")));
        assert_eq!(ended, [true, true, true, false]);
    }
}
