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

use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use roomkit_core::model::{ErrorRecord, EventId, EventStatus, RoomEvent, RoomId};
use roomkit_core::ops::{
    Decided, Inbound, InboundError, InboundResult, KIT_CLOSED, Outcome, StoreOutcome,
};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinHandle};

use super::calls::{self, Written};
use super::in_flight::InFlight;
use super::{Committed, Driven, Leases, Stop};
use crate::context::ExecutionContext;
use crate::delivery_keys::{CancelRequest, Named, NamedGuard};
use crate::error::{RoomKitError, with_causes};
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::tasks::{Kind, Marks, Tasks};
use crate::turns::Canceller;
use crate::turns::TURN_LINK_ERROR;

/// Why a call's delivery is cancelled when its caller asks it without a
/// reason: its handle's `request_cancel`, Python's default
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
    /// The commits of the call's own answers: what the call reports as its
    /// `response_events` once the kit's close aborted its run.
    answers: StoredAnswers,
    /// The commit that decides the call, once it started.
    in_flight: InFlight,
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
            answers: StoredAnswers::default(),
            in_flight: InFlight::default(),
        }
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
        self.cancelled.send_replace(Some(result));
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
        result.cancellation_reason = Some(KIT_CLOSED.to_owned());
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
        ..InboundResult::default()
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
    fn new(answer: oneshot::Sender<InboundOutput>, defer: bool, record: Arc<Record>) -> Self {
        Self {
            answer: Some(answer),
            defer,
            decided: false,
            gone: false,
            departure: Departure::default(),
            record,
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
        if answer.send(answered).is_err() {
            tracing::debug!(
                target: "roomkit::driver",
                call,
                "the caller of a call stopped waiting; the call goes on detached"
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
/// driver would commit within that section's tenure. A call made within a
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
    let context = ExecutionContext::current();
    let inbound = charged(inbound, room_id, &context);
    let (answer, decided) = oneshot::channel();
    let mut named = named;
    let record = Arc::new(Record::new(named.as_mut()));
    let kit = Arc::clone(inner);
    let mut caller = Caller::new(answer, defer, Arc::clone(&record));
    let run = async move {
        // The key names the delivery until the run is over.
        let _named = named;
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
        let mut output = closed_at_its_end(output, closing);
        if caller.departure.turns_cut
            && let Some(reason) = caller.record.cancel_reason()
            && let Ok(Ok(result)) = &mut output
            && !result.refused_before_the_lock
        {
            result.cancellation_reason.get_or_insert(reason);
        }
        caller.log_unread_end(call, &output, closing);
        output
    };
    // The kit tracks the run. Dropped, the handle detaches it: the run
    // itself stops when its caller is gone before the call's decision.
    let Some(mut task) = inner.tasks.spawn(Kind::Run, context.scope(run)) else {
        return Ran::ended(Err(RoomKitError::KitClosed));
    };
    let cancelled = Arc::clone(&record);
    tokio::select! {
        biased;
        joined = &mut task => Ran::ended(ended(joined, &record, &inner.tasks)),
        Ok(answer) = decided => Ran {
            output: answer,
            delivery: defer.then_some((task, record)),
        },
        // Its delivery cancelled, the call returns once its turns are over:
        // the delivery going on, or its set still waiting in the room's
        // lane, is not its turns' work (RFC §10.1 "Explicit delivery
        // cancellation"). The run goes on, detached.
        result = cancelled_end(&cancelled) => Ran {
            output: Ok(Ok(result)),
            delivery: defer.then_some((task, record)),
        },
    }
}

/// Reports `record`'s call as its cancellation ends it once each of `turns`,
/// the turns cut, is over for its owner (RFC §10.1 "Explicit delivery
/// cancellation": the cancellation joins the turns it cuts).
pub(super) async fn end_once_over(record: Arc<Record>, turns: Vec<Canceller>) {
    for turn in &turns {
        turn.ended().await;
    }
    record.end_cancelled();
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
/// it cut or refused, `kit_closed`, or a turn it cut or refused, the turn
/// link's.
fn by_close(error: &ErrorRecord) -> bool {
    error.error_type == KIT_CLOSED || error.error_type == TURN_LINK_ERROR
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
