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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use roomkit_core::model::{EventId, EventStatus, RoomEvent, RoomId};
use roomkit_core::ops::{
    Decided, Inbound, InboundError, InboundResult, KIT_CLOSED, LANE_CANCELLED, Outcome,
    StoreOutcome,
};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

use super::Leases;
use super::calls::{self, Written};
use crate::context::ExecutionContext;
use crate::error::{KitError, with_causes};
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::tasks::Kind;

/// What an inbound call's run comes to: the operation's output, or why the
/// kit's call failed.
pub(crate) type InboundOutput = Result<Result<InboundResult, InboundError>, KitError>;

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
/// call's decision, and whether the delivery set of the call's own event
/// had its turn in the room's lane, so that a run the kit's close aborts
/// still reports the call it decided (RFC §10.1 step 18).
#[derive(Debug, Default)]
pub(crate) struct Record {
    decided: Mutex<Option<InboundResult>>,
    delivering: AtomicBool,
    /// The call's own event, while its commit, asked for before the call's
    /// decision, runs, and what that write came to.
    committing: Mutex<Option<(RoomEvent, Written)>>,
}

impl Record {
    fn decide(&self, result: &InboundResult) {
        *self.decided.lock().unwrap_or_else(PoisonError::into_inner) = Some(result.clone());
    }

    /// The lane's turn of `event_id`'s set came: the call's own event's
    /// set delivers from now on.
    fn turn_came(&self, event_id: &EventId) {
        let decided = self.decided.lock().unwrap_or_else(PoisonError::into_inner);
        let own = decided
            .as_ref()
            .and_then(|result| result.event.as_ref())
            .is_some_and(|event| &event.id == event_id);
        if own {
            self.delivering.store(true, Ordering::Release);
        }
    }

    /// The call's own event's commit runs, its outcome told by `written`.
    fn commits(&self, event: RoomEvent, written: Written) {
        *self
            .committing
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((event, written));
    }

    /// What a run the kit's close aborted reports, as the Python
    /// implementation's lane reports a delivery it dropped: the call as it
    /// was decided, its delivery cancelled [`LANE_CANCELLED`] when its set
    /// delivered, [`KIT_CLOSED`] when it still waited for its turn. A call
    /// whose own commit still ran reads what the write came to, once it
    /// ends, as Python's caller does: its event, stored and never
    /// delivered, `kit_closed`. [`KitError::ShutDown`] for a call nothing of
    /// which was stored.
    pub(crate) async fn aborted(&self) -> InboundOutput {
        let decided = self
            .decided
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(mut result) = decided else {
            return self.committed().await;
        };
        let reason = if self.delivering.load(Ordering::Acquire) {
            LANE_CANCELLED
        } else {
            KIT_CLOSED
        };
        result
            .cancellation_reason
            .get_or_insert_with(|| reason.to_owned());
        Ok(Ok(result))
    }

    /// The result of a call aborted while its own commit ran: once the
    /// write ended, its event, never delivered; `ShutDown` for a commit
    /// that failed, or none.
    async fn committed(&self) -> InboundOutput {
        let committing = self
            .committing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some((event, mut written)) = committing else {
            return Err(KitError::ShutDown);
        };
        if calls::outcome_of(&mut written).await != Outcome::Store(StoreOutcome::Committed) {
            return Err(KitError::ShutDown);
        }
        let blocked = event.status == EventStatus::Blocked;
        Ok(Ok(InboundResult {
            reason: event.blocked_by.clone().filter(|_| blocked),
            blocked,
            cancellation_reason: (!blocked).then(|| KIT_CLOSED.to_owned()),
            event: Some(event),
            ..InboundResult::default()
        }))
    }
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
    /// What the run did once its caller was gone.
    departure: Departure,
    /// What the run leaves its caller beside its output.
    record: Arc<Record>,
}

/// What a run did once its caller was gone: expired its operation before
/// the call's decision, then cut its turns once the call was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Departure {
    /// Nothing yet.
    Unseen,
    /// The run expired its operation, its caller gone before the decision.
    Expired,
    /// The run cut its call's turns, the call decided.
    TurnsCut,
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
            departure: Departure::Unseen,
            record,
        }
    }

    /// The lane's turn of `event_id`'s set came.
    pub(super) fn turn_came(&self, event_id: &EventId) {
        self.record.turn_came(event_id);
    }

    /// Whether the call is decided.
    pub(super) fn is_decided(&self) -> bool {
        self.decided
    }

    /// The call's own `event`'s commit runs, its outcome told by `written`.
    pub(super) fn commits(&self, event: RoomEvent, written: Written) {
        self.record.commits(event, written);
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
            Some(room_id) => Err(KitError::Reentrant {
                room_id: room_id.clone(),
                call,
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
        let expires = self.gone && !self.decided && self.departure == Departure::Unseen;
        if expires {
            self.departure = Departure::Expired;
        }
        expires
    }

    /// Whether the run cuts its call's turns now: its caller stopped waiting
    /// and the call is decided, before or after its caller left, and it did
    /// not yet (RFC §10.1: the awaited path propagates its caller's
    /// cancellation, decision 2 of step 3).
    pub(super) fn cuts(&mut self) -> bool {
        let cuts = self.gone && self.decided && self.departure != Departure::TurnsCut;
        if cuts {
            self.departure = Departure::TurnsCut;
        }
        cuts
    }

    /// Whether nobody reads the run's output: its caller was answered at
    /// the decision, or stopped waiting.
    fn unread(&self) -> bool {
        self.gone || self.answer.as_ref().is_none_or(oneshot::Sender::is_closed)
    }

    /// Logs the end of a run nobody reads, as `output` says it ended.
    fn log_unread_end(&self, call: &'static str, output: &InboundOutput) {
        if !self.unread() {
            return;
        }
        match output {
            Ok(Ok(result)) => tracing::debug!(
                target: "roomkit::driver",
                call,
                error = ?result.error,
                "a call nobody waits for any more is over"
            ),
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
/// # Errors
///
/// As [`run`](super::run), and [`KitError::Reentrant`] for such a lock, or
/// the lock of a room its context stands in the admission order of;
/// [`KitError::ShutDown`] when the kit's shutdown ended the run, and
/// [`KitError::Cancelled`] when the runtime cancelled it, as it does when it
/// shuts down.
pub(crate) async fn run_inbound<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    room_id: Option<&RoomId>,
    inbound: Inbound,
    defer: bool,
) -> Ran {
    if let Err(error) = inner.open() {
        return Ran::ended(Err(error));
    }
    let context = ExecutionContext::current();
    let inbound = charged(inbound, room_id, &context);
    let (answer, decided) = oneshot::channel();
    let record = Arc::new(Record::default());
    let kit = Arc::clone(inner);
    let mut caller = Caller::new(answer, defer, Arc::clone(&record));
    let run = async move {
        let environment = kit.environment();
        let output = super::drive(
            &kit,
            environment,
            call,
            inbound,
            Leases::Refused,
            Some(&mut caller),
        )
        .await;
        caller.log_unread_end(call, &output);
        cut_at_its_end(output, kit.tasks.is_cut(), &caller.record)
    };
    // The kit tracks the run. Dropped, the handle detaches it: the run
    // itself stops when its caller is gone before the call's decision.
    let Some(mut task) = inner.tasks.spawn(Kind::Work, context.scope(run)) else {
        return Ran::ended(Err(KitError::ShutDown));
    };
    tokio::select! {
        biased;
        // Boxed: what an aborted run's caller waits for, its commit's end,
        // would make every caller's future large.
        joined = &mut task => Ran::ended(Box::pin(ended(joined, &record, inner.tasks.is_cut())).await),
        Ok(answer) = decided => Ran {
            output: answer,
            delivery: defer.then_some((task, record)),
        },
    }
}

/// `output`, a run's at its end: once the kit's close cut the work under
/// way, its turns cut by it (RFC §10.6.8), the delivery of a call whose own
/// set had its turn reads as cancelled [`LANE_CANCELLED`], as the Python
/// implementation's lane reports the cascade it cut, unless an earlier
/// reason stands: the first one stays, as Python's first cancel does. The
/// cut is read here, at the run's end, so that a run that ended before it
/// reports its result whenever its end is read.
fn cut_at_its_end(output: InboundOutput, cut: bool, record: &Record) -> InboundOutput {
    if !cut || !record.delivering.load(Ordering::Acquire) {
        return output;
    }
    output.map(|result| {
        result.map(|mut result| {
            result
                .cancellation_reason
                .get_or_insert_with(|| LANE_CANCELLED.to_owned());
            result
        })
    })
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

/// The output of a run's task that ended: its own, the close's cut read at
/// its end; for one the kit's close aborted, `cut` read then, what its
/// `record` reports ([`Record::aborted`]); for one the runtime cancelled,
/// [`KitError::Cancelled`].
pub(crate) async fn ended(
    joined: Result<InboundOutput, JoinError>,
    record: &Record,
    cut: bool,
) -> InboundOutput {
    match joined {
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(_aborted) if cut => record.aborted().await,
        Err(_cancelled) => Err(KitError::Cancelled),
    }
}
