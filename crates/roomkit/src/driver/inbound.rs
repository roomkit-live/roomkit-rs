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

use std::sync::Arc;
use std::task::{Context, Poll};

use roomkit_core::model::RoomId;
use roomkit_core::ops::{Decided, Inbound, InboundError, InboundResult};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

use super::Leases;
use crate::context::ExecutionContext;
use crate::error::{KitError, with_causes};
use crate::kit::Inner;
use crate::store::ConversationStore;

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
    /// The run of a call deferred at its decision; `None` once it ended.
    pub(crate) delivery: Option<JoinHandle<InboundOutput>>,
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
    fn new(answer: oneshot::Sender<InboundOutput>, defer: bool) -> Self {
        Self {
            answer: Some(answer),
            defer,
            decided: false,
            gone: false,
            departure: Departure::Unseen,
        }
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
    let ran = |output| Ran {
        output,
        delivery: None,
    };
    if let Err(error) = inner.open() {
        return ran(Err(error));
    }
    let context = ExecutionContext::current();
    let inbound = match room_id.and_then(|room_id| context.budget(room_id)) {
        // Into the room a running delivery delivers in.
        Some(draw) => inbound.drawing_on(draw),
        // Routed, or into another room: charged once step 2 knows its final
        // room, the delivering room or one it creates (gap G-070).
        None => match context.delivery() {
            Some((delivering, draw)) => inbound.drawing_on_from_delivery(delivering, draw),
            None => inbound,
        },
    };
    let (answer, decided) = oneshot::channel();
    let kit = Arc::clone(inner);
    let run = async move {
        let mut caller = Caller::new(answer, defer);
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
        output
    };
    // The kit tracks the run. Dropped, the handle detaches it: the run
    // itself stops when its caller is gone before the call's decision.
    let Some(mut task) = inner.tasks.spawn(context.scope(run)) else {
        return ran(Err(KitError::ShutDown));
    };
    tokio::select! {
        biased;
        joined = &mut task => ran(ended(joined, inner.tasks.is_closing())),
        Ok(answer) = decided => Ran {
            output: answer,
            delivery: defer.then_some(task),
        },
    }
}

/// The output of a run's task that ended, cancelled by the kit's shutdown
/// when `shut_down`, by the runtime otherwise. A run that ended once the
/// shutdown began, its turns cut by it (RFC §10.6.8), was ended by it too.
pub(crate) fn ended(joined: Result<InboundOutput, JoinError>, shut_down: bool) -> InboundOutput {
    match joined {
        Ok(_ended) if shut_down => Err(KitError::ShutDown),
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(_cancelled) if shut_down => Err(KitError::ShutDown),
        Err(_cancelled) => Err(KitError::Cancelled),
    }
}
