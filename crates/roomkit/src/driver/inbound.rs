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
//! `tokio::time::timeout` that expired), detaches the run once the call is
//! decided, its event committed or its instruction decided: the run goes
//! on, delivers the event and the cascade it starts, and logs its end, since
//! an event stored is delivered (RFC §13.6, decision 1 of step 2b). Before
//! the decision the run stops at its next wait, as a run its caller drove
//! itself would be dropped, and lets go of what it holds; but it first
//! completes what a stop would leave half done (`Operation::completing`):
//! a commit not answered yet, which may decide the call, and a room its step
//! 2 created, whose creation hooks run and whose channel is attached.

use std::sync::Arc;
use std::task::{Context, Poll};

use roomkit_core::model::RoomId;
use roomkit_core::ops::{Decided, Inbound, InboundError, InboundResult};
use tokio::sync::oneshot;
use tokio::task::JoinError;

use super::Leases;
use crate::context::ExecutionContext;
use crate::error::KitError;
use crate::kit::Inner;
use crate::store::ConversationStore;

/// What an inbound call's run comes to: the operation's output, or why the
/// kit's call failed.
pub(crate) type InboundOutput = Result<Result<InboundResult, InboundError>, KitError>;

/// The caller of an inbound call's run, as the run sees it: where the
/// call's decision goes, and whether the call is decided and its caller
/// still waits (decision 1 of step 2b).
#[derive(Debug)]
pub(super) struct Caller {
    /// Where the call's answer goes at its decision, when the caller's
    /// context detaches from the call's room; `None` once it went.
    answer: Option<oneshot::Sender<InboundOutput>>,
    /// Whether the call is decided (`Step::decided`).
    decided: bool,
    /// Whether the run saw that its caller stopped waiting.
    gone: bool,
}

impl Caller {
    /// The caller answered on `answer` at the decision, when its context
    /// detaches from the call's room.
    fn new(answer: oneshot::Sender<InboundOutput>) -> Self {
        Self {
            answer: Some(answer),
            decided: false,
            gone: false,
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
        let detaches = decided
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

    /// Whether the run stops: its caller stopped waiting before the call's
    /// decision, and the operation completes nothing a stop would leave
    /// half done (`completing`).
    pub(super) fn stops(&self, completing: bool) -> bool {
        self.gone && !self.decided && !completing
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
            Err(KitError::Cancelled) if !self.decided => tracing::debug!(
                target: "roomkit::driver",
                call,
                "the caller stopped waiting before the call's decision; the call stopped"
            ),
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
                %error,
                "a call nobody waits for any more failed"
            ),
        }
    }
}

/// Runs `inbound`, made by the kit's call `call`, on a task of its own,
/// within the caller's execution context, and waits for its end or, for a
/// caller whose context detaches from the call's room, its decision. A lock
/// its context holds a lease on is refused to it: a call nested under the
/// lock is adopted by the section that holds it, and one that reaches the
/// driver would commit within that section's tenure. A call made within a
/// delivery set's code, a turn or a transport's delivery, draws on the
/// reentry budget of the cascade that delivers (gap G-070). A caller that
/// stops waiting detaches the run once the call is decided, and stops it
/// before, once it completes what a stop would leave half done.
///
/// # Errors
///
/// As [`run`](super::run), and [`KitError::Reentrant`] for such a lock, or
/// the lock of a room its context stands in the admission order of;
/// [`KitError::Cancelled`] when the runtime cancelled the run's task, as it
/// does when it shuts down.
pub(crate) async fn run_inbound<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    inbound: Inbound,
) -> InboundOutput {
    let context = ExecutionContext::current();
    let inbound = match context.budget() {
        Some(budget) => inbound.drawing_on(budget),
        None => inbound,
    };
    let (answer, decided) = oneshot::channel();
    let inner = Arc::clone(inner);
    let run = async move {
        let mut caller = Caller::new(answer);
        let output = super::drive(&inner, call, inbound, Leases::Refused, Some(&mut caller)).await;
        caller.log_unread_end(call, &output);
        output
    };
    // Dropped, the handle detaches the run: the run itself stops when its
    // caller is gone before the call's decision.
    let mut task = tokio::spawn(context.scope(run));
    tokio::select! {
        biased;
        joined = &mut task => ended(joined),
        Ok(answer) = decided => answer,
    }
}

/// The output of a run's task that ended.
fn ended(joined: Result<InboundOutput, JoinError>) -> InboundOutput {
    match joined {
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(_cancelled) => Err(KitError::Cancelled),
    }
}
