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
//! detached, its end logged. A caller that stops waiting before either,
//! its future dropped, aborts the run, as it would drop a run it drove
//! itself: the run lets go of what it holds.

use std::sync::Arc;

use roomkit_core::model::RoomId;
use roomkit_core::ops::{Decided, Inbound, InboundError, InboundResult};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

use super::Leases;
use crate::context::ExecutionContext;
use crate::error::KitError;
use crate::kit::Inner;
use crate::store::ConversationStore;

/// What an inbound call's run comes to: the operation's output, or why the
/// kit's call failed.
pub(crate) type InboundOutput = Result<Result<InboundResult, InboundError>, KitError>;

/// Where a run's decision goes (`Step::decided`).
#[derive(Debug)]
pub(super) enum Decision {
    /// Nobody takes it: the run is not an inbound call's.
    Unwanted,
    /// The caller takes it when its context detaches from the call's room.
    Wanted(oneshot::Sender<InboundOutput>),
    /// The caller took it: the run goes on detached.
    Given,
}

impl Decision {
    /// Answers the caller with the call's decision, `decided`, when its
    /// `context` detaches from the call's room; a wait of the run refused in
    /// `refused` fails the call instead.
    pub(super) fn offer(
        &mut self,
        context: &ExecutionContext,
        call: &'static str,
        decided: Decided,
        refused: Option<&RoomId>,
    ) {
        let detaches = decided
            .room_id
            .as_ref()
            .is_some_and(|room_id| context.detaches(room_id));
        let caller = match std::mem::replace(self, Self::Given) {
            Self::Wanted(caller) if detaches => caller,
            kept @ (Self::Unwanted | Self::Wanted(_) | Self::Given) => {
                *self = kept;
                return;
            }
        };
        let answer = match refused {
            Some(room_id) => Err(KitError::Reentrant {
                room_id: room_id.clone(),
                call,
            }),
            None => Ok(decided.result),
        };
        if caller.send(answer).is_err() {
            tracing::debug!(
                target: "roomkit::driver",
                call,
                "the caller of a call stopped waiting before its decision"
            );
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
/// reentry budget of the cascade that delivers (gap G-070).
///
/// # Errors
///
/// As [`run`](super::run), and [`KitError::Reentrant`] for such a lock, or
/// the lock of a room its context stands in the admission order of;
/// [`KitError::Stalled`] when the runtime cancelled the run's task, as it
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
    let (decision, decided) = oneshot::channel();
    let inner = Arc::clone(inner);
    let run = async move {
        let mut decision = Decision::Wanted(decision);
        let output = super::drive(&inner, call, inbound, Leases::Refused, &mut decision).await;
        if matches!(decision, Decision::Given) {
            log_detached_end(call, &output);
        }
        output
    };
    let mut task = RunTask(Some(tokio::spawn(context.scope(run))));
    let Some(handle) = task.0.as_mut() else {
        return Err(KitError::Stalled);
    };
    let answered = tokio::select! {
        biased;
        joined = handle => Answered::Ended(joined),
        Ok(answer) = decided => Answered::Decided(answer),
    };
    match answered {
        Answered::Ended(joined) => ended(joined),
        Answered::Decided(answer) => {
            task.detach();
            answer
        }
    }
}

/// How a run answered its caller.
enum Answered {
    /// It ended.
    Ended(Result<InboundOutput, JoinError>),
    /// It decided, and goes on detached.
    Decided(InboundOutput),
}

/// The output of a run's task that ended.
fn ended(joined: Result<InboundOutput, JoinError>) -> InboundOutput {
    match joined {
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(_cancelled) => Err(KitError::Stalled),
    }
}

/// Logs the end of a run whose caller was answered at its decision: nobody
/// else reads it.
fn log_detached_end(call: &'static str, output: &InboundOutput) {
    match output {
        Ok(Ok(result)) => tracing::debug!(
            target: "roomkit::driver",
            call,
            error = ?result.error,
            "a call answered at its decision is over"
        ),
        Ok(Err(error)) => tracing::warn!(
            target: "roomkit::driver",
            call,
            %error,
            "a call answered at its decision failed after it"
        ),
        Err(error) => tracing::warn!(
            target: "roomkit::driver",
            call,
            %error,
            "a call answered at its decision failed after it"
        ),
    }
}

/// A run's task, aborted when its caller stops waiting for it, unless the
/// run goes on detached.
struct RunTask<T>(Option<JoinHandle<T>>);

impl<T> RunTask<T> {
    /// Lets the run go on on its own.
    fn detach(&mut self) {
        drop(self.0.take());
    }
}

impl<T> Drop for RunTask<T> {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}
