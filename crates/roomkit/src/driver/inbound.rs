//! An inbound call's run (RFC §10.1, §10.5): its operation is driven on a
//! task of its own, which owns the kit and runs within the caller's
//! execution context, so that the frames of the caller's own work are read
//! by the run as they would be by a call the caller drove itself (RFC
//! §13.5).
//!
//! The caller waits for the run's end. A caller that stops waiting, its
//! future dropped, aborts the run, as it would drop a run it drove itself:
//! the run lets go of what it holds.

use std::sync::Arc;

use roomkit_core::ops::{Inbound, InboundError, InboundResult};
use tokio::task::JoinHandle;

use super::Leases;
use crate::context::ExecutionContext;
use crate::error::KitError;
use crate::kit::Inner;
use crate::store::ConversationStore;

/// What an inbound call's run comes to: the operation's output, or why the
/// kit's call failed.
pub(crate) type InboundOutput = Result<Result<InboundResult, InboundError>, KitError>;

/// Runs `inbound`, made by the kit's call `call`, on a task of its own,
/// within the caller's execution context, and waits for its end. A lock its
/// context holds a lease on is refused to it: a call nested under the lock
/// is adopted by the section that holds it, and one that reaches the driver
/// would commit within that section's tenure.
///
/// # Errors
///
/// As [`run`](super::run), and [`KitError::Reentrant`] for such a lock;
/// [`KitError::Stalled`] when the runtime cancelled the run's task, as it
/// does when it shuts down.
pub(crate) async fn run_inbound<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    inbound: Inbound,
) -> InboundOutput {
    let inner = Arc::clone(inner);
    let run = async move { super::drive(&inner, call, inbound, Leases::Refused).await };
    let mut task = RunTask(tokio::spawn(ExecutionContext::current().scope(run)));
    match (&mut task.0).await {
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(_cancelled) => Err(KitError::Stalled),
    }
}

/// A run's task, aborted when its caller stops waiting for it.
struct RunTask<T>(JoinHandle<T>);

impl<T> Drop for RunTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
