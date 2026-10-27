//! What an inbound call comes to ([`InboundResult`], RFC §10.1), and, for a
//! call deferred at its commit, the delivery still running
//! ([`DeliveryHandle`], RFC §10.1 step 18 detached completion).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, DeliveryResult, ErrorRecord, Metadata, RoomEvent};
use roomkit_core::ops;
use tokio::task::JoinHandle;

use crate::context::ExecutionContext;
use crate::driver::{self, InboundOutput};
use crate::error::KitError;
use crate::tasks::Tasks;

/// What became of an inbound message (RFC §10.1 `InboundResult`), as the
/// Python implementation's `InboundResult` reports it.
#[derive(Debug, Default, PartialEq)]
pub struct InboundResult {
    /// The committed event: delivered, or the record stored `BLOCKED` when a
    /// hook or the source's permissions refused it (gap G-026). `None` when
    /// nothing was committed, as when the room refused the message or step
    /// 8a refused an edit or a delete.
    ///
    /// An instruction, which is never committed, is the exception (RFC
    /// §10.1.1 rule 5): once its pass decided it, the result carries it
    /// uncommitted, `DELIVERED` or `BLOCKED` with its `blocked_by`, at the
    /// index its hooks were shown, which the next event takes (gap G-065).
    pub event: Option<RoomEvent>,
    /// Whether the message was blocked: refused by the room, step 8a, a hook
    /// or its source's permissions, an instruction refused before anything
    /// was read, or a call past its `process_timeout` (RFC §13.6).
    pub blocked: bool,
    /// Why it was blocked, as the core's result names it: a hook's reason,
    /// `room_closed`, `source_muted`, `process_timeout`, ...
    pub reason: Option<String>,
    /// The first error met once the message was decided: a turn's, a
    /// later commit's, or the room lock's release (gap G-025). What was
    /// decided stands (RFC §13.6).
    pub error: Option<ErrorRecord>,
    /// The merged response metadata of the turns the message asked for.
    pub response_metadata: Metadata,
    /// The deliveries of the event to its transport channels; empty on a
    /// deferred call's result until its [`DeliveryHandle::wait`].
    pub delivery_results: BTreeMap<ChannelId, DeliveryResult>,
    /// For a call made with `defer_delivery`, what remains of it once its
    /// commit returned: its delivery set, the passes it spawns and their
    /// turns, in the room's lane (RFC §10.1 step 18).
    pub delivery: Option<DeliveryHandle>,
}

impl InboundResult {
    /// `result`, the core's, with `delivery` for a deferred call.
    pub(crate) fn of(result: ops::InboundResult, delivery: Option<DeliveryHandle>) -> Self {
        let ops::InboundResult {
            event,
            blocked,
            reason,
            error,
            response_metadata,
            delivery_results,
        } = result;
        Self {
            event,
            blocked,
            reason,
            error,
            response_metadata,
            delivery_results,
            delivery,
        }
    }
}

impl From<ops::InboundResult> for InboundResult {
    fn from(result: ops::InboundResult) -> Self {
        Self::of(result, None)
    }
}

/// A deferred call's grip on its delivery still running (RFC §10.1 step
/// 18), the Python implementation's `DeliveryHandle`: the event's delivery
/// set, the reentry passes it spawns, an agent's answer among them, and
/// their streamed responses, in the room's lane. Dropping it lets the
/// delivery run on.
///
/// A call adopted by the room's work that made it, a hook's or a parse's
/// (RFC §9.5.1 Reentrance), has its delivery run by that work: its handle
/// is done at once and its wait returns the result unwaited (gap G-082).
pub struct DeliveryHandle {
    /// The result at the commit, which the call returned.
    decided: ops::InboundResult,
    /// The call's run, going on past its decision; `None` for a call that
    /// ended there, adopted by the work it was made from.
    run: Option<JoinHandle<InboundOutput>>,
    tasks: Arc<Tasks>,
}

impl DeliveryHandle {
    /// The handle on `run`, the call that returned `decided` at its commit.
    pub(crate) fn new(
        decided: ops::InboundResult,
        run: Option<JoinHandle<InboundOutput>>,
        tasks: Arc<Tasks>,
    ) -> Self {
        Self {
            decided,
            run,
            tasks,
        }
    }

    /// Whether the delivery will make no further progress: it ran to its
    /// end, or the kit's close ended it.
    pub fn done(&self) -> bool {
        self.run.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Waits until the delivery is over, then reports the call as one that
    /// waited for it would have: its `delivery_results`, `error` and
    /// `response_metadata` filled.
    ///
    /// Called from the room's own work, where a call that waits returns at
    /// its decision (a delivery or a turn in the room, a hook under its
    /// lock or in its admission order), it returns the result at once,
    /// unwaited: the delivery could only wait for this wait (RFC §10.1 step
    /// 18). It follows in the room's lane.
    ///
    /// # Errors
    ///
    /// Returns what a call that waited would have returned past its commit:
    /// [`KitError::ShutDown`] for a delivery the kit's close cut, what
    /// was committed standing, or a defect of this crate, as
    /// [`KitError::Stalled`] is.
    pub async fn wait(self) -> Result<InboundResult, KitError> {
        let Some(run) = self.run.filter(|_| !waits_for_itself(&self.decided)) else {
            return Ok(self.decided.into());
        };
        Ok(driver::ended(run.await, self.tasks.is_cut())??.into())
    }
}

impl PartialEq for DeliveryHandle {
    /// The same delivery: handles of one call's run.
    fn eq(&self, other: &Self) -> bool {
        match (&self.run, &other.run) {
            (Some(own), Some(theirs)) => own.id() == theirs.id(),
            (None, None) => self.decided == other.decided,
            _ => false,
        }
    }
}

impl fmt::Debug for DeliveryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryHandle")
            .field("done", &self.done())
            .finish_non_exhaustive()
    }
}

/// Whether a wait, in the current task's context, on the delivery of the
/// call that returned `decided` would wait for itself: the context works in
/// the event's room, where the call detached.
fn waits_for_itself(decided: &ops::InboundResult) -> bool {
    decided
        .event
        .as_ref()
        .is_some_and(|event| ExecutionContext::current().detaches(&event.room_id))
}
