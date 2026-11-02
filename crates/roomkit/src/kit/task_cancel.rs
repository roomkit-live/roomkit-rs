//! Cancelling a delegated task still running in the background (RFC §23.3,
//! §23.4): Python's `DelegationMixin.cancel_task`, `_cancel_task_for`,
//! `_cancel_joined` and `_cancelled_by_notified`.
//!
//! Calls cancelling one task at once, the host's and an agent's
//! `cancel_task` among them, cancel it once: the first asks the task
//! runner, the others join it and are answered as it is. An agent whose
//! call asked is not handed the cancellation back, its call's answer
//! having told it.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, TaskStatus};

use super::RoomKit;
use crate::GetRoomOptions;
use crate::store::ConversationStore;
use crate::tasks::{DelegatedTaskResult, TaskCancel, TaskCancels, TaskReach};

/// What [`RoomKit::cancel_task`] takes beside the task: Python's
/// `cancel_task(task_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CancelTaskOptions {
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2); only a task running for one of its rooms is cancelled.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Cancels a delegated task still running in the background (RFC
    /// §23.3): Python's `cancel_task(task_id, *, organization_id=None)`.
    ///
    /// The task ends `cancelled`, as any task cancelled from outside:
    /// `ON_TASK_COMPLETED` fires, the status bus posts its end, and the
    /// channel its result was going to is told it was cancelled. A task
    /// that already ended, or that the task runner does not hold (an inline
    /// delegation), is left as it stands, and so is a task of another
    /// organization's room than `organization_id`. Calls cancelling it at
    /// once cancel it once.
    ///
    /// Returns `true` when the task was running and is now cancelled. Once
    /// the kit's close started it returns `false` doing nothing: the close
    /// ends every task its runner holds (RFC §12.10.4).
    pub async fn cancel_task(&self, task_id: &str, options: CancelTaskOptions) -> bool {
        if self.inner.open().is_err() {
            return false;
        }
        if let Some(organization_id) = options.organization_id.as_deref()
            && !self.task_in_scope(task_id, organization_id).await
        {
            return false;
        }
        self.cancel_joined(task_id, None).await
    }

    /// Whether `task_id` runs for a room of `organization_id` (RFC §17.2).
    async fn task_in_scope(&self, task_id: &str, organization_id: &str) -> bool {
        let Some(room_id) = self.inner.room_tasks.room_of(task_id) else {
            return false;
        };
        let options = GetRoomOptions {
            organization_id: Some(organization_id.to_owned()),
        };
        self.get_room(&room_id, options).await.is_ok()
    }

    /// Cancels `task_id` for a call made in `room_id` that may cancel the
    /// room's tasks of `reaches` (an agent's `cancel_task`); when
    /// `channel_id`, the caller, is the one the result was going to, it is
    /// not handed the cancellation back, the call's answer having told it.
    /// A call asking as a cancel's end is being completed joins that cancel
    /// (RFC §23.4): Python's `_cancel_task_for`.
    pub(crate) async fn cancel_task_for(
        &self,
        task_id: &str,
        channel_id: Option<&ChannelId>,
        room_id: &str,
        reaches: &[TaskReach],
    ) -> bool {
        let reach = self.inner.room_tasks.reach(task_id, room_id);
        if reach.is_some_and(|reach| reaches.contains(&reach))
            || self.cancel_ending(task_id, room_id, reaches)
        {
            return self
                .cancel_joined(task_id, channel_id.map(ChannelId::as_str))
                .await;
        }
        false
    }

    /// Whether `task_id`, a task of `room_id` of `reaches`, ended cancelled
    /// while a cancel of it is still under way, its end not yet decided
    /// whom it hands the cancellation back to: a call asking now is one of
    /// that cancel's.
    fn cancel_ending(&self, task_id: &str, room_id: &str, reaches: &[TaskReach]) -> bool {
        let Some(cancel) = self.inner.task_cancels.get(task_id) else {
            return false;
        };
        let record = &self.inner.room_tasks;
        !cancel.sealed()
            && record.ended_with(task_id, room_id).as_deref() == Some("cancelled")
            && record
                .ended_reach(task_id, room_id)
                .is_some_and(|reach| reaches.contains(&reach))
    }

    /// Cancels `task_id` once, whichever doors ask at once, each answered
    /// as the first is; `channel_id`, a calling agent, is not handed the
    /// cancellation back.
    async fn cancel_joined(&self, task_id: &str, channel_id: Option<&str>) -> bool {
        let (cancel, first) = self.inner.task_cancels.join_or_begin(task_id, channel_id);
        if !first {
            return cancel.outcome().await;
        }
        let mut finishing = Finishing {
            cancels: &self.inner.task_cancels,
            task_id,
            cancel,
            cancelled: false,
        };
        finishing.cancelled = self.task_runner().cancel(task_id).await;
        finishing.cancelled
    }

    /// Whether `result` is a cancellation its notified channel asked for:
    /// the cancel's callers read once, the cancel sealed, so a call asking
    /// after this is answered as for any ended task.
    pub(crate) fn cancelled_by_notified(
        &self,
        result: &DelegatedTaskResult,
        notify: &ChannelId,
    ) -> bool {
        let Some(cancel) = self.inner.task_cancels.get(&result.task_id) else {
            return false;
        };
        let asked = cancel.seal_for(notify.as_str());
        result.status == TaskStatus::Cancelled && asked
    }
}

/// The first call's cancel of a task, answered to the calls that joined it
/// however the first call ends: dropped before the runner answered, it
/// answers that it did not cancel.
struct Finishing<'a> {
    cancels: &'a TaskCancels,
    task_id: &'a str,
    cancel: Arc<TaskCancel>,
    cancelled: bool,
}

impl Drop for Finishing<'_> {
    fn drop(&mut self) {
        self.cancels
            .finish(self.task_id, &self.cancel, self.cancelled);
    }
}

#[cfg(test)]
mod tests;
