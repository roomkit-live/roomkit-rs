//! A delegated task's handle and its result (RFC §23.2): Python's
//! `roomkit.tasks.DelegatedTask` and `DelegatedTaskResult`.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelId, ErrorRecord, Metadata, RoomId, TaskStatus};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::error::RoomKitError;

/// The result of a delegated task once it ended (RFC §23.2): Python's
/// `DelegatedTaskResult`, a pydantic model of the same members.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelegatedTaskResult {
    /// The task's id.
    pub task_id: String,
    /// The child room the task ran in.
    pub child_room_id: RoomId,
    /// The room that delegated it.
    pub parent_room_id: RoomId,
    /// The agent that ran it.
    pub agent_id: ChannelId,
    /// How it ended, `completed` by default.
    pub status: TaskStatus,
    /// The worker's answer: a completed task's result, a task cut short's
    /// last narration (RFC §23.3 step 6).
    pub output: Option<String>,
    /// What failed it, as the failure's text.
    pub error: Option<String>,
    /// The failure itself, its type kept (what the worker's turn raised),
    /// for a caller that hands it on: held in memory, never serialized, as
    /// Python's `exception` (RFC §23.3 step 6), its record as an exception
    /// a model holds is.
    #[serde(skip)]
    pub exception: Option<ErrorRecord>,
    /// How long it ran, in milliseconds.
    pub duration_ms: f64,
    /// The delegation's context, and the worker's turn's record under
    /// `turns` (RFC §23.3 step 6).
    pub metadata: Metadata,
}

impl DelegatedTaskResult {
    /// The result of the task `task_id` of `agent_id`, run in
    /// `child_room_id` for `parent_room_id`, with Python's defaults: it
    /// completed, with no output and no error, in no time, with no
    /// metadata. A caller sets the rest by struct update.
    pub fn new(
        task_id: impl Into<String>,
        child_room_id: RoomId,
        parent_room_id: RoomId,
        agent_id: ChannelId,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            child_room_id,
            parent_room_id,
            agent_id,
            status: TaskStatus::Completed,
            output: None,
            error: None,
            exception: None,
            duration_ms: 0.0,
            metadata: Metadata::new(),
        }
    }

    /// The result of `task` ending `status`.
    pub(crate) fn of(task: &DelegatedTask, status: TaskStatus) -> Self {
        Self {
            status,
            ..Self::new(
                task.id.clone(),
                task.child_room_id.clone(),
                task.parent_room_id.clone(),
                task.agent_id.clone(),
            )
        }
    }
}

/// What [`DelegatedTask::new`] takes beside the task's ids and what was
/// asked: Python's `DelegatedTask(..., brief=None, description=None,
/// hand_back='relay')`.
#[derive(Clone, Debug, PartialEq)]
pub struct DelegatedTaskOptions {
    /// Python `brief=None`: what the worker is told, when it says more than
    /// the task.
    pub brief: Option<String>,
    /// Python `description=None`: what the worker reads, the brief, else
    /// the task, when `None`.
    pub description: Option<String>,
    /// Python `hand_back='relay'`: how the result reaches the room.
    pub hand_back: String,
}

impl Default for DelegatedTaskOptions {
    fn default() -> Self {
        Self {
            brief: None,
            description: None,
            hand_back: "relay".to_owned(),
        }
    }
}

/// What [`DelegatedTask::wait`] takes: Python's `wait(timeout=None)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WaitOptions {
    /// Python `timeout=None`: how long to wait at most; `None` waits for
    /// the task's end.
    pub timeout: Option<Duration>,
}

/// A running delegated task's handle (RFC §23.2): Python's
/// `DelegatedTask`. Cloning it is cheap: the clones share its status and
/// its result.
#[derive(Clone, Debug)]
pub struct DelegatedTask {
    /// The task's id.
    pub id: String,
    /// The room the task runs in.
    pub child_room_id: RoomId,
    /// The room that delegated it.
    pub parent_room_id: RoomId,
    /// The agent that runs it.
    pub agent_id: ChannelId,
    /// What was asked (RFC §23.3 step 4).
    pub task: String,
    /// What the worker is told, when it says more than the task.
    pub brief: Option<String>,
    /// What the worker reads: the brief, else the task, with the room's
    /// conversation when the delegation hands it over.
    pub description: String,
    /// How the result reaches the room: `relay` or `post`.
    pub hand_back: String,
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    status: Mutex<TaskStatus>,
    ended: watch::Sender<Option<DelegatedTaskResult>>,
    started: Instant,
}

impl DelegatedTask {
    /// The handle of the task `id`, `task` asked of `agent_id` in
    /// `child_room_id` for `parent_room_id`, not started yet: `pending`.
    pub fn new(
        id: impl Into<String>,
        child_room_id: RoomId,
        parent_room_id: RoomId,
        agent_id: ChannelId,
        task: impl Into<String>,
        options: DelegatedTaskOptions,
    ) -> Self {
        let (id, task) = (id.into(), task.into());
        let DelegatedTaskOptions {
            brief,
            description,
            hand_back,
        } = options;
        let description = description
            .or_else(|| brief.clone())
            .unwrap_or_else(|| task.clone());
        Self {
            id,
            child_room_id,
            parent_room_id,
            agent_id,
            task,
            brief,
            description,
            hand_back,
            state: Arc::new(State {
                status: Mutex::new(TaskStatus::Pending),
                ended: watch::Sender::new(None),
                started: Instant::now(),
            }),
        }
    }

    /// Where the task stands: `pending`, `in_progress`, or how it ended,
    /// Python's `status` attribute.
    #[doc(hidden)]
    pub fn status(&self) -> TaskStatus {
        *self
            .state
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The task's result, once it ended, Python's `result` attribute.
    #[doc(hidden)]
    pub fn result(&self) -> Option<DelegatedTaskResult> {
        self.state.ended.borrow().clone()
    }

    /// The task's result once it ends, Python's `wait(timeout=None)`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Timeout`], Python's `TimeoutError`, when `timeout`
    /// passes first.
    pub async fn wait(&self, options: WaitOptions) -> Result<DelegatedTaskResult, RoomKitError> {
        let WaitOptions { timeout } = options;
        let mut ended = self.state.ended.subscribe();
        let waiting = async move {
            let result = ended.wait_for(Option::is_some).await;
            result.ok().and_then(|ended| ended.clone())
        };
        let result = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, waiting)
                .await
                .map_err(|_| RoomKitError::Timeout)?,
            None => waiting.await,
        };
        result.ok_or_else(|| {
            RoomKitError::Internal(format!("Task {} finished without a result", self.id))
        })
    }

    /// Marks the task cancelled and lets its waiters go, and nothing else:
    /// the work goes on, and no hook or callback hears of it, as Python's
    /// `cancel()`. To cancel the work, use the task runner's `cancel`.
    pub fn cancel(&self) {
        if self.state.ended.borrow().is_some() {
            return;
        }
        let mut cancelled = DelegatedTaskResult::of(self, TaskStatus::Cancelled);
        cancelled.error = Some("cancelled".to_owned());
        cancelled.duration_ms = self.elapsed_ms();
        self.set_result(cancelled);
    }

    /// How long the task has run, in milliseconds.
    pub(crate) fn elapsed_ms(&self) -> f64 {
        self.state.started.elapsed().as_secs_f64() * 1000.0
    }

    pub(crate) fn set_status(&self, status: TaskStatus) {
        *self
            .state
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = status;
    }

    /// Ends the task with `result` and lets its waiters go.
    pub(crate) fn set_result(&self, result: DelegatedTaskResult) {
        self.set_status(result.status);
        self.state.ended.send_replace(Some(result));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use roomkit_core::model::{ChannelId, RoomId, TaskStatus};

    use super::{DelegatedTask, DelegatedTaskOptions, DelegatedTaskResult, WaitOptions};
    use crate::error::RoomKitError;

    /// Python's `_make_task()`: `review PR`, the task `t1` asked of
    /// `agent-a` in `child-1` for `parent-1`.
    fn task() -> DelegatedTask {
        DelegatedTask::new(
            "t1",
            RoomId::new("child-1").unwrap(),
            RoomId::new("parent-1").unwrap(),
            ChannelId::new("agent-a").unwrap(),
            "review PR",
            DelegatedTaskOptions::default(),
        )
    }

    fn within(milliseconds: u64) -> WaitOptions {
        WaitOptions {
            timeout: Some(Duration::from_millis(milliseconds)),
        }
    }

    // Python: tests/tasks/test_models.py::TestDelegatedTask::test_initial_state
    #[test]
    fn a_new_task_is_pending_with_no_result() {
        let task = task();
        assert_eq!(
            (
                task.status(),
                task.result(),
                task.id.as_str(),
                task.task.as_str()
            ),
            (TaskStatus::Pending, None, "t1", "review PR")
        );
    }

    // Python: tests/tasks/test_models.py::TestDelegatedTask::test_wait_returns_after_set_result
    #[tokio::test]
    async fn wait_returns_the_result_once_it_is_set() {
        let task = task();
        let result = DelegatedTaskResult {
            output: Some("done".to_owned()),
            ..DelegatedTaskResult::of(&task, TaskStatus::Completed)
        };
        let ending = task.clone();
        let setting = tokio::spawn(async move { ending.set_result(result) });

        let got = task.wait(within(2000)).await.unwrap();

        setting.await.unwrap();
        assert_eq!(
            (got.output.as_deref(), task.status()),
            (Some("done"), TaskStatus::Completed)
        );
    }

    // Python: tests/tasks/test_models.py::TestDelegatedTask::test_wait_timeout
    #[tokio::test]
    async fn a_wait_past_its_timeout_fails_with_timeout() {
        assert_eq!(task().wait(within(10)).await, Err(RoomKitError::Timeout));
    }

    // Python: tests/tasks/test_models.py::TestDelegatedTask::test_cancel, test_cancel_idempotent
    #[test]
    fn a_cancel_ends_the_task_cancelled_once() {
        let task = task();
        task.cancel();
        let first = task.result();
        task.cancel();

        let ended = first.clone().unwrap();
        assert_eq!(
            (
                task.status(),
                ended.status,
                ended.output,
                ended.error.as_deref()
            ),
            (
                TaskStatus::Cancelled,
                TaskStatus::Cancelled,
                None,
                Some("cancelled")
            )
        );
        assert_eq!(task.result(), first);
    }

    // Python: tests/tasks/test_models.py::TestDelegatedTask::test_wait_after_cancel
    #[tokio::test]
    async fn a_wait_after_a_cancel_returns_it_at_once() {
        let task = task();
        task.cancel();
        let waited = task.wait(within(1000)).await.unwrap();
        assert_eq!(waited.status, TaskStatus::Cancelled);
    }
}
