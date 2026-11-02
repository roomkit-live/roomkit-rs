//! Where background delegations run (RFC §23.1): Python's
//! `roomkit.tasks.TaskRunner` and `InMemoryTaskRunner`.

use std::fmt;
use std::sync::Arc;

use roomkit_core::model::{Metadata, TaskStatus};

use super::delegated::{DelegatedTask, DelegatedTaskResult};
use super::held::{Claim, HeldTasks};
use crate::RoomKit;
use crate::channel::BoxFuture;
use crate::error::RoomKitError;
use crate::kit::TaskEnd;
use crate::store::{ConversationStore, InMemoryStore};

/// What runs once a task ended, with its result: Python's
/// `OnCompleteCallback`, an async callable.
pub type OnCompleteCallback =
    Arc<dyn Fn(DelegatedTaskResult) -> BoxFuture<'static, ()> + Send + Sync>;

/// What [`TaskRunner::submit`] takes beside the kit and the task: Python's
/// `submit(kit, task, *, context=None, on_complete=None)`.
#[derive(Clone, Default)]
pub struct SubmitOptions {
    /// Python `context=None`: the delegation's context, kept in the
    /// result's metadata.
    pub context: Option<Metadata>,
    /// Python `on_complete=None`: run once the task ended, before its
    /// waiters are let go.
    pub on_complete: Option<OnCompleteCallback>,
}

impl fmt::Debug for SubmitOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubmitOptions")
            .field("context", &self.context)
            .field("on_complete", &self.on_complete.is_some())
            .finish()
    }
}

/// Executes delegated tasks in the background (RFC §23.1): Python's
/// `TaskRunner`. A task ends exactly once, through its `on_complete`,
/// whatever ends it: run to its end, it ends completed or failed;
/// cancelled, it ends `cancelled`.
pub trait TaskRunner<S = InMemoryStore>: Send + Sync + 'static {
    /// Starts `task` in the background. A submit that fails has not taken
    /// the task: the kit ends it itself.
    fn submit(
        &self,
        kit: RoomKit<S>,
        task: DelegatedTask,
        options: SubmitOptions,
    ) -> BoxFuture<'static, Result<(), RoomKitError>>;

    /// Cancels `task_id` and ends it `cancelled` through its
    /// `on_complete`; `true` when the task was running and is now
    /// cancelled.
    fn cancel(&self, task_id: &str) -> BoxFuture<'static, bool>;

    /// Shuts the runner down: every task it holds ends cancelled.
    fn close(&self) -> BoxFuture<'static, ()>;
}

/// The default task runner: each task a tokio task of the kit's (Python's
/// `InMemoryTaskRunner`, `asyncio.Task` instances). A task ends once,
/// whoever ends it: its work once it ran, or a cancel, whose end runs on
/// a task of its own whatever becomes of the cancel's caller; a cancel or
/// a close that finds a task ending as it stands waits for that end.
#[derive(Clone, Default)]
pub struct InMemoryTaskRunner {
    held: HeldTasks,
}

impl fmt::Debug for InMemoryTaskRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryTaskRunner")
            .field("running", &self.held.len())
            .finish()
    }
}

impl InMemoryTaskRunner {
    /// A runner holding no task.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S: ConversationStore> TaskRunner<S> for InMemoryTaskRunner {
    fn submit(
        &self,
        kit: RoomKit<S>,
        task: DelegatedTask,
        options: SubmitOptions,
    ) -> BoxFuture<'static, Result<(), RoomKitError>> {
        let held = self.held.clone();
        Box::pin(async move {
            let SubmitOptions {
                context,
                on_complete,
            } = options;
            let ends = (
                kit.clone(),
                task.clone(),
                context.clone(),
                on_complete.clone(),
            );
            let end_cancelled: BoxFuture<'static, ()> = Box::pin(async move {
                let (kit, task, context, on_complete) = ends;
                let ended = TaskEnd::cancelled(
                    context.as_ref(),
                    task.status() == TaskStatus::InProgress,
                    &task.agent_id,
                );
                kit.finish_task(&task, ended, on_complete.as_ref()).await;
            });
            let tracker = Arc::clone(&kit.inner.tasks);
            let task_id = task.id.clone();
            let work = |claim: Claim| async move {
                task.set_status(TaskStatus::InProgress);
                let ended = Box::pin(kit.run_task(&task, context.as_ref())).await;
                // Unless a cancel took its end first, it ends as it ran.
                if let Some(ending) = claim.take() {
                    kit.finish_task(&task, ended, on_complete.as_ref()).await;
                    drop(ending);
                }
            };
            match held.hold(&tracker, &task_id, end_cancelled, work) {
                Some(_) => Ok(()),
                None => Err(RoomKitError::KitClosed),
            }
        })
    }

    fn cancel(&self, task_id: &str) -> BoxFuture<'static, bool> {
        let held = self.held.clone();
        let task_id = task_id.to_owned();
        Box::pin(async move { held.cancel(&task_id).await })
    }

    fn close(&self) -> BoxFuture<'static, ()> {
        let held = self.held.clone();
        Box::pin(async move { held.close().await })
    }
}
