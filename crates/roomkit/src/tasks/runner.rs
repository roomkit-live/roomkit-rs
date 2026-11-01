//! Where background delegations run (RFC §23.1): Python's
//! `roomkit.tasks.TaskRunner` and `InMemoryTaskRunner`.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{Metadata, TaskStatus};
use tokio::task::AbortHandle;

use super::Kind;
use super::delegated::{DelegatedTask, DelegatedTaskResult};
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
/// `InMemoryTaskRunner`, `asyncio.Task` instances).
#[derive(Clone, Default)]
pub struct InMemoryTaskRunner {
    running: Arc<Mutex<HashMap<String, Running>>>,
}

/// A task the runner holds: its work, and how it ends cancelled, which
/// whoever takes it runs, so the task ends once.
struct Running {
    work: Option<AbortHandle>,
    end_cancelled: Option<BoxFuture<'static, ()>>,
}

impl fmt::Debug for InMemoryTaskRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryTaskRunner")
            .field("running", &lock(&self.running).len())
            .finish()
    }
}

impl InMemoryTaskRunner {
    /// A runner holding no task.
    pub fn new() -> Self {
        Self::default()
    }

    /// The end of `task_id`, for whoever takes it first.
    fn take_end(&self, task_id: &str) -> Option<BoxFuture<'static, ()>> {
        lock(&self.running)
            .get_mut(task_id)
            .and_then(|running| running.end_cancelled.take())
    }
}

impl<S: ConversationStore> TaskRunner<S> for InMemoryTaskRunner {
    fn submit(
        &self,
        kit: RoomKit<S>,
        task: DelegatedTask,
        options: SubmitOptions,
    ) -> BoxFuture<'static, Result<(), RoomKitError>> {
        let runner = self.clone();
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
            lock(&runner.running).insert(
                task.id.clone(),
                Running {
                    work: None,
                    end_cancelled: Some(end_cancelled),
                },
            );
            let held = runner.clone();
            let task_id = task.id.clone();
            let tracker = Arc::clone(&kit.inner.tasks);
            let work = async move {
                task.set_status(TaskStatus::InProgress);
                let ended = Box::pin(kit.run_task(&task, context.as_ref())).await;
                // Unless a cancel took its end first, it ends as it ran.
                if held.take_end(&task.id).is_some() {
                    kit.finish_task(&task, ended, on_complete.as_ref()).await;
                }
                lock(&held.running).remove(&task.id);
            };
            let Some(spawned) = tracker.spawn(Kind::Turn, work) else {
                lock(&runner.running).remove(&task_id);
                return Err(RoomKitError::KitClosed);
            };
            if let Some(running) = lock(&runner.running).get_mut(&task_id) {
                running.work = Some(spawned.abort_handle());
            }
            Ok(())
        })
    }

    fn cancel(&self, task_id: &str) -> BoxFuture<'static, bool> {
        let runner = self.clone();
        let task_id = task_id.to_owned();
        Box::pin(async move {
            let Some(end) = runner.take_end(&task_id) else {
                return false;
            };
            let work = lock(&runner.running)
                .get_mut(&task_id)
                .and_then(|running| running.work.take());
            if let Some(work) = work {
                work.abort();
            }
            end.await;
            lock(&runner.running).remove(&task_id);
            true
        })
    }

    fn close(&self) -> BoxFuture<'static, ()> {
        let runner = self.clone();
        Box::pin(async move {
            loop {
                let held: Vec<String> = lock(&runner.running).keys().cloned().collect();
                let Some(task_id) = held.into_iter().next() else {
                    return;
                };
                if !TaskRunner::<S>::cancel(&runner, &task_id).await {
                    lock(&runner.running).remove(&task_id);
                }
            }
        })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
