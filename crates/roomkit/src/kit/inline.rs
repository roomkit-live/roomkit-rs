//! An inline delegation's run, and a delegation its caller cuts before it
//! started (RFC §23.3): Python's `_run_inline`, `_inline_task` and
//! `_end_inline_cancelled`, and `delegate`'s `_end_cut`.
//!
//! The task's turn and its end run as work the kit holds until the task
//! ended ([`HeldTasks`](crate::tasks::HeldTasks)), its caller waiting for
//! that end. A caller that stops waiting, its future dropped, cuts the
//! task: its turn is stopped and it ends `cancelled` on a run of its own;
//! the kit's close ends it so too. A task whose turn ran ends as it stands,
//! whatever cuts its caller then.

use roomkit_core::model::{Metadata, TaskStatus};

use super::RoomKit;
use super::tasks::{DelegateOptions, TaskEnd};
use crate::channel::BoxFuture;
use crate::store::ConversationStore;
use crate::tasks::{Claim, DelegatedTask, HeldTasks, TaskReach, spawn_apart, until_ended};

impl<S: ConversationStore> RoomKit<S> {
    /// Runs the task's turn inline and ends it, recorded for its room as
    /// ending with the turn that asked for it, or as the strategy's that
    /// runs it without posting it (RFC §23.4), then returns once it ended.
    pub(super) async fn run_inline(&self, task: &DelegatedTask, options: &DelegateOptions) {
        let reach = if options.post_status {
            TaskReach::Turn
        } else {
            TaskReach::Strategy
        };
        self.inner
            .room_tasks
            .start(&task.id, &task.parent_room_id, reach);
        task.set_status(TaskStatus::InProgress);
        let end_cancelled = self.cancelled_end(task, options, true);
        let (kit, held_task, held_options) = (self.clone(), task.clone(), options.clone());
        let work = move |claim: Claim| async move {
            let ended = kit.inline_turn(&held_task, &held_options).await;
            // Unless a cut took its end first, it ends as it ran.
            if let Some(ending) = claim.take() {
                let result = ended.into_result(&held_task);
                kit.complete_inline(&held_task, result, &held_options).await;
                drop(ending);
            }
        };
        let held = &self.inner.inline_tasks;
        match held.hold(&self.inner.tasks, &task.id, end_cancelled, work) {
            Some(ended) => {
                let waiting = Waiting {
                    held,
                    task_id: &task.id,
                    waits: true,
                };
                until_ended(ended).await;
                waiting.done();
            }
            // The kit spawns no more work: its run is cut at once, and the
            // task ends cancelled here.
            None => self.cancelled_end(task, options, true).await,
        }
        // Its completion recorded its end; a run that never reached it is
        // recorded failed, as Python's `finally` does.
        self.inner.room_tasks.end(&task.id, "failed");
    }

    /// The task's turn, or its turns until its worker submits its result
    /// (RFC §23.3 steps 4 to 6).
    async fn inline_turn(&self, task: &DelegatedTask, options: &DelegateOptions) -> TaskEnd {
        let context = options.context.as_ref();
        if options.require_structured_result {
            let (retries, tool) = (options.max_result_retries, options.result_tool.as_ref());
            Box::pin(self.structured_turns(task, context, retries, tool)).await
        } else {
            Box::pin(self.delegated_turn(task, context)).await
        }
    }

    /// How the task ends cut before its answer: `cancelled`, with no
    /// output, its worker's turn under `turns` once it `began`, its
    /// completion run to its end (Python's `_end_inline_cancelled`).
    fn cancelled_end(
        &self,
        task: &DelegatedTask,
        options: &DelegateOptions,
        began: bool,
    ) -> BoxFuture<'static, ()> {
        let (kit, task, options) = (self.clone(), task.clone(), options.clone());
        Box::pin(async move {
            let ended = TaskEnd::cancelled(options.context.as_ref(), began, &task.agent_id);
            let result = ended.into_result(&task);
            kit.complete_inline(&task, result, &options).await;
        })
    }
}

/// The caller of an inline delegation, waiting for the task's end: dropped
/// while it waits, it cuts the task, which ends cancelled on a run of its
/// own (Python's `hold_task`, cancelled with its caller).
struct Waiting<'a> {
    held: &'a HeldTasks,
    task_id: &'a str,
    waits: bool,
}

impl Waiting<'_> {
    /// The task ended: nothing is left to cut.
    fn done(mut self) {
        self.waits = false;
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.waits {
            self.held.cancel_apart(self.task_id);
        }
    }
}

/// A delegation announced and not started yet (RFC §23.3): dropped armed,
/// its caller cut, its task ends cancelled on a run of its own, its turn
/// never begun, as Python's `delegate` ends a task cut before it started
/// (`_end_cut`).
pub(super) struct Unstarted<S: ConversationStore> {
    ending: Option<BoxFuture<'static, ()>>,
    kit: RoomKit<S>,
    task: DelegatedTask,
    context: Option<Metadata>,
}

impl<S: ConversationStore> Unstarted<S> {
    /// `task`, announced, whose run starts next.
    pub(super) fn new(kit: &RoomKit<S>, task: &DelegatedTask, options: &DelegateOptions) -> Self {
        Self {
            ending: Some(kit.cancelled_end(task, options, false)),
            kit: kit.clone(),
            task: task.clone(),
            context: options.context.clone(),
        }
    }

    /// The task's run started, or it ended: it is no longer this guard's
    /// to end.
    pub(super) fn started(mut self) {
        self.ending = None;
    }

    /// Ends the task cancelled now, before its run, on a run of its own
    /// the kit tracks, which the caller waits for: a caller cut meanwhile
    /// leaves the end to run on (Python's shielded `_end_inline_cancelled`).
    pub(super) async fn end_now(mut self) {
        let Some(ending) = self.ending.take() else {
            return;
        };
        match spawn_apart(&self.kit.inner.tasks, ending) {
            // Its outcome is its end, or its abort by the close's cut.
            Ok(run) => drop(run.await),
            // The kit spawns no more work: it ends here.
            Err(here) => here.await,
        }
    }
}

impl<S: ConversationStore> Drop for Unstarted<S> {
    fn drop(&mut self) {
        let Some(ending) = self.ending.take() else {
            return;
        };
        let Err(refused) = spawn_apart(&self.kit.inner.tasks, ending) else {
            return;
        };
        // The kit spawns no more work, its close cutting or over: the end
        // runs all the same, apart, its completion callback among it, as
        // Python's shielded `_end_cut` runs to its end.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(refused));
            return;
        }
        // No runtime to run it on: the task ends cancelled here, its handle
        // given its result, with nothing stored nor told.
        drop(refused);
        tracing::debug!(target: "roomkit::tasks", task = %self.task.id, "no runtime: a delegation cut before it started ends without its writes");
        let ended = TaskEnd::cancelled(self.context.as_ref(), false, &self.task.agent_id);
        self.kit.inner.room_tasks.end(&self.task.id, "cancelled");
        self.task.set_result(ended.into_result(&self.task));
    }
}
