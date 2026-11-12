//! A child prepared for a task that has not been announced (RFC §23.3).
//!
//! Preparation can fail after it wrote a child, or be dropped while the
//! creation's store call is still running. Wait for that write to settle,
//! then mark the child without reporting the end of an unannounced task.

use roomkit_core::model::{Room, TaskStatus};

use super::RoomKit;
use crate::channel::BoxFuture;
use crate::store::ConversationStore;
use crate::tasks::{DelegatedTask, DelegatedTaskResult, Marks, Unit, spawn_apart};
use crate::{CreateRoomOptions, RoomKitError};

pub(super) struct Preparing<S: ConversationStore> {
    kit: RoomKit<S>,
    task: DelegatedTask,
    creation: Unit,
    armed: bool,
}

impl<S: ConversationStore> Preparing<S> {
    pub(super) fn new(kit: &RoomKit<S>, task: &DelegatedTask) -> Self {
        Self {
            kit: kit.clone(),
            task: task.clone(),
            creation: Unit::new(),
            armed: true,
        }
    }

    /// Mark creation's calls and writes so cleanup can join a decided write.
    /// The lifecycle call is dropped normally, including its hooks: retaining
    /// a hook that calls close would make cleanup wait for its own shutdown.
    pub(super) async fn create_room(
        &mut self,
        options: CreateRoomOptions<S>,
    ) -> Result<Room, RoomKitError> {
        Marks::current()
            .with(self.creation)
            .scoped(self.kit.create_room(options))
            .await
    }

    /// The announced task's existing end path now owns its cleanup.
    pub(super) fn announced(mut self) {
        self.armed = false;
    }

    /// Keep the preparation's original error even if its terminal write fails.
    /// Caller cancellation while waiting cannot abort that write.
    pub(super) async fn failed(mut self, error: &RoomKitError) {
        self.task.fail_span(error);
        self.armed = false;
        let ending = self.ending(TaskStatus::Failed);
        match spawn_apart(&self.kit.inner.tasks, ending) {
            Ok(run) => drop(run.await),
            Err(here) => here.await,
        }
    }

    fn ending(&mut self, status: TaskStatus) -> BoxFuture<'static, ()> {
        let (kit, task, creation) = (self.kit.clone(), self.task.clone(), self.creation);
        let mut result = DelegatedTaskResult::of(&task, status);
        result.duration_ms = task.elapsed_ms();
        task.end_span(&result);
        let mut pending = PendingEnd {
            task_id: task.id.clone(),
            unfinished: true,
        };
        Box::pin(async move {
            kit.inner.tasks.calls_over_within(creation).await;
            record_unannounced_end(&kit, &result).await;
            pending.unfinished = false;
        })
    }
}

impl<S: ConversationStore> Drop for Preparing<S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let ending = self.ending(TaskStatus::Cancelled);
        let Err(refused) = spawn_apart(&self.kit.inner.tasks, ending) else {
            return;
        };
        // As an announced task's Unstarted guard: even a sealed kit may
        // still need a best-effort terminal write. Its store reports failure.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(refused));
        } else {
            tracing::warn!(target: "roomkit::tasks", task = %self.task.id,
                "no runtime: an unannounced child's terminal status could not be stored");
        }
    }
}

/// A bounded shutdown may cut cleanup; say that its terminal write did not finish.
struct PendingEnd {
    task_id: String,
    unfinished: bool,
}

impl Drop for PendingEnd {
    fn drop(&mut self) {
        if self.unfinished {
            tracing::warn!(target: "roomkit::tasks", task = %self.task_id,
                "the unannounced child's terminal write did not finish");
        }
    }
}

async fn record_unannounced_end<S: ConversationStore>(
    kit: &RoomKit<S>,
    result: &DelegatedTaskResult,
) {
    match kit.store().get_room(&result.child_room_id).await {
        // A refused create can have found somebody else's room. Only the
        // child this task stamped is ours to mark, never one made for cleanup.
        Ok(Some(room))
            if room
                .metadata
                .get("task_id")
                .and_then(serde_json::Value::as_str)
                == Some(result.task_id.as_str()) =>
        {
            kit.record_task_end(result).await
        }
        Ok(_) => (),
        Err(error) => tracing::warn!(target: "roomkit::tasks", task = %result.task_id, %error,
            "the unannounced child could not be read to store its terminal status"),
    }
}
