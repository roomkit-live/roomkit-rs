//! A strategy's worker delegation: one task handed to one agent, waited
//! for, and followed on the status bus (RFC §19.7.3, §19.7.4, §23.3):
//! Python's `orchestration/_worker_run.py`.
//!
//! One sequence for a loop's producer and reviewers: the worker is posted
//! pending, its task delegated and waited for, then one terminal entry is
//! posted however the delegation ends: completed, failed, raised or
//! cancelled.

use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{ChannelId, EventContent, Metadata, RoomEvent, RoomId, TaskStatus};
use serde_json::{Value, json};

use crate::error::RoomKitError;
use crate::kit::{DelegateOptions, RoomKit};
use crate::orchestration::StatusLevel;
use crate::orchestration::lifecycle::post_agent_lifecycle;
use crate::store::ConversationStore;
use crate::tasks::lines::RUN_ENTRY;
use crate::tasks::{DelegatedTask, DelegatedTaskResult, OnCompleteCallback, task_work};

/// The text a strategy's tasks carry for `event`: a plain text's body,
/// else the empty string. A message it is empty for (a media, a composite,
/// an empty text) is answered by the lead agent alone, whose own channel
/// reads it (RFC §19.7.3, §19.7.4). Python's `task_input`.
pub(crate) fn task_input(event: &RoomEvent) -> &str {
    if let EventContent::Text(text) = &event.content {
        &text.body
    } else {
        ""
    }
}

/// The text a strategy reads of a delegated task's result: its work; a
/// failed task reads as failed, never with its error, which is for the
/// logs and hooks, not for a model (RFC §9.3), nor with the narration a
/// worker cut short keeps (RFC §23.3). Python's `task_output`.
pub(crate) fn task_output(result: Option<&DelegatedTaskResult>) -> String {
    let Some(result) = result else {
        return String::new();
    };
    let work = task_work(result);
    if !work.is_empty() {
        return work.to_owned();
    }
    if result.status == TaskStatus::Cancelled {
        return "The task was cancelled.".to_owned();
    }
    let failed = result
        .error
        .as_deref()
        .is_some_and(|error| !error.is_empty())
        || result.status == TaskStatus::Failed;
    if failed {
        "The task failed.".to_owned()
    } else {
        String::new()
    }
}

/// How a worker's delegation ended: Python's `WorkerOutcome`.
#[derive(Clone, Debug)]
pub(crate) struct WorkerOutcome {
    /// What the strategy reads of it: the task's work, or that it failed
    /// or was cancelled (never an error's message).
    pub(crate) output: String,
    /// Whether the task completed.
    pub(crate) completed: bool,
    /// The delegated task.
    pub(crate) task: DelegatedTask,
}

impl WorkerOutcome {
    /// The task's result.
    pub(crate) fn result(&self) -> Option<DelegatedTaskResult> {
        self.task.result()
    }
}

/// A worker delegation's terminal entry: its level, its detail, and what it
/// adds to the status's metadata. Python's `WorkerEnd`.
pub(crate) struct WorkerEnd {
    pub(crate) level: StatusLevel,
    pub(crate) detail: String,
    pub(crate) metadata: Metadata,
}

/// How a worker's delegation shows on the status bus: Python's
/// `WorkerStatus`.
#[derive(Clone)]
pub(crate) struct WorkerStatus {
    /// Posted with every entry (the room, the strategy, the worker's
    /// role); a task that ran adds its `task_id` to its terminal entry.
    pub(crate) metadata: Metadata,
    /// The entries' action.
    pub(crate) action: &'static str,
    /// The terminal entry of a delegation that returned, from its outcome;
    /// one that raised or was cancelled is posted failed.
    pub(crate) ended: fn(&WorkerOutcome) -> WorkerEnd,
}

/// Delegates `task` to `worker_id` from `room_id` inline and waits for it,
/// posted on the status bus as `status` says: pending, then one terminal
/// entry whatever ends it. `delegation` goes to [`RoomKit::delegate`];
/// the run posts the task's entries itself. Python's `run_worker(kit,
/// room_id, worker_id, task, *, timeout=None, status, **delegation)`.
///
/// # Errors
///
/// What the delegation fails with, posted failed with its message.
pub(crate) async fn run_worker<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    worker_id: &ChannelId,
    task: &str,
    status: WorkerStatus,
    mut delegation: DelegateOptions,
) -> Result<WorkerOutcome, RoomKitError> {
    let posts = Posts {
        kit: kit.clone(),
        worker_id: worker_id.clone(),
        status,
        task_id: Arc::default(),
    };
    posts.post(StatusLevel::Pending, task, None, Metadata::new());
    // The run learns its task once it ends, before a cut reaches the run:
    // its terminal entry then names it.
    delegation.on_complete = Some(posts.task_ended_then(delegation.on_complete.take()));
    delegation.wait = true;
    delegation.post_status = false;
    let mut cut = CutPost {
        posts: &posts,
        armed: true,
    };
    let delegated = kit.delegate(room_id, worker_id, task, delegation).await;
    cut.armed = false;
    let task = match delegated {
        Ok(task) => task,
        Err(error) => {
            posts.post(
                StatusLevel::Failed,
                &error.to_string(),
                None,
                Metadata::new(),
            );
            return Err(error);
        }
    };
    let result = task.result();
    let outcome = WorkerOutcome {
        output: task_output(result.as_ref()),
        completed: result.is_some_and(|result| result.status == TaskStatus::Completed),
        task,
    };
    let end = (posts.status.ended)(&outcome);
    posts.post(end.level, &end.detail, Some(&outcome.task), end.metadata);
    Ok(outcome)
}

/// A worker delegation's entries on the status bus, each naming the task
/// once the run knows it (RFC §19.7.3).
struct Posts<S> {
    kit: RoomKit<S>,
    worker_id: ChannelId,
    status: WorkerStatus,
    task_id: Arc<Mutex<Option<String>>>,
}

impl<S: ConversationStore> Posts<S> {
    /// The completion callback that remembers the task the delegation
    /// ended, then calls `on_complete`, the caller's, when given.
    fn task_ended_then(&self, on_complete: Option<OnCompleteCallback>) -> OnCompleteCallback {
        let task_id = Arc::clone(&self.task_id);
        Arc::new(move |result: DelegatedTaskResult| {
            *task_id.lock().unwrap_or_else(PoisonError::into_inner) = Some(result.task_id.clone());
            match &on_complete {
                Some(on_complete) => on_complete(result),
                None => Box::pin(async {}),
            }
        })
    }

    fn post(
        &self,
        level: StatusLevel,
        detail: &str,
        task: Option<&DelegatedTask>,
        added: Metadata,
    ) {
        let mut metadata = self.status.metadata.clone();
        metadata.extend(added);
        metadata.insert(RUN_ENTRY.to_owned(), json!(true));
        let known = self
            .task_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(task_id) = task.map(|task| task.id.clone()).or(known) {
            metadata.insert("task_id".to_owned(), Value::String(task_id));
        }
        let action = self.status.action;
        post_agent_lifecycle(&self.kit, &self.worker_id, level, action, detail, metadata);
    }
}

/// Posts a delegation cut short, its future dropped before it returned,
/// failed `cancelled` (Python's `except asyncio.CancelledError`).
struct CutPost<'run, S: ConversationStore> {
    posts: &'run Posts<S>,
    armed: bool,
}

impl<S: ConversationStore> Drop for CutPost<'_, S> {
    fn drop(&mut self) {
        if self.armed {
            self.posts
                .post(StatusLevel::Failed, "cancelled", None, Metadata::new());
        }
    }
}
