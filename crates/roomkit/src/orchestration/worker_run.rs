//! A strategy's worker delegation: one task handed to one agent, waited
//! for, and followed on the status bus (RFC §19.7.3, §19.7.4, §23.3):
//! Python's `orchestration/_worker_run.py`.
//!
//! One sequence for the supervisor's workers (sequential, parallel,
//! supervised, per-worker) and a loop's producer and reviewers: the worker
//! is posted pending, its task delegated and waited for within the
//! strategy's bound, then one terminal entry is posted however the
//! delegation ends: completed, failed, timed out, raised or cancelled.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelId, EventContent, Metadata, RoomEvent, RoomId, TaskStatus};
use serde_json::{Value, json};

use crate::error::RoomKitError;
use crate::kit::{DelegateOptions, RoomKit};
use crate::orchestration::StatusLevel;
use crate::orchestration::lifecycle::post_agent_lifecycle;
use crate::store::ConversationStore;
use crate::tasks::lines::RUN_ENTRY;
use crate::tasks::{
    DelegatedTask, DelegatedTaskResult, OnCompleteCallback, WaitOptions, task_work,
};

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
    /// What the strategy reads of it: the task's work, or that it failed,
    /// was cancelled or timed out (never an error's message).
    pub(crate) output: String,
    /// Whether the task completed.
    pub(crate) completed: bool,
    /// The delegated task; `None` when it timed out.
    pub(crate) task: Option<DelegatedTask>,
}

impl WorkerOutcome {
    /// The task's result; `None` when it timed out.
    pub(crate) fn result(&self) -> Option<DelegatedTaskResult> {
        self.task.as_ref().and_then(DelegatedTask::result)
    }
}

/// A worker's terminal entry: completed or failed, with its output
/// (Python's `_completed_or_failed`, `WorkerStatus`'s default `ended`).
pub(crate) fn completed_or_failed(outcome: &WorkerOutcome) -> WorkerEnd {
    let level = if outcome.completed {
        StatusLevel::Completed
    } else {
        StatusLevel::Failed
    };
    WorkerEnd {
        level,
        detail: outcome.output.clone(),
        metadata: Metadata::new(),
    }
}

/// What a worker past its bound reads as (Python's `f"The task timed out
/// after {timeout:g}s."`).
pub(crate) fn timed_out_text(timeout: Duration) -> String {
    format!(
        "The task timed out after {}s.",
        general(timeout.as_secs_f64())
    )
}

/// `value` as Python's `format(value, "g")` writes it: six significant
/// digits, trailing zeros dropped, in exponent form below 1e-4 or from
/// 1e6.
fn general(value: f64) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or_default();
    if (-4..6).contains(&exponent) {
        let decimals = usize::try_from(5 - exponent).unwrap_or_default();
        return without_trailing_zeros(&format!("{value:.decimals$}")).to_owned();
    }
    let sign = if exponent < 0 { '-' } else { '+' };
    format!(
        "{}e{sign}{:02}",
        without_trailing_zeros(mantissa),
        exponent.unsigned_abs()
    )
}

/// A decimal's text without the zeros ending its fraction, nor a point
/// left alone.
fn without_trailing_zeros(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
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
/// within `timeout` (`None`: unbounded): a task past it is cancelled and
/// its delegation reads as timed out, the call answering once the task
/// ended. Posted on the status bus as `status` says: pending, then one
/// terminal entry whatever ends it; nothing without one (a supervisor's
/// own pass). `delegation` goes to [`RoomKit::delegate`]; the run posts
/// the task's entries itself. Python's `run_worker(kit, room_id,
/// worker_id, task, *, timeout, status=None, **delegation)`.
///
/// # Errors
///
/// What the delegation fails with, posted failed with its message.
pub(crate) async fn run_worker<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    worker_id: &ChannelId,
    task: &str,
    timeout: Option<Duration>,
    status: Option<WorkerStatus>,
    mut delegation: DelegateOptions,
) -> Result<WorkerOutcome, RoomKitError> {
    let posts = Posts {
        kit: kit.clone(),
        worker_id: worker_id.clone(),
        status,
        run: Arc::default(),
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
    let opened = |task: &DelegatedTask| {
        let mut run = posts.run();
        run.opened = true;
        run.task = Some(task.clone());
    };
    let delegating = kit.delegate_watched(room_id, worker_id, task.to_owned(), delegation, &opened);
    let delegated = match timeout {
        Some(bound) => match tokio::time::timeout(bound, delegating).await {
            Ok(delegated) => delegated,
            Err(_elapsed) => {
                cut.armed = false;
                return Ok(posts.timed_out(bound).await);
            }
        },
        None => delegating.await,
    };
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
        task: Some(task),
    };
    posts.ended(&outcome);
    Ok(outcome)
}

/// A worker delegation's entries on the status bus, each naming the task
/// once the run knows it (RFC §19.7.3).
struct Posts<S> {
    kit: RoomKit<S>,
    worker_id: ChannelId,
    status: Option<WorkerStatus>,
    run: Arc<Mutex<Run>>,
}

impl<S> Clone for Posts<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            worker_id: self.worker_id.clone(),
            status: self.status.clone(),
            run: Arc::clone(&self.run),
        }
    }
}

/// What the run knows of its delegation: the task it ended, whether the
/// task ends however the delegation ends (its completion then names it),
/// the task it opened, and whether the run was cut before it returned.
#[derive(Default)]
struct Run {
    task_id: Option<String>,
    opened: bool,
    task: Option<DelegatedTask>,
    cut: bool,
}

impl<S: ConversationStore> Posts<S> {
    fn run(&self) -> std::sync::MutexGuard<'_, Run> {
        self.run.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The completion callback that remembers the task the delegation
    /// ended, posting the run's cut once it names it, then calls
    /// `on_complete`, the caller's, when given.
    fn task_ended_then(&self, on_complete: Option<OnCompleteCallback>) -> OnCompleteCallback {
        let posts = self.clone();
        Arc::new(move |result: DelegatedTaskResult| {
            let cut = {
                let mut run = posts.run();
                run.task_id = Some(result.task_id.clone());
                run.cut
            };
            if cut {
                posts.cancelled(&result.task_id);
            }
            match &on_complete {
                Some(on_complete) => on_complete(result),
                None => Box::pin(async {}),
            }
        })
    }

    /// The outcome of a delegation past its bound, `bound`, once its task
    /// ended: the delegation's future dropped cut the task, which ends
    /// cancelled on a run of its own, waited for here as Python's bound
    /// waits for the cancelled delegation; its terminal entry then names
    /// it. A delegation cut before it opened its task names none.
    async fn timed_out(&self, bound: Duration) -> WorkerOutcome {
        let opened = self.run().task.clone();
        if let Some(task) = opened {
            // Its end is the cut's, whatever it says: the outcome is the
            // bound's.
            drop(task.wait(WaitOptions::default()).await);
        }
        let outcome = WorkerOutcome {
            output: timed_out_text(bound),
            completed: false,
            task: None,
        };
        self.ended(&outcome);
        outcome
    }

    /// Posts the terminal entry of a delegation that returned, from its
    /// outcome.
    fn ended(&self, outcome: &WorkerOutcome) {
        let Some(status) = &self.status else {
            return;
        };
        let end = (status.ended)(outcome);
        self.post(end.level, &end.detail, outcome.task.as_ref(), end.metadata);
    }

    /// The run's terminal entry once it was cut: it names the task, the cut
    /// task's line on the status bus ending with it (RFC §23.4).
    fn cancelled(&self, task_id: &str) {
        self.post_named(
            StatusLevel::Failed,
            "cancelled",
            Some(task_id.to_owned()),
            Metadata::new(),
        );
    }

    fn post(
        &self,
        level: StatusLevel,
        detail: &str,
        task: Option<&DelegatedTask>,
        added: Metadata,
    ) {
        let known = self.run().task_id.clone();
        let task_id = task.map(|task| task.id.clone()).or(known);
        self.post_named(level, detail, task_id, added);
    }

    fn post_named(
        &self,
        level: StatusLevel,
        detail: &str,
        task_id: Option<String>,
        added: Metadata,
    ) {
        let Some(status) = &self.status else {
            return;
        };
        let mut metadata = status.metadata.clone();
        metadata.extend(added);
        metadata.insert(RUN_ENTRY.to_owned(), json!(true));
        if let Some(task_id) = task_id {
            metadata.insert("task_id".to_owned(), Value::String(task_id));
        }
        let action = status.action;
        post_agent_lifecycle(&self.kit, &self.worker_id, level, action, detail, metadata);
    }
}

/// Posts a delegation cut short, its future dropped before it returned,
/// failed `cancelled` (Python's `except asyncio.CancelledError`): at once
/// when the run knows its task, or when the delegation was cut before it
/// opened one; else once the task's cut end names it, as Python's inline
/// run ends its task before the cut reaches its caller.
struct CutPost<'run, S: ConversationStore> {
    posts: &'run Posts<S>,
    armed: bool,
}

impl<S: ConversationStore> Drop for CutPost<'_, S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (known, deferred) = {
            let mut run = self.posts.run();
            run.cut = run.opened && run.task_id.is_none();
            (run.task_id.clone(), run.cut)
        };
        match known {
            Some(task_id) => self.posts.cancelled(&task_id),
            None if !deferred => {
                self.posts
                    .post(StatusLevel::Failed, "cancelled", None, Metadata::new());
            }
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    // Python: orchestration/_worker_run.py `f"The task timed out after {timeout:g}s."`,
    // as tests/orchestration/test_worker_delegation_bound.py reads it.
    #[test]
    fn a_timeout_is_written_as_python_s_general_format() {
        use std::time::Duration;

        use super::{general, timed_out_text};
        assert_eq!(
            timed_out_text(Duration::from_millis(50)),
            "The task timed out after 0.05s."
        );
        assert_eq!(
            timed_out_text(Duration::from_secs(120)),
            "The task timed out after 120s."
        );
        assert_eq!(general(1.5), "1.5");
        assert_eq!(general(0.000_01), "1e-05");
        assert_eq!(general(1_234_567.0), "1.23457e+06");
        assert_eq!(general(0.123_456_789), "0.123457");
    }

    use std::future::{Future, pending, ready};
    use std::sync::Arc;
    use std::time::Duration;

    use roomkit_core::model::{
        ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
        Metadata, RoomContext, RoomEvent, RoomId,
    };
    use serde_json::json;
    use tokio::sync::Notify;

    use super::{WorkerEnd, WorkerOutcome, WorkerStatus, run_worker};
    use crate::channel::{Channel, ChannelError};
    use crate::kit::{CreateRoomOptions, DelegateOptions, RoomKit, RoomKitOptions};
    use crate::orchestration::StatusLevel;
    use crate::orchestration::lifecycle::post_agent_lifecycle;
    use crate::store::{ConversationStore, ListRoomsOptions};
    use crate::tasks::lines::room_task_lines;

    /// A worker whose turn starts and never ends.
    #[derive(Clone, Default)]
    struct Endless {
        started: Arc<Notify>,
    }

    impl Channel for Endless {
        fn channel_id(&self) -> ChannelId {
            ChannelId::new("worker").unwrap_or_else(|error| panic!("{error}"))
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::AI
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Intelligence
        }

        fn handle_inbound(
            &self,
            _message: InboundMessage,
            _context: RoomContext,
        ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
            ready(Err("a worker is sent nothing".into()))
        }

        fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
            ready(Ok(ChannelOutput::empty()))
        }

        fn on_event(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
            let started = Arc::clone(&self.started);
            async move {
                started.notify_one();
                pending().await
            }
        }
    }

    fn ended(outcome: &WorkerOutcome) -> WorkerEnd {
        WorkerEnd {
            level: StatusLevel::Completed,
            detail: outcome.output.clone(),
            metadata: Metadata::new(),
        }
    }

    // Python: tests/orchestration/test_worker_run_task_line.py::test_a_worker_cut_after_its_progress_leaves_its_line_ended
    // (the inline door, cancelled, under each action; the timed-out cases
    // and the background door come with the supervisor's bound runs).
    #[tokio::test]
    async fn a_worker_cut_after_its_progress_leaves_its_line_ended() {
        for action in ["task", "iteration"] {
            let kit = RoomKit::new(RoomKitOptions::default());
            let worker = Endless::default();
            kit.register_channel(worker.clone())
                .unwrap_or_else(|error| panic!("{error}"));
            let room = RoomId::new("r").unwrap_or_else(|error| panic!("{error}"));
            let options = CreateRoomOptions {
                room_id: Some(room.clone()),
                ..CreateRoomOptions::default()
            };
            kit.create_room(options)
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            let status = WorkerStatus {
                metadata: Metadata::from_iter([
                    ("room_id".to_owned(), json!("r")),
                    ("strategy".to_owned(), json!("sequential")),
                ]),
                action,
                ended,
            };
            let (running, worker_id) = (kit.clone(), worker.channel_id());
            let run = tokio::spawn(async move {
                let delegation = DelegateOptions::default();
                let status = Some(status);
                let ran = run_worker(
                    &running, &room, &worker_id, "Count.", None, status, delegation,
                );
                drop(ran.await);
            });
            tokio::time::timeout(Duration::from_secs(5), worker.started.notified())
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            let rooms = kit.store().list_rooms(ListRoomsOptions::default()).await;
            let child = rooms
                .unwrap_or_else(|error| panic!("{error}"))
                .into_iter()
                .find(|room| room.id.as_str().starts_with("r::task-"))
                .unwrap_or_else(|| panic!("no child room"));
            let task_id = child.metadata["task_id"].clone();
            // The worker's progress, as Python's post_task_progress posts it.
            let progress = Metadata::from_iter([
                ("room_id".to_owned(), json!("r")),
                ("task_id".to_owned(), task_id),
                ("child_room_id".to_owned(), json!(child.id.as_str())),
            ]);
            let worker_id = worker.channel_id();
            post_agent_lifecycle(
                &kit,
                &worker_id,
                StatusLevel::Info,
                "task",
                "3 of 5",
                progress,
            );

            run.abort();
            drop(run.await);
            crate::testing::idle(&kit).await;
            let lines = room_task_lines(kit.status_bus(), "r", 50).await;
            kit.close().await.unwrap_or_else(|error| panic!("{error}"));

            let read: Vec<(bool, String, String)> = lines
                .iter()
                .map(|line| {
                    let member = |key: &str| {
                        line.get(key)
                            .and_then(|value| value.as_str())
                            .unwrap_or("")
                            .to_owned()
                    };
                    (
                        member("task_id").starts_with("task-"),
                        member("status"),
                        member("progress"),
                    )
                })
                .collect();
            assert_eq!(
                read,
                [(true, "failed".to_owned(), "3 of 5".to_owned())],
                "{action}"
            );
        }
    }
}
