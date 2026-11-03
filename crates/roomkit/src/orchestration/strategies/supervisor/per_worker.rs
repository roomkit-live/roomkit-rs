//! The supervisor's per-worker tools (RFC §19.7.3): one `delegate_to_<id>`
//! per worker, the model deciding when to delegate; a call delegates its
//! task to that worker from the room of the call and answers with the
//! worker's status and result, or, when the supervisor does not wait for
//! its workers, starts the worker in the background and answers at once,
//! the worker's outcome handed back to the supervisor once it ended.
//! Python's `orchestration/strategies/supervisor/_inject_per_worker.py`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use serde_json::{Value, json};

use super::Team;
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::kit::{DelegateOptions, RoomKit};
use crate::orchestration::StatusLevel;
use crate::orchestration::agents::HeldAgent;
use crate::orchestration::background::{
    BackgroundRun, background_failure_text, start_background_run,
};
use crate::orchestration::lifecycle::post_agent_lifecycle;
use crate::orchestration::worker_run::{
    WorkerOutcome, WorkerStatus, completed_or_failed, follow_worker, run_worker,
};
use crate::store::ConversationStore;
use crate::tasks::DelegatedTask;
use crate::tasks::handback::{CALLER_HANDS_BACK, bounded, result_text};
use crate::tasks::json::dumps_object;
use crate::tasks::text::identifier;
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::served::OwnerToolEntry;

/// The agent a background worker's run posts its terminal entry as.
const ORCHESTRATION: &str = "orchestration";

/// What serves one install's `delegate_to_<id>` tools: its team, whether a
/// call waits for its worker, and the workers busy in the background, per
/// room, a worker busy in one room free in another (Python's
/// `_PerWorkerToolServer`).
pub(super) struct PerWorker {
    team: Arc<Team>,
    wait: bool,
    pending: Mutex<HashSet<(RoomId, ChannelId)>>,
}

impl PerWorker {
    pub(super) fn new(team: Arc<Team>, wait: bool) -> Arc<Self> {
        Arc::new(Self {
            team,
            wait,
            pending: Mutex::default(),
        })
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, HashSet<(RoomId, ChannelId)>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The `delegate_to_<id>` declaration of one worker (Python's
/// `_worker_tool`).
fn definition(worker: &HeldAgent) -> Value {
    let id = worker.channel_id.as_str();
    let description = worker
        .description
        .clone()
        .filter(|description| !description.is_empty())
        .unwrap_or_else(|| format!("Worker agent {id}"));
    json!({
        "name": format!("delegate_to_{id}"),
        "description": format!("Delegate a task to {id}. {description}"),
        "parameters": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "Description of the task to delegate",
                },
            },
            "required": ["task"],
        },
    })
}

/// One `delegate_to_<id>` entry per worker of the install `server` serves,
/// each a strategy's tool, its call waiting by design (RFC §21.6).
pub(super) fn entries<S: ConversationStore>(server: &Arc<PerWorker>) -> Vec<OwnerToolEntry<S>> {
    server
        .team
        .workers
        .iter()
        .map(|worker| {
            let (server, worker_id) = (Arc::clone(server), worker.channel_id.clone());
            OwnerToolEntry {
                definition: definition(worker),
                serve: Arc::new(move |kit, call| {
                    let (server, worker_id) = (Arc::clone(&server), worker_id.clone());
                    Box::pin(async move { serve(&kit, &server, &worker_id, call).await })
                }),
                waits: true,
                owner: None,
            }
        })
        .collect()
}

/// Answers one `delegate_to_<id>` call: waiting for the worker, or
/// starting it in the background (Python's `_PerWorkerToolServer.serve`).
async fn serve<S: ConversationStore>(
    kit: &RoomKit<S>,
    server: &Arc<PerWorker>,
    worker_id: &ChannelId,
    call: OwnerCall,
) -> OwnerToolAnswer {
    if server.wait {
        return delegate_and_wait(kit, &server.team, worker_id, call).await;
    }
    let task = call
        .arguments
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match delegate_in_background(kit, server, &call.room_id, worker_id, task).await {
        Ok(answer) => OwnerToolAnswer::Served(Value::String(answer)),
        Err(error) => delegation_failed(worker_id, &error),
    }
}

/// Starts `worker_id` on `task` as a task of the kit's task runner and
/// answers at once; its background run waits for it within the team's
/// task timeout and hands its outcome back to the supervisor (Python's
/// `_PerWorkerToolServer._delegate_in_background`). A worker already on
/// this room's task answers so, and starts nothing.
async fn delegate_in_background<S: ConversationStore>(
    kit: &RoomKit<S>,
    server: &Arc<PerWorker>,
    room_id: &RoomId,
    worker_id: &ChannelId,
    task: &str,
) -> Result<String, RoomKitError> {
    let notify = ChannelId::new(CALLER_HANDS_BACK).unwrap_or_else(|_| unreachable!());
    let key = (room_id.clone(), worker_id.clone());
    if !server.pending().insert(key.clone()) {
        return Ok(already_working(worker_id));
    }
    // A start refused (a kit closing) or cut (its call's turn cut) frees the
    // worker: it is not stuck already running.
    let mut starting = Starting {
        server,
        key: Some(key),
    };
    // Read in the call: the outcome continues its turn's chain (RFC §23.3).
    let chain_depth = ExecutionContext::current().turn_depth().unwrap_or(0);
    let options = DelegateOptions {
        notify: Some(notify),
        // The background run follows it (RFC §23.3).
        post_status: false,
        ..server.team.delegation()
    };
    let delegated = kit.delegate(room_id, worker_id, task, options).await?;
    // Refused, a task delegated is the runner's to end.
    let run = background_run(kit, server, room_id, delegated.clone());
    start_background_run(kit, run, chain_depth)?;
    starting.key = None;
    Ok(dispatched(worker_id, &delegated.id))
}

/// A worker's background start under way: dropped before it went through,
/// it frees the worker in its room (Python's `except BaseException`).
struct Starting<'server> {
    server: &'server PerWorker,
    key: Option<(RoomId, ChannelId)>,
}

impl Drop for Starting<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.server.pending().remove(&key);
        }
    }
}

/// The worker's background run: its task, waited for and followed as a
/// waited delegation is, the worker freed in `room_id` before its outcome
/// is handed back (Python's `_PerWorkerToolServer._background_run`).
fn background_run<S: ConversationStore>(
    kit: &RoomKit<S>,
    server: &Arc<PerWorker>,
    room_id: &RoomId,
    delegated: DelegatedTask,
) -> BackgroundRun<WorkerOutcome> {
    let worker_id = delegated.agent_id.clone();
    let metadata = Metadata::from_iter([
        ("room_id".to_owned(), json!(room_id.as_str())),
        ("mode".to_owned(), json!("per_worker_async")),
    ]);
    let mut run_metadata = metadata.clone();
    run_metadata.insert("worker".to_owned(), json!(worker_id.as_str()));
    let poster = kit.downgrade();
    let post = move |level: StatusLevel, detail: &str| {
        let Some(kit) = poster.upgrade() else {
            return;
        };
        let orchestration = ChannelId::new(ORCHESTRATION).unwrap_or_else(|_| unreachable!());
        post_agent_lifecycle(
            &kit,
            &orchestration,
            level,
            "worker",
            detail,
            run_metadata.clone(),
        );
    };
    let status = WorkerStatus {
        metadata,
        action: "task",
        ended: completed_or_failed,
    };
    let (follower, timeout) = (kit.clone(), Some(server.team.task_timeout));
    let work =
        Box::pin(async move { Ok(follow_worker(&follower, delegated, timeout, status).await) });
    let (pending, key) = (Arc::clone(server), (room_id.clone(), worker_id.clone()));
    BackgroundRun {
        room_id: room_id.clone(),
        notify: server.team.supervisor.channel_id.clone(),
        work,
        told: Box::new(move |outcome| worker_told(&worker_id, outcome)),
        ended: worker_ended,
        post: Box::new(post),
        release: Box::new(move |_returned| {
            pending.pending().remove(&key);
        }),
    }
}

/// What the supervisor reads of a background worker: its outcome bounded
/// and set apart as a worker's; for a run that failed (`None`), that the
/// task could not be completed (Python's `_worker_told`).
fn worker_told(worker_id: &ChannelId, outcome: Option<&WorkerOutcome>) -> String {
    let worker = identifier(worker_id.as_str(), "worker");
    let Some(outcome) = outcome else {
        return background_failure_text(&format!("task for {worker}"));
    };
    let status = if outcome.completed {
        "completed"
    } else {
        "did not complete"
    };
    let output = if outcome.output.is_empty() {
        "No output"
    } else {
        &outcome.output
    };
    result_text(
        &format!("[Your background task for {worker} {status}. Share the outcome with the user.]"),
        &bounded(output),
    )
}

/// A background worker's terminal entry, once handed back: completed or
/// failed, as its task ended (Python's `_worker_ended`).
fn worker_ended(outcome: &WorkerOutcome) -> (StatusLevel, &'static str) {
    if outcome.completed {
        (StatusLevel::Completed, "completed")
    } else {
        (StatusLevel::Failed, "not completed")
    }
}

/// What the model reads when the worker is still on this room's task
/// (Python's `_already_working`).
fn already_working(worker_id: &ChannelId) -> String {
    let message = format!(
        "{worker_id} is already working on this. Do NOT call this tool again. Tell the user to \
         ask again shortly."
    );
    dumps_object(
        [
            ("status", &json!("already_running")),
            ("worker", &json!(worker_id.as_str())),
            ("message", &json!(message)),
        ],
        true,
    )
}

/// What the model reads when the worker was started in the background
/// (Python's `_dispatched`).
fn dispatched(worker_id: &ChannelId, task_id: &str) -> String {
    let message = format!(
        "Task dispatched to {worker_id}. It is running in the background. Do NOT call this tool \
         again. Tell the user to ask again shortly for results."
    );
    dumps_object(
        [
            ("status", &json!("delegated")),
            ("task_id", &json!(task_id)),
            ("worker", &json!(worker_id.as_str())),
            ("message", &json!(message)),
        ],
        true,
    )
}

/// A delegation that failed: the channel reads it as any failed call, the
/// class for the model, the message for the observers (RFC §9.3).
fn delegation_failed(worker_id: &ChannelId, error: &RoomKitError) -> OwnerToolAnswer {
    tracing::error!(
        target: "roomkit::orchestration",
        worker = %worker_id,
        error = %error,
        "Delegation to {worker_id} failed"
    );
    OwnerToolAnswer::Failed {
        kind: error.class_name(),
        message: error.to_string(),
    }
}

/// Runs `worker_id` on the call's task from the room of the call, within
/// the team's task timeout, and answers with the worker's status and
/// result (Python's `_PerWorkerToolServer._delegate_and_wait`).
async fn delegate_and_wait<S: ConversationStore>(
    kit: &RoomKit<S>,
    team: &Team,
    worker_id: &ChannelId,
    call: OwnerCall,
) -> OwnerToolAnswer {
    let room_id = &call.room_id;
    let task = call
        .arguments
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let status = WorkerStatus {
        metadata: Metadata::from_iter([
            ("room_id".to_owned(), json!(room_id.as_str())),
            ("mode".to_owned(), json!("per_worker_wait")),
        ]),
        action: "task",
        ended: completed_or_failed,
    };
    let (timeout, delegation) = (Some(team.task_timeout), team.delegation());
    let ran = run_worker(
        kit,
        room_id,
        worker_id,
        task,
        timeout,
        Some(status),
        delegation,
    )
    .await;
    match ran {
        Ok(outcome) => {
            let status = outcome
                .result()
                .and_then(|result| serde_json::to_value(result.status).ok())
                .unwrap_or_else(|| json!("failed"));
            let answer = dumps_object(
                [
                    ("status", &status),
                    ("worker", &json!(worker_id.as_str())),
                    ("result", &json!(outcome.output)),
                ],
                true,
            );
            OwnerToolAnswer::Served(Value::String(answer))
        }
        Err(error) => delegation_failed(worker_id, &error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use roomkit_core::model::{ChannelId, ChannelType, RoomEvent, RoomId};

    use super::{PerWorker, Team, delegate_in_background};
    use crate::kit::{CreateRoomOptions, RoomKit, RoomKitOptions};
    use crate::orchestration::agents::HeldAgent;
    use crate::testing::{RecordingTransport, RecordingTransportOptions};
    use crate::{HookOptions, HookTrigger};

    fn agent(name: &str) -> HeldAgent {
        HeldAgent {
            channel_id: ChannelId::new(name).unwrap_or_else(|error| panic!("{error}")),
            role: None,
            description: None,
            purpose: None,
        }
    }

    // Python: orchestration/strategies/supervisor/_inject_per_worker.py
    // _delegate_in_background, its `except BaseException` (no Python test
    // cuts the start).
    #[tokio::test]
    async fn a_start_cut_short_frees_the_worker_in_its_room() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let room = RoomId::new("r").unwrap_or_else(|error| panic!("{error}"));
        let created = CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(created)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let worker = agent("worker");
        let channel = RecordingTransport::new(
            worker.channel_id.clone(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(channel)
            .unwrap_or_else(|error| panic!("{error}"));
        // Holds the delegation at its child room's creation.
        let held = |_event: RoomEvent, _context| std::future::pending::<()>();
        kit.hook(HookTrigger::OnRoomCreated, held, HookOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        let team = Arc::new(Team {
            supervisor: agent("boss"),
            workers: vec![worker.clone()],
            share_channels: Vec::new(),
            task_timeout: Duration::from_secs(5),
            max_revisions: 3,
        });
        let server = PerWorker::new(team, false);

        let starting = delegate_in_background(&kit, &server, &room, &worker.channel_id, "Go.");
        let cut = tokio::time::timeout(Duration::from_millis(20), starting).await;

        assert!(cut.is_err(), "the delegation was not held");
        assert!(server.pending().is_empty());
    }
}
