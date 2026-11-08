//! The supervisor's `delegate_workers` tool (RFC §19.7.3): one call runs
//! the whole team for the room of the call, supervised step by step when
//! sequential, all at once when parallel, and answers with what the
//! supervisor reviews; with `async_delivery`, it answers at once and runs
//! the team in the background, the workers' results handed back to the
//! supervisor once they ended. Python's
//! `orchestration/strategies/supervisor/_inject_strategy.py`,
//! `delegate.py`'s `_async_run_and_deliver` and `execution.py`'s
//! `_run_parallel`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::auto::run_workers;
use super::results::{
    Presented, WorkerResult, format_supervisor_review, format_worker_results, worker_roles_csv,
    workers_text,
};
use super::supervised::run_supervised_sequential;
use super::{Team, WorkerStrategy};
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::orchestration::StatusLevel;
use crate::orchestration::background::{
    BackgroundRun, background_failure_text, start_background_run,
};
use crate::orchestration::gather::gather_or_cut;
use crate::orchestration::lifecycle::post_agent_lifecycle;
use crate::orchestration::worker_run::{WorkerStatus, completed_or_failed, run_worker};
use crate::store::ConversationStore;
use crate::tasks::handback::bounded;
use crate::tasks::json::dumps_object;
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::served::OwnerToolEntry;

/// The team tool's name: Python's `_STRATEGY_TOOL_NAME`.
pub(super) const TEAM_TOOL: &str = "delegate_workers";

/// How long a room's answer is served again to a repeated call: Python's
/// `_StrategyToolServer._DEDUP_WINDOW`.
const DEDUP_WINDOW: Duration = Duration::from_secs(30);

/// The agent a background team's run posts its terminal entry as.
const ORCHESTRATION: &str = "orchestration";

/// The tool a supervisor follows its background team with, when it was
/// given it (RFC §23.4): Python's `TASK_STATUS_TOOL`.
const TASK_STATUS_TOOL: &str = "task_status";

/// A delegated child room's segment: the supervised flow runs the
/// supervisor there to frame and judge, where it must answer rather than
/// run the team again (Python's `_is_subtask_room`).
fn is_subtask_room(room_id: &RoomId) -> bool {
    room_id.as_str().contains("::task-")
}

/// The `delegate_workers` declaration: the whole team, in one call
/// (Python's `_strategy_tool`).
fn definition(team: &Team, strategy: WorkerStrategy) -> Value {
    let roles = worker_roles_csv(&team.workers);
    json!({
        "name": TEAM_TOOL,
        "description": format!(
            "Delegate a task to ALL workers ({roles}) at once. Call this tool exactly ONCE with \
             the topic. All workers run automatically in {strategy} mode. Do NOT split into \
             separate calls per worker."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The topic or task — sent to all workers as-is",
                },
            },
            "required": ["task"],
        },
    })
}

/// The `delegate_workers` entry the room's owner serves the supervisor in
/// the room it is installed in: a strategy's tool, its call waiting by
/// design (RFC §21.6).
pub(super) fn entry<S: ConversationStore>(
    team: Arc<Team>,
    strategy: WorkerStrategy,
    async_delivery: bool,
) -> OwnerToolEntry<S> {
    let server = Arc::new(TeamServer {
        team,
        strategy,
        async_delivery,
        rooms: Mutex::default(),
        answers: Mutex::default(),
        running: Mutex::default(),
    });
    let definition = definition(&server.team, strategy);
    OwnerToolEntry {
        definition,
        serve: Arc::new(move |kit, call| {
            let server = Arc::clone(&server);
            Box::pin(async move { server.serve(&kit, call).await })
        }),
        waits: true,
        owner: None,
    }
}

/// Serves `delegate_workers`: one run of the team at a time per room, and
/// a room's answer served again to a repeat within the window.
struct TeamServer {
    team: Arc<Team>,
    strategy: WorkerStrategy,
    /// Whether a call answers at once and runs the team in the background.
    async_delivery: bool,
    /// One lock per room, held while its team runs: another room's call
    /// does not wait on it. A lock lives while a call holds it.
    rooms: Mutex<HashMap<RoomId, Weak<tokio::sync::Mutex<()>>>>,
    /// Each room's last answer, and when it was given.
    answers: Mutex<HashMap<RoomId, (String, Instant)>>,
    /// The rooms whose team runs in the background, until it ended
    /// (Python's `_running`).
    running: Mutex<HashSet<RoomId>>,
}

impl TeamServer {
    /// Answers one `delegate_workers` call made in the room of `call`.
    async fn serve<S: ConversationStore>(
        self: &Arc<Self>,
        kit: &RoomKit<S>,
        call: OwnerCall,
    ) -> OwnerToolAnswer {
        let room_id = call.room_id;
        if is_subtask_room(&room_id) {
            return OwnerToolAnswer::Served(Value::String(subtask_refusal()));
        }
        let task = call
            .arguments
            .get("task")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let lock = self.room_lock(&room_id);
        let _running = lock.lock().await;
        if let Some(answer) = self.fresh_answer(&room_id) {
            return OwnerToolAnswer::Served(Value::String(answer));
        }
        if self.async_delivery {
            return match self.dispatch(kit, &room_id, task, &call.channel_id) {
                Ok(answer) => OwnerToolAnswer::Served(Value::String(answer)),
                Err(error) => OwnerToolAnswer::Failed {
                    kind: error.class_name(),
                    message: error.to_string(),
                },
            };
        }
        match self.run(kit, &room_id, task).await {
            Ok(review) => {
                self.remember(&room_id, &review);
                OwnerToolAnswer::Served(Value::String(review))
            }
            Err(error) => {
                // The channel reads it as any failed call: the class for
                // the model, the message for the observers (RFC §9.3).
                tracing::error!(
                    target: "roomkit::orchestration",
                    room_id = %room_id,
                    error = %error,
                    "Strategy delegation failed"
                );
                OwnerToolAnswer::Failed {
                    kind: error.class_name(),
                    message: error.to_string(),
                }
            }
        }
    }

    /// Runs the team for `room_id` on `task`: what the supervisor reviews.
    async fn run<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        task: &str,
    ) -> Result<String, RoomKitError> {
        let team = &self.team;
        if self.strategy == WorkerStrategy::Sequential {
            let steps = run_supervised_sequential(kit, room_id, team, task).await?;
            return Ok(super::prompts::format_supervised_digest(
                task,
                &steps,
                team.max_revisions,
            ));
        }
        let results = run_parallel(kit, room_id, team, task).await?;
        Ok(format_supervisor_review(task, &results, &team.workers))
    }

    /// Starts the team for `room_id` in the background and answers at once
    /// (Python's `_StrategyToolServer._dispatch`): the supervisor's tool
    /// loop does not wait for its workers. A room whose team still runs
    /// answers `already_running` and starts nothing. `caller` is the agent
    /// whose call it is, pointed at `task_status` when it was given it.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit's close began: nothing
    /// starts, and the room is not left busy.
    fn dispatch<S: ConversationStore>(
        self: &Arc<Self>,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        task: &str,
        caller: &ChannelId,
    ) -> Result<String, RoomKitError> {
        let follows = kit.inner.owner_tools.serves(caller, TASK_STATUS_TOOL);
        if !self.running().insert(room_id.clone()) {
            return Ok(already_running(follows));
        }
        // Read in the call: the outcome continues its turn's chain (RFC
        // §23.3).
        let chain_depth = ExecutionContext::current().turn_depth().unwrap_or(0);
        let run = self.background_run(kit, room_id, task);
        if let Err(error) = start_background_run(kit, run, chain_depth) {
            self.running().remove(room_id);
            return Err(error);
        }
        let answer = dispatched(&self.team, follows);
        self.remember(room_id, &answer);
        Ok(answer)
    }

    /// The team's background run in `room_id` on `task`: its workers run as
    /// a call waiting for them runs them, the room freed and its answer
    /// forgotten before the results are handed back to the supervisor, so
    /// that a dispatch in answer to them starts anew (Python's
    /// `_async_run_and_deliver`, `_start_team`'s `_clear`).
    fn background_run<S: ConversationStore>(
        self: &Arc<Self>,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        task: &str,
    ) -> BackgroundRun<Vec<Presented>> {
        let workers: Vec<Value> = self
            .team
            .workers
            .iter()
            .map(|worker| json!(worker.channel_id.as_str()))
            .collect();
        let metadata = Metadata::from_iter([
            ("room_id".to_owned(), json!(room_id.as_str())),
            ("strategy".to_owned(), json!(self.strategy.as_str())),
            ("workers".to_owned(), Value::Array(workers)),
        ]);
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
                "pipeline",
                detail,
                metadata.clone(),
            );
        };
        let (worker_kit, room, team) = (kit.clone(), room_id.clone(), Arc::clone(&self.team));
        let (strategy, task) = (self.strategy, task.to_owned());
        let work =
            Box::pin(async move { run_workers(&worker_kit, &room, &team, strategy, &task).await });
        let (server, room) = (Arc::clone(self), room_id.clone());
        BackgroundRun {
            room_id: room_id.clone(),
            notify: self.team.supervisor.channel_id.clone(),
            work,
            told: Box::new(outcome_text),
            ended: pipeline_ended,
            post: Box::new(post),
            release: Box::new(move |_returned| {
                server.running().remove(&room);
                server.forget(&room);
            }),
        }
    }

    fn running(&self) -> std::sync::MutexGuard<'_, HashSet<RoomId>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets `room_id`'s answer: the room's background run ended, and a
    /// dispatch after it starts a new run rather than reading the answer
    /// that promised its results.
    fn forget(&self, room_id: &RoomId) {
        self.answers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(room_id);
    }

    /// The lock of `room_id`'s runs, made when no call holds it.
    fn room_lock(&self, room_id: &RoomId) -> Arc<tokio::sync::Mutex<()>> {
        let mut rooms = self.rooms.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(lock) = rooms.get(room_id).and_then(Weak::upgrade) {
            return lock;
        }
        rooms.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        rooms.insert(room_id.clone(), Arc::downgrade(&lock));
        lock
    }

    /// `room_id`'s last answer, while it is within the window.
    fn fresh_answer(&self, room_id: &RoomId) -> Option<String> {
        let answers = self.answers.lock().unwrap_or_else(PoisonError::into_inner);
        let (answer, given) = answers.get(room_id)?;
        (given.elapsed() < DEDUP_WINDOW).then(|| answer.clone())
    }

    /// Serves `answer` again to a repeat in `room_id` within the window,
    /// dropping the answers past it.
    fn remember(&self, room_id: &RoomId, answer: &str) {
        let now = Instant::now();
        let mut answers = self.answers.lock().unwrap_or_else(PoisonError::into_inner);
        answers.insert(room_id.clone(), (answer.to_owned(), now));
        answers.retain(|_, (_, given)| now.duration_since(*given) < DEDUP_WINDOW);
    }
}

/// `Use task_status to <what>. ` for a supervisor given `task_status`,
/// nothing otherwise: an agent is never pointed at a tool it was not given
/// (RFC §23.4, Python's `_follow_hint`).
fn follow_hint(follows: bool, what: &str) -> String {
    if follows {
        format!("Use {TASK_STATUS_TOOL} to {what}. ")
    } else {
        String::new()
    }
}

/// What a dispatch answers once the team runs in the background (Python's
/// `_dispatch`).
fn dispatched(team: &Team, follows: bool) -> String {
    let workers: Vec<Value> = team
        .workers
        .iter()
        .map(|worker| json!(worker.channel_id.as_str()))
        .collect();
    let message = format!(
        "Workers are running in the background. {}Their combined results will be handed to you \
         when they are done.",
        follow_hint(follows, "follow progress")
    );
    dumps_object(
        [
            ("status", &json!("dispatched")),
            ("workers", &Value::Array(workers)),
            ("message", &json!(message)),
        ],
        true,
    )
}

/// What a dispatch answers while the room's team still runs (Python's
/// `_dispatch`).
fn already_running(follows: bool) -> String {
    let message = format!(
        "Workers are already running for this room. Do NOT call this tool again. {}",
        follow_hint(follows, "see progress")
    );
    dumps_object(
        [
            ("status", &json!("already_running")),
            ("message", &json!(message.trim())),
        ],
        true,
    )
}

/// Why a background team's work did not complete, or `None` when it did:
/// a supervised step the supervisor left unvalidated, or no worker's task
/// completed (Python's `_why_failed`).
fn why_failed(results: &[Presented]) -> Option<&'static str> {
    if results.iter().any(|result| result.approved == Some(false)) {
        return Some("a step was not validated");
    }
    if !results.iter().any(|result| result.completed) {
        return Some("no worker completed");
    }
    None
}

/// A background team's terminal entry, once handed back: failed when its
/// work did not complete (Python's `_pipeline_ended`).
#[expect(
    clippy::ptr_arg,
    reason = "a background run's `ended` reads the work's own output, a `Vec`"
)]
fn pipeline_ended(results: &Vec<Presented>) -> (StatusLevel, String) {
    if let Some(why) = why_failed(results) {
        return (StatusLevel::Failed, why.to_owned());
    }
    let done = results.iter().filter(|result| result.completed).count();
    (
        StatusLevel::Completed,
        format!("{done} worker(s) completed"),
    )
}

/// What the supervisor reads of its background team: each worker's output
/// bounded, under a header that says whether the work completed; for a
/// run that failed (`None`), that the work could not be completed, without
/// the failure's message (Python's `_outcome_text`).
fn outcome_text(results: Option<&Vec<Presented>>) -> String {
    let Some(results) = results else {
        return background_failure_text("workers");
    };
    let each_bounded: Vec<Presented> = results
        .iter()
        .map(|result| Presented {
            output: bounded(&result.output),
            ..result.clone()
        })
        .collect();
    let header = if why_failed(results).is_none() {
        "[Your background workers completed. Share their results with the user.]"
    } else {
        "[Your background workers could not complete the work. Tell the user what failed.]"
    };
    workers_text(header, &format_worker_results(&each_bounded))
}

/// What `delegate_workers` answers inside a delegated step's room: Python's
/// `_SUBTASK_REFUSAL`.
fn subtask_refusal() -> String {
    let error = json!(
        "delegate_workers is unavailable here: you are already running inside a delegated step. \
         Respond directly to the instruction with the requested text — do not call \
         delegate_workers."
    );
    dumps_object([("error", &error)], true)
}

/// Runs every worker at once on the same task, each within the team's
/// task timeout: one past it is read as failed without cutting the
/// others. Each result says whether the worker's task completed (Python's
/// `_run_parallel`).
///
/// # Errors
///
/// The first error a delegation fails with, as soon as it fails: the other
/// workers are cut, each posting its end (Python's `gather_or_cut`,
/// RMK-1279).
pub(super) async fn run_parallel<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    team: &Arc<Team>,
    task: &str,
) -> Result<Vec<WorkerResult>, RoomKitError> {
    let status = WorkerStatus {
        metadata: Metadata::from_iter([
            ("room_id".to_owned(), json!(room_id.as_str())),
            ("strategy".to_owned(), json!("parallel")),
        ]),
        action: "task",
        ended: completed_or_failed,
    };
    let runs = team.workers.iter().map(|worker| {
        let (kit, room_id, team) = (kit.clone(), room_id.clone(), Arc::clone(team));
        let (channel_id, task, status) =
            (worker.channel_id.clone(), task.to_owned(), status.clone());
        async move {
            let (timeout, delegation) = (Some(team.task_timeout), team.delegation());
            let status = Some(status);
            let outcome = run_worker(
                &kit,
                &room_id,
                &channel_id,
                &task,
                timeout,
                status,
                delegation,
            )
            .await?;
            Ok(WorkerResult {
                worker: channel_id,
                output: outcome.output,
                completed: outcome.completed,
            })
        }
    });
    gather_or_cut(runs.collect()).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use roomkit_core::model::{ChannelId, Metadata, RoomId};
    use serde_json::{Value, json};

    use super::{TeamServer, WorkerStrategy};
    use crate::kit::{RoomKit, RoomKitOptions};
    use crate::orchestration::agents::tests::held_agent;
    use crate::orchestration::strategies::supervisor::Team;
    use crate::turn::{OwnerCall, OwnerToolAnswer};

    // Python: tests/orchestration/test_supervisor_strategy.py::TestSubTaskRecursionGuard::test_delegate_workers_refuses_inside_subtask_room
    #[tokio::test]
    async fn the_team_tool_refuses_a_call_from_a_delegated_step_s_room() {
        let server = Arc::new(TeamServer {
            team: Arc::new(Team {
                supervisor: held_agent("sup"),
                workers: vec![held_agent("w1")],
                share_channels: Vec::new(),
                task_timeout: Duration::from_secs(120),
                max_revisions: 3,
            }),
            strategy: WorkerStrategy::Parallel,
            async_delivery: false,
            rooms: std::sync::Mutex::default(),
            answers: std::sync::Mutex::default(),
            running: std::sync::Mutex::default(),
        });
        let call = OwnerCall {
            room_id: RoomId::new("r1::task-abc").unwrap_or_else(|error| panic!("{error}")),
            channel_id: ChannelId::new("sup").unwrap_or_else(|error| panic!("{error}")),
            tool_call_id: "c1".to_owned(),
            arguments: Metadata::from_iter([("task".to_owned(), json!("Recurse."))]),
        };
        let kit = RoomKit::new(RoomKitOptions::default());
        let OwnerToolAnswer::Served(Value::String(answer)) = server.serve(&kit, call).await else {
            panic!("the call was not answered");
        };
        assert_eq!(
            answer,
            "{\"error\": \"delegate_workers is unavailable here: you are already running inside a \
             delegated step. Respond directly to the instruction with the requested text \\u2014 do \
             not call delegate_workers.\"}"
        );
    }
}
