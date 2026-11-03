//! The supervisor's `delegate_workers` tool (RFC §19.7.3): one call runs
//! the whole team for the room of the call, supervised step by step when
//! sequential, all at once when parallel, and answers with what the
//! supervisor reviews. Python's
//! `orchestration/strategies/supervisor/_inject_strategy.py` and
//! `execution.py`'s `_run_parallel`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use roomkit_core::model::{Metadata, RoomId};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::results::{WorkerResult, format_supervisor_review, worker_roles_csv};
use super::supervised::run_supervised_sequential;
use super::{Team, WorkerStrategy};
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::orchestration::strategies::loop_review::join_all;
use crate::orchestration::worker_run::{WorkerStatus, completed_or_failed, run_worker};
use crate::store::ConversationStore;
use crate::tasks::json::dumps_object;
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::served::OwnerToolEntry;

/// The team tool's name: Python's `_STRATEGY_TOOL_NAME`.
pub(super) const TEAM_TOOL: &str = "delegate_workers";

/// How long a room's answer is served again to a repeated call: Python's
/// `_StrategyToolServer._DEDUP_WINDOW`.
const DEDUP_WINDOW: Duration = Duration::from_secs(30);

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
) -> OwnerToolEntry<S> {
    let server = Arc::new(TeamServer {
        team,
        strategy,
        rooms: Mutex::default(),
        answers: Mutex::default(),
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
    /// One lock per room, held while its team runs: another room's call
    /// does not wait on it. A lock lives while a call holds it.
    rooms: Mutex<HashMap<RoomId, Weak<tokio::sync::Mutex<()>>>>,
    /// Each room's last answer, and when it was given.
    answers: Mutex<HashMap<RoomId, (String, Instant)>>,
}

impl TeamServer {
    /// Answers one `delegate_workers` call made in the room of `call`.
    async fn serve<S: ConversationStore>(
        &self,
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
/// The first error a delegation fails with, by the workers' order, once
/// all are over.
pub(super) async fn run_parallel<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    team: &Team,
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
        let status = Some(status.clone());
        let timeout = Some(team.task_timeout);
        async move {
            let channel_id = &worker.channel_id;
            let delegation = team.delegation();
            let outcome =
                run_worker(kit, room_id, channel_id, task, timeout, status, delegation).await?;
            Ok(WorkerResult {
                worker: channel_id.clone(),
                output: outcome.output,
                completed: outcome.completed,
            })
        }
    });
    join_all(runs.collect()).await.into_iter().collect()
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
        let server = TeamServer {
            team: Arc::new(Team {
                supervisor: held_agent("sup"),
                workers: vec![held_agent("w1")],
                share_channels: Vec::new(),
                task_timeout: Duration::from_secs(120),
                max_revisions: 3,
            }),
            strategy: WorkerStrategy::Parallel,
            rooms: std::sync::Mutex::default(),
            answers: std::sync::Mutex::default(),
        };
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
