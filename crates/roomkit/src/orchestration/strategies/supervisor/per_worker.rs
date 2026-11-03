//! The supervisor's per-worker tools (RFC §19.7.3): one `delegate_to_<id>`
//! per worker, the model deciding when to delegate; a call delegates its
//! task to that worker from the room of the call and answers with the
//! worker's status and result. Python's
//! `orchestration/strategies/supervisor/_inject_per_worker.py`, its tools
//! waiting for their worker.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, Metadata};
use serde_json::{Value, json};

use super::Team;
use crate::kit::RoomKit;
use crate::orchestration::agents::HeldAgent;
use crate::orchestration::worker_run::{WorkerStatus, completed_or_failed, run_worker};
use crate::store::ConversationStore;
use crate::tasks::json::dumps_object;
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::served::OwnerToolEntry;

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

/// One `delegate_to_<id>` entry per worker of `team`, each a strategy's
/// tool, its call waiting by design (RFC §21.6).
pub(super) fn entries<S: ConversationStore>(team: &Arc<Team>) -> Vec<OwnerToolEntry<S>> {
    team.workers
        .iter()
        .map(|worker| {
            let (team, worker_id) = (Arc::clone(team), worker.channel_id.clone());
            OwnerToolEntry {
                definition: definition(worker),
                serve: Arc::new(move |kit, call| {
                    let (team, worker_id) = (Arc::clone(&team), worker_id.clone());
                    Box::pin(async move { delegate_and_wait(&kit, &team, &worker_id, call).await })
                }),
                waits: true,
                owner: None,
            }
        })
        .collect()
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
        Err(error) => {
            // The channel reads it as any failed call: the class for the
            // model, the message for the observers (RFC §9.3).
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
    }
}
