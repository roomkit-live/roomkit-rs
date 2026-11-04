//! A room's tasks and observations (RFC §14.1), with the Python
//! implementation's SQL.

use deadpool_postgres::GenericClient;
use roomkit::StoreError;
use roomkit::model::{ChannelId, Observation, RoomId, Task, TaskId, TaskStatus};
use serde_json::Value;

use crate::error::Failure;
use crate::events::room_gone;
use crate::rows::{self, OBSERVATION_COLUMNS, TASK_COLUMNS};
use crate::values::{Instant, execute, name, query, query_opt};

/// Stores a new task, refused [`StoreError::RoomNotFound`] for a room the
/// store lacks (RFC §14.3).
pub(crate) async fn add_task<C: GenericClient>(client: &C, task: &Task) -> Result<(), Failure> {
    let added = execute(
        client,
        "INSERT INTO tasks \
         (id, room_id, title, description, assigned_to, status, created_at, metadata) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &task.id.as_str(),
            &task.room_id.as_str(),
            &task.title,
            &task.description,
            &task.assigned_to,
            &name(&task.status)?,
            &Instant(task.created_at),
            &Value::Object(task.metadata.clone()),
        ],
    )
    .await;
    match added {
        Ok(_) => Ok(()),
        Err(Failure::Database(error)) => Err(room_gone(error, &task.room_id)),
        Err(other) => Err(other),
    }
}

/// The task `task_id`.
pub(crate) async fn task<C: GenericClient>(
    client: &C,
    task_id: &TaskId,
) -> Result<Option<Task>, Failure> {
    let row = query_opt(
        client,
        &format!("SELECT {TASK_COLUMNS} FROM tasks WHERE id = $1"),
        &[&task_id.as_str()],
    )
    .await?;
    row.as_ref().map(rows::task).transpose()
}

/// The room's tasks, of `status` when it names one.
pub(crate) async fn tasks<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    status: Option<TaskStatus>,
) -> Result<Vec<Task>, Failure> {
    let rows = match status {
        Some(status) => {
            query(
                client,
                &format!("SELECT {TASK_COLUMNS} FROM tasks WHERE room_id = $1 AND status = $2"),
                &[&room_id.as_str(), &name(&status)?],
            )
            .await?
        }
        None => {
            query(
                client,
                &format!("SELECT {TASK_COLUMNS} FROM tasks WHERE room_id = $1"),
                &[&room_id.as_str()],
            )
            .await?
        }
    };
    rows.iter().map(rows::task).collect()
}

/// Writes `task` over its row, refused [`StoreError::TaskNotFound`] when the
/// store holds none.
pub(crate) async fn update_task<C: GenericClient>(client: &C, task: &Task) -> Result<(), Failure> {
    let updated = execute(
        client,
        "UPDATE tasks SET title=$2, description=$3, assigned_to=$4, status=$5, metadata=$6 \
         WHERE id=$1",
        &[
            &task.id.as_str(),
            &task.title,
            &task.description,
            &task.assigned_to,
            &name(&task.status)?,
            &Value::Object(task.metadata.clone()),
        ],
    )
    .await?;
    if updated == 0 {
        return Err(StoreError::TaskNotFound(task.id.clone()).into());
    }
    Ok(())
}

/// Stores a new observation, refused [`StoreError::RoomNotFound`] for a room
/// the store lacks (RFC §14.3).
pub(crate) async fn add_observation<C: GenericClient>(
    client: &C,
    observation: &Observation,
) -> Result<(), Failure> {
    // No channel is Python's empty string (its `channel_id: str = ""`).
    let channel = observation
        .channel_id
        .as_ref()
        .map_or("", ChannelId::as_str);
    let added = execute(
        client,
        "INSERT INTO observations \
         (id, room_id, channel_id, content, category, confidence, created_at, metadata) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &observation.id.as_str(),
            &observation.room_id.as_str(),
            &channel,
            &observation.content,
            &observation.category,
            &observation.confidence.value(),
            &Instant(observation.created_at),
            &Value::Object(observation.metadata.clone()),
        ],
    )
    .await;
    match added {
        Ok(_) => Ok(()),
        Err(Failure::Database(error)) => Err(room_gone(error, &observation.room_id)),
        Err(other) => Err(other),
    }
}

/// The room's observations.
pub(crate) async fn observations<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<Vec<Observation>, Failure> {
    let rows = query(
        client,
        &format!("SELECT {OBSERVATION_COLUMNS} FROM observations WHERE room_id = $1"),
        &[&room_id.as_str()],
    )
    .await?;
    rows.iter().map(rows::observation).collect()
}
