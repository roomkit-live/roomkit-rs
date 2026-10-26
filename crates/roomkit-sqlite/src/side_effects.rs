//! A room's tasks and observations (RFC §5.10, §5.11, §10.1 step 15), in
//! the `tasks` and `observations` tables, in the order they were stored.

use roomkit::model::{Observation, RoomId, Task};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Failure;
use crate::records::{self, Kind};

/// The JSON `sql` reads for the record `id`, if the store holds it.
fn stored(connection: &Connection, sql: &str, id: &str) -> Result<Option<String>, Failure> {
    Ok(connection
        .query_row(sql, [id], |row| row.get(0))
        .optional()?)
}

/// Stores `task`, in place of a task of the same id.
pub(crate) fn store_task(connection: &Connection, task: &Task) -> Result<(), Failure> {
    let kept = stored(
        connection,
        "SELECT data FROM tasks WHERE id = ?",
        task.id.as_str(),
    )?;
    connection.execute(
        "INSERT INTO tasks(id, room_id, status, data) VALUES(?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
             room_id = excluded.room_id, status = excluded.status, data = excluded.data",
        params![
            task.id.as_str(),
            task.room_id.as_str(),
            records::name(&task.status)?,
            records::written(task, Kind::Task, kept.as_deref())?,
        ],
    )?;
    Ok(())
}

/// Stores `observation`, in place of an observation of the same id.
pub(crate) fn store_observation(
    connection: &Connection,
    observation: &Observation,
) -> Result<(), Failure> {
    let kept = stored(
        connection,
        "SELECT data FROM observations WHERE id = ?",
        observation.id.as_str(),
    )?;
    connection.execute(
        "INSERT INTO observations(id, room_id, data) VALUES(?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET room_id = excluded.room_id, data = excluded.data",
        params![
            observation.id.as_str(),
            observation.room_id.as_str(),
            records::written(observation, Kind::Observation, kept.as_deref())?,
        ],
    )?;
    Ok(())
}

/// The room's records of the table `sql` reads.
fn every<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    room_id: &RoomId,
) -> Result<Vec<T>, Failure> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map([room_id.as_str()], |row| row.get::<_, String>(0))?;
    rows.map(|data| records::read(&data?)).collect()
}

/// The room's tasks.
pub(crate) fn tasks(connection: &Connection, room_id: &RoomId) -> Result<Vec<Task>, Failure> {
    every(
        connection,
        "SELECT data FROM tasks WHERE room_id = ? ORDER BY rowid",
        room_id,
    )
}

/// The room's observations.
pub(crate) fn observations(
    connection: &Connection,
    room_id: &RoomId,
) -> Result<Vec<Observation>, Failure> {
    every(
        connection,
        "SELECT data FROM observations WHERE room_id = ? ORDER BY rowid",
        room_id,
    )
}
