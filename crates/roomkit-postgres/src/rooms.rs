//! Rooms, their private state and their delivery cursor (RFC §14.1), with
//! the Python implementation's SQL.

use deadpool_postgres::{GenericClient, Object};
use roomkit::StoreError;
use roomkit::model::{Metadata, Room, RoomId, RoomStatus, Timestamp};
use serde_json::Value;

use crate::error::Failure;
use crate::rows::{self, ROOM_COLUMNS};
use crate::values::{Instant, execute, integer, name, query_opt};

/// Python's `_SET_PRIVATE_STATE`: a room's private state written under a key,
/// and the copy its metadata may still hold removed, in one statement, the
/// room's row held against deletion once for both.
const SET_PRIVATE_STATE: &str = "
WITH held AS MATERIALIZED (SELECT id FROM rooms WHERE id = $1 FOR KEY SHARE),
written AS (
    INSERT INTO private_state (room_id, key, value)
    SELECT id, $2, $3::jsonb FROM held
    ON CONFLICT (room_id, key) DO UPDATE SET value = EXCLUDED.value
)
UPDATE rooms SET metadata = metadata - $2::text
WHERE id IN (SELECT id FROM held) AND metadata ? $2::text
";

/// Python's `_DROP_PRIVATE_STATE`.
const DROP_PRIVATE_STATE: &str = "
WITH dropped AS (DELETE FROM private_state WHERE room_id = $1 AND key = $2)
UPDATE rooms SET metadata = metadata - $2::text WHERE id = $1 AND metadata ? $2::text
";

/// Stores a new room, never replaced: refused
/// [`StoreError::RoomAlreadyExists`] for an id the store holds (RFC §14.3).
pub(crate) async fn create<C: GenericClient>(client: &C, room: &Room) -> Result<(), Failure> {
    let latest_index = room.latest_index.map(integer).transpose()?;
    let created = execute(
        client,
        "INSERT INTO rooms \
         (id, organization_id, status, event_count, latest_index, \
          metadata, timers, agent_response_policy, \
          created_at, updated_at, paused_at, closed_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT (id) DO NOTHING",
        &[
            &room.id.as_str(),
            &room.organization_id,
            &name(&room.status)?,
            &integer(room.event_count)?,
            &latest_index,
            &Value::Object(room.metadata.clone()),
            &serde_json::to_value(&room.timers)?,
            &name(&room.agent_response_policy)?,
            &Instant(room.created_at),
            &Instant(room.updated_at),
            &room.paused_at.map(Instant),
            &room.closed_at.map(Instant),
        ],
    )
    .await?;
    if created == 0 {
        return Err(StoreError::RoomAlreadyExists(room.id.clone()).into());
    }
    Ok(())
}

/// The room `room_id`, if the store holds it.
pub(crate) async fn get<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<Option<Room>, Failure> {
    let row = query_opt(
        client,
        &format!("SELECT {ROOM_COLUMNS} FROM rooms WHERE id = $1"),
        &[&room_id.as_str()],
    )
    .await?;
    row.as_ref().map(rows::room).transpose()
}

/// Writes `room` over its row, as Python's `_write_room_row`: the delivery
/// cursor and the counters the store manages are no part of the write, so
/// that a stale copy never rewinds them, and the room returned carries the
/// stored values. A member of the timers the model does not read stays.
pub(crate) async fn write<C: GenericClient>(client: &C, mut room: Room) -> Result<Room, Failure> {
    let stored = query_opt(
        client,
        "UPDATE rooms SET organization_id=$2, status=$3, metadata=$4, timers=timers || $5,\
         agent_response_policy=$6, updated_at=$7, closed_at=$8, paused_at=$9 \
         WHERE id=$1 RETURNING delivered_index, event_count, latest_index",
        &[
            &room.id.as_str(),
            &room.organization_id,
            &name(&room.status)?,
            &Value::Object(room.metadata.clone()),
            &serde_json::to_value(&room.timers)?,
            &name(&room.agent_response_policy)?,
            &Instant(room.updated_at),
            &room.closed_at.map(Instant),
            &room.paused_at.map(Instant),
        ],
    )
    .await?;
    let Some(stored) = stored else {
        return Err(StoreError::RoomNotFound(room.id).into());
    };
    room.delivered_index = i64::from(stored.try_get::<_, i32>("delivered_index")?);
    room.event_count = counter(stored.try_get("event_count")?)?;
    room.latest_index = stored
        .try_get::<_, Option<i32>>("latest_index")?
        .map(counter)
        .transpose()?;
    Ok(room)
}

/// A counter's column as the model holds it.
pub(crate) fn counter(value: i32) -> Result<u64, Failure> {
    u64::try_from(value).map_err(|_| Failure::OutOfRange("a counter"))
}

/// Atomic compare-and-set (RFC §5.1): the room's row held `FOR UPDATE` from
/// the comparison to the write, so no commit of any process lands in between;
/// `None` with nothing written when it no longer holds what was read.
pub(crate) async fn update_if(
    client: &mut Object,
    room: Room,
    status: RoomStatus,
    paused_at: Option<Timestamp>,
    last_activity_at: Option<Timestamp>,
) -> Result<Option<Room>, Failure> {
    let transaction = client.transaction().await?;
    let row = query_opt(
        &transaction,
        &format!("SELECT {ROOM_COLUMNS} FROM rooms WHERE id = $1 FOR UPDATE"),
        &[&room.id.as_str()],
    )
    .await?;
    let Some(stored) = row.as_ref().map(rows::room).transpose()? else {
        return Ok(None);
    };
    let holds = stored.status == status
        && stored.paused_at == paused_at
        && stored.timers.last_activity_at == last_activity_at;
    if !holds {
        return Ok(None);
    }
    let written = write(&transaction, room).await?;
    transaction.commit().await?;
    Ok(Some(written))
}

/// Whether the store holds the room, its row unread.
pub(crate) async fn exists<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<bool, Failure> {
    let found = query_opt(
        client,
        "SELECT 1 FROM rooms WHERE id = $1",
        &[&room_id.as_str()],
    )
    .await?;
    Ok(found.is_some())
}

/// The room's delivery cursor, `-1` for a room the store does not hold.
pub(crate) async fn delivered_index<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<i64, Failure> {
    let row = query_opt(
        client,
        "SELECT delivered_index FROM rooms WHERE id = $1",
        &[&room_id.as_str()],
    )
    .await?;
    Ok(row
        .map(|row| row.try_get::<_, i32>(0))
        .transpose()?
        .map_or(-1, i64::from))
}

/// Atomic compare-and-set on the delivery cursor (RFC §10.1 step 14): to
/// `index`, from `index - 1` or, `force`d, from any lower value; whether it
/// moved.
pub(crate) async fn advance<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    index: u64,
    force: bool,
) -> Result<bool, Failure> {
    let Ok(index) = i32::try_from(index) else {
        return Ok(false);
    };
    let sql = if force {
        "UPDATE rooms SET delivered_index=$2 WHERE id=$1 AND delivered_index < $2"
    } else {
        "UPDATE rooms SET delivered_index=$2 WHERE id=$1 AND delivered_index = $2 - 1"
    };
    Ok(execute(client, sql, &[&room_id.as_str(), &index]).await? == 1)
}

/// The value the room keeps for itself under `key`; one that is not a map
/// reads as none.
pub(crate) async fn private_state<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    key: &str,
) -> Result<Option<Metadata>, Failure> {
    let row = query_opt(
        client,
        "SELECT value FROM private_state WHERE room_id = $1 AND key = $2",
        &[&room_id.as_str(), &key],
    )
    .await?;
    Ok(
        match row.map(|row| row.try_get::<_, Value>(0)).transpose()? {
            Some(Value::Object(value)) => Some(value),
            Some(
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_),
            )
            | None => None,
        },
    )
}

/// One statement: the value written beside the room, or the key removed, and
/// the copy the room's metadata may still hold removed; nothing for a room
/// the store does not hold.
pub(crate) async fn set_private_state<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    key: &str,
    value: Option<&Metadata>,
) -> Result<(), Failure> {
    match value {
        Some(value) => {
            let value = Value::Object(value.clone());
            execute(
                client,
                SET_PRIVATE_STATE,
                &[&room_id.as_str(), &key, &value],
            )
            .await?;
        }
        None => {
            execute(client, DROP_PRIVATE_STATE, &[&room_id.as_str(), &key]).await?;
        }
    }
    Ok(())
}

/// Atomic override: one partial update, no read-modify-write, so concurrent
/// patches merge and the counters are never touched; `None` for a room the
/// store does not hold.
pub(crate) async fn patch_metadata<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    patch: Metadata,
    unset: &[String],
    now: Timestamp,
) -> Result<Option<Room>, Failure> {
    let row = query_opt(
        client,
        &format!(
            "UPDATE rooms SET metadata = (metadata - $2::text[]) || $3::jsonb, updated_at = $4 \
             WHERE id = $1 RETURNING {ROOM_COLUMNS}"
        ),
        &[
            &room_id.as_str(),
            &unset,
            &Value::Object(patch),
            &Instant(now),
        ],
    )
    .await?;
    row.as_ref().map(rows::room).transpose()
}

/// Deletes the room, its rows gone with it (`ON DELETE CASCADE`); whether the
/// store held it.
pub(crate) async fn delete<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<bool, Failure> {
    Ok(execute(
        client,
        "DELETE FROM rooms WHERE id = $1",
        &[&room_id.as_str()],
    )
    .await?
        == 1)
}
