//! Rooms, their private state and their delivery cursor (RFC §14.1), in
//! the `rooms` and `private_state` tables.

use roomkit::StoreError;
use roomkit::model::{Metadata, Room, RoomId, RoomUpdates};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::error::Failure;
use crate::records::{self, Kind};

/// A room as stored, with its row's JSON, which a rewrite keeps the members
/// of it does not model.
pub(crate) struct Stored {
    pub(crate) room: Room,
    pub(crate) data: String,
}

/// The room a row holds: its JSON, with the delivery cursor of its
/// column, which the cursor's advance writes alone (RFC §13.5), and no
/// latest index while it gave out none (gap G-012), as Python reads it.
pub(crate) fn room_of(data: &str, delivered_index: i64) -> Result<Room, Failure> {
    let mut room: Room = records::read(data)?;
    room.delivered_index = delivered_index;
    if room.event_count == 0 {
        room.latest_index = None;
    }
    Ok(room)
}

/// The room `room_id`, if the store holds it.
pub(crate) fn get(connection: &Connection, room_id: &RoomId) -> Result<Option<Stored>, Failure> {
    let row = connection
        .query_row(
            "SELECT data, delivered_index FROM rooms WHERE id = ?",
            [room_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    row.map(|(data, delivered_index)| {
        Ok(Stored {
            room: room_of(&data, delivered_index)?,
            data,
        })
    })
    .transpose()
}

/// The room `room_id`, refused [`StoreError::RoomNotFound`] when the store
/// does not hold it.
pub(crate) fn held(connection: &Connection, room_id: &RoomId) -> Result<Stored, Failure> {
    get(connection, room_id)?.ok_or_else(|| StoreError::RoomNotFound(room_id.clone()).into())
}

/// Refuses [`StoreError::RoomNotFound`] for a room the store does not hold.
pub(crate) fn check(connection: &Connection, room_id: &RoomId) -> Result<(), Failure> {
    let held = connection
        .query_row(
            "SELECT 1 FROM rooms WHERE id = ?",
            [room_id.as_str()],
            |_| Ok(()),
        )
        .optional()?;
    held.ok_or_else(|| StoreError::RoomNotFound(room_id.clone()).into())
}

/// Stores a new room, its index sequence at 0; refused
/// [`StoreError::RoomAlreadyExists`] for an id the store holds (RFC §14.3).
pub(crate) fn create(connection: &Connection, room: &Room) -> Result<(), Failure> {
    let created = connection.execute(
        "INSERT OR IGNORE INTO rooms(id, created_ts, organization_id, status, delivered_index, data)
         VALUES(?, ?, ?, ?, ?, ?)",
        params![
            room.id.as_str(),
            records::seconds(room.created_at),
            room.organization_id,
            records::name(&room.status)?,
            room.delivered_index,
            records::written(room, Kind::Room, None)?,
        ],
    )?;
    if created == 0 {
        return Err(StoreError::RoomAlreadyExists(room.id.clone()).into());
    }
    connection.execute(
        "INSERT OR IGNORE INTO event_sequences(room_id, next_index) VALUES(?, 0)",
        [room.id.as_str()],
    )?;
    Ok(())
}

/// Writes `room` over its row, keeping the delivery cursor's column, which
/// only the cursor's advance moves.
pub(crate) fn write(connection: &Connection, room: &Room, stored: &str) -> Result<(), Failure> {
    connection.execute(
        "UPDATE rooms SET created_ts = ?, organization_id = ?, status = ?, data = ? WHERE id = ?",
        params![
            records::seconds(room.created_at),
            room.organization_id,
            records::name(&room.status)?,
            records::written(room, Kind::Room, Some(stored))?,
            room.id.as_str(),
        ],
    )?;
    Ok(())
}

/// Writes `updates` on the stored room and returns it: the private state it
/// names apart, each key removing the metadata's copy (RFC §14.1).
pub(crate) fn update(
    connection: &Connection,
    room_id: &RoomId,
    updates: RoomUpdates,
) -> Result<Room, Failure> {
    let Stored { mut room, data } = held(connection, room_id)?;
    apply(connection, &mut room, &data, updates)?;
    Ok(room)
}

/// Writes `updates` on `room`, as `stored` holds it, and the room over its
/// row: the private state it names apart, each key removing the metadata's
/// copy (RFC §14.1).
pub(crate) fn apply(
    connection: &Connection,
    room: &mut Room,
    stored: &str,
    mut updates: RoomUpdates,
) -> Result<(), Failure> {
    for (key, value) in std::mem::take(&mut updates.private_state) {
        write_private(connection, &room.id, &key, value.as_ref())?;
        room.metadata.remove(&key);
    }
    updates.apply(room);
    write(connection, room, stored)
}

/// The value the room keeps for itself under `key`; one that is not a map
/// reads as none.
pub(crate) fn private_state(
    connection: &Connection,
    room_id: &RoomId,
    key: &str,
) -> Result<Option<Metadata>, Failure> {
    let value: Option<String> = connection
        .query_row(
            "SELECT value FROM private_state WHERE room_id = ? AND key = ?",
            [room_id.as_str(), key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(
        match value
            .map(|value| serde_json::from_str(&value))
            .transpose()?
        {
            Some(Value::Object(value)) => Some(value),
            Some(_) | None => None,
        },
    )
}

/// Keeps `value` under `key` for the room, or removes the key for `None`.
fn write_private(
    connection: &Connection,
    room_id: &RoomId,
    key: &str,
    value: Option<&Metadata>,
) -> Result<(), Failure> {
    match value {
        Some(value) => connection.execute(
            "INSERT INTO private_state(room_id, key, value) VALUES(?, ?, ?)
             ON CONFLICT(room_id, key) DO UPDATE SET value = excluded.value",
            params![room_id.as_str(), key, serde_json::to_string(value)?],
        )?,
        None => connection.execute(
            "DELETE FROM private_state WHERE room_id = ? AND key = ?",
            params![room_id.as_str(), key],
        )?,
    };
    Ok(())
}

/// Writes the room's private state under `key`, nothing for a room the
/// store does not hold (RFC §14.1 `set_private_state`).
pub(crate) fn set_private_state(
    connection: &Connection,
    room_id: &RoomId,
    key: &str,
    value: Option<Metadata>,
) -> Result<(), Failure> {
    let updates = RoomUpdates {
        private_state: [(key.to_owned(), value)].into(),
        ..RoomUpdates::default()
    };
    match update(connection, room_id, updates) {
        Ok(_) | Err(Failure::Refused(StoreError::RoomNotFound(_))) => Ok(()),
        Err(failure) => Err(failure),
    }
}

/// The room's delivery cursor, `-1` for a room the store does not hold.
pub(crate) fn delivered_index(connection: &Connection, room_id: &RoomId) -> Result<i64, Failure> {
    let cursor = connection
        .query_row(
            "SELECT delivered_index FROM rooms WHERE id = ?",
            [room_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(cursor.unwrap_or(-1))
}

/// Moves the delivery cursor to `index`, from `index - 1` or, `force`d, from
/// any lower value (RFC §13.5); whether it moved.
pub(crate) fn advance(
    connection: &Connection,
    room_id: &RoomId,
    index: u64,
    force: bool,
) -> Result<bool, Failure> {
    let Ok(index) = i64::try_from(index) else {
        return Ok(false);
    };
    let moved = if force {
        connection.execute(
            "UPDATE rooms SET delivered_index = ? WHERE id = ? AND delivered_index < ?",
            params![index, room_id.as_str(), index],
        )?
    } else {
        connection.execute(
            "UPDATE rooms SET delivered_index = ? WHERE id = ? AND delivered_index = ?",
            params![index, room_id.as_str(), index - 1],
        )?
    };
    Ok(moved > 0)
}
