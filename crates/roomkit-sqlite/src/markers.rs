//! A room's read markers (RFC §14.1 `mark_read`, `mark_all_read`,
//! `get_unread_count`, `list_read_markers`), in the `read_markers` table:
//! each channel's high-water mark on the events' index, written and read
//! with the Python implementation's SQL.

use std::collections::BTreeMap;

use roomkit::model::{ChannelId, EventId, RoomId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Failure;

/// Moves `channel_id`'s marker to the index of the room's event
/// `event_id`, when that is higher; an event of another room, or none,
/// marks nothing.
pub(crate) fn mark(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
    event_id: &EventId,
) -> Result<(), Failure> {
    connection.execute(
        "INSERT INTO read_markers(room_id, channel_id, event_index)
         SELECT ?1, ?2, idx FROM events WHERE id = ?3 AND room_id = ?1
         ON CONFLICT(room_id, channel_id) DO UPDATE
         SET event_index = MAX(read_markers.event_index, excluded.event_index)",
        params![room_id.as_str(), channel_id.as_str(), event_id.as_str()],
    )?;
    Ok(())
}

/// Moves `channel_id`'s marker to the room's highest index, when that is
/// higher; an empty room makes no group, so no marker.
pub(crate) fn mark_all(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<(), Failure> {
    connection.execute(
        "INSERT INTO read_markers(room_id, channel_id, event_index)
         SELECT ?1, ?2, MAX(idx) FROM events WHERE room_id = ?1 GROUP BY room_id
         ON CONFLICT(room_id, channel_id) DO UPDATE
         SET event_index = MAX(read_markers.event_index, excluded.event_index)",
        params![room_id.as_str(), channel_id.as_str()],
    )?;
    Ok(())
}

/// How many of the room's rows are above `channel_id`'s marker, every row
/// without one.
pub(crate) fn unread(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<u64, Failure> {
    let marker: Option<i64> = connection
        .query_row(
            "SELECT event_index FROM read_markers WHERE room_id = ? AND channel_id = ?",
            params![room_id.as_str(), channel_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let counted: i64 = match marker {
        Some(read) => connection.query_row(
            "SELECT COUNT(*) FROM events WHERE room_id = ? AND idx > ?",
            params![room_id.as_str(), read],
            |row| row.get(0),
        )?,
        None => connection.query_row(
            "SELECT COUNT(*) FROM events WHERE room_id = ?",
            [room_id.as_str()],
            |row| row.get(0),
        )?,
    };
    u64::try_from(counted).map_err(|_| Failure::OutOfRange("an event count"))
}

/// Every channel's marker in the room.
pub(crate) fn list(
    connection: &Connection,
    room_id: &RoomId,
) -> Result<BTreeMap<ChannelId, u64>, Failure> {
    let mut statement =
        connection.prepare("SELECT channel_id, event_index FROM read_markers WHERE room_id = ?")?;
    let rows = statement.query_map([room_id.as_str()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut markers = BTreeMap::new();
    for row in rows {
        let (channel_id, index) = row?;
        let channel_id: ChannelId = serde_json::from_value(serde_json::Value::String(channel_id))?;
        let index = u64::try_from(index).map_err(|_| Failure::OutOfRange("an index"))?;
        markers.insert(channel_id, index);
    }
    Ok(markers)
}
