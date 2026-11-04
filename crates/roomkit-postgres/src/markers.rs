//! How far each channel read a room (RFC §14.1), with the Python
//! implementation's SQL: a marker is a high-water mark, it never moves back.

use std::collections::BTreeMap;

use deadpool_postgres::GenericClient;
use roomkit::model::{ChannelId, EventId, RoomId};
use serde_json::Value;

use crate::error::Failure;
use crate::rooms::counter;
use crate::values::{execute, query, query_opt};

/// Marks the channel read up to the event; an event of another room, or none,
/// marks nothing.
pub(crate) async fn mark<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: &ChannelId,
    event_id: &EventId,
) -> Result<(), Failure> {
    execute(
        client,
        "INSERT INTO read_markers (room_id, channel_id, event_index) \
         SELECT $1, $2, index FROM events WHERE id = $3 AND room_id = $1 \
         ON CONFLICT (room_id, channel_id) DO UPDATE \
         SET event_index = GREATEST(read_markers.event_index, EXCLUDED.event_index)",
        &[&room_id.as_str(), &channel_id.as_str(), &event_id.as_str()],
    )
    .await?;
    Ok(())
}

/// Marks the channel read up to the room's latest event. An empty room gets
/// no marker: one at 0 would read its first event, index 0, before it
/// arrives.
pub(crate) async fn mark_all<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<(), Failure> {
    execute(
        client,
        "INSERT INTO read_markers (room_id, channel_id, event_index) \
         SELECT $1, $2, MAX(index) FROM events WHERE room_id = $1 \
         HAVING MAX(index) IS NOT NULL \
         ON CONFLICT (room_id, channel_id) DO UPDATE \
         SET event_index = GREATEST(read_markers.event_index, EXCLUDED.event_index)",
        &[&room_id.as_str(), &channel_id.as_str()],
    )
    .await?;
    Ok(())
}

/// How many of the room's events the channel has not read.
pub(crate) async fn unread<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<u64, Failure> {
    let marker = query_opt(
        client,
        "SELECT event_index FROM read_markers WHERE room_id = $1 AND channel_id = $2",
        &[&room_id.as_str(), &channel_id.as_str()],
    )
    .await?;
    let row = match marker {
        None => {
            query_opt(
                client,
                "SELECT count(*) FROM events WHERE room_id = $1",
                &[&room_id.as_str()],
            )
            .await?
        }
        Some(marker) => {
            let index: i32 = marker.try_get("event_index")?;
            query_opt(
                client,
                "SELECT count(*) FROM events WHERE room_id = $1 AND index > $2",
                &[&room_id.as_str(), &index],
            )
            .await?
        }
    };
    let counted: i64 = row.map_or(Ok(0), |row| row.try_get(0))?;
    u64::try_from(counted).map_err(|_| Failure::OutOfRange("an unread count"))
}

/// Each channel's marker in the room.
pub(crate) async fn list<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<BTreeMap<ChannelId, u64>, Failure> {
    let rows = query(
        client,
        "SELECT channel_id, event_index FROM read_markers WHERE room_id = $1",
        &[&room_id.as_str()],
    )
    .await?;
    let mut markers = BTreeMap::new();
    for row in rows {
        let channel: ChannelId = serde_json::from_value(Value::String(row.try_get("channel_id")?))?;
        markers.insert(channel, counter(row.try_get("event_index")?)?);
    }
    Ok(markers)
}
