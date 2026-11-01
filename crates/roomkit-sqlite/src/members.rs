//! A room's bindings and participants (RFC §5.5, §5.7, §14.1), in the
//! `bindings` and `participants` tables, in the order the room took them.

use roomkit::StoreError;
use roomkit::model::{ChannelBinding, ChannelId, Participant, ParticipantId, RoomId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Failure;
use crate::records::{self, Kind};
use crate::rooms;

/// The JSON of the room's binding of `channel_id`, if it has one.
fn stored_binding(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<Option<String>, Failure> {
    Ok(connection
        .query_row(
            "SELECT data FROM bindings WHERE room_id = ? AND channel_id = ?",
            [room_id.as_str(), channel_id.as_str()],
            |row| row.get(0),
        )
        .optional()?)
}

/// Stores `binding`, in place of the room's binding of its channel, in its
/// row: the room lists its bindings in the order they were attached, as
/// the Python implementation's stores do.
pub(crate) fn store_binding(
    connection: &Connection,
    binding: &ChannelBinding,
) -> Result<(), Failure> {
    rooms::check(connection, &binding.room_id)?;
    let stored = stored_binding(connection, &binding.room_id, &binding.channel_id)?;
    connection.execute(
        "INSERT INTO bindings(room_id, channel_id, participant_id, channel_type, data)
         VALUES(?, ?, ?, ?, ?)
         ON CONFLICT(room_id, channel_id) DO UPDATE SET
             participant_id = excluded.participant_id,
             channel_type = excluded.channel_type,
             data = excluded.data",
        params![
            binding.room_id.as_str(),
            binding.channel_id.as_str(),
            binding.participant_id.as_ref().map(ParticipantId::as_str),
            binding.channel_type.as_str(),
            records::written(binding, Kind::Binding, stored.as_deref())?,
        ],
    )?;
    Ok(())
}

/// Removes the room's binding of `channel_id`; whether it had one.
pub(crate) fn remove_binding(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<bool, Failure> {
    let removed = connection.execute(
        "DELETE FROM bindings WHERE room_id = ? AND channel_id = ?",
        [room_id.as_str(), channel_id.as_str()],
    )?;
    Ok(removed > 0)
}

/// The room's binding of `channel_id`, if it has one.
/// Whether the room `room_id` binds `channel_id`, its row unread.
pub(crate) fn binding_exists(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<bool, Failure> {
    let held = connection
        .query_row(
            "SELECT 1 FROM bindings WHERE room_id = ? AND channel_id = ?",
            [room_id.as_str(), channel_id.as_str()],
            |_| Ok(()),
        )
        .optional()?;
    Ok(held.is_some())
}

pub(crate) fn binding(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<Option<ChannelBinding>, Failure> {
    stored_binding(connection, room_id, channel_id)?
        .map(|data| records::read(&data))
        .transpose()
}

/// The room's bindings.
pub(crate) fn bindings(
    connection: &Connection,
    room_id: &RoomId,
) -> Result<Vec<ChannelBinding>, Failure> {
    every(
        connection,
        "SELECT data FROM bindings WHERE room_id = ? ORDER BY rowid",
        room_id,
    )
}

/// The room's participants, in the order they were created.
pub(crate) fn participants(
    connection: &Connection,
    room_id: &RoomId,
) -> Result<Vec<Participant>, Failure> {
    every(
        connection,
        "SELECT data FROM participants WHERE room_id = ? ORDER BY rowid",
        room_id,
    )
}

/// The records `sql` selects from the room's rows.
fn every<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    room_id: &RoomId,
) -> Result<Vec<T>, Failure> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map([room_id.as_str()], |row| row.get::<_, String>(0))?;
    rows.map(|data| records::read(&data?)).collect()
}

/// The JSON of the room's participant `participant_id`, if it has one.
fn stored_participant(
    connection: &Connection,
    room_id: &RoomId,
    participant_id: &ParticipantId,
) -> Result<Option<String>, Failure> {
    Ok(connection
        .query_row(
            "SELECT data FROM participants WHERE room_id = ? AND id = ?",
            [room_id.as_str(), participant_id.as_str()],
            |row| row.get(0),
        )
        .optional()?)
}

/// Stores a new participant of its room; a room the store does not hold is
/// refused [`StoreError::RoomNotFound`], an id the room has
/// [`StoreError::ParticipantExists`] (RFC §5.5, §14.3).
pub(crate) fn create_participant(
    connection: &Connection,
    participant: &Participant,
) -> Result<(), Failure> {
    rooms::check(connection, &participant.room_id)?;
    if stored_participant(connection, &participant.room_id, &participant.id)?.is_some() {
        return Err(StoreError::ParticipantExists {
            room_id: participant.room_id.clone(),
            participant_id: participant.id.clone(),
        }
        .into());
    }
    connection.execute(
        "INSERT INTO participants(room_id, id, data) VALUES(?, ?, ?)",
        params![
            participant.room_id.as_str(),
            participant.id.as_str(),
            records::written(participant, Kind::Participant, None)?,
        ],
    )?;
    Ok(())
}

/// The room's participant `participant_id`, if it has one.
pub(crate) fn participant(
    connection: &Connection,
    room_id: &RoomId,
    participant_id: &ParticipantId,
) -> Result<Option<Participant>, Failure> {
    stored_participant(connection, room_id, participant_id)?
        .map(|data| records::read(&data))
        .transpose()
}

/// Writes `participant` whole, in its place; one the room does not have is
/// refused [`StoreError::ParticipantNotFound`].
pub(crate) fn update_participant(
    connection: &Connection,
    participant: &Participant,
) -> Result<(), Failure> {
    let Some(stored) = stored_participant(connection, &participant.room_id, &participant.id)?
    else {
        return Err(StoreError::ParticipantNotFound {
            room_id: participant.room_id.clone(),
            participant_id: participant.id.clone(),
        }
        .into());
    };
    connection.execute(
        "UPDATE participants SET data = ? WHERE room_id = ? AND id = ?",
        params![
            records::written(participant, Kind::Participant, Some(&stored))?,
            participant.room_id.as_str(),
            participant.id.as_str(),
        ],
    )?;
    Ok(())
}

/// Removes the room's participant `participant_id`, if it has one.
pub(crate) fn delete_participant(
    connection: &Connection,
    room_id: &RoomId,
    participant_id: &ParticipantId,
) -> Result<(), Failure> {
    connection.execute(
        "DELETE FROM participants WHERE room_id = ? AND id = ?",
        [room_id.as_str(), participant_id.as_str()],
    )?;
    Ok(())
}
