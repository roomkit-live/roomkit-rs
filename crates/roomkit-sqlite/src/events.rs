//! A room's timeline (RFC §8.1, §10.3, §13.4, §14.1, §14.3), in the
//! `events` table: its commits, reads, updates and hard deletes, each row
//! written with what Python derives from it (`event_rows`).

use std::collections::BTreeSet;

use roomkit::StoreError;
use roomkit::model::{ChannelId, EventId, RoomEvent, RoomId, Timestamp};
use roomkit_core::rules::edits::{self, Mutation};
use roomkit_core::rules::{idempotency, lifecycle};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Failure;
use crate::event_rows::{mark_edits_deleted, remove_row, write_row};
use crate::records;
use crate::rooms;

/// An event as stored, with its row's JSON.
struct Stored {
    event: RoomEvent,
    data: String,
}

/// The event `event_id`, of `room_id`'s timeline when it names one.
fn stored(
    connection: &Connection,
    event_id: &EventId,
    room_id: Option<&RoomId>,
) -> Result<Option<Stored>, Failure> {
    let data: Option<String> = connection
        .query_row(
            "SELECT data FROM events WHERE id = ?1 AND (?2 IS NULL OR room_id = ?2)",
            params![event_id.as_str(), room_id.map(RoomId::as_str)],
            |row| row.get(0),
        )
        .optional()?;
    data.map(|data| {
        Ok(Stored {
            event: records::read(&data)?,
            data,
        })
    })
    .transpose()
}

/// The event `event_id`, whichever room holds it.
pub(crate) fn get(
    connection: &Connection,
    event_id: &EventId,
) -> Result<Option<RoomEvent>, Failure> {
    Ok(stored(connection, event_id, None)?.map(|stored| stored.event))
}

/// Commits `event` to `room_id` as one transaction, all or nothing (RFC
/// §10.1 step 12, §14.3), as Python's store commits it: the target an edit
/// or a delete changes written whole, its delivery results kept, a
/// delete's target's edits marked deleted, then the event at the room's
/// next index, which the store reserves, and the room's counters bumped,
/// its activity moved and a PAUSED room resumed for a commit that is its
/// activity, `at` (RFC §5.1); the event as stored.
pub(crate) fn commit(
    connection: &Connection,
    room_id: &RoomId,
    mut event: RoomEvent,
    target: Option<RoomEvent>,
    activity: Option<Timestamp>,
) -> Result<RoomEvent, Failure> {
    // A blank key is no key: the event is stored with none (RFC §13.4).
    event.idempotency_key = idempotency::key(event.idempotency_key);
    let rooms::Stored { mut room, data } = rooms::held(connection, room_id)?;
    check_key(connection, &event)?;
    if let Some(mut target) = target {
        let Some(held) = stored(connection, &target.id, Some(room_id))? else {
            return Err(StoreError::EventNotFound(target.id).into());
        };
        target.delivery_results = held.event.delivery_results;
        write_row(connection, &target, Some(&held.data))?;
        if deletes(&event, &target.id) {
            mark_edits_deleted(connection, room_id, &target.id)?;
        }
    }
    event.index = reserve_index(connection, room_id)?;
    room.event_count = room.event_count.saturating_add(1);
    room.latest_index = Some(event.index);
    if let Some(at) = activity {
        lifecycle::commit_activity(&mut room, at);
    }
    rooms::write(connection, &room, &data)?;
    write_row(connection, &event, None)?;
    Ok(event)
}

/// Reserves the room's next index, in the caller's transaction, as
/// Python's `_reserve_event_index`: a hard-deleted index is never given
/// again (gap G-064), nor one another writer of the file reserved.
fn reserve_index(connection: &Connection, room_id: &RoomId) -> Result<u64, Failure> {
    connection.execute(
        "INSERT OR IGNORE INTO event_sequences(room_id, next_index) VALUES(?, 0)",
        [room_id.as_str()],
    )?;
    let next: i64 = connection.query_row(
        "SELECT next_index FROM event_sequences WHERE room_id = ?",
        [room_id.as_str()],
        |row| row.get(0),
    )?;
    connection.execute(
        "UPDATE event_sequences SET next_index = ? WHERE room_id = ?",
        params![next.saturating_add(1), room_id.as_str()],
    )?;
    u64::try_from(next).map_err(|_| Failure::OutOfRange("an index"))
}

/// Whether `event` is a delete of `target`, whose commit marks the target's
/// edits deleted too (RFC §10.3).
fn deletes(event: &RoomEvent, target: &EventId) -> bool {
    matches!(
        edits::mutation(event),
        Ok(Some(Mutation::Delete(delete))) if &delete.target_event_id == target
    )
}

/// Refuses an event whose source channel holds its idempotency key in the
/// room (RFC §13.4, §14.3).
fn check_key(connection: &Connection, event: &RoomEvent) -> Result<(), Failure> {
    let Some(key) = &event.idempotency_key else {
        return Ok(());
    };
    let held = connection
        .query_row(
            "SELECT 1 FROM events WHERE room_id = ? AND source_channel_id = ? AND idempotency_key = ?",
            params![event.room_id.as_str(), event.source.channel_id.as_str(), key],
            |_| Ok(()),
        )
        .optional()?;
    match held {
        Some(()) => Err(StoreError::IdempotencyKeyTaken {
            room_id: event.room_id.clone(),
            channel_id: event.source.channel_id.clone(),
            key: key.clone(),
        }
        .into()),
        None => Ok(()),
    }
}

/// Writes `event` whole over its stored row and returns it (RFC §14.1
/// `update_event`); its `delivery_results` stay as stored (RFC §14.3).
pub(crate) fn update(connection: &Connection, mut event: RoomEvent) -> Result<RoomEvent, Failure> {
    let Some(held) = stored(connection, &event.id, Some(&event.room_id))? else {
        return Err(StoreError::EventNotFound(event.id).into());
    };
    event.delivery_results = held.event.delivery_results;
    write_row(connection, &event, Some(&held.data))?;
    Ok(event)
}

/// The room's events `sql` selects, in its order.
fn select(
    connection: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<RoomEvent>, Failure> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map(params, |row| row.get::<_, String>(0))?;
    rows.map(|data| records::read(&data?)).collect()
}

/// Removes `event_id` with, when `cascade_replies`, its replies one level
/// deep, and the edits and deletes of what it removes; returns them in
/// §10.3's order: the target, its replies by index, the changes by index
/// (RFC §14.1 `delete_event`, gap G-063). The counters stay (gap G-064).
pub(crate) fn delete(
    connection: &Connection,
    room_id: &RoomId,
    event_id: &EventId,
    cascade_replies: bool,
) -> Result<Vec<RoomEvent>, Failure> {
    let Some(target) = stored(connection, event_id, Some(room_id))? else {
        return Ok(Vec::new());
    };
    let replies = if cascade_replies {
        select(
            connection,
            "SELECT data FROM events WHERE room_id = ? AND parent_event_id = ? AND id != ?
             ORDER BY idx",
            params![room_id.as_str(), event_id.as_str(), event_id.as_str()],
        )?
    } else {
        Vec::new()
    };
    let gone: BTreeSet<&EventId> = replies
        .iter()
        .map(|reply| &reply.id)
        .chain([event_id])
        .collect();
    let changes: Vec<RoomEvent> = select(
        connection,
        "SELECT data FROM events WHERE room_id = ? AND type IN ('edit', 'delete') ORDER BY idx",
        [room_id.as_str()],
    )?
    .into_iter()
    .filter(|change| !gone.contains(&change.id) && changes_one_of(change, &gone))
    .collect();
    let removed: Vec<RoomEvent> = [target.event]
        .into_iter()
        .chain(replies)
        .chain(changes)
        .collect();
    for event in &removed {
        remove_row(connection, event)?;
    }
    Ok(removed)
}

/// Whether `event` is an edit or a delete of one of the events `gone`.
fn changes_one_of(event: &RoomEvent, gone: &BTreeSet<&EventId>) -> bool {
    edits::mutation(event)
        .ok()
        .flatten()
        .is_some_and(|mutation| gone.contains(mutation.target_event_id()))
}

/// The event the room committed with `key`, from `channel_id`, or from any
/// channel for none, the lowest index first (RFC §13.4, §22.1); a blank key
/// names none.
pub(crate) fn by_key(
    connection: &Connection,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
    key: &str,
) -> Result<Option<RoomEvent>, Failure> {
    if !idempotency::is_key(key) {
        return Ok(None);
    }
    let found = select(
        connection,
        "SELECT data FROM events WHERE room_id = ?1 AND idempotency_key = ?2
             AND (?3 IS NULL OR source_channel_id = ?3)
         ORDER BY idx LIMIT 1",
        params![room_id.as_str(), key, channel_id.map(ChannelId::as_str)],
    )?;
    Ok(found.into_iter().next())
}
