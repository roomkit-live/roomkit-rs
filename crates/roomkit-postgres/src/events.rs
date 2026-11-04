//! A room's timeline (RFC §8.1, §10.3, §13.4, §14.1, §14.3), in the `events`
//! table, with the Python implementation's SQL: one transaction per write,
//! the room's row held `FOR UPDATE` while a commit counts it.

use deadpool_postgres::{GenericClient, Object, Transaction};
use roomkit::StoreError;
use roomkit::model::{
    ChannelId, EventContent, EventId, EventSource, Metadata, ParticipantId, RoomEvent, RoomId,
    Timestamp,
};
use roomkit_core::rules::edits::{self, Mutation};
use roomkit_core::rules::{idempotency, lifecycle};
use serde_json::{Map, Value};
use tokio_postgres::error::SqlState;

use crate::error::Failure;
use crate::rows::{self, EVENT_COLUMNS, ROOM_COLUMNS};
use crate::values::{Instant, execute, integer, name, query, query_opt};

/// The unique index of a key per room and source channel (RFC §13.4, §14.3).
const KEY_INDEX: &str = "idx_events_idempotency_channel";

/// The non-column members of an event's source, as Python's `_source_extra`
/// writes them.
fn source_extra(source: &EventSource) -> Result<Value, Failure> {
    let mut extra = Map::new();
    if !source.raw_payload.is_empty() {
        extra.insert(
            "raw_payload".to_owned(),
            Value::Object(source.raw_payload.clone()),
        );
    }
    if let Some(id) = source.external_id.as_ref().filter(|id| !id.is_empty()) {
        extra.insert("external_id".to_owned(), Value::String(id.clone()));
    }
    if let Some(id) = source
        .provider_message_id
        .as_ref()
        .filter(|id| !id.is_empty())
    {
        extra.insert("provider_message_id".to_owned(), Value::String(id.clone()));
    }
    if let Some(verification) = &source.verification {
        extra.insert(
            "verification".to_owned(),
            serde_json::to_value(verification)?,
        );
    }
    Ok(Value::Object(extra))
}

/// Inserts `event`'s row; an event under a key its channel holds in the room
/// is refused [`StoreError::IdempotencyKeyTaken`] (the caller's
/// [`a_held_id_as_such`] reads the id first), and one of a room the store
/// lacks [`StoreError::RoomNotFound`].
async fn insert(transaction: &Transaction<'_>, event: &RoomEvent) -> Result<(), Failure> {
    let visibility = name(&event.visibility)?;
    let response_visibility = event.response_visibility.as_ref().map(name).transpose()?;
    let addressed_to: Option<Vec<&str>> = event
        .addressed_to
        .as_ref()
        .map(|channels| channels.iter().map(ChannelId::as_str).collect());
    let participant = event
        .source
        .participant_id
        .as_ref()
        .map(ParticipantId::as_str);
    let parent = event.parent_event_id.as_ref().map(EventId::as_str);
    let responds_to = event.responds_to.as_ref().map(EventId::as_str);
    let inserted = execute(
        transaction,
        "INSERT INTO events \
         (id, room_id, type, content, source_channel_id, source_channel_type, \
          source_direction, source_participant_id, source_provider, source_extra, \
          status, visibility, addressed_to, response_visibility, index, chain_depth, \
          correlation_id, parent_event_id, idempotency_key, blocked_by, \
          metadata, channel_data, created_at, responds_to, delivery_results) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,\
                 $16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
        &[
            &event.id.as_str(),
            &event.room_id.as_str(),
            &name(&event.r#type)?,
            &serde_json::to_value(&event.content)?,
            &event.source.channel_id.as_str(),
            &event.source.channel_type.as_str(),
            &name(&event.source.direction)?,
            &participant,
            &event.source.provider,
            &source_extra(&event.source)?,
            &name(&event.status)?,
            &visibility,
            &addressed_to,
            &response_visibility,
            &integer(event.index)?,
            &i32::try_from(event.chain_depth).map_err(|_| Failure::OutOfRange("a chain depth"))?,
            &event.correlation_id,
            &parent,
            &event.idempotency_key,
            &event.blocked_by,
            &Value::Object(event.metadata.clone()),
            &Value::Object(event.channel_data.clone()),
            &Instant(event.created_at),
            &responds_to,
            &Value::Object(event.delivery_results.clone()),
        ],
    )
    .await;
    match inserted {
        Ok(_) => Ok(()),
        Err(Failure::Database(error)) => Err(refused_row(error, event)),
        Err(other) => Err(other),
    }
}

/// What a refused insert of `event` means: a key its channel holds, read by
/// the unique violation on the key's index; a room the store lacks, the
/// foreign key's violation; else the driver's own error.
fn refused_row(error: tokio_postgres::Error, event: &RoomEvent) -> Failure {
    let (code, constraint) = error
        .as_db_error()
        .map_or((None, None), |db| (Some(db.code()), db.constraint()));
    if code == Some(&SqlState::UNIQUE_VIOLATION) && constraint == Some(KEY_INDEX) {
        return Failure::KeyRefused {
            driver: error,
            taken: StoreError::IdempotencyKeyTaken {
                room_id: event.room_id.clone(),
                channel_id: event.source.channel_id.clone(),
                key: event.idempotency_key.clone().unwrap_or_default(),
            },
        };
    }
    room_gone(error, &event.room_id)
}

/// A foreign key's violation as the room it names missing (RFC §14.3): every
/// table's foreign key this store writes through is its room's. Any other
/// error stays the driver's.
pub(crate) fn room_gone(error: tokio_postgres::Error, room_id: &RoomId) -> Failure {
    if error.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
        return StoreError::RoomNotFound(room_id.clone()).into();
    }
    error.into()
}

/// `outcome` of an event's insert, its transaction over: a key refused for an
/// event whose id the store holds is a second write of that event, which
/// stays the driver's refusal of the id (RFC §14.3), whichever unique index
/// Postgres reported first (Python's `_a_held_id_as_such`).
async fn a_held_id_as_such(
    client: &Object,
    event_id: &EventId,
    outcome: Result<RoomEvent, Failure>,
) -> Result<RoomEvent, Failure> {
    let Err(Failure::KeyRefused { driver, taken }) = outcome else {
        return outcome;
    };
    let held = query_opt(
        client,
        "SELECT 1 FROM events WHERE id = $1",
        &[&event_id.as_str()],
    )
    .await?;
    Err(match held {
        Some(_) => Failure::Database(driver),
        None => Failure::Refused(taken),
    })
}

/// Atomically reserves and returns a room's next monotonic index, never given
/// again when its event is deleted.
async fn reserve_index(transaction: &Transaction<'_>, room_id: &RoomId) -> Result<u64, Failure> {
    let row = query_opt(
        transaction,
        "INSERT INTO event_sequences(room_id, next_index) VALUES($1, 1) \
         ON CONFLICT(room_id) DO UPDATE SET next_index = event_sequences.next_index + 1 \
         RETURNING next_index - 1 AS event_index",
        &[&room_id.as_str()],
    )
    .await;
    let row = match row {
        Ok(row) => row,
        Err(Failure::Database(error)) => return Err(room_gone(error, room_id)),
        Err(other) => return Err(other),
    };
    let row = row.ok_or(Failure::OutOfRange("a reserved index"))?;
    crate::rooms::counter(row.try_get("event_index")?)
}

/// Keeps automatic assignment ahead of an index the caller supplied.
async fn observe_index(transaction: &Transaction<'_>, event: &RoomEvent) -> Result<(), Failure> {
    let next = integer(event.index)?
        .checked_add(1)
        .ok_or(Failure::OutOfRange("an index"))?;
    let observed = execute(
        transaction,
        "INSERT INTO event_sequences(room_id, next_index) VALUES($1, $2) \
         ON CONFLICT(room_id) DO UPDATE \
         SET next_index = GREATEST(event_sequences.next_index, EXCLUDED.next_index)",
        &[&event.room_id.as_str(), &next],
    )
    .await;
    match observed {
        Ok(_) => Ok(()),
        Err(Failure::Database(error)) => Err(room_gone(error, &event.room_id)),
        Err(other) => Err(other),
    }
}

/// Takes the room's row before its sequence, the order of a commit, as Python's
/// `_hold_room_first`. The event's foreign key would otherwise take it after, and
/// a commit holding the room would wait on the sequence this write holds, while
/// this write waits on the room: the server kills one of them. A missing room stays
/// the foreign key's refusal, a few statements later.
async fn hold_room_first(transaction: &Transaction<'_>, room_id: &RoomId) -> Result<(), Failure> {
    execute(
        transaction,
        "SELECT 1 FROM rooms WHERE id = $1 FOR KEY SHARE",
        &[&room_id.as_str()],
    )
    .await?;
    Ok(())
}

/// Stores `event` as it is, its index the caller's, the room's counters
/// unmoved and its next index kept past it, as Python's `add_event`.
pub(crate) async fn add(client: &mut Object, mut event: RoomEvent) -> Result<RoomEvent, Failure> {
    // A blank key is no key: the event is stored with none (RFC §13.4).
    event.idempotency_key = idempotency::key(event.idempotency_key);
    let id = event.id.clone();
    let outcome = add_in_transaction(client, event).await;
    a_held_id_as_such(client, &id, outcome).await
}

async fn add_in_transaction(client: &mut Object, event: RoomEvent) -> Result<RoomEvent, Failure> {
    let transaction = client.transaction().await?;
    hold_room_first(&transaction, &event.room_id).await?;
    observe_index(&transaction, &event).await?;
    insert(&transaction, &event).await?;
    transaction.commit().await?;
    Ok(event)
}

/// Stores `event` at `room_id`'s next index, the room's counters unmoved, as
/// Python's `add_event_auto_index`.
pub(crate) async fn add_auto_indexed(
    client: &mut Object,
    room_id: &RoomId,
    mut event: RoomEvent,
) -> Result<RoomEvent, Failure> {
    event.idempotency_key = idempotency::key(event.idempotency_key);
    let id = event.id.clone();
    let outcome = auto_indexed_in_transaction(client, room_id, event).await;
    a_held_id_as_such(client, &id, outcome).await
}

async fn auto_indexed_in_transaction(
    client: &mut Object,
    room_id: &RoomId,
    mut event: RoomEvent,
) -> Result<RoomEvent, Failure> {
    let transaction = client.transaction().await?;
    hold_room_first(&transaction, room_id).await?;
    event.index = reserve_index(&transaction, room_id).await?;
    insert(&transaction, &event).await?;
    transaction.commit().await?;
    Ok(event)
}

/// Commits `event` to `room_id` in one transaction (RFC §10.1 step 12, §8.1,
/// §14.3): the room's row held `FOR UPDATE`, which serializes its counters
/// across processes; a change's target written, the whole rolled back when
/// the room no longer holds it; the index reserved, the event inserted and
/// the counters moved together. An `activity` commit moves
/// `last_activity_at` and resumes a PAUSED room (RFC §5.1).
pub(crate) async fn commit(
    client: &mut Object,
    room_id: &RoomId,
    mut event: RoomEvent,
    target: Option<RoomEvent>,
    activity: Option<Timestamp>,
) -> Result<RoomEvent, Failure> {
    event.idempotency_key = idempotency::key(event.idempotency_key);
    let id = event.id.clone();
    let outcome = commit_in_transaction(client, room_id, event, target, activity).await;
    a_held_id_as_such(client, &id, outcome).await
}

async fn commit_in_transaction(
    client: &mut Object,
    room_id: &RoomId,
    mut event: RoomEvent,
    target: Option<RoomEvent>,
    activity: Option<Timestamp>,
) -> Result<RoomEvent, Failure> {
    let transaction = client.transaction().await?;
    let row = query_opt(
        &transaction,
        &format!("SELECT {ROOM_COLUMNS} FROM rooms WHERE id = $1 FOR UPDATE"),
        &[&room_id.as_str()],
    )
    .await?;
    let Some(row) = row else {
        return Err(StoreError::RoomNotFound(room_id.clone()).into());
    };
    let mut room = rows::room(&row)?;
    if let Some(target) = &target {
        if update_row(&transaction, target, Some(room_id))
            .await?
            .is_none()
        {
            return Err(StoreError::EventNotFound(target.id.clone()).into());
        }
        if deletes(&event, &target.id) {
            mark_edits_deleted(&transaction, room_id, &target.id).await?;
        }
    }
    event.index = reserve_index(&transaction, room_id).await?;
    insert(&transaction, &event).await?;
    if let Some(at) = activity {
        lifecycle::commit_activity(&mut room, at);
    }
    execute(
        &transaction,
        "UPDATE rooms SET event_count = $2, latest_index = $3, timers = timers || $4, \
         status = $5, paused_at = $6, updated_at = $7 WHERE id = $1",
        &[
            &room_id.as_str(),
            // The increment is free, the row held above: no count of the
            // timeline per message.
            &integer(room.event_count)?
                .checked_add(1)
                .ok_or(Failure::OutOfRange("an event count"))?,
            &integer(event.index)?,
            &serde_json::to_value(&room.timers)?,
            &name(&room.status)?,
            &room.paused_at.map(Instant),
            &Instant(room.updated_at),
        ],
    )
    .await?;
    transaction.commit().await?;
    Ok(event)
}

/// Whether `event` is a delete of `target`, whose commit marks the target's
/// edits deleted too (RFC §10.3).
fn deletes(event: &RoomEvent, target: &EventId) -> bool {
    matches!(
        edits::mutation(event),
        Ok(Some(Mutation::Delete(delete))) if &delete.target_event_id == target
    )
}

/// Marks every edit of `target_id` deleted, in the caller's transaction: an
/// edit holds a version of the deleted text (RFC §10.3).
async fn mark_edits_deleted(
    transaction: &Transaction<'_>,
    room_id: &RoomId,
    target_id: &EventId,
) -> Result<(), Failure> {
    execute(
        transaction,
        "UPDATE events SET metadata = metadata || '{\"deleted\": true}'::jsonb \
         WHERE room_id = $1 AND content->>'type' = 'edit' \
         AND content->>'target_event_id' = $2",
        &[&room_id.as_str(), &target_id.as_str()],
    )
    .await?;
    Ok(())
}

/// What the row keeps for the record it rewrites: its content when the record
/// holds it as the model reads it, unchanged, with whatever its parts hold
/// that the model does not; else the record's. An edit's content replaces it.
fn kept_content(stored: Value, content: &EventContent) -> Result<Value, Failure> {
    let unchanged =
        serde_json::from_value::<EventContent>(stored.clone()).is_ok_and(|read| &read == content);
    if unchanged {
        return Ok(stored);
    }
    Ok(serde_json::to_value(content)?)
}

/// Rewrites `event`'s stored row, in `room_id` when one is named; the row's
/// delivery results, `None` when no row was written. Shared by `update_event`
/// and the target a change's commit writes (RFC §14.3): the delivery results
/// stay as stored, which only `record_delivery_results` writes (RFC §10.3).
async fn update_row(
    transaction: &Transaction<'_>,
    event: &RoomEvent,
    room_id: Option<&RoomId>,
) -> Result<Option<Metadata>, Failure> {
    let room = room_id.map(RoomId::as_str);
    let held = query_opt(
        transaction,
        "SELECT content, delivery_results FROM events \
         WHERE id = $1 AND ($2::text IS NULL OR room_id = $2) FOR UPDATE",
        &[&event.id.as_str(), &room],
    )
    .await?;
    let Some(held) = held else {
        return Ok(None);
    };
    let content = kept_content(held.try_get("content")?, &event.content)?;
    let participant = event
        .source
        .participant_id
        .as_ref()
        .map(ParticipantId::as_str);
    execute(
        transaction,
        "UPDATE events SET content=$2, status=$3, visibility=$4, \
         metadata=$5, idempotency_key=$6, blocked_by=$7, \
         source_channel_id=$8, source_channel_type=$9, \
         source_participant_id=$10, source_provider=$11, source_extra=$12 \
         WHERE id=$1",
        &[
            &event.id.as_str(),
            &content,
            &name(&event.status)?,
            &name(&event.visibility)?,
            &Value::Object(event.metadata.clone()),
            // A blank key is no key (RFC §13.4).
            &idempotency::key(event.idempotency_key.clone()),
            &event.blocked_by,
            &event.source.channel_id.as_str(),
            &event.source.channel_type.as_str(),
            &participant,
            &event.source.provider,
            &source_extra(&event.source)?,
        ],
    )
    .await?;
    Ok(Some(match held.try_get("delivery_results")? {
        Value::Object(results) => results,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Metadata::new()
        }
    }))
}

/// Writes `event` over its stored row and returns it, its delivery results as
/// stored (RFC §14.1 `update_event`, §14.3).
pub(crate) async fn update(
    client: &mut Object,
    mut event: RoomEvent,
) -> Result<RoomEvent, Failure> {
    let transaction = client.transaction().await?;
    let Some(results) = update_row(&transaction, &event, Some(&event.room_id)).await? else {
        return Err(StoreError::EventNotFound(event.id).into());
    };
    transaction.commit().await?;
    event.delivery_results = results;
    Ok(event)
}

/// Writes `target`, the target of a change kept out of the timeline, and its
/// edits' marks when it is now deleted, in one transaction; no row of the
/// change (RFC §10.3).
pub(crate) async fn write_target(
    client: &mut Object,
    room_id: &RoomId,
    target: RoomEvent,
) -> Result<(), Failure> {
    let transaction = client.transaction().await?;
    if update_row(&transaction, &target, Some(room_id))
        .await?
        .is_none()
    {
        return Err(StoreError::EventNotFound(target.id).into());
    }
    if edits::is_deleted(&target) {
        mark_edits_deleted(&transaction, room_id, &target.id).await?;
    }
    transaction.commit().await?;
    Ok(())
}

/// The event `event_id`, whichever room holds it.
pub(crate) async fn get<C: GenericClient>(
    client: &C,
    event_id: &EventId,
) -> Result<Option<RoomEvent>, Failure> {
    let row = query_opt(
        client,
        &format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = $1"),
        &[&event_id.as_str()],
    )
    .await?;
    row.as_ref().map(rows::event).transpose()
}

/// Writes `results` as the event's delivery results, the column alone: the
/// rest of the row, a change committed since included, stays as stored.
pub(crate) async fn record_results<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    event_id: &EventId,
    results: Metadata,
) -> Result<(), Failure> {
    execute(
        client,
        "UPDATE events SET delivery_results = $3 WHERE id = $1 AND room_id = $2",
        &[
            &event_id.as_str(),
            &room_id.as_str(),
            &Value::Object(results),
        ],
    )
    .await?;
    Ok(())
}

/// Removes `event_id` with, when `cascade_replies`, its replies one level
/// deep, and the edits and deletes of what it removes; returns them in
/// §10.3's order: the root, its replies by index, the changes by index. The
/// counters stay (gap G-064).
pub(crate) async fn delete(
    client: &mut Object,
    room_id: &RoomId,
    event_id: &EventId,
    cascade_replies: bool,
) -> Result<Vec<RoomEvent>, Failure> {
    let transaction = client.transaction().await?;
    let root = query_opt(
        &transaction,
        "SELECT 1 FROM events WHERE room_id = $1 AND id = $2",
        &[&room_id.as_str(), &event_id.as_str()],
    )
    .await?;
    if root.is_none() {
        return Ok(Vec::new());
    }
    let thread = query(
        &transaction,
        &format!(
            "DELETE FROM events WHERE room_id = $1 \
             AND (id = $2 OR ($3 AND parent_event_id = $2)) RETURNING {EVENT_COLUMNS}"
        ),
        &[&room_id.as_str(), &event_id.as_str(), &cascade_replies],
    )
    .await?;
    let gone = thread
        .iter()
        .map(|row| row.try_get("id"))
        .collect::<Result<Vec<&str>, _>>()?;
    let changes = query(
        &transaction,
        &format!(
            "DELETE FROM events WHERE room_id = $1 \
             AND content->>'type' IN ('edit', 'delete') \
             AND content->>'target_event_id' = ANY($2::text[]) RETURNING {EVENT_COLUMNS}"
        ),
        &[&room_id.as_str(), &gone],
    )
    .await?;
    let mut thread = thread
        .iter()
        .map(rows::event)
        .collect::<Result<Vec<_>, _>>()?;
    let mut changes = changes
        .iter()
        .map(rows::event)
        .collect::<Result<Vec<_>, _>>()?;
    transaction.commit().await?;
    // RETURNING's order is not guaranteed.
    thread.sort_by_key(|event| (&event.id != event_id, event.index));
    changes.sort_by_key(|event| event.index);
    thread.append(&mut changes);
    Ok(thread)
}

/// Whether the room holds an event with `key`, from `channel_id`, or from any
/// channel for none (RFC §13.4, §22.1), its row unread: a blank key names none.
pub(crate) async fn key_held<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
    key: &str,
) -> Result<bool, Failure> {
    if !idempotency::is_key(key) {
        return Ok(false);
    }
    let found = match channel_id {
        None => {
            query_opt(
                client,
                "SELECT 1 FROM events WHERE room_id = $1 AND idempotency_key = $2 LIMIT 1",
                &[&room_id.as_str(), &key],
            )
            .await?
        }
        Some(channel) => {
            query_opt(
                client,
                "SELECT 1 FROM events WHERE room_id = $1 AND source_channel_id = $2 \
                 AND idempotency_key = $3",
                &[&room_id.as_str(), &channel.as_str(), &key],
            )
            .await?
        }
    };
    Ok(found.is_some())
}

/// The event the room committed with `key`, from `channel_id`, or from any
/// channel for none, the lowest index first (RFC §13.4, §22.1); a blank key
/// names none.
pub(crate) async fn by_key<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
    key: &str,
) -> Result<Option<RoomEvent>, Failure> {
    if !idempotency::is_key(key) {
        return Ok(None);
    }
    let row = match channel_id {
        None => {
            query_opt(
                client,
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM events \
                     WHERE room_id = $1 AND idempotency_key = $2 ORDER BY index LIMIT 1"
                ),
                &[&room_id.as_str(), &key],
            )
            .await?
        }
        Some(channel) => {
            query_opt(
                client,
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM events \
                     WHERE room_id = $1 AND source_channel_id = $2 AND idempotency_key = $3"
                ),
                &[&room_id.as_str(), &channel.as_str(), &key],
            )
            .await?
        }
    };
    row.as_ref().map(rows::event).transpose()
}
