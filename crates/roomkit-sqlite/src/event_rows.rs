//! An event's row and what the Python implementation derives from it and
//! reads: its idempotency key (`idempotency`), its text in the full-text
//! index (`events_fts`) and its room's next index (`event_sequences`).

use roomkit::model::{EventContent, EventId, ParticipantId, RoomEvent, RoomId};
use roomkit_core::rules::edits;
use rusqlite::{Connection, params};

use crate::error::Failure;
use crate::records::{self, Kind};

/// The text of a message the full-text index holds, as the Python
/// implementation reads it: the body, or a media's caption, of the content
/// and of a composite's parts, one level deep.
#[expect(
    clippy::match_same_arms,
    reason = "the named arms decide every content the model knows, the wildcard one a later version adds"
)]
fn searchable_text(content: &EventContent) -> String {
    let parts = match content {
        EventContent::Composite(composite) => composite.parts(),
        EventContent::Text(_)
        | EventContent::Rich(_)
        | EventContent::Media(_)
        | EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_) => &[],
        _ => &[],
    };
    std::iter::once(content)
        .chain(parts)
        .filter_map(text_of)
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The text a content gives the full-text index: its body, or a media's
/// caption.
#[expect(
    clippy::match_same_arms,
    reason = "the named arms decide every content the model knows, the wildcard one a later version adds"
)]
fn text_of(content: &EventContent) -> Option<&str> {
    match content {
        EventContent::Text(text) => Some(text.body.as_str()),
        EventContent::Rich(rich) => Some(rich.body.as_str()),
        EventContent::System(system) => Some(system.body.as_str()),
        EventContent::Template(template) => template.body.as_deref(),
        EventContent::Media(media) => media.caption.as_deref(),
        EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::Composite(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_) => None,
        _ => None,
    }
}

/// Writes `event`'s row, over the one `stored` read when it is a rewrite,
/// with its idempotency key, its text in the full-text index, which a
/// deleted message leaves (RFC §10.3), and its room's next index kept ahead
/// of it.
pub(crate) fn write_row(
    connection: &Connection,
    event: &RoomEvent,
    stored: Option<&str>,
) -> Result<(), Failure> {
    let data = records::written(event, Kind::Event, stored)?;
    let columns = params![
        event.id.as_str(),
        event.room_id.as_str(),
        records::integer(event.index)?,
        records::name(&event.r#type)?,
        records::name(&event.visibility)?,
        event.source.channel_id.as_str(),
        event.source.channel_type.as_str(),
        event
            .source
            .participant_id
            .as_ref()
            .map(ParticipantId::as_str),
        event.correlation_id,
        event.parent_event_id.as_ref().map(EventId::as_str),
        event.idempotency_key,
        records::seconds(event.created_at),
        data,
        event.responds_to.as_ref().map(EventId::as_str),
    ];
    if stored.is_some() {
        connection.execute(
            "UPDATE events SET room_id = ?2, idx = ?3, type = ?4, visibility = ?5,
                 source_channel_id = ?6, source_channel_type = ?7, participant_id = ?8,
                 correlation_id = ?9, parent_event_id = ?10, idempotency_key = ?11,
                 created_ts = ?12, data = ?13, responds_to = ?14
             WHERE id = ?1",
            columns,
        )?;
    } else {
        connection.execute(
            "INSERT INTO events(id, room_id, idx, type, visibility, source_channel_id,
                 source_channel_type, participant_id, correlation_id, parent_event_id,
                 idempotency_key, created_ts, data, responds_to)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            columns,
        )?;
    }
    if let Some(key) = &event.idempotency_key {
        connection.execute(
            "INSERT OR IGNORE INTO idempotency(room_id, channel_id, key) VALUES(?, ?, ?)",
            params![
                event.room_id.as_str(),
                event.source.channel_id.as_str(),
                key
            ],
        )?;
    }
    index_text(connection, event)?;
    connection.execute(
        "INSERT INTO event_sequences(room_id, next_index) VALUES(?, ?)
         ON CONFLICT(room_id) DO UPDATE SET
             next_index = MAX(event_sequences.next_index, excluded.next_index)",
        params![
            event.room_id.as_str(),
            records::integer(event.index)?.saturating_add(1)
        ],
    )?;
    Ok(())
}

/// Deletes an event's text from the full-text index. The text's row shares
/// its event row's rowid, the one key FTS5 indexes: by `event_id`, every
/// rewrite scanned the whole index (RMK-1031).
pub(crate) const DELETE_TEXT: &str =
    "DELETE FROM events_fts WHERE rowid = (SELECT rowid FROM events WHERE id = ?)";

/// Writes an event's text in the full-text index, under its event row's
/// rowid.
pub(crate) const INSERT_TEXT: &str = "INSERT INTO events_fts(rowid, text, event_id, room_id) \
     VALUES((SELECT rowid FROM events WHERE id = ?1), ?2, ?1, ?3)";

/// Puts `event`'s text in the full-text index, in place of what it held for
/// it; a deleted message's text is in it no more (RFC §10.3).
fn index_text(connection: &Connection, event: &RoomEvent) -> Result<(), Failure> {
    connection.execute(DELETE_TEXT, [event.id.as_str()])?;
    let text = if edits::is_deleted(event) {
        String::new()
    } else {
        searchable_text(&event.content)
    };
    if !text.is_empty() {
        connection.execute(
            INSERT_TEXT,
            [event.id.as_str(), text.as_str(), event.room_id.as_str()],
        )?;
    }
    Ok(())
}

/// Removes `event`'s row, its idempotency key and its text.
pub(crate) fn remove_row(connection: &Connection, event: &RoomEvent) -> Result<(), Failure> {
    if let Some(key) = &event.idempotency_key {
        connection.execute(
            "DELETE FROM idempotency WHERE room_id = ? AND channel_id = ? AND key = ?",
            params![
                event.room_id.as_str(),
                event.source.channel_id.as_str(),
                key
            ],
        )?;
    }
    // The text first: it is found by the row's rowid.
    connection.execute(DELETE_TEXT, [event.id.as_str()])?;
    connection.execute("DELETE FROM events WHERE id = ?", [event.id.as_str()])?;
    Ok(())
}

/// Marks every edit of `target` deleted, as the Python implementation's
/// store does when it commits a delete: an edit holds a version of the
/// deleted text (RFC §10.3), which a Python history withholds by the edit's
/// own mark. Edits hold no text of their own in the full-text index.
pub(crate) fn mark_edits_deleted(
    connection: &Connection,
    room_id: &RoomId,
    target: &EventId,
) -> Result<(), Failure> {
    connection.execute(
        "UPDATE events SET data = json_set(data, '$.metadata.deleted', json('true'))
         WHERE room_id = ? AND json_extract(data, '$.content.type') = 'edit'
             AND json_extract(data, '$.content.target_event_id') = ?",
        [room_id.as_str(), target.as_str()],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{DELETE_TEXT, INSERT_TEXT};
    use crate::rooms::DELETE_ROOM_TEXTS;

    /// RMK-1031: the full-text index is written by its rows' rowids, the one
    /// key FTS5 indexes. A full scan of the virtual table reads `INDEX n:`
    /// with no constraint after it; by `event_id` or `room_id`, each commit,
    /// delete and room delete read that way.
    #[test]
    fn the_full_text_rows_are_written_by_rowid() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("schema.sql"))
            .unwrap();
        let plans: Vec<String> = [DELETE_TEXT, INSERT_TEXT, DELETE_ROOM_TEXTS]
            .iter()
            .flat_map(|sql| {
                let mut statement = connection
                    .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                    .unwrap();
                // EXPLAIN needs every parameter bound, any value.
                let nulls = vec![rusqlite::types::Null; statement.parameter_count()];
                statement
                    .query_map(rusqlite::params_from_iter(nulls), |row| {
                        row.get::<_, String>(3)
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect::<Vec<_>>()
            })
            .collect();
        let scans: Vec<&String> = plans
            .iter()
            .filter(|plan| plan.starts_with("SCAN events_fts VIRTUAL TABLE") && plan.ends_with(':'))
            .collect();
        assert!(!plans.is_empty() && scans.is_empty(), "{plans:?}");
    }
}
