//! The full-text search over the events' text (`events_fts`): Python's
//! `SQLiteStore.search_events`.

use roomkit::model::{RoomEvent, RoomId};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};

use crate::error::Failure;
use crate::records;

/// The events whose text holds every word of `query`, the most relevant
/// first, `limit` at most, of `room_id` alone when given. A row the room
/// refused (`blocked`) is not served, as a default timeline read does not
/// serve it (RFC §14.1); a message an accepted delete marked is no longer
/// indexed (RFC §10.3).
pub(crate) fn search(
    connection: &Connection,
    query: &str,
    room_id: Option<&RoomId>,
    limit: u64,
) -> Result<Vec<RoomEvent>, Failure> {
    let words = words(query);
    if words.is_empty() {
        return Ok(Vec::new());
    }
    // Each word quoted: free text from a user is matched as words, never
    // read as the full-text query language.
    let matched = words
        .iter()
        .map(|word| format!("\"{word}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let mut conditions = vec!["events_fts MATCH ?"];
    let mut values = vec![Value::Text(matched)];
    if let Some(room_id) = room_id {
        conditions.push("events_fts.room_id = ?");
        values.push(Value::Text(room_id.as_str().to_owned()));
    }
    conditions.push("json_extract(e.data, '$.status') IS NOT ?");
    values.push(Value::Text("blocked".to_owned()));
    values.push(Value::Integer(records::integer(limit)?));
    let sql = format!(
        "SELECT e.data FROM events_fts JOIN events e ON e.id = events_fts.event_id \
         WHERE {} ORDER BY events_fts.rank LIMIT ?",
        conditions.join(" AND ")
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values), |row| row.get::<_, String>(0))?;
    let mut events = Vec::new();
    for data in rows {
        events.push(records::read(&data?)?);
    }
    Ok(events)
}

/// The words of `query`, as Python's `\w+` reads them: runs of letters,
/// digits and underscores.
fn words(query: &str) -> Vec<&str> {
    query
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|word| !word.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_is_read_as_its_words() {
        assert_eq!(
            words("refund, \"order\" 42* OR né_e"),
            ["refund", "order", "42", "OR", "né_e"]
        );
        assert_eq!(words(" *\"- "), Vec::<&str>::new());
    }
}
