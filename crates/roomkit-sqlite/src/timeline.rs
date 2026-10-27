//! A page of a room's timeline, and a count of its rows (RFC §14.1,
//! "Reading a timeline"), selected as the Python implementation's
//! `list_events` and `get_event_count` select them: the filter's criteria
//! on the row's columns, applied before the page is cut, and the window
//! read from the index on `(room_id, idx)`.

use roomkit::model::{EventStatus, RoomEvent, RoomId, Visibility};
use roomkit::{EventFilter, EventQuery};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};

use crate::error::Failure;
use crate::records;

/// The conditions a row must meet, and the values they bind, in order.
struct Criteria {
    conditions: Vec<String>,
    values: Vec<Value>,
}

impl Criteria {
    /// The rows of `room_id`.
    fn of(room_id: &RoomId) -> Self {
        Self {
            conditions: vec!["room_id = ?".to_owned()],
            values: vec![Value::Text(room_id.as_str().to_owned())],
        }
    }

    /// Adds `condition`, which binds `values`.
    fn and(&mut self, condition: &str, values: impl IntoIterator<Item = Value>) {
        self.conditions.push(condition.to_owned());
        self.values.extend(values);
    }

    /// Adds `column IN (...)` for `names`, or `NOT IN` when `excluded`; an
    /// empty list admits nothing, and excludes nothing.
    fn listed(&mut self, column: &'static str, names: Vec<String>, excluded: bool) {
        if names.is_empty() {
            if !excluded {
                self.and("0", []);
            }
            return;
        }
        // The column is this module's own, never a caller's.
        let marks = vec!["?"; names.len()].join(", ");
        let operator = if excluded { "NOT IN" } else { "IN" };
        let condition = format!("{column} {operator} ({marks})");
        self.and(&condition, names.into_iter().map(Value::Text));
    }

    /// The `WHERE` clause.
    fn clause(&self) -> String {
        self.conditions.join(" AND ")
    }
}

/// The criteria of the rows `filter` admits, of the visibility asked for
/// when it names none, in `room_id`; with no filter, the rows the room
/// received.
fn criteria(
    room_id: &RoomId,
    filter: Option<&EventFilter>,
    visibility: Option<&Visibility>,
) -> Result<Criteria, Failure> {
    let received = EventFilter::default();
    let EventFilter {
        event_types,
        exclude_types,
        visibility: filtered,
        source_channel_id,
        source_channel_type,
        correlation_id,
        participant_id,
        responds_to,
        parent_event_id,
        top_level_only,
        after_time,
        before_time,
        include_blocked,
    } = filter.unwrap_or(&received);
    let text = |value: &str| Value::Text(value.to_owned());
    let mut criteria = Criteria::of(room_id);
    if let Some(visibility) = filtered.as_ref().or(visibility) {
        criteria.and("visibility = ?", [text(&records::name(visibility)?)]);
    }
    if let Some(types) = event_types {
        let names = types.iter().map(records::name).collect::<Result<_, _>>()?;
        criteria.listed("type", names, false);
    }
    if let Some(types) = exclude_types {
        let names = types.iter().map(records::name).collect::<Result<_, _>>()?;
        criteria.listed("type", names, true);
    }
    if let Some(channel) = source_channel_id {
        criteria.and("source_channel_id = ?", [text(channel.as_str())]);
    }
    if let Some(kind) = source_channel_type {
        criteria.and("source_channel_type = ?", [text(kind.as_str())]);
    }
    if let Some(correlation) = correlation_id {
        criteria.and("correlation_id = ?", [text(correlation)]);
    }
    if let Some(participant) = participant_id {
        criteria.and("participant_id = ?", [text(participant.as_str())]);
    }
    if let Some(answered) = responds_to {
        criteria.and("responds_to = ?", [text(answered.as_str())]);
    }
    if let Some(parent) = parent_event_id {
        criteria.and("parent_event_id = ?", [text(parent.as_str())]);
    }
    if *top_level_only {
        criteria.and("parent_event_id IS NULL", []);
    }
    if let Some(after) = after_time {
        criteria.and("created_ts > ?", [Value::Real(records::seconds(*after))]);
    }
    if let Some(before) = before_time {
        criteria.and("created_ts < ?", [Value::Real(records::seconds(*before))]);
    }
    if !include_blocked {
        let blocked = records::name(&EventStatus::Blocked)?;
        criteria.and("json_extract(data, '$.status') IS NOT ?", [text(&blocked)]);
    }
    Ok(criteria)
}

/// The page of `room_id`'s timeline `query` asks for, in ascending index
/// order. The window is read from the newest end, descending, for a
/// `before_index` cursor and for `newest_first` in offset mode, then put
/// back in ascending order; `offset` counts under no cursor only.
pub(crate) fn page(
    connection: &Connection,
    room_id: &RoomId,
    query: &EventQuery,
) -> Result<Vec<RoomEvent>, Failure> {
    let (sql, values, from_newest) = page_query(room_id, query)?;
    let mut statement = connection.prepare_cached(&sql)?;
    let rows = statement.query_map(params_from_iter(values), |row| row.get::<_, String>(0))?;
    let mut events = rows
        .map(|data| records::read(&data?))
        .collect::<Result<Vec<_>, _>>()?;
    if from_newest {
        events.reverse();
    }
    Ok(events)
}

/// The statement of [`page`], its values, and whether it reads from the
/// newest end, descending.
fn page_query(room_id: &RoomId, query: &EventQuery) -> Result<(String, Vec<Value>, bool), Failure> {
    let EventQuery {
        offset,
        limit,
        visibility_filter,
        event_filter,
        after_index,
        before_index,
        newest_first,
    } = query;
    let mut criteria = criteria(room_id, event_filter.as_ref(), visibility_filter.as_ref())?;
    let cursor = after_index.is_some() || before_index.is_some();
    // A cursor past SQLite's integers bounds the page as it bounds an
    // in-memory store's: no index is above it, and every index below.
    if let Some(after) = after_index {
        let after = i64::try_from(*after).unwrap_or(i64::MAX);
        criteria.and("idx > ?", [Value::Integer(after)]);
    }
    if let Some(before) = before_index.and_then(|before| i64::try_from(before).ok()) {
        criteria.and("idx < ?", [Value::Integer(before)]);
    }
    let from_newest = before_index.is_some() || (!cursor && *newest_first);
    let skipped = if cursor { 0 } else { *offset };
    let order = if from_newest { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT data FROM events WHERE {} ORDER BY idx {order} LIMIT ? OFFSET ?",
        criteria.clause()
    );
    let mut values = criteria.values;
    values.push(Value::Integer(i64::try_from(*limit).unwrap_or(i64::MAX)));
    values.push(Value::Integer(i64::try_from(skipped).unwrap_or(i64::MAX)));
    Ok((sql, values, from_newest))
}

/// How many of `room_id`'s rows `filter` admits, every row with none: the
/// rows [`page`] serves under the same filter, with no page.
pub(crate) fn count(
    connection: &Connection,
    room_id: &RoomId,
    filter: Option<&EventFilter>,
) -> Result<u64, Failure> {
    let every_row = EventFilter {
        include_blocked: true,
        ..EventFilter::default()
    };
    let criteria = criteria(room_id, Some(filter.unwrap_or(&every_row)), None)?;
    let sql = format!("SELECT COUNT(*) FROM events WHERE {}", criteria.clause());
    let counted: i64 =
        connection.query_row(&sql, params_from_iter(criteria.values), |row| row.get(0))?;
    u64::try_from(counted).map_err(|_| Failure::OutOfRange("an event count"))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit::model::EventType;

    use super::*;

    /// The plan SQLite makes for the page `query` asks for, on the store's
    /// schema: its details, one per step.
    fn plan(query: &EventQuery) -> Vec<String> {
        let mut connection = Connection::open_in_memory().unwrap();
        crate::schema::prepare(&mut connection).unwrap();
        let room_id = RoomId::new("room_1").unwrap();
        let (sql, values, _) = page_query(&room_id, query).unwrap();
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap();
        statement
            .query_map(params_from_iter(values), |row| row.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// Each window is read from the index on `(room_id, idx)`, in its
    /// order: no scan of the table, no sort of its rows.
    #[test]
    fn each_window_is_read_from_the_room_s_index() {
        let filtered = EventFilter {
            event_types: Some(vec![EventType::Message]),
            ..EventFilter::default()
        };
        let queries = [
            EventQuery::default(),
            EventQuery {
                newest_first: true,
                offset: 10,
                ..EventQuery::default()
            },
            EventQuery {
                after_index: Some(100),
                ..EventQuery::default()
            },
            EventQuery {
                before_index: Some(100),
                event_filter: Some(filtered),
                ..EventQuery::default()
            },
        ];
        let plans: Vec<Vec<String>> = queries.iter().map(plan).collect();
        let indexed = |plan: &Vec<String>| {
            plan.len() == 1
                && plan
                    .first()
                    .is_some_and(|step| step.contains("USING INDEX idx_events_room_idx"))
        };
        assert_eq!(
            plans.iter().filter(|plan| !indexed(plan)).count(),
            0,
            "{plans:#?}"
        );
    }
}
