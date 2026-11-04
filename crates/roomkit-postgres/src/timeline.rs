//! A page of a room's timeline, a count of its rows (RFC §14.1, "Reading a
//! timeline") and its threads' summaries, as Python's `list_events`,
//! `get_event_count` and `get_thread_summaries`: the filter's criteria in
//! SQL, so that a page is cut after the refused rows are gone. Every value is
//! a parameter; the statement's text holds only this module's own.

use std::collections::BTreeMap;

use deadpool_postgres::GenericClient;
use roomkit::EventFilter;
use roomkit::model::{EventId, EventStatus, RoomEvent, RoomId, ThreadSummary, Visibility};
use roomkit::store::ListEventsOptions;
use tokio_postgres::Row;

use crate::error::Failure;
use crate::rows::{self, EVENT_COLUMNS};
use crate::values::{Instant, Owned, borrowed, bound, name, query, query_opt};

/// The conditions a row must meet, and the values they bind, numbered in
/// order.
struct Criteria {
    conditions: Vec<String>,
    values: Vec<Owned>,
}

impl Criteria {
    /// The rows of `room_id`.
    fn of(room_id: &RoomId) -> Self {
        Self {
            conditions: vec!["room_id = $1".to_owned()],
            values: vec![Box::new(room_id.as_str().to_owned())],
        }
    }

    /// Adds `value`, compared by `condition`, which `$` stands for its number
    /// in.
    fn and(&mut self, condition: &str, value: Owned) {
        self.values.push(value);
        self.conditions
            .push(condition.replace('$', &format!("${}", self.values.len())));
    }

    /// The `WHERE` clause.
    fn clause(&self) -> String {
        self.conditions.join(" AND ")
    }
}

/// Appends the conditions of a page, or of the count that stands for it,
/// under `filter`, as Python's `_apply_event_filter_sql`: the visibility the
/// caller resolved, the filter's own fields, then the received-rows default
/// (RFC §14.1), unless `include_blocked` lifts it.
fn apply_filter(
    criteria: &mut Criteria,
    filter: Option<&EventFilter>,
    visibility: Option<&Visibility>,
) -> Result<(), Failure> {
    if let Some(visibility) = visibility {
        criteria.and("visibility = $", Box::new(name(visibility)?));
    }
    let blocked = || name(&EventStatus::Blocked);
    let Some(filter) = filter else {
        criteria.and("status != $", Box::new(blocked()?));
        return Ok(());
    };
    let EventFilter {
        event_types,
        exclude_types,
        visibility: _,
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
    } = filter;
    if let Some(types) = event_types {
        let names = types.iter().map(name).collect::<Result<Vec<_>, _>>()?;
        criteria.and("type = ANY($)", Box::new(names));
    }
    if let Some(types) = exclude_types {
        let names = types.iter().map(name).collect::<Result<Vec<_>, _>>()?;
        criteria.and("type != ALL($)", Box::new(names));
    }
    if let Some(channel) = source_channel_id {
        criteria.and(
            "source_channel_id = $",
            Box::new(channel.as_str().to_owned()),
        );
    }
    if let Some(kind) = source_channel_type {
        criteria.and(
            "source_channel_type = $",
            Box::new(kind.as_str().to_owned()),
        );
    }
    if let Some(correlation) = correlation_id {
        criteria.and("correlation_id = $", Box::new(correlation.clone()));
    }
    if let Some(answered) = responds_to {
        criteria.and("responds_to = $", Box::new(answered.as_str().to_owned()));
    }
    if let Some(participant) = participant_id {
        criteria.and(
            "source_participant_id = $",
            Box::new(participant.as_str().to_owned()),
        );
    }
    if let Some(parent) = parent_event_id {
        criteria.and("parent_event_id = $", Box::new(parent.as_str().to_owned()));
    }
    if *top_level_only {
        criteria
            .conditions
            .push("parent_event_id IS NULL".to_owned());
    }
    if let Some(after) = after_time {
        criteria.and("created_at > $", Box::new(Instant(*after)));
    }
    if let Some(before) = before_time {
        criteria.and("created_at < $", Box::new(Instant(*before)));
    }
    if !include_blocked {
        criteria.and("status != $", Box::new(blocked()?));
    }
    Ok(())
}

/// The page of `room_id`'s timeline `options` asks for, in ascending index
/// order whatever the window: read from the newest end, descending, for a
/// `before_index` cursor and for `newest_first` in offset mode, then put
/// back; `offset` counts under no cursor only.
pub(crate) async fn page<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    options: &ListEventsOptions,
) -> Result<Vec<RoomEvent>, Failure> {
    let ListEventsOptions {
        offset,
        limit,
        visibility_filter,
        event_filter,
        after_index,
        before_index,
        newest_first,
    } = options;
    let mut criteria = Criteria::of(room_id);
    let cursor = after_index.is_some() || before_index.is_some();
    // A cursor past the column's integers bounds the page as an in-memory
    // store's: no index is above it, and every index below.
    if let Some(after) = after_index {
        let after = i32::try_from(*after).unwrap_or(i32::MAX);
        criteria.and("index > $", Box::new(after));
    }
    if let Some(before) = before_index.and_then(|before| i32::try_from(before).ok()) {
        criteria.and("index < $", Box::new(before));
    }
    let visibility = event_filter
        .as_ref()
        .and_then(|filter| filter.visibility.as_ref())
        .or(visibility_filter.as_ref());
    apply_filter(&mut criteria, event_filter.as_ref(), visibility)?;
    let descending = before_index.is_some() || (*newest_first && !cursor);
    let order = if descending {
        "index DESC, created_at DESC, id DESC"
    } else {
        "index, created_at, id"
    };
    let sql = format!(
        "SELECT {EVENT_COLUMNS} FROM events WHERE {} ORDER BY {order} LIMIT ${}",
        criteria.clause(),
        criteria.values.len() + 1,
    );
    let mut values = criteria.values;
    values.push(Box::new(bound(*limit)));
    let sql = if cursor {
        sql
    } else {
        values.push(Box::new(bound(*offset)));
        format!("{sql} OFFSET ${}", values.len())
    };
    let rows: Vec<Row> = query(client, &sql, &borrowed(&values)).await?;
    let mut events = rows
        .iter()
        .map(rows::event)
        .collect::<Result<Vec<_>, _>>()?;
    if descending {
        events.reverse();
    }
    Ok(events)
}

/// How many of `room_id`'s rows `filter` admits, every row with none: the
/// rows [`page`] serves under the same filter, with no page.
pub(crate) async fn count<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    filter: Option<&EventFilter>,
) -> Result<u64, Failure> {
    let mut criteria = Criteria::of(room_id);
    if let Some(filter) = filter {
        apply_filter(&mut criteria, Some(filter), filter.visibility.as_ref())?;
    }
    let sql = format!("SELECT count(*) FROM events WHERE {}", criteria.clause());
    let row = query_opt(client, &sql, &borrowed(&criteria.values)).await?;
    let counted: i64 = row.map_or(Ok(0), |row| row.try_get(0))?;
    u64::try_from(counted).map_err(|_| Failure::OutOfRange("an event count"))
}

/// The summary of each root of `roots` with replies in `room_id`, the replies
/// the room received alone.
pub(crate) async fn thread_summaries<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    roots: &[EventId],
) -> Result<BTreeMap<EventId, ThreadSummary>, Failure> {
    if roots.is_empty() {
        return Ok(BTreeMap::new());
    }
    let roots: Vec<&str> = roots.iter().map(EventId::as_str).collect();
    let rows = query(
        client,
        "SELECT parent_event_id, count(*) AS reply_count, max(created_at) AS last_reply_at \
         FROM events WHERE room_id = $1 AND parent_event_id = ANY($2) AND status != $3 \
         GROUP BY parent_event_id",
        &[&room_id.as_str(), &roots, &name(&EventStatus::Blocked)?],
    )
    .await?;
    let mut summaries = BTreeMap::new();
    for row in rows {
        let root: EventId =
            serde_json::from_value(serde_json::Value::String(row.try_get("parent_event_id")?))?;
        let summary = ThreadSummary {
            reply_count: u64::try_from(row.try_get::<_, i64>("reply_count")?)
                .map_err(|_| Failure::OutOfRange("a count"))?,
            last_reply_at: row
                .try_get::<_, Option<Instant>>("last_reply_at")?
                .map(|at| at.0),
            ..ThreadSummary::new(root.clone())
        };
        summaries.insert(root, summary);
    }
    Ok(summaries)
}
