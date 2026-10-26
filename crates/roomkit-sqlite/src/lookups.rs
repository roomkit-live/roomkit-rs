//! The rooms a filter admits, and the routing lookups (RFC §10.4 step 1,
//! §14.1 `list_rooms`, `find_room_id_by_binding`,
//! `find_room_id_by_participant`, gap G-058), read with SQL over the
//! `rooms`, `bindings` and `participants` tables.

use roomkit::RoomFilter;
use roomkit::model::{ChannelId, IdentityId, ParticipantId, Room, RoomId};
use roomkit_core::rules::routing;
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params_from_iter};

use crate::error::Failure;
use crate::records;
use crate::rooms::room_of;

/// A condition on the room `r`, with its parameters.
struct Condition {
    sql: &'static str,
    params: Vec<Sql>,
}

impl Condition {
    fn new(sql: &'static str, params: Vec<Sql>) -> Self {
        Self { sql, params }
    }
}

/// The rooms `filter` admits that meet `conditions`.
fn admitted(
    connection: &Connection,
    filter: &RoomFilter,
    mut conditions: Vec<Condition>,
) -> Result<Vec<Room>, Failure> {
    if let Some(organization) = &filter.organization_id {
        conditions.push(Condition::new(
            "r.organization_id = ?",
            vec![Sql::Text(organization.clone())],
        ));
    }
    if let Some(status) = filter.status {
        conditions.push(Condition::new(
            "r.status = ?",
            vec![Sql::Text(records::name(&status)?)],
        ));
    }
    if let Some(channel_id) = &filter.channel_id {
        conditions.push(reached_through(channel_id));
    }
    let mut sql = String::from("SELECT r.data, r.delivered_index FROM rooms r WHERE 1 = 1");
    let mut params = Vec::new();
    for condition in conditions {
        sql.push_str(" AND ");
        sql.push_str(condition.sql);
        params.extend(condition.params);
    }
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(params), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    // The filter's timers, read on the room itself (RFC §5.1).
    let timers = RoomFilter {
        timers_armed: filter.timers_armed,
        ..RoomFilter::default()
    };
    let mut rooms = Vec::new();
    for row in rows {
        let (data, delivered_index) = row?;
        let room = room_of(&data, delivered_index)?;
        if timers.matches(&room, &[], &[]) {
            rooms.push(room);
        }
    }
    Ok(rooms)
}

/// The rooms `channel_id` reaches: those binding it, and those holding a
/// participant reached through it (RFC §5.5 `connected_via`).
fn reached_through(channel_id: &ChannelId) -> Condition {
    Condition::new(
        "(EXISTS (SELECT 1 FROM bindings b WHERE b.room_id = r.id AND b.channel_id = ?)
          OR EXISTS (SELECT 1 FROM participants p, json_each(p.data, '$.connected_via') v
                     WHERE p.room_id = r.id AND v.value = ?))",
        vec![
            Sql::Text(channel_id.to_string()),
            Sql::Text(channel_id.to_string()),
        ],
    )
}

/// The rooms `filter` admits, oldest first (RFC §14.1 `list_rooms`).
pub(crate) fn list(connection: &Connection, filter: &RoomFilter) -> Result<Vec<Room>, Failure> {
    let mut rooms = admitted(connection, filter, Vec::new())?;
    rooms.sort_by(routing::recency);
    Ok(rooms)
}

/// The names a stored id is compared with for the sender: their address,
/// and the identity the caller resolved for them.
fn names(participant_id: &ParticipantId, identity_id: Option<&IdentityId>) -> Vec<Sql> {
    let mut names = vec![Sql::Text(participant_id.to_string())];
    names.extend(identity_id.map(|identity| Sql::Text(identity.to_string())));
    names
}

/// The newest of `rooms`, as the default router orders them (RFC §10.4).
fn newest(rooms: Vec<Room>) -> Option<RoomId> {
    rooms
        .into_iter()
        .max_by(routing::recency)
        .map(|room| room.id)
}

/// The newest room `filter` admits whose binding of `channel_id` names the
/// sender (RFC §14.1 `find_room_id_by_binding`, gap G-058).
pub(crate) fn find_by_binding(
    connection: &Connection,
    channel_id: &ChannelId,
    participant_id: &ParticipantId,
    identity_id: Option<&IdentityId>,
    filter: &RoomFilter,
) -> Result<Option<RoomId>, Failure> {
    let mut params = vec![Sql::Text(channel_id.to_string())];
    let names = names(participant_id, identity_id);
    let sql = if names.len() == 1 {
        "EXISTS (SELECT 1 FROM bindings b WHERE b.room_id = r.id AND b.channel_id = ?
                 AND b.participant_id = ?)"
    } else {
        "EXISTS (SELECT 1 FROM bindings b WHERE b.room_id = r.id AND b.channel_id = ?
                 AND b.participant_id IN (?, ?))"
    };
    params.extend(names);
    let rooms = admitted(connection, filter, vec![Condition::new(sql, params)])?;
    Ok(newest(rooms))
}

/// The newest room `filter` admits holding a participant reached through
/// `channel_id` that the sender's address or identity names (RFC §14.1
/// `find_room_id_by_participant`, gap G-058).
pub(crate) fn find_by_participant(
    connection: &Connection,
    channel_id: &ChannelId,
    participant_id: &ParticipantId,
    identity_id: Option<&IdentityId>,
    filter: &RoomFilter,
) -> Result<Option<RoomId>, Failure> {
    let mut params = vec![Sql::Text(channel_id.to_string())];
    let sql = match identity_id {
        None => {
            params.push(Sql::Text(participant_id.to_string()));
            "EXISTS (SELECT 1 FROM participants p WHERE p.room_id = r.id
                     AND EXISTS (SELECT 1 FROM json_each(p.data, '$.connected_via') WHERE value = ?)
                     AND p.id = ?)"
        }
        Some(identity) => {
            params.extend(names(participant_id, Some(identity)));
            params.push(Sql::Text(identity.to_string()));
            "EXISTS (SELECT 1 FROM participants p WHERE p.room_id = r.id
                     AND EXISTS (SELECT 1 FROM json_each(p.data, '$.connected_via') WHERE value = ?)
                     AND (p.id IN (?, ?) OR json_extract(p.data, '$.identity_id') = ?))"
        }
    };
    let rooms = admitted(connection, filter, vec![Condition::new(sql, params)])?;
    Ok(newest(rooms))
}
