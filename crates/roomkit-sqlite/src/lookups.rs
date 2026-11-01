//! The rooms a store pages, and the routing lookups (RFC §10.4 step 1,
//! §14.1 `list_rooms`, `find_rooms`, `find_latest_room`, `find_room_ids_by_channel`,
//! `find_room_id_by_binding`, `find_room_id_by_participant`, gap G-058),
//! read with the Python implementation's SQL over the `rooms`, `bindings`
//! and `participants` tables.

use roomkit::RoomFilter;
use roomkit::model::{ChannelId, IdentityId, ParticipantId, Room, RoomId};
use roomkit::store::{
    FindLatestRoomOptions, FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions,
    FindRoomIdsByChannelOptions, FindRoomsOptions,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension, params_from_iter};
use serde_json::Value;

use crate::error::Failure;
use crate::records;
use crate::rooms::room_of;

/// A count as SQLite takes it, a value past its range read as its largest.
fn count(value: u64) -> Sql {
    Sql::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

/// The room id a row's `id` column holds.
fn room_id_of(id: String) -> Result<RoomId, Failure> {
    Ok(serde_json::from_value(Value::String(id))?)
}

/// Narrows a lookup joined on `rooms r` to the statuses and the
/// organization `room_filter` names (RFC §10.4, §14.1, §17.2), as Python's
/// `_filter_rooms`.
fn filter_rooms(
    conditions: &mut Vec<String>,
    params: &mut Vec<Sql>,
    room_filter: Option<&RoomFilter>,
) -> Result<(), Failure> {
    let Some(room_filter) = room_filter else {
        return Ok(());
    };
    if let Some(statuses) = &room_filter.statuses {
        conditions.push(format!("r.status IN ({})", placeholders(statuses.len())));
        for status in statuses {
            params.push(Sql::Text(records::name(status)?));
        }
    }
    if let Some(organization) = &room_filter.organization_id {
        conditions.push("r.organization_id = ?".to_owned());
        params.push(Sql::Text(organization.clone()));
    }
    Ok(())
}

/// `?, ?` for `count` values, the parameters of an `IN` list.
fn placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

/// The rooms `sql` selects, its `r.data` and `r.delivered_index` columns.
fn rooms_of(connection: &Connection, sql: &str, params: Vec<Sql>) -> Result<Vec<Room>, Failure> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(params_from_iter(params), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut rooms = Vec::new();
    for row in rows {
        let (data, delivered_index) = row?;
        rooms.push(room_of(&data, delivered_index)?);
    }
    Ok(rooms)
}

/// A page of every room, by creation then id (RFC §14.1 `list_rooms`).
pub(crate) fn list(connection: &Connection, offset: u64, limit: u64) -> Result<Vec<Room>, Failure> {
    rooms_of(
        connection,
        "SELECT r.data, r.delivered_index FROM rooms r ORDER BY r.created_ts, r.id
         LIMIT ? OFFSET ?",
        vec![count(limit), count(offset)],
    )
}

/// A page of the rooms `options` names, by creation then id: the
/// organization and the status in SQL, the metadata compared on each room,
/// as Python's store compares it, reading no further than the page.
pub(crate) fn find(
    connection: &Connection,
    options: FindRoomsOptions,
) -> Result<Vec<Room>, Failure> {
    let mut conditions = vec!["1 = 1".to_owned()];
    let mut params = Vec::new();
    if let Some(organization) = options.organization_id {
        conditions.push("r.organization_id = ?".to_owned());
        params.push(Sql::Text(organization));
    }
    if let Some(status) = options.status {
        conditions.push("r.status = ?".to_owned());
        params.push(Sql::Text(records::name(&status)?));
    }
    let sql = format!(
        "SELECT r.data, r.delivered_index FROM rooms r WHERE {} ORDER BY r.created_ts, r.id",
        conditions.join(" AND ")
    );
    let Some(filter) = options.metadata_filter.filter(|filter| !filter.is_empty()) else {
        params.extend([count(options.limit), count(options.offset)]);
        return rooms_of(connection, &format!("{sql} LIMIT ? OFFSET ?"), params);
    };
    let offset = usize::try_from(options.offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(options.limit).unwrap_or(usize::MAX);
    Ok(rooms_of(connection, &sql, params)?
        .into_iter()
        .filter(|room| {
            filter
                .iter()
                .all(|(key, value)| room.metadata.get(key) == Some(value))
        })
        .skip(offset)
        .take(limit)
        .collect())
}

/// The newest room of `participant_id`, in the status `options` names: one
/// holding their participant record, or binding them on a channel of the
/// type `options` names, with the Python implementation's SQL
/// (`find_latest_room`).
pub(crate) fn find_latest(
    connection: &Connection,
    participant_id: &ParticipantId,
    options: &FindLatestRoomOptions,
) -> Result<Option<Room>, Failure> {
    let participant = Sql::Text(participant_id.as_str().to_owned());
    let channel_type = match &options.channel_type {
        Some(kind) => Sql::Text(kind.as_str().to_owned()),
        None => Sql::Null,
    };
    let mut conditions = vec![
        "(EXISTS(SELECT 1 FROM participants p WHERE p.room_id = r.id AND p.id = ?)
          OR EXISTS(SELECT 1 FROM bindings b WHERE b.room_id = r.id
                    AND b.participant_id = ?
                    AND (? IS NULL OR b.channel_type = ?)))"
            .to_owned(),
    ];
    let mut params = vec![
        participant.clone(),
        participant,
        channel_type.clone(),
        channel_type,
    ];
    if let Some(status) = options.status {
        conditions.push("r.status = ?".to_owned());
        params.push(Sql::Text(records::name(&status)?));
    }
    let sql = format!(
        "SELECT r.data, r.delivered_index FROM rooms r WHERE {}
         ORDER BY r.created_ts DESC, r.id DESC LIMIT 1",
        conditions.join(" AND ")
    );
    Ok(rooms_of(connection, &sql, params)?.into_iter().next())
}

/// The rooms binding `channel_id` among those `options` admit, the oldest
/// first, at most `options.limit`, of a group binding or not when it names
/// one (RFC §10.4, §14.1 `find_room_ids_by_channel`).
pub(crate) fn find_ids_by_channel(
    connection: &Connection,
    channel_id: &ChannelId,
    options: &FindRoomIdsByChannelOptions,
) -> Result<Vec<RoomId>, Failure> {
    let mut conditions = vec!["b.channel_id = ?".to_owned()];
    let mut params = vec![Sql::Text(channel_id.to_string())];
    filter_rooms(&mut conditions, &mut params, options.room_filter.as_ref())?;
    if let Some(group) = options.group {
        conditions.push("COALESCE(json_extract(b.data, '$.group'), 0) = ?".to_owned());
        params.push(Sql::Integer(i64::from(group)));
    }
    params.push(count(u64::try_from(options.limit).unwrap_or(u64::MAX)));
    let sql = format!(
        "SELECT r.id FROM bindings b JOIN rooms r ON r.id = b.room_id
         WHERE {} ORDER BY r.created_ts, r.id LIMIT ?",
        conditions.join(" AND ")
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(params), |row| row.get::<_, String>(0))?;
    let mut room_ids = Vec::new();
    for row in rows {
        room_ids.push(room_id_of(row?)?);
    }
    Ok(room_ids)
}

/// The names a stored id is compared with for the sender, sorted: their
/// address, and the identity the caller resolved for them, as Python's
/// `sender_names`.
fn sender_names(participant_id: &str, identity_id: Option<&IdentityId>) -> Vec<String> {
    let mut names = vec![participant_id.to_owned()];
    names.extend(identity_id.map(ToString::to_string));
    names.sort();
    names.dedup();
    names
}

/// The newest room of the union of `branches`, each a list of conditions
/// on a row of `from` joined on `rooms r`.
fn newest_of(
    connection: &Connection,
    from: &str,
    branches: &[Vec<String>],
    params: Vec<Sql>,
) -> Result<Option<RoomId>, Failure> {
    let found = branches
        .iter()
        .map(|branch| {
            format!(
                "SELECT r.id, r.created_ts FROM {from} WHERE {}",
                branch.join(" AND ")
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let sql = format!("SELECT id FROM ({found}) ORDER BY created_ts DESC, id DESC LIMIT 1");
    let room_id = connection
        .query_row(&sql, params_from_iter(params), |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    room_id.map(room_id_of).transpose()
}

/// The newest room `options` admit whose binding of `channel_id` names the
/// sender, by `participant_id` or the identity the caller resolved, or,
/// with a recipient key, names their address through its recipient (RFC
/// §5.7, §10.4 step 1, §14.1 `find_room_id_by_binding`), as Python's store
/// reads it: two branches, each read through an index.
pub(crate) fn find_by_binding(
    connection: &Connection,
    channel_id: &ChannelId,
    participant_id: &str,
    options: &FindRoomIdByBindingOptions,
) -> Result<Option<RoomId>, Failure> {
    let names = sender_names(participant_id, options.identity_id.as_ref());
    let mut by_name = vec![
        "b.channel_id = ?".to_owned(),
        format!("b.participant_id IN ({})", placeholders(names.len())),
    ];
    let mut params = vec![Sql::Text(channel_id.to_string())];
    params.extend(names.into_iter().map(Sql::Text));
    filter_rooms(&mut by_name, &mut params, options.room_filter.as_ref())?;
    let mut branches = vec![by_name];
    if let Some(recipient_key) = &options.recipient_key
        && !participant_id.trim().is_empty()
    {
        // A binding naming no correspondent, not a group, names its
        // recipient (RFC §5.7): its value under the key, a string or an
        // integer, compared to the address.
        let mut unnamed = vec![
            "b.channel_id = ?".to_owned(),
            "b.participant_id IS NULL".to_owned(),
            "NOT COALESCE(json_extract(b.data, '$.group'), 0)".to_owned(),
            "EXISTS (SELECT 1 FROM json_each(b.data, '$.metadata') WHERE key = ? \
             AND type IN ('text', 'integer') AND CAST(value AS TEXT) = ?)"
                .to_owned(),
        ];
        params.extend([
            Sql::Text(channel_id.to_string()),
            Sql::Text(recipient_key.clone()),
            Sql::Text(participant_id.to_owned()),
        ]);
        filter_rooms(&mut unnamed, &mut params, options.room_filter.as_ref())?;
        branches.push(unnamed);
    }
    newest_of(
        connection,
        "bindings b JOIN rooms r ON r.id = b.room_id",
        &branches,
        params,
    )
}

/// The newest room `options` admit holding a participant reached through
/// `channel_id` whom the sender's address or identity names, by its id or
/// its identity (RFC §5.5, §10.4 step 1, §14.1
/// `find_room_id_by_participant`), as Python's store reads it: two
/// branches, each read through an index.
pub(crate) fn find_by_participant(
    connection: &Connection,
    channel_id: &ChannelId,
    participant_id: &str,
    options: &FindRoomIdByParticipantOptions,
) -> Result<Option<RoomId>, Failure> {
    let names = sender_names(participant_id, options.identity_id.as_ref());
    let mut params = Vec::new();
    let mut branches = Vec::new();
    for named in ["p.id", "json_extract(p.data, '$.identity_id')"] {
        let mut branch = vec![
            format!("{named} IN ({})", placeholders(names.len())),
            "EXISTS (SELECT 1 FROM json_each(p.data, '$.connected_via') WHERE value = ?)"
                .to_owned(),
        ];
        params.extend(names.iter().cloned().map(Sql::Text));
        params.push(Sql::Text(channel_id.to_string()));
        filter_rooms(&mut branch, &mut params, options.room_filter.as_ref())?;
        branches.push(branch);
    }
    newest_of(
        connection,
        "participants p JOIN rooms r ON r.id = p.room_id",
        &branches,
        params,
    )
}
