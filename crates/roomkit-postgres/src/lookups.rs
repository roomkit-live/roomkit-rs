//! The rooms a store pages, and the routing lookups (RFC §10.4 step 1, §14.1
//! `list_rooms`, `find_rooms`, `find_latest_room`, `find_room_ids_by_channel`,
//! `find_room_id_by_binding`, `find_room_id_by_participant`), with the Python
//! implementation's SQL: room ids break ties byte by byte (`COLLATE "C"`), as
//! the other stores compare them, whatever collation the database has. Every
//! value is a parameter; the statements' text holds only this module's own.

use deadpool_postgres::GenericClient;
use roomkit::RoomFilter;
use roomkit::model::{ChannelId, IdentityId, ParticipantId, Room, RoomId};
use roomkit::store::{
    FindLatestRoomOptions, FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions,
    FindRoomIdsByChannelOptions, FindRoomsOptions,
};
use serde_json::Value;

use crate::error::Failure;
use crate::rows::{self, ROOM_COLUMNS};
use crate::values::{Owned, borrowed, bound, name, query, query_opt};

/// The room id a row's column holds.
fn room_id_of(id: String) -> Result<RoomId, Failure> {
    Ok(serde_json::from_value(Value::String(id))?)
}

/// `values` with `value` added; the number it binds.
fn bind(values: &mut Vec<Owned>, value: Owned) -> usize {
    values.push(value);
    values.len()
}

/// The conditions narrowing a lookup joined on `rooms r` to the statuses and
/// the organization `room_filter` names (RFC §10.4, §14.1, §17.2), as
/// Python's `_room_filter`.
fn room_filter(
    values: &mut Vec<Owned>,
    room_filter: Option<&RoomFilter>,
) -> Result<String, Failure> {
    let mut clause = String::new();
    let Some(room_filter) = room_filter else {
        return Ok(clause);
    };
    if let Some(statuses) = &room_filter.statuses {
        let names = statuses.iter().map(name).collect::<Result<Vec<_>, _>>()?;
        let number = bind(values, Box::new(names));
        clause = format!("{clause} AND r.status = ANY(${number}::text[])");
    }
    if let Some(organization) = &room_filter.organization_id {
        let number = bind(values, Box::new(organization.clone()));
        clause = format!("{clause} AND r.organization_id = ${number}");
    }
    Ok(clause)
}

/// A page of every room, by creation then id (RFC §14.1 `list_rooms`).
pub(crate) async fn list<C: GenericClient>(
    client: &C,
    offset: u64,
    limit: u64,
) -> Result<Vec<Room>, Failure> {
    let rows = query(
        client,
        &format!(
            "SELECT {ROOM_COLUMNS} FROM rooms ORDER BY created_at, id COLLATE \"C\" \
             LIMIT $1 OFFSET $2"
        ),
        &[&bound(limit), &bound(offset)],
    )
    .await?;
    rows.iter().map(rows::room).collect()
}

/// A page of the rooms `options` names, by creation then id, the metadata
/// compared by the column's containment.
pub(crate) async fn find<C: GenericClient>(
    client: &C,
    options: FindRoomsOptions,
) -> Result<Vec<Room>, Failure> {
    let mut clauses = Vec::new();
    let mut values: Vec<Owned> = Vec::new();
    if let Some(organization) = options.organization_id {
        let number = bind(&mut values, Box::new(organization));
        clauses.push(format!("organization_id = ${number}"));
    }
    if let Some(status) = options.status {
        let number = bind(&mut values, Box::new(name(&status)?));
        clauses.push(format!("status = ${number}"));
    }
    if let Some(filter) = options.metadata_filter.filter(|filter| !filter.is_empty()) {
        let number = bind(&mut values, Box::new(Value::Object(filter)));
        clauses.push(format!("metadata @> ${number}::jsonb"));
    }
    let clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let limit = bind(&mut values, Box::new(bound(options.limit)));
    let offset = bind(&mut values, Box::new(bound(options.offset)));
    let rows = query(
        client,
        &format!(
            "SELECT {ROOM_COLUMNS} FROM rooms {clause} \
             ORDER BY created_at, id COLLATE \"C\" LIMIT ${limit} OFFSET ${offset}"
        ),
        &borrowed(&values),
    )
    .await?;
    rows.iter().map(rows::room).collect()
}

/// The newest room of `participant_id`, in the status `options` names: one
/// holding their participant record, or binding them on a channel of the type
/// `options` names.
pub(crate) async fn find_latest<C: GenericClient>(
    client: &C,
    participant_id: &ParticipantId,
    options: &FindLatestRoomOptions,
) -> Result<Option<Room>, Failure> {
    let mut values: Vec<Owned> = vec![Box::new(participant_id.as_str().to_owned())];
    let mut sql = format!(
        "SELECT {} FROM rooms r WHERE (\
           EXISTS (SELECT 1 FROM participants p WHERE p.room_id = r.id AND p.id = $1) \
           OR EXISTS (SELECT 1 FROM bindings b WHERE b.room_id = r.id AND b.participant_id = $1",
        ROOM_COLUMNS
            .split(", ")
            .map(|column| format!("r.{column}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(kind) = &options.channel_type {
        let number = bind(&mut values, Box::new(kind.as_str().to_owned()));
        sql = format!("{sql} AND b.channel_type = ${number}");
    }
    sql.push_str("))");
    if let Some(status) = options.status {
        let number = bind(&mut values, Box::new(name(&status)?));
        sql = format!("{sql} AND r.status = ${number}");
    }
    sql.push_str(" ORDER BY r.created_at DESC, r.id COLLATE \"C\" DESC LIMIT 1");
    let row = query_opt(client, &sql, &borrowed(&values)).await?;
    row.as_ref().map(rows::room).transpose()
}

/// The rooms binding `channel_id` among those `options` admit, the oldest
/// first, at most `options.limit`, of a group binding or not when it names
/// one. `ORDER BY` is no decoration: without it the same stored state could
/// route two ways (RFC §10.4).
pub(crate) async fn find_ids_by_channel<C: GenericClient>(
    client: &C,
    channel_id: &ChannelId,
    options: &FindRoomIdsByChannelOptions,
) -> Result<Vec<RoomId>, Failure> {
    let mut values: Vec<Owned> = vec![Box::new(channel_id.as_str().to_owned())];
    let mut sql = "SELECT b.room_id FROM bindings b JOIN rooms r ON r.id = b.room_id \
                   WHERE b.channel_id = $1"
        .to_owned();
    sql.push_str(&room_filter(&mut values, options.room_filter.as_ref())?);
    if let Some(group) = options.group {
        let number = bind(&mut values, Box::new(group));
        sql = format!("{sql} AND b.is_group = ${number}");
    }
    let limit = u64::try_from(options.limit).unwrap_or(u64::MAX);
    let number = bind(&mut values, Box::new(bound(limit)));
    let sql = format!("{sql} ORDER BY r.created_at, b.room_id COLLATE \"C\" LIMIT ${number}");
    let rows = query(client, &sql, &borrowed(&values)).await?;
    rows.iter()
        .map(|row| room_id_of(row.try_get("room_id")?))
        .collect()
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

/// The newest room `options` admit whose binding of `channel_id` names the
/// sender, or, with a recipient key, names their address through its
/// recipient (RFC §5.7): two branches, each read through
/// `idx_bindings_channel_participant`, where an `OR` would read every binding
/// of the channel.
pub(crate) async fn find_by_binding<C: GenericClient>(
    client: &C,
    channel_id: &ChannelId,
    participant_id: &str,
    options: &FindRoomIdByBindingOptions,
) -> Result<Option<RoomId>, Failure> {
    const SELECT: &str =
        "SELECT b.room_id, r.created_at FROM bindings b JOIN rooms r ON r.id = b.room_id";
    let names = sender_names(participant_id, options.identity_id.as_ref());
    let mut values: Vec<Owned> = vec![Box::new(channel_id.as_str().to_owned()), Box::new(names)];
    let filter = room_filter(&mut values, options.room_filter.as_ref())?;
    let mut branches = vec![format!(
        "{SELECT} WHERE b.channel_id = $1 AND b.participant_id = ANY($2::text[]){filter}"
    )];
    // The value is compared to the address: a blank address finds none.
    if let Some(recipient_key) = &options.recipient_key
        && !participant_id.trim().is_empty()
    {
        let key = bind(&mut values, Box::new(recipient_key.clone()));
        let address = bind(&mut values, Box::new(participant_id.to_owned()));
        let filter = room_filter(&mut values, options.room_filter.as_ref())?;
        branches.push(format!(
            "{SELECT} WHERE b.channel_id = $1 AND b.participant_id IS NULL AND NOT b.is_group \
             AND CASE jsonb_typeof(b.metadata -> ${key}) WHEN 'string' THEN true \
             WHEN 'number' THEN b.metadata ->> ${key} ~ '^-?[0-9]+$' ELSE false END \
             AND b.metadata ->> ${key} = ${address}{filter}"
        ));
    }
    let sql = format!(
        "SELECT room_id FROM ({}) found ORDER BY created_at DESC, room_id COLLATE \"C\" DESC \
         LIMIT 1",
        branches.join(" UNION ALL ")
    );
    let row = query_opt(client, &sql, &borrowed(&values)).await?;
    row.map(|row| room_id_of(row.try_get("room_id")?))
        .transpose()
}

/// The newest room `options` admit holding a participant reached through
/// `channel_id`, whatever their status (RFC §5.5), named by their id, their
/// identity or the sender's own record (RFC §11.2).
pub(crate) async fn find_by_participant<C: GenericClient>(
    client: &C,
    channel_id: &ChannelId,
    participant_id: &str,
    options: &FindRoomIdByParticipantOptions,
) -> Result<Option<RoomId>, Failure> {
    let mut names = sender_names(participant_id, options.identity_id.as_ref());
    names.push(roomkit_core::rules::routing::own_record_id(
        channel_id.as_str(),
        participant_id,
    ));
    names.sort();
    names.dedup();
    let mut values: Vec<Owned> = vec![Box::new(names), Box::new(channel_id.as_str().to_owned())];
    let mut sql = "SELECT p.room_id FROM participants p JOIN rooms r ON r.id = p.room_id \
                   WHERE (p.id = ANY($1::text[]) OR p.identity_id = ANY($1::text[])) \
                   AND $2 = ANY(p.connected_via)"
        .to_owned();
    sql.push_str(&room_filter(&mut values, options.room_filter.as_ref())?);
    sql.push_str(" ORDER BY r.created_at DESC, p.room_id COLLATE \"C\" DESC LIMIT 1");
    let row = query_opt(client, &sql, &borrowed(&values)).await?;
    row.map(|row| room_id_of(row.try_get("room_id")?))
        .transpose()
}
