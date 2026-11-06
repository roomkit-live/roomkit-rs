//! A row into its record, Python's `postgres_mappers`: each record is read
//! through the JSON form its model reads, so that a row of the Python
//! implementation meets the same checks a record of its wire does.

use roomkit::model::{
    ChannelBinding, ChannelType, Identity, Observation, Participant, Room, RoomEvent, Task,
};
use serde_json::{Map, Value, json};
use tokio_postgres::Row;

use crate::error::Failure;
use crate::values::Instant;

/// The columns of `rooms`, named so that a column a later version adds does
/// not change the shape of a statement a connection prepared.
pub(crate) const ROOM_COLUMNS: &str = "id, organization_id, status, event_count, latest_index, \
     delivered_index, metadata, timers, agent_response_policy, created_at, updated_at, paused_at, \
     closed_at";

/// The columns of `events`.
pub(crate) const EVENT_COLUMNS: &str = "id, room_id, type, content, source_channel_id, \
     source_channel_type, source_direction, source_participant_id, source_provider, source_extra, \
     status, visibility, addressed_to, response_visibility, index, chain_depth, correlation_id, \
     parent_event_id, responds_to, idempotency_key, blocked_by, metadata, channel_data, \
     created_at, delivery_results";

/// The columns of `bindings`.
pub(crate) const BINDING_COLUMNS: &str = "channel_id, room_id, channel_type, category, direction, \
     access, muted, output_muted, visibility, participant_id, is_group, last_read_index, \
     attached_at, capabilities, metadata, rate_limit, retry_policy";

/// The columns of `participants`.
pub(crate) const PARTICIPANT_COLUMNS: &str = "id, room_id, channel_id, display_name, role, \
     status, identification, identity_id, candidates, connected_via, external_id, joined_at, \
     resolved_at, resolved_by, metadata";

/// The columns of `identities`.
pub(crate) const IDENTITY_COLUMNS: &str = "id, organization_id, display_name, email, phone, \
     channel_addresses, external_id, external_ids, metadata";

/// The columns of `tasks`.
pub(crate) const TASK_COLUMNS: &str =
    "id, room_id, title, description, assigned_to, status, created_at, metadata";

/// The columns of `observations`.
pub(crate) const OBSERVATION_COLUMNS: &str =
    "id, room_id, channel_id, content, category, confidence, created_at, metadata";

/// A text column that may be `NULL`.
fn text(row: &Row, column: &str) -> Result<Option<String>, Failure> {
    Ok(row.try_get(column)?)
}

/// A column's instant as its record's JSON writes it.
fn instant(row: &Row, column: &str) -> Result<Option<String>, Failure> {
    let at: Option<Instant> = row.try_get(column)?;
    Ok(at.map(|at| at.0.to_string()))
}

/// A JSONB column, as [`jsonb_value`] reads it.
fn jsonb(row: &Row, column: &str) -> Result<Value, Failure> {
    jsonb_value(row.try_get(column)?)
}

/// A nullable JSONB column, as [`jsonb_value`] reads it.
fn nullable_jsonb(row: &Row, column: &str) -> Result<Option<Value>, Failure> {
    row.try_get::<_, Option<Value>>(column)?
        .map(jsonb_value)
        .transpose()
}

/// A JSONB column's value as Python's store reads it: its pool decodes the
/// column, and its mappers decode a string once more, so that a JSON string
/// reads as the value its text holds, and one holding no JSON text fails
/// (HITL f9737c41). Such a row comes from a write by hand.
pub(crate) fn jsonb_value(value: Value) -> Result<Value, Failure> {
    match value {
        Value::String(text) => Ok(serde_json::from_str(&text)?),
        value @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_)) => Ok(value),
    }
}

/// The room a row holds.
pub(crate) fn room(row: &Row) -> Result<Room, Failure> {
    Ok(serde_json::from_value(json!({
        "id": text(row, "id")?,
        "organization_id": text(row, "organization_id")?,
        "status": text(row, "status")?,
        "event_count": row.try_get::<_, i32>("event_count")?,
        "latest_index": row.try_get::<_, Option<i32>>("latest_index")?,
        "delivered_index": row.try_get::<_, i32>("delivered_index")?,
        "metadata": jsonb(row, "metadata")?,
        "timers": jsonb(row, "timers")?,
        "agent_response_policy": text(row, "agent_response_policy")?,
        "created_at": instant(row, "created_at")?,
        "updated_at": instant(row, "updated_at")?,
        "paused_at": instant(row, "paused_at")?,
        "closed_at": instant(row, "closed_at")?,
    }))?)
}

/// The event a row holds; columns beyond those of `events` are ignored.
pub(crate) fn event(row: &Row) -> Result<RoomEvent, Failure> {
    let mut source_extra = match jsonb(row, "source_extra")? {
        Value::Object(extra) => extra,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Map::new()
        }
    };
    let mut extra = |member: &str| source_extra.shift_remove(member).unwrap_or(Value::Null);
    let raw_payload = match extra("raw_payload") {
        Value::Null => json!({}),
        payload @ (Value::Bool(_)
        | Value::Number(_)
        | Value::String(_)
        | Value::Array(_)
        | Value::Object(_)) => payload,
    };
    Ok(serde_json::from_value(json!({
        "id": text(row, "id")?,
        "room_id": text(row, "room_id")?,
        "type": text(row, "type")?,
        "content": jsonb(row, "content")?,
        "source": {
            "channel_id": text(row, "source_channel_id")?,
            "channel_type": text(row, "source_channel_type")?,
            "direction": text(row, "source_direction")?,
            "participant_id": text(row, "source_participant_id")?,
            "provider": text(row, "source_provider")?,
            "raw_payload": raw_payload,
            "external_id": extra("external_id"),
            "provider_message_id": extra("provider_message_id"),
            "verification": extra("verification"),
        },
        "status": text(row, "status")?,
        "visibility": text(row, "visibility")?,
        // NULL and [] mean different things: unaddressed, addressed to nobody.
        "addressed_to": row.try_get::<_, Option<Vec<String>>>("addressed_to")?,
        "response_visibility": text(row, "response_visibility")?,
        "index": row.try_get::<_, i32>("index")?,
        "chain_depth": row.try_get::<_, i32>("chain_depth")?,
        "correlation_id": text(row, "correlation_id")?,
        "parent_event_id": text(row, "parent_event_id")?,
        "responds_to": text(row, "responds_to")?,
        "idempotency_key": text(row, "idempotency_key")?,
        "blocked_by": text(row, "blocked_by")?,
        "metadata": jsonb(row, "metadata")?,
        "channel_data": jsonb(row, "channel_data")?,
        "created_at": instant(row, "created_at")?,
        "delivery_results": jsonb(row, "delivery_results")?,
    }))?)
}

/// The binding a row holds.
pub(crate) fn binding(row: &Row) -> Result<ChannelBinding, Failure> {
    let mut binding = json!({
        "channel_id": text(row, "channel_id")?,
        "room_id": text(row, "room_id")?,
        "channel_type": text(row, "channel_type")?,
        "category": text(row, "category")?,
        "direction": text(row, "direction")?,
        "access": text(row, "access")?,
        "muted": row.try_get::<_, bool>("muted")?,
        "output_muted": row.try_get::<_, bool>("output_muted")?,
        "participant_id": text(row, "participant_id")?,
        "group": row.try_get::<_, bool>("is_group")?,
        "last_read_index": row.try_get::<_, Option<i32>>("last_read_index")?,
        "attached_at": instant(row, "attached_at")?,
        "capabilities": jsonb(row, "capabilities")?,
        "metadata": jsonb(row, "metadata")?,
        "rate_limit": nullable_jsonb(row, "rate_limit")?,
        "retry_policy": nullable_jsonb(row, "retry_policy")?,
    });
    // The column is nullable; a binding without one takes the model's default.
    if let (Some(visibility), Value::Object(members)) = (text(row, "visibility")?, &mut binding) {
        members.insert("visibility".to_owned(), Value::String(visibility));
    }
    Ok(serde_json::from_value(binding)?)
}

/// The participant a row holds.
pub(crate) fn participant(row: &Row) -> Result<Participant, Failure> {
    Ok(serde_json::from_value(json!({
        "id": text(row, "id")?,
        "room_id": text(row, "room_id")?,
        "channel_id": text(row, "channel_id")?,
        "display_name": text(row, "display_name")?,
        "role": text(row, "role")?,
        "status": text(row, "status")?,
        "identification": text(row, "identification")?,
        "identity_id": text(row, "identity_id")?,
        // NULL and [] mean different things: none offered, none left.
        "candidates": row.try_get::<_, Option<Vec<String>>>("candidates")?,
        "connected_via": row.try_get::<_, Vec<String>>("connected_via")?,
        "external_id": text(row, "external_id")?,
        "joined_at": instant(row, "joined_at")?,
        "resolved_at": instant(row, "resolved_at")?,
        "resolved_by": text(row, "resolved_by")?,
        "metadata": jsonb(row, "metadata")?,
    }))?)
}

/// Whether `value` is a list of strings, what Python's model reads under each
/// key of an identity's addresses.
pub(crate) fn is_address_list(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|addresses| addresses.iter().all(Value::is_string))
}

/// The identity a row holds. The Python implementation takes any key in its
/// addresses; one that is no channel type's name for roomkit-rs is a member
/// the model does not read, and stays in the row, whose addresses are
/// rewritten from the row itself.
pub(crate) fn identity(row: &Row) -> Result<Identity, Failure> {
    let mut addresses = jsonb(row, "channel_addresses")?;
    if let Value::Object(addresses) = &mut addresses {
        // Python's model takes any key, but a list of strings under each: a
        // member left out must be one it reads.
        if !addresses.values().all(is_address_list) {
            return Err(Failure::OutOfRange("an identity's addresses"));
        }
        addresses.retain(|channel_type, _| ChannelType::new(channel_type.as_str()).is_ok());
    }
    Ok(serde_json::from_value(json!({
        "id": text(row, "id")?,
        "organization_id": text(row, "organization_id")?,
        "display_name": text(row, "display_name")?,
        "email": text(row, "email")?,
        "phone": text(row, "phone")?,
        "channel_addresses": addresses,
        "external_id": text(row, "external_id")?,
        "external_ids": jsonb(row, "external_ids")?,
        "metadata": jsonb(row, "metadata")?,
    }))?)
}

/// The task a row holds.
pub(crate) fn task(row: &Row) -> Result<Task, Failure> {
    Ok(serde_json::from_value(json!({
        "id": text(row, "id")?,
        "room_id": text(row, "room_id")?,
        "title": text(row, "title")?,
        "description": text(row, "description")?,
        "assigned_to": text(row, "assigned_to")?,
        "status": text(row, "status")?,
        "created_at": instant(row, "created_at")?,
        "metadata": jsonb(row, "metadata")?,
    }))?)
}

/// The observation a row holds.
pub(crate) fn observation(row: &Row) -> Result<Observation, Failure> {
    let mut observation = json!({
        "id": text(row, "id")?,
        "room_id": text(row, "room_id")?,
        // Nullable, and read as Python's empty string for no channel.
        "channel_id": text(row, "channel_id")?.unwrap_or_default(),
        "content": text(row, "content")?,
        "category": text(row, "category")?,
        "created_at": instant(row, "created_at")?,
        "metadata": jsonb(row, "metadata")?,
    });
    // Nullable, and read as the model's default without one.
    if let (Some(confidence), Value::Object(members)) = (
        row.try_get::<_, Option<f64>>("confidence")?,
        &mut observation,
    ) {
        members.insert("confidence".to_owned(), json!(confidence));
    }
    Ok(serde_json::from_value(observation)?)
}
