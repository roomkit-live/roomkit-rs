//! A room's bindings and participants (RFC §14.1), with the Python
//! implementation's SQL.

use deadpool_postgres::GenericClient;
use roomkit::StoreError;
use roomkit::model::{ChannelBinding, ChannelId, IdentityId, Participant, ParticipantId, RoomId};
use serde_json::Value;

use crate::error::Failure;
use crate::events::room_gone;
use crate::rooms;
use crate::rows::{self, BINDING_COLUMNS, PARTICIPANT_COLUMNS};
use crate::values::{Instant, Param, execute, integer, jsonb_read, name, query, query_opt};

/// A binding's values, each column's form, in the order [`add_binding`] and
/// [`update_binding`] bind them.
struct BindingValues {
    channel_type: String,
    category: String,
    direction: String,
    access: String,
    visibility: String,
    participant: Option<String>,
    last_read_index: Option<i32>,
    capabilities: Value,
    metadata: Value,
    rate_limit: Option<Value>,
    retry_policy: Option<Value>,
}

impl BindingValues {
    fn of(binding: &ChannelBinding) -> Result<Self, Failure> {
        Ok(Self {
            channel_type: binding.channel_type.as_str().to_owned(),
            category: name(&binding.category)?,
            direction: name(&binding.direction)?,
            access: name(&binding.access)?,
            visibility: name(&binding.visibility)?,
            participant: binding
                .participant_id
                .as_ref()
                .map(|id| id.as_str().to_owned()),
            last_read_index: binding.last_read_index.map(integer).transpose()?,
            capabilities: serde_json::to_value(&binding.capabilities)?,
            metadata: Value::Object(binding.metadata.clone()),
            rate_limit: binding
                .rate_limit
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?,
            retry_policy: binding
                .retry_policy
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?,
        })
    }
}

/// Stores `binding`, in place of the room's binding of its channel; its
/// `attached_at` stays the first's. A member the model does not read, of the
/// capabilities or of a policy, stays. Refused [`StoreError::RoomNotFound`]
/// for a room the store lacks (RFC §14.3).
pub(crate) async fn add_binding<C: GenericClient>(
    client: &C,
    binding: &ChannelBinding,
) -> Result<(), Failure> {
    let values = BindingValues::of(binding)?;
    let attached_at = Instant(binding.attached_at);
    let written = execute(
        client,
        concat!(
            "INSERT INTO bindings \
             (channel_id, room_id, channel_type, category, direction, \
              access, muted, output_muted, visibility, participant_id, \
              last_read_index, attached_at, capabilities, metadata, \
              rate_limit, retry_policy, is_group) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) \
             ON CONFLICT (room_id, channel_id) DO UPDATE SET \
              channel_type=$3, category=$4, direction=$5, access=$6, \
              muted=$7, output_muted=$8, visibility=$9, participant_id=$10, \
              last_read_index=$11, capabilities=",
            jsonb_read!("bindings.capabilities"),
            " || $13, metadata=$14, \
              rate_limit=CASE WHEN $15::jsonb IS NULL THEN NULL ELSE COALESCE(",
            jsonb_read!("bindings.rate_limit"),
            ", '{}'::jsonb) || $15 END, \
              retry_policy=CASE WHEN $16::jsonb IS NULL THEN NULL ELSE COALESCE(",
            jsonb_read!("bindings.retry_policy"),
            ", '{}'::jsonb) || $16 END, is_group=$17"
        ),
        &[
            &binding.channel_id.as_str(),
            &binding.room_id.as_str(),
            &values.channel_type,
            &values.category,
            &values.direction,
            &values.access,
            &binding.muted,
            &binding.output_muted,
            &values.visibility,
            &values.participant,
            &values.last_read_index,
            &attached_at,
            &values.capabilities,
            &values.metadata,
            &values.rate_limit,
            &values.retry_policy,
            &binding.group,
        ],
    )
    .await;
    match written {
        Ok(_) => Ok(()),
        Err(Failure::Database(error)) => Err(room_gone(error, &binding.room_id)),
        Err(other) => Err(other),
    }
}

/// Writes `binding` over the room's binding of its channel, if there is one;
/// its `attached_at` stays. Refused [`StoreError::RoomNotFound`] for a room
/// the store lacks (RFC §14.3).
pub(crate) async fn update_binding<C: GenericClient>(
    client: &C,
    binding: &ChannelBinding,
) -> Result<(), Failure> {
    let values = BindingValues::of(binding)?;
    let updated = execute(
        client,
        concat!(
            "UPDATE bindings SET channel_type=$3, category=$4, direction=$5, \
             access=$6, muted=$7, output_muted=$8, visibility=$9, \
             participant_id=$10, last_read_index=$11, capabilities=",
            jsonb_read!("capabilities"),
            " || $12, metadata=$13, \
             rate_limit=CASE WHEN $14::jsonb IS NULL THEN NULL ELSE COALESCE(",
            jsonb_read!("rate_limit"),
            ", '{}'::jsonb) || $14 END, \
             retry_policy=CASE WHEN $15::jsonb IS NULL THEN NULL ELSE COALESCE(",
            jsonb_read!("retry_policy"),
            ", '{}'::jsonb) || $15 END, is_group=$16 \
             WHERE room_id=$1 AND channel_id=$2"
        ),
        &[
            &binding.room_id.as_str(),
            &binding.channel_id.as_str(),
            &values.channel_type,
            &values.category,
            &values.direction,
            &values.access,
            &binding.muted,
            &binding.output_muted,
            &values.visibility,
            &values.participant,
            &values.last_read_index,
            &values.capabilities,
            &values.metadata,
            &values.rate_limit,
            &values.retry_policy,
            &binding.group,
        ],
    )
    .await?;
    if updated == 0 && !rooms::exists(client, &binding.room_id).await? {
        return Err(StoreError::RoomNotFound(binding.room_id.clone()).into());
    }
    Ok(())
}

/// The room's binding of `channel_id`.
pub(crate) async fn binding<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<Option<ChannelBinding>, Failure> {
    let row = query_opt(
        client,
        &format!("SELECT {BINDING_COLUMNS} FROM bindings WHERE room_id = $1 AND channel_id = $2"),
        &[&room_id.as_str(), &channel_id.as_str()],
    )
    .await?;
    row.as_ref().map(rows::binding).transpose()
}

/// Whether the room binds `channel_id`, its row unread.
pub(crate) async fn binding_exists<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<bool, Failure> {
    let found = query_opt(
        client,
        "SELECT 1 FROM bindings WHERE room_id = $1 AND channel_id = $2",
        &[&room_id.as_str(), &channel_id.as_str()],
    )
    .await?;
    Ok(found.is_some())
}

/// Removes the room's binding of `channel_id`; whether it held one.
pub(crate) async fn remove_binding<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    channel_id: &ChannelId,
) -> Result<bool, Failure> {
    let removed = execute(
        client,
        "DELETE FROM bindings WHERE room_id = $1 AND channel_id = $2",
        &[&room_id.as_str(), &channel_id.as_str()],
    )
    .await?;
    Ok(removed == 1)
}

/// The room's bindings.
pub(crate) async fn bindings<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<Vec<ChannelBinding>, Failure> {
    let rows = query(
        client,
        &format!(
            "SELECT {BINDING_COLUMNS} FROM bindings WHERE room_id = $1 ORDER BY attachment_order"
        ),
        &[&room_id.as_str()],
    )
    .await?;
    rows.iter().map(rows::binding).collect()
}

/// A participant's values, each column's form.
struct ParticipantValues<'a> {
    role: String,
    status: String,
    identification: String,
    identity: Option<&'a str>,
    candidates: Option<Vec<&'a str>>,
    connected_via: Vec<&'a str>,
    resolved_at: Option<Instant>,
    metadata: Value,
}

impl<'a> ParticipantValues<'a> {
    fn of(participant: &'a Participant) -> Result<Self, Failure> {
        Ok(Self {
            role: name(&participant.role)?,
            status: name(&participant.status)?,
            identification: name(&participant.identification)?,
            identity: participant.identity_id.as_ref().map(IdentityId::as_str),
            candidates: participant
                .candidates
                .as_ref()
                .map(|candidates| candidates.iter().map(IdentityId::as_str).collect()),
            connected_via: participant
                .connected_via
                .iter()
                .map(ChannelId::as_str)
                .collect(),
            resolved_at: participant.resolved_at.map(Instant),
            metadata: Value::Object(participant.metadata.clone()),
        })
    }
}

/// Stores a new participant: refused [`StoreError::ParticipantExists`] for an
/// id its room has, [`StoreError::RoomNotFound`] for a room the store lacks
/// (RFC §14.3).
pub(crate) async fn add_participant<C: GenericClient>(
    client: &C,
    participant: &Participant,
) -> Result<(), Failure> {
    let values = ParticipantValues::of(participant)?;
    let params: [Param<'_>; 15] = [
        &participant.id.as_str(),
        &participant.room_id.as_str(),
        &participant.channel_id.as_str(),
        &participant.display_name,
        &values.role,
        &values.status,
        &values.identification,
        &values.identity,
        &participant.external_id,
        &Instant(participant.joined_at),
        &values.resolved_at,
        &participant.resolved_by,
        &values.metadata,
        &values.connected_via,
        &values.candidates,
    ];
    let inserted = execute(
        client,
        "INSERT INTO participants \
         (id, room_id, channel_id, display_name, role, status, \
          identification, identity_id, external_id, joined_at, \
          resolved_at, resolved_by, metadata, connected_via, candidates) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
         ON CONFLICT (room_id, id) DO NOTHING",
        &params,
    )
    .await;
    match inserted {
        Ok(0) => Err(StoreError::ParticipantExists {
            room_id: participant.room_id.clone(),
            participant_id: participant.id.clone(),
        }
        .into()),
        Ok(_) => Ok(()),
        Err(Failure::Database(error)) => Err(room_gone(error, &participant.room_id)),
        Err(other) => Err(other),
    }
}

/// The room's participant `participant_id`.
pub(crate) async fn participant<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    participant_id: &ParticipantId,
) -> Result<Option<Participant>, Failure> {
    let row = query_opt(
        client,
        &format!("SELECT {PARTICIPANT_COLUMNS} FROM participants WHERE room_id = $1 AND id = $2"),
        &[&room_id.as_str(), &participant_id.as_str()],
    )
    .await?;
    row.as_ref().map(rows::participant).transpose()
}

/// Writes `participant` over its row, refused
/// [`StoreError::ParticipantNotFound`] when its room has none.
pub(crate) async fn update_participant<C: GenericClient>(
    client: &C,
    participant: &Participant,
) -> Result<(), Failure> {
    let values = ParticipantValues::of(participant)?;
    let updated = execute(
        client,
        "UPDATE participants SET channel_id=$3, display_name=$4, role=$5, \
         status=$6, identification=$7, identity_id=$8, external_id=$9, \
         resolved_at=$10, resolved_by=$11, metadata=$12, connected_via=$13, candidates=$14 \
         WHERE room_id=$1 AND id=$2",
        &[
            &participant.room_id.as_str(),
            &participant.id.as_str(),
            &participant.channel_id.as_str(),
            &participant.display_name,
            &values.role,
            &values.status,
            &values.identification,
            &values.identity,
            &participant.external_id,
            &values.resolved_at,
            &participant.resolved_by,
            &values.metadata,
            &values.connected_via,
            &values.candidates,
        ],
    )
    .await?;
    if updated == 0 {
        return Err(StoreError::ParticipantNotFound {
            room_id: participant.room_id.clone(),
            participant_id: participant.id.clone(),
        }
        .into());
    }
    Ok(())
}

/// Removes the room's participant `participant_id`, if it has one.
pub(crate) async fn delete_participant<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
    participant_id: &ParticipantId,
) -> Result<(), Failure> {
    execute(
        client,
        "DELETE FROM participants WHERE room_id = $1 AND id = $2",
        &[&room_id.as_str(), &participant_id.as_str()],
    )
    .await?;
    Ok(())
}

/// The room's participants.
pub(crate) async fn participants<C: GenericClient>(
    client: &C,
    room_id: &RoomId,
) -> Result<Vec<Participant>, Failure> {
    let rows = query(
        client,
        &format!("SELECT {PARTICIPANT_COLUMNS} FROM participants WHERE room_id = $1"),
        &[&room_id.as_str()],
    )
    .await?;
    rows.iter().map(rows::participant).collect()
}
