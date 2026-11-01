//! The kit's calls on a room's members, `ensure_participant`,
//! `resolve_participant`, `add_member`, `remove_member`, `rename_member`
//! and `is_member` (roomkit-specs `conformance/README.md`, "Calls"), and
//! the identities a scenario's setup stores for them to read.

use std::sync::Arc;

use roomkit::model::{
    ChannelId, Identity, IdentityId, ParticipantId, ParticipantRole, ParticipantStatus, RoomId,
};
use roomkit::{
    AddMemberOptions, ConversationStore, EnsureParticipantOptions, IsMemberOptions,
    RemoveMemberOptions, RenameMemberOptions, ResolveParticipantOptions,
};
use serde_json::Value;

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::run::Run;

/// Each call's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "ensure_participant" => &["room_id", "channel_id", "participant_id", "display_name"],
        "resolve_participant" => &["room_id", "participant_id", "identity_id", "resolved_by"],
        "add_member" => &[
            "room_id",
            "channel_id",
            "participant_id",
            "identity_id",
            "display_name",
            "role",
        ],
        "remove_member" => &["room_id", "participant_id", "status"],
        "rename_member" => &["room_id", "participant_id", "display_name"],
        "is_member" => &["room_id", "identity_id"],
        _ => return None,
    })
}

pub(crate) fn handles(call: &str) -> bool {
    arguments(call).is_some()
}

fn organization_id(spec: &Value) -> Option<String> {
    spec.get("organization_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Stores each of a setup's `identities` in the kit's store.
pub(crate) async fn store_identities(run: &Arc<Run>, identities: &Value) -> Result<(), String> {
    for spec in identities.as_array().into_iter().flatten() {
        let identity: Identity =
            serde_json::from_value(spec.clone()).map_err(|error| format!("{spec}: {error}"))?;
        run.kit()
            .store()
            .create_identity(identity)
            .await
            .map_err(|error| format!("create identity {spec}: {error}"))?;
    }
    Ok(())
}

/// Makes the call `spec` names, its references already resolved.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let kit = run.kit();
    let room: RoomId = field(spec, "room_id")?;
    let participant = || field::<ParticipantId>(spec, "participant_id");
    let channel = || field::<ChannelId>(spec, "channel_id");
    let identity = || field::<IdentityId>(spec, "identity_id");
    let outcome = match name {
        "ensure_participant" => {
            let options = EnsureParticipantOptions {
                display_name: optional(spec, "display_name")?,
                organization_id: organization_id(spec),
            };
            let outcome = kit
                .ensure_participant(&room, &channel()?, &participant()?, options)
                .await;
            finish(run, name, outcome, dump)
        }
        "resolve_participant" => {
            let options = ResolveParticipantOptions {
                resolved_by: optional(spec, "resolved_by")?
                    .unwrap_or(ResolveParticipantOptions::default().resolved_by),
                organization_id: organization_id(spec),
            };
            let outcome = kit
                .resolve_participant(&room, &participant()?, &identity()?, options)
                .await;
            finish(run, name, outcome, dump)
        }
        "add_member" => {
            let options = AddMemberOptions {
                identity_id: optional(spec, "identity_id")?,
                display_name: optional(spec, "display_name")?,
                role: optional::<ParticipantRole>(spec, "role")?.unwrap_or_default(),
                organization_id: organization_id(spec),
            };
            let outcome = kit
                .add_member(&room, &channel()?, &participant()?, options)
                .await;
            finish(run, name, outcome, dump)
        }
        "remove_member" => {
            let options = RemoveMemberOptions {
                status: optional::<ParticipantStatus>(spec, "status")?
                    .unwrap_or(ParticipantStatus::Left),
                organization_id: organization_id(spec),
            };
            let outcome = kit.remove_member(&room, &participant()?, options).await;
            finish(run, name, outcome, dump)
        }
        "rename_member" => {
            let options = RenameMemberOptions {
                organization_id: organization_id(spec),
            };
            let display_name: String = field(spec, "display_name")?;
            let outcome = kit
                .rename_member(&room, &participant()?, &display_name, options)
                .await;
            finish(run, name, outcome, dump)
        }
        _ => {
            let options = IsMemberOptions {
                organization_id: organization_id(spec),
            };
            let outcome = kit.is_member(&room, &identity()?, options).await;
            finish(run, name, outcome, dump)
        }
    };
    Ok((outcome, None))
}
