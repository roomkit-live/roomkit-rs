//! The store's reads and writes of a room's records a scenario names beside
//! the first ones of `store_calls`: its conversation and its count of
//! events, its bindings, its participants and its observations (RFC §14.1;
//! roomkit-specs `conformance/README.md`, "Calls on the store").

use std::sync::Arc;

use roomkit::ConversationStore;
use roomkit::model::{ChannelId, Observation, ParticipantId, RoomId};
use roomkit::store::{GetConversationOptions, GetEventCountOptions};
use serde_json::Value;

use crate::calls::{Called, Unreadable, field, optional};
use crate::run::Run;
use crate::store_calls::{event_filter, stored};
use crate::templates::complete_observation;

/// Each record operation's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "store.get_conversation" => &["room_id", "limit", "after_index"],
        "store.get_event_count" => &["room_id", "event_filter"],
        "store.get_binding" => &["room_id", "channel_id"],
        "store.delete_participant" => &["room_id", "participant_id"],
        "store.list_bindings" | "store.list_participants" | "store.list_observations" => {
            &["room_id"]
        }
        "store.add_observation" => &["observation"],
        _ => return None,
    })
}

/// Makes the record operation `name` names, its references already
/// resolved; `None` for a call that is not one.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Option<Result<Called, Unreadable>> {
    Some(match name {
        "store.get_conversation" | "store.get_event_count" => events(run, name, spec).await,
        "store.get_binding" | "store.list_bindings" => bindings(run, name, spec).await,
        "store.delete_participant" | "store.list_participants" => {
            participants(run, name, spec).await
        }
        "store.add_observation" | "store.list_observations" => observations(run, name, spec).await,
        _ => return None,
    })
}

/// The room's conversation, and its count of events.
async fn events(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let room: RoomId = field(spec, "room_id")?;
    Ok(if name == "store.get_conversation" {
        let options = GetConversationOptions {
            limit: optional(spec, "limit")?.unwrap_or(GetConversationOptions::default().limit),
            after_index: field::<Option<u64>>(spec, "after_index")?,
        };
        stored(run, name, store.get_conversation(&room, options).await)
    } else {
        let options = GetEventCountOptions {
            event_filter: event_filter(spec)?,
        };
        stored(run, name, store.get_event_count(&room, options).await)
    })
}

/// A binding read by its room and channel, and a room's bindings.
async fn bindings(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let room: RoomId = field(spec, "room_id")?;
    Ok(if name == "store.get_binding" {
        let channel: ChannelId = field(spec, "channel_id")?;
        stored(run, name, store.get_binding(&room, &channel).await)
    } else {
        stored(run, name, store.list_bindings(&room).await)
    })
}

/// A participant removed from its room, and a room's participants.
async fn participants(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let room: RoomId = field(spec, "room_id")?;
    Ok(if name == "store.delete_participant" {
        let participant: ParticipantId = field(spec, "participant_id")?;
        stored(
            run,
            name,
            store.delete_participant(&room, &participant).await,
        )
    } else {
        stored(run, name, store.list_participants(&room).await)
    })
}

/// An observation added from its template, its room and channel given,
/// and a room's observations.
async fn observations(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    if name == "store.list_observations" {
        let room: RoomId = field(spec, "room_id")?;
        return Ok(stored(run, name, store.list_observations(&room).await));
    }
    let template = &spec["observation"];
    let (room_id, channel_id): (String, String) =
        (field(template, "room_id")?, field(template, "channel_id")?);
    let observation: Observation =
        serde_json::from_value(complete_observation(template, &room_id, &channel_id))
            .map_err(|error| format!("observation: {error}"))?;
    Ok(stored(run, name, store.add_observation(observation).await))
}
