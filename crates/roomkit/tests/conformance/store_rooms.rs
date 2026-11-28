//! The store's room operations a scenario names beside the first ones of
//! `store_calls`: the whole-record write, the room pages, a room's context,
//! its delivery cursor, its private state and the two room finders (RFC
//! §14.1; roomkit-specs `conformance/README.md`, "Calls on the store").

use std::sync::Arc;

use roomkit::ConversationStore;
use roomkit::RoomFilter;
use roomkit::model::{ChannelId, IdentityId, Metadata, Room, RoomId, RoomStatus};
use roomkit::store::{
    AdvanceDeliveredIndexOptions, FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions,
    FindRoomsOptions, ListRoomsOptions,
};
use serde_json::Value;

use crate::calls::{Called, Unreadable, field, optional};
use crate::run::Run;
use crate::store_calls::{kept_created_at, stamped, stored};

/// Each room operation's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "store.update_room" => &["room"],
        "store.list_rooms" => &["offset", "limit"],
        "store.find_rooms" => &[
            "organization_id",
            "status",
            "metadata_filter",
            "limit",
            "offset",
        ],
        "store.load_room_context" | "store.get_delivered_index" => &["room_id"],
        "store.advance_delivered_index" => &["room_id", "index", "force"],
        "store.get_private_state" => &["room_id", "key"],
        "store.set_private_state" => &["room_id", "key", "value"],
        "store.find_room_id_by_binding" => &[
            "channel_id",
            "participant_id",
            "identity_id",
            "room_filter",
            "recipient_key",
        ],
        "store.find_room_id_by_participant" => {
            &["channel_id", "participant_id", "identity_id", "room_filter"]
        }
        _ => return None,
    })
}

/// The members a `room_filter` argument may hold, Python's `RoomFilter`.
const ROOM_FILTER: &[&str] = &["statuses", "organization_id"];

/// The `room_filter` argument, Python's `RoomFilter`; none when absent.
fn room_filter(spec: &Value) -> Result<Option<RoomFilter>, Unreadable> {
    let Some(filter) = spec.get("room_filter").filter(|filter| !filter.is_null()) else {
        return Ok(None);
    };
    let unknown: Vec<&String> = filter
        .as_object()
        .ok_or("room_filter: not an object")?
        .keys()
        .filter(|member| !ROOM_FILTER.contains(&member.as_str()))
        .collect();
    if !unknown.is_empty() {
        return Err(format!("room_filter: unknown members {unknown:?}"));
    }
    Ok(Some(RoomFilter {
        statuses: field::<Option<Vec<RoomStatus>>>(filter, "statuses")?,
        organization_id: field::<Option<String>>(filter, "organization_id")?,
    }))
}

/// Makes the room operation `name` names, its references already resolved;
/// `None` for a call that is not one.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Option<Result<Called, Unreadable>> {
    Some(match name {
        "store.update_room" => update_room(run, spec).await,
        "store.list_rooms" | "store.find_rooms" => pages(run, name, spec).await,
        "store.load_room_context"
        | "store.get_delivered_index"
        | "store.advance_delivered_index" => context_and_cursor(run, name, spec).await,
        "store.get_private_state" | "store.set_private_state" => {
            private_state(run, name, spec).await
        }
        "store.find_room_id_by_binding" | "store.find_room_id_by_participant" => {
            finders(run, name, spec).await
        }
        _ => return None,
    })
}

/// The whole-record write, its room's `created_at` the stored room's when
/// the template leaves it out.
async fn update_room(run: &Arc<Run>, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let template = kept_created_at(store, &spec["room"]).await?;
    let room: Room = stamped(&template, &["created_at", "updated_at"], "room")?;
    Ok(stored(
        run,
        "store.update_room",
        store.update_room(room).await,
    ))
}

/// A page of every room, or of the rooms the call's filters name.
async fn pages(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    if name == "store.list_rooms" {
        let defaults = ListRoomsOptions::default();
        let options = ListRoomsOptions {
            offset: optional(spec, "offset")?.unwrap_or(defaults.offset),
            limit: optional(spec, "limit")?.unwrap_or(defaults.limit),
        };
        return Ok(stored(run, name, store.list_rooms(options).await));
    }
    let defaults = FindRoomsOptions::default();
    let options = FindRoomsOptions {
        organization_id: field::<Option<String>>(spec, "organization_id")?,
        status: field::<Option<RoomStatus>>(spec, "status")?,
        metadata_filter: field::<Option<Metadata>>(spec, "metadata_filter")?,
        limit: optional(spec, "limit")?.unwrap_or(defaults.limit),
        offset: optional(spec, "offset")?.unwrap_or(defaults.offset),
    };
    Ok(stored(run, name, store.find_rooms(options).await))
}

/// A room's context, and its delivery cursor read or moved.
async fn context_and_cursor(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let room: RoomId = field(spec, "room_id")?;
    Ok(match name {
        "store.load_room_context" => stored(run, name, store.load_room_context(&room).await),
        "store.get_delivered_index" => stored(run, name, store.get_delivered_index(&room).await),
        _ => {
            let index: u64 = field(spec, "index")?;
            let options = AdvanceDeliveredIndexOptions {
                force: optional(spec, "force")?.unwrap_or_default(),
            };
            let outcome = store.advance_delivered_index(&room, index, options).await;
            stored(run, name, outcome)
        }
    })
}

/// A room's private state under a key, read or written.
async fn private_state(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let (room, key): (RoomId, String) = (field(spec, "room_id")?, field(spec, "key")?);
    Ok(if name == "store.get_private_state" {
        stored(run, name, store.get_private_state(&room, &key).await)
    } else {
        let value: Option<Metadata> = field(spec, "value")?;
        stored(run, name, store.set_private_state(&room, &key, value).await)
    })
}

/// The two room finders of a sender, by a binding or a participant record.
async fn finders(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let channel: ChannelId = field(spec, "channel_id")?;
    let participant: String = field(spec, "participant_id")?;
    let identity_id = field::<Option<IdentityId>>(spec, "identity_id")?;
    let room_filter = room_filter(spec)?;
    Ok(if name == "store.find_room_id_by_binding" {
        let options = FindRoomIdByBindingOptions {
            identity_id,
            room_filter,
            recipient_key: field::<Option<String>>(spec, "recipient_key")?,
        };
        let outcome = store
            .find_room_id_by_binding(&channel, &participant, options)
            .await;
        stored(run, name, outcome)
    } else {
        let options = FindRoomIdByParticipantOptions {
            identity_id,
            room_filter,
        };
        let outcome = store
            .find_room_id_by_participant(&channel, &participant, options)
            .await;
        stored(run, name, outcome)
    })
}
