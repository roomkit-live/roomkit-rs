//! The store's calls a scenario names, made on the kit's store: a call
//! `store.<operation>` is the operation of the store's own interface (RFC
//! §14.1; roomkit-specs `conformance/README.md`, "Calls on the store"), its
//! arguments by the operation's names, a record a template completed as the
//! record requires ("Templates").

use std::sync::Arc;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventType, Participant, ParticipantId, Room,
    RoomEvent, RoomId, RoomStatus, Timestamp, Visibility,
};
use roomkit::store::{
    CommitEventOptions, FindLatestRoomOptions, FindRoomIdByChannelOptions, GetTimelineOptions,
};
use roomkit::{ConversationStore, EventFilter, RoomKitError, StoreError};
use serde_json::Value;

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::run::Run;
use crate::templates::{complete_event, complete_participant, now, system_source};

/// Each store call's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "store.create_room" => &["room"],
        "store.delete_room" | "store.room_exists" => &["room_id"],
        "store.add_binding" | "store.update_binding" => &["binding"],
        "store.remove_binding" | "store.binding_exists" => &["room_id", "channel_id"],
        "store.add_participant" => &["participant"],
        "store.commit_event" => &["room_id", "event", "target", "activity"],
        "store.find_latest_room" => &["participant_id", "channel_type", "status"],
        "store.find_room_id_by_channel" => &["channel_id", "status"],
        "store.get_timeline" => &[
            "room_id",
            "event_filter",
            "limit",
            "after_index",
            "newest_first",
        ],
        "store.get_thread_summaries" => &["room_id", "root_event_ids"],
        _ => return None,
    })
}

/// The members an `event_filter` argument may hold, Python's `EventFilter`.
const EVENT_FILTER: &[&str] = &[
    "event_types",
    "exclude_types",
    "visibility",
    "source_channel_id",
    "source_channel_type",
    "correlation_id",
    "participant_id",
    "responds_to",
    "parent_event_id",
    "top_level_only",
    "after_time",
    "before_time",
    "include_blocked",
];

/// A record from `template`, the members `stamped` names written now when
/// it leaves them out.
fn stamped<T: serde::de::DeserializeOwned>(
    template: &Value,
    stamped: &[&str],
    what: &str,
) -> Result<T, Unreadable> {
    let mut record = template.clone();
    if let Value::Object(members) = &mut record {
        for member in stamped {
            members.entry(*member).or_insert_with(now);
        }
    }
    serde_json::from_value(record).map_err(|error| format!("{what}: {error}"))
}

/// An event from the template `spec[key]`, in the call's room.
fn event(spec: &Value, key: &str) -> Result<RoomEvent, Unreadable> {
    let room_id = spec["room_id"].as_str().unwrap_or_default();
    serde_json::from_value(complete_event(&spec[key], room_id, &system_source()))
        .map_err(|error| format!("{key}: {error}"))
}

/// The `event_filter` argument, Python's `EventFilter`.
fn event_filter(spec: &Value) -> Result<Option<EventFilter>, Unreadable> {
    let Some(filter) = spec.get("event_filter") else {
        return Ok(None);
    };
    let unknown: Vec<&String> = filter
        .as_object()
        .ok_or("event_filter: not an object")?
        .keys()
        .filter(|member| !EVENT_FILTER.contains(&member.as_str()))
        .collect();
    if !unknown.is_empty() {
        return Err(format!("event_filter: unknown members {unknown:?}"));
    }
    Ok(Some(EventFilter {
        event_types: optional::<Vec<EventType>>(filter, "event_types")?,
        exclude_types: optional::<Vec<EventType>>(filter, "exclude_types")?,
        visibility: optional::<Visibility>(filter, "visibility")?,
        source_channel_id: optional::<ChannelId>(filter, "source_channel_id")?,
        source_channel_type: optional::<ChannelType>(filter, "source_channel_type")?,
        correlation_id: optional(filter, "correlation_id")?,
        participant_id: optional::<ParticipantId>(filter, "participant_id")?,
        responds_to: optional::<EventId>(filter, "responds_to")?,
        parent_event_id: optional::<EventId>(filter, "parent_event_id")?,
        top_level_only: optional(filter, "top_level_only")?.unwrap_or_default(),
        after_time: optional::<Timestamp>(filter, "after_time")?,
        before_time: optional::<Timestamp>(filter, "before_time")?,
        include_blocked: optional(filter, "include_blocked")?.unwrap_or_default(),
    }))
}

/// A store call's outcome, its error named as a kit call's is.
fn stored<T: serde::Serialize>(run: &Run, call: &str, outcome: Result<T, StoreError>) -> Called {
    finish(run, call, outcome.map_err(RoomKitError::from), dump)
}

/// Makes the store call `spec` names, its references already resolved;
/// returns its outcome and, for one that returns an event, the event's id.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let store = run.kit().store();
    Ok(match name {
        "store.commit_event" => {
            let room: RoomId = field(spec, "room_id")?;
            let target = match spec.get("target") {
                Some(_) => Some(event(spec, "target")?),
                None => None,
            };
            let options = CommitEventOptions {
                target,
                activity: optional(spec, "activity")?.unwrap_or(true),
            };
            let outcome = store
                .commit_event(&room, event(spec, "event")?, options)
                .await;
            let id = outcome.as_ref().ok().map(|event| event.id.to_string());
            (stored(run, name, outcome), id)
        }
        _ => (read_or_write(run, name, spec).await?, None),
    })
}

/// The store calls whose result names no event to label.
async fn read_or_write(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let store = run.kit().store();
    let room_id = || field::<RoomId>(spec, "room_id");
    let channel_id = || field::<ChannelId>(spec, "channel_id");
    Ok(match name {
        "store.create_room" => {
            let room: Room = stamped(&spec["room"], &["created_at", "updated_at"], "room")?;
            stored(run, name, store.create_room(room).await)
        }
        "store.delete_room" => stored(run, name, store.delete_room(&room_id()?).await),
        "store.add_binding" => {
            let binding: ChannelBinding = stamped(&spec["binding"], &["attached_at"], "binding")?;
            stored(run, name, store.add_binding(binding).await)
        }
        "store.update_binding" => {
            let binding: ChannelBinding = stamped(&spec["binding"], &["attached_at"], "binding")?;
            stored(run, name, store.update_binding(binding).await)
        }
        "store.remove_binding" => {
            let outcome = store.remove_binding(&room_id()?, &channel_id()?).await;
            stored(run, name, outcome)
        }
        "store.add_participant" => {
            let template = &spec["participant"];
            let room = template["room_id"].as_str().unwrap_or_default();
            let participant: Participant =
                serde_json::from_value(complete_participant(template, room))
                    .map_err(|error| format!("participant: {error}"))?;
            stored(run, name, store.add_participant(participant).await)
        }
        "store.room_exists" => stored(run, name, store.room_exists(&room_id()?).await),
        "store.binding_exists" => {
            let outcome = store.binding_exists(&room_id()?, &channel_id()?).await;
            stored(run, name, outcome)
        }
        _ => lookup(run, name, spec).await?,
    })
}

/// The store's lookups and timeline reads.
async fn lookup(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let store = run.kit().store();
    Ok(match name {
        "store.find_latest_room" => {
            let participant: ParticipantId = field(spec, "participant_id")?;
            let options = FindLatestRoomOptions {
                channel_type: optional::<ChannelType>(spec, "channel_type")?,
                status: optional::<RoomStatus>(spec, "status")?,
            };
            stored(
                run,
                name,
                store.find_latest_room(&participant, options).await,
            )
        }
        "store.find_room_id_by_channel" => {
            let channel: ChannelId = field(spec, "channel_id")?;
            let options = FindRoomIdByChannelOptions {
                status: optional::<RoomStatus>(spec, "status")?,
            };
            let outcome = store.find_room_id_by_channel(&channel, options).await;
            stored(run, name, outcome)
        }
        "store.get_timeline" => {
            let room: RoomId = field(spec, "room_id")?;
            let defaults = GetTimelineOptions::default();
            let options = GetTimelineOptions {
                event_filter: event_filter(spec)?,
                limit: optional(spec, "limit")?.unwrap_or(defaults.limit),
                after_index: optional(spec, "after_index")?,
                newest_first: optional(spec, "newest_first")?.unwrap_or_default(),
            };
            stored(run, name, store.get_timeline(&room, options).await)
        }
        "store.get_thread_summaries" => {
            let room: RoomId = field(spec, "room_id")?;
            let roots: Vec<EventId> = field(spec, "root_event_ids")?;
            stored(run, name, store.get_thread_summaries(&room, &roots).await)
        }
        other => return Err(format!("no call {other}")),
    })
}
