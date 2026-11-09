//! The store's calls a scenario names, made on the store of the kit the
//! run reads through (its own while open, else a process still open, as
//! the store outlives the process that closed), and the
//! kit's calls on a room's read markers: a call
//! `store.<operation>` is the operation of the store's own interface (RFC
//! §14.1; roomkit-specs `conformance/README.md`, "Calls on the store"), its
//! arguments by the operation's names, a record a template completed as the
//! record requires ("Templates").

use std::sync::Arc;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventType, IdentityId, Metadata, Participant,
    ParticipantId, Room, RoomEvent, RoomId, RoomStatus, Task, TaskId, TaskStatus, Timestamp,
    Verification, VerificationRequest, Visibility,
};
use roomkit::store::{
    CommitEventOptions, DeleteEventOptions, FindLatestRoomOptions, FindRoomIdByChannelOptions,
    GetTimelineOptions, GetVerificationOptions, ListTasksOptions,
};
use roomkit::{
    ConversationStore, EventFilter, ListReadMarkersOptions, MarkAllReadOptions, MarkReadOptions,
    RoomKitError, StoreError,
};
use serde_json::Value;

use crate::calls::{Called, Unreadable, dump, field, finish, optional};
use crate::errors::named;
use crate::run::Run;
use crate::templates::{complete_event, complete_participant, complete_task, now, system_source};

/// Each store call's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "store.create_room" => &["room"],
        "store.delete_room"
        | "store.room_exists"
        | "store.list_read_markers"
        | "list_read_markers"
        | "store.get_room" => &["room_id"],
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
        "store.add_event" | "store.update_event" => &["event"],
        "store.get_event" => &["event_id"],
        "store.check_idempotency" | "store.get_event_by_idempotency_key" => {
            &["room_id", "key", "channel_id"]
        }
        "store.record_delivery_results" => &["room_id", "event_id", "results"],
        "store.write_change_target" => &["room_id", "target"],
        "store.update_room_if" => &["room", "status", "paused_at", "last_activity_at"],
        "store.add_task" | "store.update_task" => &["task"],
        "store.get_task" => &["task_id"],
        "store.list_tasks" => &["room_id", "status"],
        "store.add_event_auto_index" => &["room_id", "event"],
        "store.delete_event" => &["room_id", "event_id", "cascade_replies"],
        "store.mark_read" | "mark_read" => &["room_id", "channel_id", "event_id"],
        "store.mark_all_read" | "store.get_unread_count" | "mark_all_read" => {
            &["room_id", "channel_id"]
        }
        "store.put_verification" => &["verification"],
        "store.get_verification" => &["identity_id"],
        "store.list_verifications_in_force" => &[],
        "store.create_verification_request" | "store.update_verification_request" => &["request"],
        "store.get_verification_request" => &["request_id"],
        _ => return None,
    })
}

/// Whether the runner makes `call` here: a store call, or one of the kit's
/// calls on a room's read markers.
pub(crate) fn handles(call: &str) -> bool {
    call.starts_with("store.")
        || matches!(call, "mark_read" | "mark_all_read" | "list_read_markers")
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

/// An event from the template `spec[key]`, in the call's room, or the
/// template's when the call names none.
fn event(spec: &Value, key: &str) -> Result<RoomEvent, Unreadable> {
    let room_id = spec
        .get("room_id")
        .or_else(|| spec[key].get("room_id"))
        .and_then(Value::as_str)
        .unwrap_or_default();
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

/// A store call's outcome, its error named as a kit call's is, but a
/// task's, which no kit call meets.
fn stored<T: serde::Serialize>(run: &Run, call: &str, outcome: Result<T, StoreError>) -> Called {
    match outcome {
        Err(StoreError::TaskNotFound(_)) => Err(named("task_not_found")),
        outcome => finish(run, call, outcome.map_err(RoomKitError::from), dump),
    }
}

/// A task from the template `spec["task"]`, in its own room.
fn task(spec: &Value) -> Result<Task, Unreadable> {
    let template = &spec["task"];
    let room = template["room_id"].as_str().unwrap_or_default();
    serde_json::from_value(complete_task(template, room)).map_err(|error| format!("task: {error}"))
}

/// A store call's outcome that is an event, and the event's id, which the
/// call's label names.
fn stored_event(
    run: &Run,
    call: &str,
    outcome: Result<RoomEvent, StoreError>,
) -> (Called, Option<String>) {
    let id = outcome.as_ref().ok().map(|event| event.id.to_string());
    (stored(run, call, outcome), id)
}

/// Makes the call `spec` names, its references already resolved; returns
/// its outcome and, for one that returns an event, the event's id.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    Ok(match name {
        "store.check_idempotency" | "store.get_event_by_idempotency_key" => {
            (idempotency(run, name, spec).await?, None)
        }
        "store.add_event" => stored_event(run, name, store.add_event(event(spec, "event")?).await),
        "store.update_event" => {
            stored_event(run, name, store.update_event(event(spec, "event")?).await)
        }
        "store.add_event_auto_index" => {
            let room: RoomId = field(spec, "room_id")?;
            let outcome = store
                .add_event_auto_index(&room, event(spec, "event")?)
                .await;
            stored_event(run, name, outcome)
        }
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
            stored_event(run, name, outcome)
        }
        "mark_read" | "mark_all_read" | "list_read_markers" => {
            (read_markers(run, name, spec).await?, None)
        }
        _ => (read_or_write(run, name, spec).await?, None),
    })
}

/// Resolve a key in the scope the caller names, with no channel meaning the room.
async fn idempotency(run: &Run, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let room: RoomId = field(spec, "room_id")?;
    let key: String = field(spec, "key")?;
    let channel: Option<ChannelId> = field(spec, "channel_id")?;
    Ok(if name == "store.check_idempotency" {
        stored(
            run,
            name,
            store.check_idempotency(&room, &key, channel.as_ref()).await,
        )
    } else {
        stored(
            run,
            name,
            store
                .get_event_by_idempotency_key(&room, &key, channel.as_ref())
                .await,
        )
    })
}

/// The store calls whose result names no event to label.
async fn read_or_write(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
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
    let reader = run.reader();
    let store = reader.store();
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
        _ => events_and_markers(run, name, spec).await?,
    })
}

/// The store's hard delete and its read markers.
async fn events_and_markers(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let (room, channel) = (
        || field::<RoomId>(spec, "room_id"),
        || field::<ChannelId>(spec, "channel_id"),
    );
    Ok(match name {
        "store.delete_event" => {
            let event: EventId = field(spec, "event_id")?;
            let options = DeleteEventOptions {
                cascade_replies: optional(spec, "cascade_replies")?.unwrap_or(true),
            };
            stored(
                run,
                name,
                store.delete_event(&room()?, &event, options).await,
            )
        }
        "store.mark_read" => {
            let event: EventId = field(spec, "event_id")?;
            let outcome = store.mark_read(&room()?, &channel()?, &event).await;
            stored(run, name, outcome)
        }
        "store.mark_all_read" => {
            stored(run, name, store.mark_all_read(&room()?, &channel()?).await)
        }
        "store.get_unread_count" => {
            let outcome = store.get_unread_count(&room()?, &channel()?).await;
            stored(run, name, outcome)
        }
        "store.list_read_markers" => stored(run, name, store.list_read_markers(&room()?).await),
        _ => writes_and_tasks(run, name, spec).await?,
    })
}

/// The store's event and room writes, and its tasks.
async fn writes_and_tasks(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let room = || field::<RoomId>(spec, "room_id");
    Ok(match name {
        "store.get_event" => {
            let event: EventId = field(spec, "event_id")?;
            stored(run, name, store.get_event(&event).await)
        }
        "store.record_delivery_results" => {
            let (event, results): (EventId, Metadata) =
                (field(spec, "event_id")?, field(spec, "results")?);
            let outcome = store
                .record_delivery_results(&room()?, &event, results)
                .await;
            stored(run, name, outcome)
        }
        "store.write_change_target" => {
            let outcome = store
                .write_change_target(&room()?, event(spec, "target")?)
                .await;
            stored(run, name, outcome)
        }
        "store.get_room" => stored(run, name, store.get_room(&room()?).await),
        "store.update_room_if" => {
            let room: Room = stamped(&spec["room"], &["created_at", "updated_at"], "room")?;
            let status: RoomStatus = field(spec, "status")?;
            let (paused_at, last_activity_at): (Option<Timestamp>, Option<Timestamp>) =
                (field(spec, "paused_at")?, field(spec, "last_activity_at")?);
            let outcome = store
                .update_room_if(room, status, paused_at, last_activity_at)
                .await;
            stored(run, name, outcome)
        }
        "store.add_task" => stored(run, name, store.add_task(task(spec)?).await),
        "store.update_task" => stored(run, name, store.update_task(task(spec)?).await),
        "store.get_task" => {
            let task: TaskId = field(spec, "task_id")?;
            stored(run, name, store.get_task(&task).await)
        }
        "store.list_tasks" => {
            let options = ListTasksOptions {
                status: optional::<TaskStatus>(spec, "status")?,
            };
            stored(run, name, store.list_tasks(&room()?, options).await)
        }
        _ => verifications(run, name, spec).await?,
    })
}

/// The store's verifications and their requests (RFC §11.7, §14.1): a
/// record taken as written, with its model's defaults.
async fn verifications(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let reader = run.reader();
    let store = reader.store();
    let request = || -> Result<VerificationRequest, Unreadable> { field(spec, "request") };
    Ok(match name {
        "store.put_verification" => {
            let verification: Verification = field(spec, "verification")?;
            stored(run, name, store.put_verification(verification).await)
        }
        "store.get_verification" => {
            let identity: IdentityId = field(spec, "identity_id")?;
            let options = GetVerificationOptions {
                organization_id: optional::<String>(spec, "organization_id")?,
            };
            stored(run, name, store.get_verification(&identity, options).await)
        }
        "store.list_verifications_in_force" => {
            stored(run, name, store.list_verifications_in_force().await)
        }
        "store.create_verification_request" => stored(
            run,
            name,
            store.create_verification_request(request()?).await,
        ),
        "store.get_verification_request" => {
            let request_id: String = field(spec, "request_id")?;
            stored(run, name, store.get_verification_request(&request_id).await)
        }
        "store.update_verification_request" => stored(
            run,
            name,
            store.update_verification_request(request()?).await,
        ),
        other => return Err(format!("no call {other}")),
    })
}

/// The kit's calls on a room's read markers, for the organization the call
/// names.
async fn read_markers(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let kit = run.kit();
    let room: RoomId = field(spec, "room_id")?;
    let organization_id = optional::<String>(spec, "organization_id")?;
    Ok(match name {
        "mark_read" => {
            let (channel, event): (ChannelId, EventId) =
                (field(spec, "channel_id")?, field(spec, "event_id")?);
            let options = MarkReadOptions { organization_id };
            finish(
                run,
                name,
                kit.mark_read(&room, &channel, &event, options).await,
                dump,
            )
        }
        "mark_all_read" => {
            let channel: ChannelId = field(spec, "channel_id")?;
            let options = MarkAllReadOptions { organization_id };
            finish(
                run,
                name,
                kit.mark_all_read(&room, &channel, options).await,
                dump,
            )
        }
        _ => {
            let options = ListReadMarkersOptions { organization_id };
            finish(run, name, kit.list_read_markers(&room, options).await, dump)
        }
    })
}
