//! The kit's calls a scenario names, made on the tokio runtime: each takes
//! the RFC's name and arguments (roomkit-specs `conformance/README.md`,
//! "Calls"), its result is written in the RFC's serialized form, and an
//! error is named by its RFC meaning ("Errors"), after the table of
//! `docs/rfc-gaps.md` G-085.

use std::collections::BTreeSet;
use std::time::Duration;

use roomkit::model::{
    Access, ChannelCategory, ChannelId, ChannelType, EventContent, EventDirection, EventId,
    EventSource, EventType, InboundMessage, Metadata, Participant, ParticipantId, RoomEvent,
    RoomId, RoomTimers, Visibility,
};
use roomkit::{
    ArchiveRoomOptions, AttachChannelOptions, CheckAllTimersOptions, CloseRoomOptions,
    CommitEventOptions, ConversationStore, CreateRoomOptions, DeleteEventOptions,
    DetachChannelOptions, HookExecution, HookSpec, HookTrigger, HostChannel, InboundResult,
    KitError, MuteOptions, PostOptions, ProcessInboundOptions, SendEventOptions, SetAccessOptions,
    SetVisibilityOptions, UnmuteOptions, UpdateEventOptions,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::agents;
use crate::errors::{error_of, named};
use crate::run::Run;
use crate::scripted::{ScriptedChannel, hook_handler};
use crate::templates::{complete_event, complete_participant, system_source};

/// Why a call could not even be made: a scenario the runner cannot read.
pub(crate) type Unreadable = String;

/// What a call came to: its result in the serialized form, or its error
/// named, `{"name": ..., "reason": ...}`.
pub(crate) type Called = Result<Value, Value>;

fn field<T: DeserializeOwned>(spec: &Value, key: &str) -> Result<T, Unreadable> {
    serde_json::from_value(spec.get(key).cloned().unwrap_or(Value::Null))
        .map_err(|error| format!("{key}: {error}"))
}

fn optional<T: DeserializeOwned>(spec: &Value, key: &str) -> Result<Option<T>, Unreadable> {
    match spec.get(key) {
        None => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|error| format!("{key}: {error}")),
    }
}

fn room_id(spec: &Value) -> Result<RoomId, Unreadable> {
    field(spec, "room_id")
}

fn channel_id(spec: &Value) -> Result<ChannelId, Unreadable> {
    field(spec, "channel_id")
}

fn organization_id(spec: &Value) -> Option<String> {
    spec.get("organization_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn dump<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// An inbound result in the serialized form (RFC §10.1 `InboundResult`).
fn inbound_result(result: &InboundResult) -> Value {
    json!({
        "event": result.event.as_ref().map(dump),
        "blocked": result.blocked,
        "reason": result.reason,
        "error": result.error.as_ref().map(|record| record.message.clone()),
        "response_metadata": result.response_metadata,
        "delivery_results": dump(&result.delivery_results),
    })
}

/// Turns a kit call's outcome into what the suite reads of it.
fn finish<T>(
    run: &Run,
    call: &str,
    outcome: Result<T, KitError>,
    dumped: impl FnOnce(&T) -> Value,
) -> Called {
    match outcome {
        Ok(value) => Ok(dumped(&value)),
        Err(error) => Err(error_of(&error).unwrap_or_else(|| {
            run.problem(format!(
                "{call} failed with an error the suite has no name for: {error:?}"
            ));
            named("<unmapped>")
        })),
    }
}

/// Makes the call `spec` names, its references already resolved.
pub(crate) async fn call(
    run: &std::sync::Arc<Run>,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let name = spec["call"].as_str().unwrap_or_default();
    let event_of = |value: &Value| value.get("id").and_then(Value::as_str).map(str::to_owned);
    Ok(match name {
        "process_inbound" => {
            let message: InboundMessage = field(spec, "message")?;
            let options = ProcessInboundOptions {
                room_id: optional(spec, "room_id")?,
                organization_id: organization_id(spec),
                ..ProcessInboundOptions::default()
            };
            let outcome = run.kit().process_inbound(message, options).await;
            let id = outcome
                .as_ref()
                .ok()
                .and_then(|r| r.event.as_ref())
                .map(|e| e.id.to_string());
            (finish(run, name, outcome, inbound_result), id)
        }
        "send_event" => {
            let room = room_id(spec)?;
            let (channel, content) = (channel_id(spec)?, field::<EventContent>(spec, "content")?);
            let options = send_event_options(spec)?;
            let outcome = run
                .kit()
                .send_event(&room, &channel, content, options)
                .await;
            let called = finish(run, name, outcome, dump);
            let id = called.as_ref().ok().and_then(event_of);
            (called, id)
        }
        "post" => {
            let room = room_id(spec)?;
            let content = field::<EventContent>(spec, "content")?;
            let options = post_options(spec)?;
            let outcome = run.kit().post(&room, content, options).await;
            let called = finish(run, name, outcome, dump);
            let id = called.as_ref().ok().and_then(event_of);
            (called, id)
        }
        "commit_event" => {
            let room = room_id(spec)?;
            let record: RoomEvent = serde_json::from_value(complete_event(
                &spec["event"],
                spec["room_id"].as_str().unwrap_or_default(),
                &system_source(),
            ))
            .map_err(|error| format!("event: {error}"))?;
            let options = CommitEventOptions {
                organization_id: organization_id(spec),
            };
            let outcome = run.kit().commit_event(&room, record, options).await;
            let called = finish(run, name, outcome, dump);
            let id = called.as_ref().ok().and_then(event_of);
            (called, id)
        }
        _ => (lifecycle_call(run, name, spec).await?, None),
    })
}

async fn lifecycle_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    Ok(match name {
        "update_event" => {
            let (room, event): (RoomId, EventId) = (room_id(spec)?, field(spec, "event_id")?);
            let options = UpdateEventOptions {
                content: optional::<EventContent>(spec, "content")?,
                source: optional::<EventSource>(spec, "source")?,
                metadata: optional::<Metadata>(spec, "metadata")?,
                organization_id: organization_id(spec),
            };
            let outcome = run.kit().update_event(&room, &event, options).await;
            finish(run, name, outcome, dump)
        }
        "delete_event" => {
            let (room, event): (RoomId, EventId) = (room_id(spec)?, field(spec, "event_id")?);
            let options = DeleteEventOptions {
                cascade_replies: optional::<bool>(spec, "cascade_replies")?.unwrap_or(true),
                organization_id: organization_id(spec),
            };
            let outcome = run.kit().delete_event(&room, &event, options).await;
            finish(run, name, outcome, dump)
        }
        "create_room" => create_room(run, &spec["room"]).await?,
        "attach_channel" => {
            let (room, channel) = (room_id(spec)?, channel_id(spec)?);
            let options = AttachChannelOptions {
                organization_id: organization_id(spec),
                ..binding_options(spec)?
            };
            let outcome = run.kit().attach_channel(&room, &channel, options).await;
            finish(run, name, outcome, dump)
        }
        "detach_channel" => {
            let (room, channel) = (room_id(spec)?, channel_id(spec)?);
            let options = DetachChannelOptions {
                organization_id: organization_id(spec),
            };
            // A detach of a channel the room does not bind returns `false`;
            // the suite names it `channel_not_bound`, as Python's runner does.
            match run.kit().detach_channel(&room, &channel, options).await {
                Ok(false) => Err(named("channel_not_bound")),
                outcome => finish(run, name, outcome, |_| Value::Null),
            }
        }
        "mute" | "unmute" | "set_access" | "set_visibility" => {
            binding_call(run, name, spec).await?
        }
        "close_room" => {
            let room = room_id(spec)?;
            let options = CloseRoomOptions {
                organization_id: organization_id(spec),
            };
            finish(run, name, run.kit().close_room(&room, options).await, dump)
        }
        "archive_room" => {
            let room = room_id(spec)?;
            let options = ArchiveRoomOptions {
                organization_id: organization_id(spec),
            };
            finish(
                run,
                name,
                run.kit().archive_room(&room, options).await,
                dump,
            )
        }
        "check_timers" => {
            let room = room_id(spec)?;
            let outcome = match run
                .kit()
                .check_all_timers(CheckAllTimersOptions::default())
                .await
            {
                Ok(_) => run
                    .kit()
                    .store()
                    .get_room(&room)
                    .await
                    .map_err(KitError::from),
                Err(error) => Err(error),
            };
            finish(run, name, outcome, dump)
        }
        "register_channel" => register_channel(run, &spec["channel"])?,
        "unregister_channel" => {
            let channel = channel_id(spec)?;
            let unregistered = run.kit().unregister_channel(&channel);
            finish(run, name, Ok::<_, KitError>(unregistered), |_| Value::Null)
        }
        "register_hook" => finish(run, name, register_hook(run, &spec["hook"])?, |()| {
            Value::Null
        }),
        other => return Err(format!("no call {other}")),
    })
}

/// A binding's change the scenario names: a mute, an unmute, its access or
/// its visibility.
async fn binding_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let (room, channel) = (room_id(spec)?, channel_id(spec)?);
    let organization_id = organization_id(spec);
    let kit = run.kit();
    let outcome = match name {
        "mute" => {
            kit.mute(&room, &channel, MuteOptions { organization_id })
                .await
        }
        "unmute" => {
            kit.unmute(&room, &channel, UnmuteOptions { organization_id })
                .await
        }
        "set_access" => {
            let access = field::<Access>(spec, "access")?;
            let options = SetAccessOptions { organization_id };
            kit.set_access(&room, &channel, access, options).await
        }
        _ => {
            let visibility = field::<Visibility>(spec, "visibility")?;
            let options = SetVisibilityOptions { organization_id };
            kit.set_visibility(&room, &channel, visibility, options)
                .await
        }
    };
    Ok(finish(run, name, outcome, dump))
}

fn send_event_options(spec: &Value) -> Result<SendEventOptions, Unreadable> {
    let mut options = SendEventOptions::default();
    if let Some(event_type) = optional::<EventType>(spec, "event_type")? {
        options.event_type = event_type;
    }
    options.participant_id = optional::<ParticipantId>(spec, "participant_id")?;
    if let Some(visibility) = optional::<Visibility>(spec, "visibility")? {
        options.visibility = visibility;
    }
    options.response_visibility = optional(spec, "response_visibility")?;
    options.addressed_to = optional(spec, "addressed_to")?;
    options.parent_event_id = optional(spec, "parent_event_id")?;
    options.idempotency_key = optional(spec, "idempotency_key")?;
    options.metadata = optional(spec, "metadata")?.unwrap_or_default();
    options.chain_depth = optional(spec, "chain_depth")?.unwrap_or_default();
    options.standalone = optional(spec, "standalone")?.unwrap_or_default();
    options.organization_id = organization_id(spec);
    Ok(options)
}

fn post_options(spec: &Value) -> Result<PostOptions, Unreadable> {
    Ok(PostOptions {
        participant_id: optional(spec, "participant_id")?,
        addressed_to: optional(spec, "addressed_to")?.unwrap_or_default(),
        metadata: optional(spec, "metadata")?.unwrap_or_default(),
        channel_id: optional(spec, "channel_id")?,
        organization_id: organization_id(spec),
        idempotency_key: optional(spec, "idempotency_key")?,
        chain_depth: optional(spec, "chain_depth")?.unwrap_or_default(),
    })
}

fn binding_options(spec: &Value) -> Result<AttachChannelOptions, Unreadable> {
    let mut options = AttachChannelOptions::default();
    if let Some(access) = optional::<Access>(spec, "access")? {
        options.access = access;
    }
    options.muted = optional(spec, "muted")?.unwrap_or_default();
    if let Some(visibility) = optional::<Visibility>(spec, "visibility")? {
        options.visibility = visibility;
    }
    options.category = optional::<ChannelCategory>(spec, "category")?;
    options.participant_id = optional(spec, "participant_id")?;
    options.metadata = optional(spec, "metadata")?.unwrap_or_default();
    Ok(options)
}

/// Creates the room `room` describes, for its organization when it names one.
pub(crate) async fn create_room(
    run: &std::sync::Arc<Run>,
    room: &Value,
) -> Result<Called, Unreadable> {
    let timers = match room.get("timers") {
        Some(timers) => Some(RoomTimers {
            inactive_after_seconds: optional(timers, "inactive_after_seconds")?.flatten(),
            closed_after_seconds: optional(timers, "closed_after_seconds")?.flatten(),
            ..RoomTimers::default()
        }),
        None => None,
    };
    let options = CreateRoomOptions {
        room_id: Some(field(room, "id")?),
        metadata: optional::<Metadata>(room, "metadata")?.unwrap_or_default(),
        organization_id: organization_id(room),
        timers,
        ..Default::default()
    };
    let outcome = run.kit().create_room(options).await;
    Ok(finish(run, "create_room", outcome, dump))
}

/// Attaches a setup's binding.
pub(crate) async fn attach_binding(
    run: &std::sync::Arc<Run>,
    room: &Value,
    binding: &Value,
) -> Result<(), String> {
    let id: RoomId = field(room, "id")?;
    let channel: ChannelId = field(binding, "channel_id")?;
    let options = AttachChannelOptions {
        organization_id: organization_id(room),
        ..binding_options(binding)?
    };
    let outcome = run.kit().attach_channel(&id, &channel, options).await;
    outcome
        .map(drop)
        .map_err(|error| format!("attach {channel}: {error}"))
}

/// Seeds a participant record (RFC §5.5) of the setup, as the store keeps
/// it.
pub(crate) async fn add_participant(
    run: &std::sync::Arc<Run>,
    room: &Value,
    participant: &Value,
) -> Result<(), String> {
    let room_id = room["id"].as_str().unwrap_or_default();
    let record: Participant = serde_json::from_value(complete_participant(participant, room_id))
        .map_err(|error| format!("participant: {error}"))?;
    run.kit()
        .store()
        .create_participant(record)
        .await
        .map_err(|error| format!("participant: {error}"))
}

/// Registers the channel a scenario describes. An id the RFC's grammar
/// refuses (§7.3) is refused where roomkit-rs builds it, as a channel id.
pub(crate) fn register_channel(
    run: &std::sync::Arc<Run>,
    channel: &Value,
) -> Result<Called, Unreadable> {
    let Ok(id) = ChannelId::new(channel["id"].as_str().unwrap_or_default()) else {
        return Ok(Err(named("invalid_argument")));
    };
    let registered = if channel["type"] == "host" {
        run.kit().register_channel(HostChannel::with_id(id))
    } else if channel.get("model").is_some() {
        let (agent, model) = agents::scripted_agent(channel)?;
        crate::run::lock(&run.models).insert(id.to_string(), model);
        run.kit().register_channel(agent)
    } else {
        run.kit()
            .register_channel(ScriptedChannel::new(channel, run)?)
    };
    Ok(finish(run, "register_channel", registered, |()| {
        Value::Null
    }))
}

/// Registers the hook a scenario describes.
pub(crate) fn register_hook(
    run: &std::sync::Arc<Run>,
    hook: &Value,
) -> Result<Result<(), KitError>, Unreadable> {
    let trigger: HookTrigger = field(hook, "trigger")?;
    let execution: HookExecution = optional(hook, "execution")?.unwrap_or(HookExecution::Sync);
    if trigger.is_tool_trigger() {
        return agents::register_tool_hook(run, hook, trigger, execution);
    }
    let name = hook["name"].as_str().unwrap_or_default();
    let mut spec = HookSpec::new(name, trigger, execution);
    spec.priority = optional(hook, "priority")?.unwrap_or_default();
    if let Some(timeout) = optional::<u64>(hook, "timeout_ms")? {
        spec.timeout = Duration::from_millis(timeout);
    }
    spec.fail_closed = optional(hook, "fail_closed")?.unwrap_or_default();
    spec.needs_lock = optional(hook, "needs_lock")?.unwrap_or(true);
    spec.filter.channel_types = optional::<BTreeSet<ChannelType>>(hook, "channel_types")?;
    spec.filter.channel_ids = optional::<BTreeSet<ChannelId>>(hook, "channel_ids")?;
    spec.filter.directions = optional::<BTreeSet<EventDirection>>(hook, "directions")?;
    Ok(run.kit().hook(spec, hook_handler(hook, run)).map(drop))
}
