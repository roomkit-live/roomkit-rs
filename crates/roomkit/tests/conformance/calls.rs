//! The kit's calls a scenario names, made on the tokio runtime: each takes
//! the RFC's name and arguments (roomkit-specs `conformance/README.md`,
//! "Calls"), its result is written in the RFC's serialized form, and an
//! error is named by its RFC meaning ("Errors"), after the table of
//! `docs/rfc-gaps.md` G-085.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use roomkit::model::{
    Access, AgentResponsePolicy, ChannelCategory, ChannelId, ChannelType, DeliveryStatus,
    EventContent, EventDirection, EventId, EventSource, EventType, InboundMessage, Metadata,
    Observation, Participant, ParticipantId, RoomEvent, RoomId, RoomTimers, Visibility,
};
use roomkit::orchestration::StatusEntry;
use roomkit::providers::sms::WebhookMeta;
use roomkit::{
    AddRoomHookOptions, ArchiveRoomOptions, AttachChannelOptions, CheckAllTimersOptions,
    CloseRoomOptions, CommitEventOptions, ConversationStore, CreateRoomOptions, DeleteEventOptions,
    DeliverOptions, DeliveryStrategy, DetachChannelOptions, HookOptions, HookTrigger, HostChannel,
    Immediate, InboundResult, MuteOptions, MuteOutputOptions, PostOptions,
    ProcessDeliveryStatusOptions, ProcessInboundOptions, ProcessWebhookOptions, Queued,
    RegenerateResponseOptions, RegenerateTargetOptions, RemoveRoomHookOptions, RoomKitError,
    SendEventOptions, SendGreetingOptions, SetAccessOptions, SetAgentResponsePolicyOptions,
    SetRoomTimersOptions, SetVisibilityOptions, UnmuteOptions, UnmuteOutputOptions,
    UpdateBindingMetadataOptions, UpdateEventOptions, WaitForIdle,
};
use roomkit_ai::{AiGenerationEvent, AiResponseEvent, ThinkingEvent, ToolRoundEvent};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::agents;
use crate::ai_hooks::turn_hook_handler;
use crate::errors::{error_of, named, visibility_input_error};
use crate::run::Run;
use crate::scripted::{ScriptedChannel, hook_handler, session_hook_handler, status_hook_handler};
use crate::task_calls::payload_hook_handler;
use crate::templates::{complete_event, complete_participant, system_source};

/// Why a call could not even be made: a scenario the runner cannot read.
pub(crate) type Unreadable = String;

/// What a call came to: its result in the serialized form, or its error
/// named, `{"name": ..., "reason": ...}`.
pub(crate) type Called = Result<Value, Value>;

pub(crate) fn field<T: DeserializeOwned>(spec: &Value, key: &str) -> Result<T, Unreadable> {
    serde_json::from_value(spec.get(key).cloned().unwrap_or(Value::Null))
        .map_err(|error| format!("{key}: {error}"))
}

pub(crate) fn optional<T: DeserializeOwned>(
    spec: &Value,
    key: &str,
) -> Result<Option<T>, Unreadable> {
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

pub(crate) fn dump<T: serde::Serialize>(value: &T) -> Value {
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
        "response_events": dump(&result.response_events),
        "unavailable_targets": result.unavailable_targets.as_ref().map_or_else(|| json!([]), dump),
        "delivery_results": dump(&result.delivery_results),
        "cancellation_reason": result.cancellation_reason,
    })
}

/// Turns a kit call's outcome into what the suite reads of it.
pub(crate) fn finish<T>(
    run: &Run,
    call: &str,
    outcome: Result<T, RoomKitError>,
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

/// Makes the call `spec` names, its references already resolved; a call
/// that takes no kit is refused one (README, "Calls").
pub(crate) async fn checked_call(
    run: &std::sync::Arc<Run>,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    if spec.get("kit").is_some() {
        run.kit_of(spec)?;
    }
    call(run, spec).await
}

/// Makes the call `spec` names, its references already resolved.
pub(crate) async fn call(
    run: &std::sync::Arc<Run>,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    if let Some(error) = visibility_input_error(spec) {
        return Ok((Err(error), None));
    }
    let name = spec["call"].as_str().unwrap_or_default();
    let event_of = |value: &Value| value.get("id").and_then(Value::as_str).map(str::to_owned);
    Ok(match name {
        "process_inbound" => {
            let message: InboundMessage = field(spec, "message")?;
            let options = ProcessInboundOptions {
                room_id: optional(spec, "room_id")?,
                organization_id: organization_id(spec),
                defer_delivery: optional::<bool>(spec, "defer_delivery")?.unwrap_or(false),
            };
            let outcome = run.kit_of(spec)?.process_inbound(message, options).await;
            keep_handle(run, spec, &outcome);
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
        "deliver" => {
            let room = room_id(spec)?;
            let content = field::<String>(spec, "content")?;
            let options = deliver_options(spec)?;
            let outcome = run.kit().deliver(&room, content, options).await;
            let called = finish(run, name, outcome, dump);
            let id = called
                .as_ref()
                .ok()
                .and_then(|value| value.get("event_id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            (called, id)
        }
        store if crate::store_calls::handles(store) => {
            crate::store_calls::call(run, store, spec).await?
        }
        task if crate::task_calls::handles(task) => {
            crate::task_calls::call(run, task, spec).await?
        }
        member if crate::member_calls::handles(member) => {
            crate::member_calls::call(run, member, spec).await?
        }
        input if crate::input_calls::handles(input) => {
            crate::input_calls::call(run, input, spec).await?
        }
        verification if crate::verification_calls::handles(verification) => {
            crate::verification_calls::call(run, verification, spec).await?
        }
        "cancel_delivery" | "steer" => (cancel_or_steer(run, name, spec).await?, None),
        strategy if crate::strategy_calls::arguments(strategy).is_some() => {
            (crate::strategy_calls::call(run, name, spec).await?, None)
        }
        "save_conversation_state" => (crate::routing_calls::call(run, name, spec).await?, None),
        "default_router.route" => (
            crate::routing_calls::default_route(run, name, spec).await?,
            None,
        ),
        "delivery.cancel" | "delivery.wait" => (handle_call(run, name, spec).await?, None),
        "regenerate_response" | "regenerate_target" => regenerate_call(run, name, spec).await?,
        _ => (other_call(run, name, spec).await?, None),
    })
}

/// `regenerate_response` or `regenerate_target` (RFC §13.6), its label
/// naming the event it answers again.
async fn regenerate_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let room = room_id(spec)?;
    let organization_id = organization_id(spec);
    if name == "regenerate_target" {
        let options = RegenerateTargetOptions { organization_id };
        let outcome = run.kit().regenerate_target(&room, options).await;
        let id = outcome
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map(|event| event.id.to_string());
        return Ok((finish(run, name, outcome, dump), id));
    }
    let options = RegenerateResponseOptions {
        trigger_id: optional(spec, "trigger_id")?,
        organization_id,
    };
    let outcome = run.kit().regenerate_response(&room, options).await;
    let id = outcome
        .as_ref()
        .ok()
        .and_then(|result| result.as_ref()?.event.as_ref())
        .map(|event| event.id.to_string());
    let called = finish(run, name, outcome, |result| {
        result.as_ref().map_or(Value::Null, inbound_result)
    });
    Ok((called, id))
}

/// `close`: closes the kit the call names, a process that stops (README,
/// `close`).
async fn close(run: &std::sync::Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let kit = run.kit_of(spec)?;
    run.closed(spec);
    Ok(finish(run, name, kit.close().await, |()| Value::Null))
}

/// `send_greeting`: an agent's greeting (§19.1).
async fn send_greeting(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let room = room_id(spec)?;
    let options = SendGreetingOptions {
        channel_id: optional::<ChannelId>(spec, "channel_id")?,
        agent_id: optional::<ChannelId>(spec, "agent_id")?,
        greeting: optional::<String>(spec, "greeting")?,
        organization_id: organization_id(spec),
    };
    let outcome = run.kit().send_greeting(&room, options).await;
    Ok(finish(run, name, outcome, dump))
}

/// `cancel_delivery`, or `steer`: a directive to an agent's running turns.
/// Keeps the completion handle of a deferred call by its label (RFC §10.1
/// step 18), for the `delivery.*` calls that name it.
fn keep_handle(run: &Run, spec: &Value, outcome: &Result<InboundResult, RoomKitError>) {
    let (Some(label), Ok(result)) = (spec.get("as").and_then(Value::as_str), outcome) else {
        return;
    };
    if let Some(handle) = &result.delivery {
        crate::run::lock(&run.handles).insert(label.to_owned(), handle.clone());
    }
}

/// `delivery.cancel` or `delivery.wait` on the handle its `of` names; a
/// cancel the handle refuses where its cleanup would wait for its caller
/// reads `waits_for_itself`.
async fn handle_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let label = field::<String>(spec, "of")?;
    let Some(handle) = crate::run::lock(&run.handles).get(&label).cloned() else {
        return Err(format!("no deferred call is labelled {label}"));
    };
    if name == "delivery.wait" {
        return Ok(finish(run, name, handle.wait().await, inbound_result));
    }
    let mut options = roomkit::CancelOptions::default();
    if let Some(reason) = optional::<String>(spec, "reason")? {
        options.reason = reason;
    }
    if let Some(timeout) = optional::<u64>(spec, "timeout_ms")? {
        options.timeout = Duration::from_millis(timeout);
    }
    match handle.cancel(options).await {
        Err(RoomKitError::Runtime(_)) => Ok(Err(named("waits_for_itself"))),
        outcome => Ok(finish(run, name, outcome, inbound_result)),
    }
}

async fn cancel_or_steer(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    if name == "steer" {
        let directive = field::<roomkit::model::SteeringDirective>(spec, "directive")?;
        let channel = field::<ChannelId>(spec, "channel_id")?;
        let options = roomkit::SteerOptions {
            room_id: optional::<RoomId>(spec, "room_id")?,
            organization_id: organization_id(spec),
            ..roomkit::SteerOptions::default()
        };
        let reached = run.kit().steer(directive, &channel, options).await;
        return Ok(finish(run, name, reached, |reached| json!(reached)));
    }
    let key = field::<String>(spec, "key")?;
    let mut options = roomkit::CancelDeliveryOptions {
        organization_id: organization_id(spec),
        ..roomkit::CancelDeliveryOptions::default()
    };
    if let Some(reason) = optional::<String>(spec, "reason")? {
        options.reason = reason;
    }
    let cancelled = run.kit().cancel_delivery(&key, options).await;
    Ok(Ok(json!(cancelled)))
}

/// A process's close, a discussion's or a handoff's call, or a call of the
/// room's lifecycle.
async fn other_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    if name == "close" {
        return close(run, name, spec).await;
    }
    if crate::discussion_calls::arguments(name).is_some() {
        return crate::discussion_calls::call(run, name, spec).await;
    }
    if crate::handoff_calls::arguments(name).is_some() {
        return crate::handoff_calls::call(run, name, spec).await;
    }
    if name == "send_greeting" {
        return send_greeting(run, name, spec).await;
    }
    if matches!(name, "set_room_timers" | "set_agent_response_policy") {
        return setting_call(run, name, spec).await;
    }
    lifecycle_call(run, name, spec).await
}

/// A host's setter of a room's settings: its timers, its agent response
/// policy (README, `set_room_timers`, `set_agent_response_policy`).
async fn setting_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let room = room_id(spec)?;
    let outcome = if name == "set_room_timers" {
        let options = SetRoomTimersOptions {
            organization_id: organization_id(spec),
        };
        let timers: RoomTimers = field(spec, "timers")?;
        run.kit().set_room_timers(&room, timers, options).await
    } else {
        let options = SetAgentResponsePolicyOptions {
            organization_id: organization_id(spec),
        };
        let policy: AgentResponsePolicy = field(spec, "policy")?;
        run.kit()
            .set_agent_response_policy(&room, policy, options)
            .await
    };
    Ok(finish(run, name, outcome, dump))
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
        "create_room" => {
            let orchestration = crate::strategy_calls::room_orchestration(run, spec)?;
            create_room(run, &spec["room"], orchestration).await?
        }
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
        "mute" | "unmute" | "set_access" | "set_visibility" | "update_binding_metadata" => {
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
                    .map_err(RoomKitError::from),
                Err(error) => Err(error),
            };
            finish(run, name, outcome, dump)
        }
        "register_channel" => register_channel(run, &spec["channel"])?,
        "unregister_channel" => {
            let channel = channel_id(spec)?;
            let unregistered = run.kit().unregister_channel(&channel);
            finish(run, name, unregistered, |_| Value::Null)
        }
        "register_hook" => finish(run, name, register_hook(run, &spec["hook"])?, |()| {
            Value::Null
        }),
        "add_room_hook" | "remove_room_hook" => room_hook_call(run, name, spec).await?,
        "mute_output" | "unmute_output" => output_call(run, name, spec).await?,
        "process_delivery_status" | "process_webhook" => status_call(run, name, spec).await?,
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
        "update_binding_metadata" => {
            let metadata = field::<Metadata>(spec, "metadata")?;
            let options = UpdateBindingMetadataOptions { organization_id };
            kit.update_binding_metadata(&room, &channel, metadata, options)
                .await
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

pub(crate) fn send_event_options(spec: &Value) -> Result<SendEventOptions, Unreadable> {
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

fn deliver_options(spec: &Value) -> Result<DeliverOptions<crate::Store>, Unreadable> {
    let strategy: Option<std::sync::Arc<dyn DeliveryStrategy<crate::Store>>> =
        match optional::<String>(spec, "strategy")?.as_deref() {
            None => None,
            Some("immediate") => Some(std::sync::Arc::new(Immediate)),
            Some("wait_for_idle") => Some(std::sync::Arc::new(WaitForIdle::default())),
            Some("queued") => Some(std::sync::Arc::new(Queued::default())),
            Some(other) => return Err(format!("strategy {other}: no such strategy")),
        };
    Ok(DeliverOptions {
        channel_id: optional(spec, "channel_id")?,
        strategy,
        metadata: optional(spec, "metadata")?.unwrap_or_default(),
        addressed_to: optional(spec, "addressed_to")?,
        idempotency_key: optional(spec, "idempotency_key")?,
        session_id: optional(spec, "session_id")?,
        chain_depth: optional(spec, "chain_depth")?.unwrap_or_default(),
        instruction: optional(spec, "instruction")?.unwrap_or_default(),
        organization_id: organization_id(spec),
    })
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

/// Creates the room `room` describes, for its organization when it names
/// one, with the strategy `orchestration` names.
#[expect(
    clippy::option_option,
    reason = "CreateRoomOptions::orchestration's three cases"
)]
pub(crate) async fn create_room(
    run: &std::sync::Arc<Run>,
    room: &Value,
    orchestration: Option<Option<crate::strategy_calls::Strategy>>,
) -> Result<Called, Unreadable> {
    // The whole timers, an explicit last activity among them, which the
    // room keeps (Python's `create_room(timers=)`).
    let timers = optional::<RoomTimers>(room, "timers")?;
    let options = CreateRoomOptions {
        room_id: Some(field(room, "id")?),
        metadata: optional::<Metadata>(room, "metadata")?.unwrap_or_default(),
        organization_id: organization_id(room),
        timers,
        orchestration,
        ..Default::default()
    };
    let outcome = run.kit().create_room(options).await;
    Ok(finish(run, "create_room", outcome, dump))
}

/// Writes a setup room's `store_metadata` through the store: how a scenario
/// seeds a key only the framework writes, which a room's creation drops.
pub(crate) async fn patch_store_metadata(
    run: &std::sync::Arc<Run>,
    room: &Value,
) -> Result<(), String> {
    let Some(patch) = optional::<Metadata>(room, "store_metadata")? else {
        return Ok(());
    };
    let id: RoomId = field(room, "id")?;
    run.kit()
        .store()
        .patch_room_metadata(
            &id,
            patch,
            roomkit::store::PatchRoomMetadataOptions::default(),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
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
        .add_participant(record)
        .await
        .map(drop)
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
        run.kit()
            .register_channel(HostChannel::new(roomkit::HostChannelOptions {
                channel_id: id,
            }))
    } else if channel.get("acp").is_some() {
        let (agent, wire) = crate::acp_agents::scripted_acp_channel(channel, id.clone(), run)?;
        crate::run::lock(&run.acp_wires).insert(id.to_string(), wire);
        run.kit().register_channel(agent)
    } else if channel.get("model").is_some() {
        let (agent, models) = agents::scripted_agent(channel, run)?;
        crate::run::lock(&run.models).extend(models);
        match agent {
            agents::Scripted::Channel(agent) => run.kit().register_channel(*agent),
            agents::Scripted::Agent(agent) => run.kit().register_channel(*agent),
        }
    } else {
        run.kit()
            .register_channel(ScriptedChannel::new(channel, run)?)
    };
    Ok(finish(run, "register_channel", registered, |()| {
        Value::Null
    }))
}

/// Registers `channel` on `kit`, another process of the setup: a scripted
/// agent answers from the model the run's own agent of its id has (README,
/// `processes`).
pub(crate) fn register_process_channel(
    run: &std::sync::Arc<Run>,
    kit: &crate::Kit,
    channel: &Value,
) -> Result<(), Unreadable> {
    let id = ChannelId::new(channel["id"].as_str().unwrap_or_default())
        .map_err(|error| error.to_string())?;
    let registered = if channel["type"] == "host" {
        kit.register_channel(HostChannel::new(roomkit::HostChannelOptions {
            channel_id: id,
        }))
    } else if channel.get("model").is_some() {
        match agents::scripted_agent_of(channel, run, true)?.0 {
            agents::Scripted::Channel(agent) => kit.register_channel(*agent),
            agents::Scripted::Agent(agent) => kit.register_channel(*agent),
        }
    } else {
        kit.register_channel(ScriptedChannel::new(channel, run)?)
    };
    registered.map_err(|error| format!("register {channel}: {error}"))
}

/// A change of a channel's speech output the scenario names: its mute or
/// its unmute.
async fn output_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let (room, channel) = (room_id(spec)?, channel_id(spec)?);
    let organization_id = organization_id(spec);
    let kit = run.kit();
    let outcome = if name == "mute_output" {
        let options = MuteOutputOptions { organization_id };
        kit.mute_output(&room, &channel, options).await
    } else {
        let options = UnmuteOutputOptions { organization_id };
        kit.unmute_output(&room, &channel, options).await
    };
    Ok(finish(run, name, outcome, dump))
}

/// A provider's report the scenario names: a delivery status, or a
/// webhook read into its metadata.
async fn status_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let organization_id = organization_id(spec);
    let kit = run.kit();
    let outcome = if name == "process_delivery_status" {
        let status = field::<DeliveryStatus>(spec, "status")?;
        let options = ProcessDeliveryStatusOptions { organization_id };
        kit.process_delivery_status(status, options).await
    } else {
        let (meta, channel) = (field::<WebhookMeta>(spec, "meta")?, channel_id(spec)?);
        let options = ProcessWebhookOptions { organization_id };
        kit.process_webhook(meta, &channel, options).await
    };
    Ok(finish(run, name, outcome, |()| Value::Null))
}

/// A room hook's call the scenario names: its addition or its removal.
async fn room_hook_call(
    run: &std::sync::Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<Called, Unreadable> {
    let room = room_id(spec)?;
    if name == "add_room_hook" {
        let added = add_room_hook(run, &room, spec)?.await;
        return Ok(finish(run, name, added, |()| Value::Null));
    }
    let hook = field::<String>(spec, "name")?;
    let options = RemoveRoomHookOptions {
        organization_id: organization_id(spec),
    };
    let removed = run.kit().remove_room_hook(&room, &hook, options).await;
    Ok(finish(run, name, removed, |removed| Value::Bool(*removed)))
}

/// Adds the hook `spec` describes for the room `room` alone: a room's hook
/// declares no filter and keeps the default timeout.
fn add_room_hook(
    run: &std::sync::Arc<Run>,
    room: &RoomId,
    spec: &Value,
) -> Result<impl Future<Output = Result<(), RoomKitError>>, Unreadable> {
    let hook = &spec["hook"];
    let trigger: HookTrigger = field(hook, "trigger")?;
    let defaults = AddRoomHookOptions::default();
    let execution = optional(hook, "execution")?.unwrap_or(HookOptions::default().execution);
    let options = AddRoomHookOptions {
        priority: optional(hook, "priority")?.unwrap_or(defaults.priority),
        name: hook["name"].as_str().unwrap_or_default().to_owned(),
        fail_closed: optional(hook, "fail_closed")?.unwrap_or(defaults.fail_closed),
        needs_lock: optional(hook, "needs_lock")?.unwrap_or(defaults.needs_lock),
        organization_id: organization_id(spec),
    };
    let (kit, room) = (run.kit().clone(), room.clone());
    let router = hook
        .get("router")
        .map(crate::routing_calls::router)
        .transpose()?;
    let (tool, event) = (
        trigger
            .is_tool_trigger()
            .then(|| agents::tool_hook_handler(hook, run)),
        hook_handler(hook, run),
    );
    let status = payload_hook_handler::<StatusEntry>(hook, run, "status_entry");
    let feedback = payload_hook_handler::<Observation>(hook, run, "observation");
    let input = crate::input_calls::input_hook_handler(hook, run);
    let generation = turn_hook_handler::<AiGenerationEvent>(hook, run);
    let tool_round = turn_hook_handler::<ToolRoundEvent>(hook, run);
    let response = turn_hook_handler::<AiResponseEvent>(hook, run);
    let thinking = turn_hook_handler::<ThinkingEvent>(hook, run);
    let speak_queue = turn_hook_handler::<roomkit::SpeakQueueEvent>(hook, run);
    let decision = turn_hook_handler::<roomkit::DispatchDecisionEvent>(hook, run);
    Ok(async move {
        if let Some(router) = router {
            let added = kit.add_room_hook(&room, trigger, execution, router.as_hook(), options);
            return added.await.map(drop);
        }
        let added = match (tool, trigger) {
            (Some(tool), _) => {
                kit.add_room_hook(&room, trigger, execution, tool, options)
                    .await
            }
            (None, HookTrigger::OnStatusPosted) => {
                kit.add_room_hook(&room, trigger, execution, status, options)
                    .await
            }
            (None, HookTrigger::OnFeedback) => {
                kit.add_room_hook(&room, trigger, execution, feedback, options)
                    .await
            }
            (None, HookTrigger::OnUserInputRequired) => {
                kit.add_room_hook(&room, trigger, execution, input, options)
                    .await
            }
            (None, HookTrigger::BeforeAiGeneration) => {
                kit.add_room_hook(&room, trigger, execution, generation, options)
                    .await
            }
            (None, HookTrigger::AfterToolRound) => {
                kit.add_room_hook(&room, trigger, execution, tool_round, options)
                    .await
            }
            (None, HookTrigger::OnAiResponse) => {
                kit.add_room_hook(&room, trigger, execution, response, options)
                    .await
            }
            (None, HookTrigger::OnAiThinking) => {
                kit.add_room_hook(&room, trigger, execution, thinking, options)
                    .await
            }
            (None, HookTrigger::OnSpeakQueue) => {
                kit.add_room_hook(&room, trigger, execution, speak_queue, options)
                    .await
            }
            (None, HookTrigger::OnDispatchDecision) => {
                kit.add_room_hook(&room, trigger, execution, decision, options)
                    .await
            }
            (None, _) => {
                kit.add_room_hook(&room, trigger, execution, event, options)
                    .await
            }
        };
        added.map(drop)
    })
}

/// Registers the hook a scenario describes.
pub(crate) fn register_hook(
    run: &std::sync::Arc<Run>,
    hook: &Value,
) -> Result<Result<(), RoomKitError>, Unreadable> {
    let trigger: HookTrigger = field(hook, "trigger")?;
    let defaults = HookOptions::default();
    let options = HookOptions {
        execution: optional(hook, "execution")?.unwrap_or(defaults.execution),
        priority: optional(hook, "priority")?.unwrap_or(defaults.priority),
        name: hook["name"].as_str().unwrap_or_default().to_owned(),
        timeout: optional::<u64>(hook, "timeout_ms")?
            .map_or(defaults.timeout, Duration::from_millis),
        channel_types: optional::<BTreeSet<ChannelType>>(hook, "channel_types")?,
        channel_ids: optional::<BTreeSet<ChannelId>>(hook, "channel_ids")?,
        directions: optional::<BTreeSet<EventDirection>>(hook, "directions")?,
        fail_closed: optional(hook, "fail_closed")?.unwrap_or(defaults.fail_closed),
        needs_lock: optional(hook, "needs_lock")?.unwrap_or(defaults.needs_lock),
        ..defaults
    };
    let kit = run.kit();
    if let Some(router) = hook.get("router") {
        let router = crate::routing_calls::router(router)?;
        return Ok(kit.hook(trigger, router.as_hook(), options).map(drop));
    }
    let registered = if trigger.is_tool_trigger() {
        kit.hook(trigger, agents::tool_hook_handler(hook, run), options)
    } else if trigger == HookTrigger::OnSessionStarted {
        kit.hook(trigger, session_hook_handler(hook, run), options)
    } else if trigger == HookTrigger::OnDeliveryStatus {
        kit.hook(trigger, status_hook_handler(hook, run), options)
    } else if trigger == HookTrigger::OnStatusPosted {
        let handler = payload_hook_handler::<StatusEntry>(hook, run, "status_entry");
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::OnFeedback {
        let handler = payload_hook_handler::<Observation>(hook, run, "observation");
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::OnUserInputRequired {
        let handler = crate::input_calls::input_hook_handler(hook, run);
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::BeforeAiGeneration {
        let handler = turn_hook_handler::<AiGenerationEvent>(hook, run);
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::AfterToolRound {
        let handler = turn_hook_handler::<ToolRoundEvent>(hook, run);
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::OnAiResponse {
        let handler = turn_hook_handler::<AiResponseEvent>(hook, run);
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::OnSpeakQueue {
        let handler = turn_hook_handler::<roomkit::SpeakQueueEvent>(hook, run);
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::OnDispatchDecision {
        let handler = turn_hook_handler::<roomkit::DispatchDecisionEvent>(hook, run);
        kit.hook(trigger, handler, options)
    } else if trigger == HookTrigger::OnAiThinking {
        let handler = turn_hook_handler::<ThinkingEvent>(hook, run);
        kit.hook(trigger, handler, options)
    } else {
        kit.hook(trigger, hook_handler(hook, run), options)
    };
    Ok(registered.map(drop))
}
