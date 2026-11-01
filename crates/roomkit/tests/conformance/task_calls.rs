//! The kit's calls of task delegation, the status bus, the turn's host and
//! a room's observations (roomkit-specs `conformance/README.md`, "Calls on
//! the status bus, a room's observations and the turn's host"), made on the
//! tokio runtime, and the hooks whose payload is a status entry or an
//! observation.
//!
//! `status.post` is the bus's `post_async`, which returns once the bus's
//! subscribers ran. A call `host.<verb>` is a verb of the turn's host, made
//! by an action of a scripted intelligence channel's turn: one made where
//! no turn's host is cannot be read.

use std::sync::{Arc, Weak};

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, EventContent, HookResult, Metadata, Observation, RoomContext, RoomId,
};
use roomkit::orchestration::{PostAsyncOptions, RecentOptions, StatusLevel};
use roomkit::tasks::DelegatedTask;
use roomkit::{
    DelegateOptions, HookError, ListObservationsOptions, ListTasksOptions, PostStatusOptions,
    SubmitFeedbackOptions, TurnHost, UpdateRoomMetadataOptions, current_turn_host,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, dump, field, finish, optional, send_event_options};
use crate::run::{Run, Scope, lock};
use crate::scripted::perform;
use crate::templates::complete_observation;

/// The framework events of the Level 2 features the suite exercises: the
/// status bus's (RFC §19.8) and a feedback's (RFC §15.9).
pub(crate) const LEVEL_2_EVENTS: [&str; 2] = ["status_posted", "feedback"];

const STATUS: &[&str] = &["agent_id", "action", "status", "detail", "metadata"];

/// Each call's arguments, beside `call` and `organization_id`; `None` for
/// a call this module does not make.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "status.post" | "host.post_status" => STATUS,
        "status.recent" => &["n", "agent_id", "status"],
        "update_room_metadata" | "host.update_room_metadata" => &["room_id", "metadata"],
        "submit_feedback" => &[
            "room_id",
            "rating",
            "event_id",
            "channel_id",
            "comment",
            "dimension",
            "metadata",
        ],
        "list_observations" => &["room_id"],
        "list_tasks" => &["room_id", "status"],
        "delegate" => &[
            "room_id",
            "agent_id",
            "task",
            "wait",
            "context",
            "share_channels",
            "notify",
            "post_status",
            "brief",
            "conversation",
            "hand_back",
        ],
        "task.wait" => &["task_id"],
        "host.send_event" => &[
            "room_id",
            "channel_id",
            "content",
            "event_type",
            "participant_id",
            "visibility",
            "response_visibility",
            "addressed_to",
            "parent_event_id",
            "idempotency_key",
            "metadata",
            "chain_depth",
            "standalone",
        ],
        "host.add_observation" => &["observation"],
        _ => return None,
    })
}

/// Whether this module makes the call `call`.
pub(crate) fn handles(call: &str) -> bool {
    arguments(call).is_some()
}

fn organization_id(spec: &Value) -> Option<String> {
    spec.get("organization_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Makes the call `spec` names, its references already resolved; returns
/// its outcome and, for one that returns an event, the event's id.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let kit = run.kit();
    let room = || field::<RoomId>(spec, "room_id");
    Ok(match name {
        "status.post" => {
            let options = PostAsyncOptions {
                detail: optional(spec, "detail")?.unwrap_or_default(),
                metadata: optional(spec, "metadata")?,
            };
            let (agent, action, status) = status(spec)?;
            let entry = kit
                .status_bus()
                .post_async(agent, action, status, options)
                .await;
            (Ok(dump(&entry)), None)
        }
        "status.recent" => {
            let options = RecentOptions {
                n: optional(spec, "n")?.unwrap_or(RecentOptions::default().n),
                agent_id: optional(spec, "agent_id")?,
                status: optional(spec, "status")?,
            };
            (Ok(dump(&kit.status_bus().recent(options).await)), None)
        }
        "update_room_metadata" => {
            let options = UpdateRoomMetadataOptions {
                organization_id: organization_id(spec),
            };
            let metadata: Metadata = field(spec, "metadata")?;
            let outcome = kit.update_room_metadata(&room()?, metadata, options).await;
            (finish(run, name, outcome, dump), None)
        }
        "submit_feedback" => {
            let defaults = SubmitFeedbackOptions::default();
            let options = SubmitFeedbackOptions {
                event_id: optional(spec, "event_id")?,
                channel_id: optional(spec, "channel_id")?,
                comment: optional(spec, "comment")?.unwrap_or(defaults.comment),
                dimension: optional(spec, "dimension")?.unwrap_or(defaults.dimension),
                metadata: optional(spec, "metadata")?,
                organization_id: organization_id(spec),
            };
            let rating: f64 = field(spec, "rating")?;
            let outcome = kit.submit_feedback(&room()?, rating, options).await;
            (finish(run, name, outcome, |()| Value::Null), None)
        }
        "list_observations" => {
            let options = ListObservationsOptions {
                organization_id: organization_id(spec),
            };
            let outcome = kit.list_observations(&room()?, options).await;
            (finish(run, name, outcome, dump), None)
        }
        "list_tasks" => {
            let options = ListTasksOptions {
                status: optional(spec, "status")?,
                organization_id: organization_id(spec),
            };
            let outcome = kit.list_tasks(&room()?, options).await;
            (finish(run, name, outcome, dump), None)
        }
        "delegate" => delegate(run, spec).await?,
        "task.wait" => {
            let task_id: String = field(spec, "task_id")?;
            let task = lock(&run.tasks)
                .get(&task_id)
                .cloned()
                .ok_or_else(|| format!("no task {task_id:?} was delegated"))?;
            let outcome = task.wait(roomkit::tasks::WaitOptions::default()).await;
            (finish(run, name, outcome, dump), None)
        }
        verb => host_call(run, verb, spec).await?,
    })
}

/// `delegate` with the scenario's keywords: the task, its label its id.
async fn delegate(run: &Arc<Run>, spec: &Value) -> Result<(Called, Option<String>), Unreadable> {
    let defaults = DelegateOptions::default();
    if optional::<u64>(spec, "conversation")?.unwrap_or(0) != 0 {
        return Err("conversation is not in 0.x".to_owned());
    }
    let options = DelegateOptions {
        wait: optional(spec, "wait")?.unwrap_or(defaults.wait),
        context: optional(spec, "context")?,
        share_channels: optional(spec, "share_channels")?,
        notify: optional(spec, "notify")?,
        post_status: optional(spec, "post_status")?.unwrap_or(defaults.post_status),
        brief: optional(spec, "brief")?,
        hand_back: optional(spec, "hand_back")?.unwrap_or(defaults.hand_back),
        organization_id: organization_id(spec),
        ..DelegateOptions::default()
    };
    let (room, agent, task): (RoomId, ChannelId, String) = (
        field(spec, "room_id")?,
        field(spec, "agent_id")?,
        field(spec, "task")?,
    );
    let outcome = run.kit().delegate(&room, &agent, task, options).await;
    let id = outcome.as_ref().ok().map(|task| task.id.clone());
    if let Ok(task) = &outcome {
        lock(&run.tasks).insert(task.id.clone(), task.clone());
    }
    Ok((finish(run, "delegate", outcome, task_record), id))
}

/// A task as the suite reads it (README, "Calls").
fn task_record(task: &DelegatedTask) -> Value {
    json!({
        "id": task.id,
        "child_room_id": task.child_room_id,
        "parent_room_id": task.parent_room_id,
        "agent_id": task.agent_id,
        "task": task.task,
        "brief": task.brief,
        "description": task.description,
        "hand_back": task.hand_back,
        "status": task.status(),
        "result": task.result(),
    })
}

/// A verb of the turn's host the current code runs within.
async fn host_call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let host: TurnHost =
        current_turn_host().ok_or_else(|| format!("{name} is made outside a turn"))?;
    let room = || field::<RoomId>(spec, "room_id");
    Ok(match name {
        "host.send_event" => {
            let channel: ChannelId = field(spec, "channel_id")?;
            let content: EventContent = field(spec, "content")?;
            let options = send_event_options(spec)?;
            let outcome = host.send_event(&room()?, &channel, content, options).await;
            let called = finish(run, name, outcome, dump);
            let id = called
                .as_ref()
                .ok()
                .and_then(|event| event.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            (called, id)
        }
        "host.post_status" => {
            let options = PostStatusOptions {
                detail: optional(spec, "detail")?.unwrap_or_default(),
                metadata: optional(spec, "metadata")?,
            };
            let (agent, action, status) = status(spec)?;
            let outcome = host.post_status(agent, action, status, options).await;
            (finish(run, name, outcome, |()| Value::Null), None)
        }
        "host.update_room_metadata" => {
            let options = UpdateRoomMetadataOptions {
                organization_id: organization_id(spec),
            };
            let metadata: Metadata = field(spec, "metadata")?;
            let outcome = host.update_room_metadata(&room()?, metadata, options).await;
            (finish(run, name, outcome, dump), None)
        }
        "host.add_observation" => {
            let template = &spec["observation"];
            let (room_id, channel_id) = (
                template["room_id"].as_str().unwrap_or_default(),
                template["channel_id"].as_str().unwrap_or_default(),
            );
            let observation: Observation =
                serde_json::from_value(complete_observation(template, room_id, channel_id))
                    .map_err(|error| format!("observation: {error}"))?;
            let outcome = host.add_observation(observation).await;
            (finish(run, name, outcome, |()| Value::Null), None)
        }
        other => return Err(format!("no call {other}")),
    })
}

/// A status call's agent, action and level.
fn status(spec: &Value) -> Result<(String, String, StatusLevel), Unreadable> {
    Ok((
        field(spec, "agent_id")?,
        field(spec, "action")?,
        field(spec, "status")?,
    ))
}

/// What a scripted hook whose payload is no event gives.
type PayloadHookFuture<P> =
    std::pin::Pin<Box<dyn Future<Output = Result<HookResult<P>, HookError>> + Send>>;

/// The handler of the scripted hook `spec` whose payload `P` is no event,
/// a status entry or an observation: it records its call, the payload
/// under `member` (README, "Expectations"), then reacts.
pub(crate) fn payload_hook_handler<P: Serialize + Send + 'static>(
    spec: &Value,
    run: &Arc<Run>,
    member: &'static str,
) -> impl Fn(P, RoomContext) -> PayloadHookFuture<P> + Send + Sync + 'static {
    let reactions = Arc::new(crate::scripted::Reactions::new(spec.get("reactions")));
    let name = spec["name"].as_str().unwrap_or_default().to_owned();
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |payload: P, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        let payload = serde_json::to_value(&payload).unwrap_or_default();
        Box::pin(async move {
            let allow = HookResult::allow(AllowOptions::default());
            let Some(run) = run.upgrade() else {
                return Ok(allow);
            };
            let room_id = context.room.id.to_string();
            run.record_payload_hook(&name, &trigger, &room_id, member, payload);
            if let Some(reaction) = reactions.pick(&json!({ "room_id": room_id })) {
                perform(&run, &reaction, &Scope::in_room(&room_id)).await?;
            }
            Ok(allow)
        })
    }
}
