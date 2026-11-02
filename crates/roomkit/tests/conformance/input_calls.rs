//! The kit's calls of human input (roomkit-specs `conformance/README.md`,
//! "Calls"): `answer_input` and `reject_input`, the kit's doors to a
//! turn's request (RFC §10.6.5), and the scripted hook on
//! `on_user_input_required`, whose actions name the request `$request`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use roomkit::human_input::PendingInputEvent;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookAction, HookResult, RoomContext};
use roomkit::{HookError, RejectInputOptions};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::{Run, Scope};
use crate::scripted::{Reactions, perform};

/// The framework event of a human-input request's decision (RFC §8.2,
/// §9.3), recorded beside the §26.1 table's.
pub(crate) const INPUT_EVENTS: [&str; 1] = ["user_input_required"];

/// Each call's arguments, beside `call` and `organization_id`.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "answer_input" => &["request_id", "value"],
        "reject_input" => &["request_id", "reason"],
        _ => return None,
    })
}

pub(crate) fn handles(call: &str) -> bool {
    arguments(call).is_some()
}

/// Makes the call `spec` names, its references already resolved.
pub(crate) async fn call(
    run: &Arc<Run>,
    name: &str,
    spec: &Value,
) -> Result<(Called, Option<String>), Unreadable> {
    let request: String = field(spec, "request_id")?;
    let outcome = match name {
        "answer_input" => {
            let value: String = field(spec, "value")?;
            run.kit().answer_input(&request, &value).await
        }
        "reject_input" => {
            let reason: Option<String> = optional(spec, "reason")?;
            let options = RejectInputOptions {
                reason: reason.unwrap_or_default(),
            };
            run.kit().reject_input(&request, options).await
        }
        other => return Err(format!("no call {other}")),
    };
    Ok((
        finish(run, name, outcome, |taken| Value::Bool(*taken)),
        None,
    ))
}

/// What a scripted hook on `on_user_input_required` gives.
type InputHookFuture =
    Pin<Box<dyn Future<Output = Result<HookResult<PendingInputEvent>, HookError>> + Send>>;

/// The handler of the scripted hook `spec` on `on_user_input_required`: it
/// records its call, the request under `request`, then reacts, its actions
/// naming the request `$request`; its `result` is the hook's (README,
/// "Scripted agents").
pub(crate) fn input_hook_handler(
    spec: &Value,
    run: &Arc<Run>,
) -> impl Fn(PendingInputEvent, RoomContext) -> InputHookFuture + Send + Sync + 'static {
    let reactions = Arc::new(Reactions::new(spec.get("reactions")));
    let name = spec["name"].as_str().unwrap_or_default().to_owned();
    let trigger = spec["trigger"].as_str().unwrap_or_default().to_owned();
    let run = Arc::downgrade(run);
    move |event: PendingInputEvent, context: RoomContext| {
        let reactions = Arc::clone(&reactions);
        let (name, trigger, run) = (name.clone(), trigger.clone(), Weak::clone(&run));
        Box::pin(async move {
            let allow = HookResult::allow(AllowOptions::default());
            let Some(run) = run.upgrade() else {
                return Ok(allow);
            };
            let room_id = context.room.id.to_string();
            run.record_request_hook(&name, &trigger, &room_id, request_record(&event));
            let Some(reaction) = reactions.pick(&json!({ "room_id": room_id })) else {
                return Ok(allow);
            };
            let scope = Scope::with_request(&room_id, &event.pending_id);
            perform(&run, &reaction, &scope).await?;
            match reaction.get("result") {
                Some(result) => Ok(input_hook_result(result, &event)?),
                None => Ok(allow),
            }
        })
    }
}

/// A scenario's hook result on a request: an allow, a block with its
/// reason, or a modify whose `event` is a patch on `request`, its members
/// replacing the request's.
fn input_hook_result(
    spec: &Value,
    request: &PendingInputEvent,
) -> Result<HookResult<PendingInputEvent>, String> {
    let action: HookAction = serde_json::from_value(spec["action"].clone())
        .map_err(|error| format!("action: {error}"))?;
    let mut result = HookResult::new(action);
    result.reason = spec["reason"].as_str().map(str::to_owned);
    if let Some(Value::Object(patch)) = spec.get("event") {
        let mut patched = serde_json::to_value(request).map_err(|error| error.to_string())?;
        for (member, value) in patch {
            if patched.get(member).is_none() {
                return Err(format!("a request has no member {member}"));
            }
            patched[member] = value.clone();
        }
        result.event =
            Some(serde_json::from_value(patched).map_err(|error| format!("event: {error}"))?);
    }
    Ok(result)
}

/// A request as a scenario reads it (README, "Expectations").
fn request_record(event: &PendingInputEvent) -> Value {
    json!({
        "pending_id": event.pending_id,
        "tool_name": event.tool_name,
        "arguments": event.arguments,
        "room_id": event.room_id,
        "tool_call_id": event.tool_call_id,
        "channel_id": event.channel_id,
        "channel_type": event.channel_type,
        "actor_id": event.actor_id,
    })
}
