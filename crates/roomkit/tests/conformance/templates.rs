//! Templates: the records a scenario writes in part, completed with what
//! the record requires (roomkit-specs `conformance/README.md`, "Templates"),
//! and a hook's result built from them.
use std::time::{SystemTime, UNIX_EPOCH};

use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, RoomEvent, Timestamp};
use serde_json::{Map, Value, json};

/// The instant a scenario's record is written at, at millisecond precision.
#[expect(
    clippy::disallowed_methods,
    reason = "a template's instant is the wall clock's, as the Python runner's is"
)]
fn now() -> Value {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let millis = i64::try_from(elapsed.as_millis()).unwrap_or_default();
    let now = Timestamp::from_unix_millis(millis).unwrap_or(Timestamp::UNIX_EPOCH);
    serde_json::to_value(now).unwrap_or_default()
}

fn fresh_id() -> Value {
    Value::String(uuid::Uuid::new_v4().simple().to_string())
}

/// A record from a scenario's template, its missing required members filled
/// (README, "Templates"), in the serialized form.
pub(crate) fn complete_event(template: &Value, room_id: &str, source: &Value) -> Value {
    let mut event =
        json!({ "id": fresh_id(), "room_id": room_id, "type": "message", "created_at": now() });
    merge(&mut event, template);
    let mut merged_source = source.clone();
    if let Some(given) = template.get("source") {
        merge(&mut merged_source, given);
    }
    event["source"] = merged_source;
    event
}

pub(crate) fn complete_participant(template: &Value, room_id: &str) -> Value {
    let mut participant = json!({ "room_id": room_id, "joined_at": now() });
    merge(&mut participant, template);
    participant
}

pub(crate) fn complete_task(template: &Value, room_id: &str) -> Value {
    let mut task = json!({ "id": fresh_id(), "room_id": room_id, "created_at": now() });
    merge(&mut task, template);
    task
}

pub(crate) fn complete_observation(template: &Value, room_id: &str, channel_id: &str) -> Value {
    let mut observation = json!({ "id": fresh_id(), "room_id": room_id, "channel_id": channel_id, "created_at": now() });
    merge(&mut observation, template);
    observation
}

fn merge(target: &mut Value, patch: &Value) {
    if let (Value::Object(target), Value::Object(patch)) = (target, patch) {
        for (key, value) in patch {
            target.insert(key.clone(), value.clone());
        }
    }
}

pub(crate) fn system_source() -> Value {
    json!({ "channel_id": "system", "channel_type": "system", "direction": "inbound" })
}

/// A `HookResult` from a scenario's, a modify's event read as a patch on the
/// payload.
pub(crate) fn hook_result(
    spec: Option<&Value>,
    payload: &RoomEvent,
    room_id: &str,
) -> Result<HookResult, serde_json::Error> {
    let Some(spec) = spec else {
        return Ok(HookResult::allow(AllowOptions::default()));
    };
    let mut data: Map<String, Value> = spec.as_object().cloned().unwrap_or_default();
    if data.get("action") == Some(&json!("modify"))
        && let Some(patch) = data.get("event").cloned()
    {
        let mut patched = serde_json::to_value(payload)?;
        merge(&mut patched, &patch);
        data.insert("event".to_owned(), patched);
    }
    let injected: Vec<Value> = data
        .get("injected_events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|item| {
            json!({
                "event": complete_event(&item["event"], room_id, &system_source()),
                "target_channel_ids": item.get("target_channel_ids").cloned().unwrap_or(Value::Null),
            })
        })
        .collect();
    data.insert("injected_events".to_owned(), Value::Array(injected));
    let tasks: Vec<Value> = data
        .get("tasks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|t| complete_task(t, room_id))
        .collect();
    data.insert("tasks".to_owned(), Value::Array(tasks));
    let observations: Vec<Value> = data
        .get("observations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|o| complete_observation(o, room_id, "system"))
        .collect();
    data.insert("observations".to_owned(), Value::Array(observations));
    serde_json::from_value(Value::Object(data))
}
