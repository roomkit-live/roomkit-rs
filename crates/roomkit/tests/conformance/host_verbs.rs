//! Declared owner callbacks reached only through the public registration and host.

#[path = "host_verbs/tools.rs"]
mod tools;
#[path = "host_verbs/validate.rs"]
mod validate;
pub(crate) use tools::call_from_tool;
pub(crate) use validate::{handler_shape, tool_shape};

use std::sync::{Arc, Weak};

use roomkit::model::{Metadata, ResponseMetadata};
use roomkit::turns::{DeclareHostVerbOptions, DeclaredVerb};
use roomkit::{ChannelError, ClassedError, RoomKitError, current_turn_host};
use roomkit_ai::{
    current_response_metadata, current_tool_actor_id, current_tool_allowed_names,
    current_tool_call, current_tool_chain_depth, current_tool_requester, current_tool_room,
    current_tool_room_id,
};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable, field, finish, optional};
use crate::run::{Run, lock};
use crate::scripted::ScriptedFailure;

pub(crate) fn arguments(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "declare_host_verb" => Some(&["name", "handler", "between_turns"]),
        "host.call" => Some(&["verb", "arguments", "record_keys", "inspect_error"]),
        "current_turn_host" => Some(&[]),
        _ => None,
    }
}

pub(crate) async fn call(run: &Arc<Run>, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    if name == "current_turn_host" {
        // Whether a turn's host is bound where the action runs (RFC §10.6.5).
        return Ok(Ok(Value::Bool(current_turn_host().is_some())));
    }
    if name == "host.call" {
        let mut result = invoke(spec).await?;
        if spec.get("record_keys").is_some() {
            result["record"] = record(&spec["record_keys"]);
        }
        return Ok(Ok(result));
    }
    let script = spec["handler"].clone();
    let weak = Arc::downgrade(run);
    let verb = DeclaredVerb::new(move |arguments| {
        let script = script.clone();
        let weak = Weak::clone(&weak);
        async move {
            let run = weak
                .upgrade()
                .ok_or_else(|| ScriptedFailure("scenario ended".into()))?;
            declared(&run, &script, arguments).await
        }
    });
    let name_of_verb: String = field(spec, "name")?;
    let options = DeclareHostVerbOptions {
        between_turns: optional(spec, "between_turns")?.unwrap_or(false),
    };
    let outcome = run.kit().declare_host_verb(&name_of_verb, verb, options);
    Ok(finish(run, name, outcome, |()| Value::Null))
}

async fn declared(run: &Run, script: &Value, arguments: Metadata) -> Result<Value, ChannelError> {
    let mut context = context(&arguments);
    if script.get("record_keys").is_some() {
        context["record"] = record(&script["record_keys"]);
    }
    let observation = script["as"].as_str().map(|label| {
        let mut observed = lock(&run.observed);
        let actions = observed.actions.entry(label.to_owned()).or_default();
        let index = actions.len();
        actions.push(json!({"result": context, "error": null}));
        (label, index)
    });
    if let Some(gate) = script["gate"].as_str() {
        let mut cancelled = Cancelled {
            run,
            gate: script["cancelled"].as_str(),
        };
        run.gates.hold(gate).await;
        cancelled.gate = None;
    }
    if script.get("record_keys").is_some() {
        context["record_after_gate"] = record(&script["record_keys"]);
        if let Some((label, index)) = observation {
            lock(&run.observed).actions.get_mut(label).unwrap()[index]["result"] = context.clone();
        }
    }
    change_record(script)?;
    if let Some(label) = script["raises_captured"].as_str() {
        let error = lock(&run.captured_errors)
            .remove(label)
            .ok_or_else(|| ScriptedFailure(format!("no captured error {label}")))?;
        return Err(Box::new(error));
    }
    if let Some(failure) = script.get("raises") {
        let message = failure["message"].as_str().unwrap_or_default().to_owned();
        return Err(match failure["kind"].as_str() {
            Some("ValueError") => Box::new(RoomKitError::Value(message)),
            Some("RuntimeError") => Box::new(RoomKitError::Runtime(message)),
            Some("PermissionError") => Box::new(ClassedError::new(
                "builtins:PermissionError",
                ScriptedFailure(message),
            )),
            _ => {
                return Err(Box::new(ScriptedFailure(
                    "unknown declared error kind".into(),
                )));
            }
        });
    }
    if script["returns_context"] == true {
        return Ok(context);
    }
    if script["returns_arguments"] == true {
        return Ok(Value::Object(arguments));
    }
    Ok(script["returns"].clone())
}

struct Cancelled<'a> {
    run: &'a Run,
    gate: Option<&'a str>,
}
impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        if let Some(gate) = self.gate {
            self.run.gates.reach(gate);
        }
    }
}

fn context(arguments: &Metadata) -> Value {
    let call = current_tool_call();
    let room = current_tool_room();
    let names = current_tool_allowed_names().map(|names| {
        let mut names: Vec<_> = names.into_iter().collect();
        names.sort();
        names
    });
    json!({
        "arguments": arguments,
        "room_id": current_tool_room_id(),
        "organization_id": room.as_ref().and_then(|room| room.organization_id.as_ref()),
        "room_metadata": room.as_ref().map(|room| &room.metadata),
        "channel_id": call.as_ref().and_then(|call| call.channel_id.as_ref()),
        "tool_call_id": call.as_ref().map(|call| &call.tool_call_id),
        "actor_id": current_tool_actor_id(),
        "requester": current_tool_requester(),
        "chain_depth": current_tool_chain_depth(),
        "allowed_names": names,
        "has_record": current_response_metadata().is_some(),
        "has_turn_host": current_turn_host().is_some(),
    })
}

fn record(keys: &Value) -> Value {
    current_response_metadata().map_or(Value::Null, |record| {
        Value::Object(
            keys.as_array()
                .into_iter()
                .flatten()
                .filter_map(|key| {
                    let key = key.as_str()?;
                    record.get(key).map(|value| (key.to_owned(), value))
                })
                .collect(),
        )
    })
}

fn change_record(script: &Value) -> Result<(), ScriptedFailure> {
    let keys = [
        "writes_record",
        "removes_record",
        "updates_record",
        "removes_entries",
    ];
    if !keys.iter().any(|key| script.get(key).is_some()) {
        return Ok(());
    }
    let record = current_response_metadata()
        .ok_or_else(|| ScriptedFailure("record change outside a turn".into()))?;
    for (key, value) in script["writes_record"].as_object().into_iter().flatten() {
        record.insert(key, value.clone());
    }
    for key in script["removes_record"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        record.pop(key);
    }
    change_entries(&record, script)
}

fn change_entries(record: &ResponseMetadata, script: &Value) -> Result<(), ScriptedFailure> {
    for (key, entries) in script["updates_record"].as_object().into_iter().flatten() {
        let value = record.get(key).unwrap_or_else(|| json!({}));
        let mut value = value
            .as_object()
            .cloned()
            .ok_or_else(|| ScriptedFailure("record entry is not a mapping".into()))?;
        value.extend(
            entries
                .as_object()
                .cloned()
                .ok_or_else(|| ScriptedFailure("updates are not a mapping".into()))?,
        );
        record.insert(key, Value::Object(value));
    }
    for (key, entries) in script["removes_entries"].as_object().into_iter().flatten() {
        let value = record
            .get(key)
            .ok_or_else(|| ScriptedFailure("record entry missing".into()))?;
        let mut value = value
            .as_object()
            .cloned()
            .ok_or_else(|| ScriptedFailure("record entry is not a mapping".into()))?;
        for entry in entries
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            value.shift_remove(entry);
        }
        record.insert(key, Value::Object(value));
    }
    Ok(())
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "an unexpected error fails the fixture instead of being normalized"
)]
async fn invoke(spec: &Value) -> Result<Value, Unreadable> {
    let host = current_turn_host().ok_or("scripted host call outside a turn")?;
    let verb: String = field(spec, "verb")?;
    let arguments: Option<Metadata> = match spec.get("arguments") {
        None | Some(Value::Null) => None,
        Some(_) => Some(field(spec, "arguments")?),
    };
    Ok(match host.call(&verb, arguments).await {
        Ok(result) => json!({"result": result, "error": null}),
        Err(error) if spec["inspect_error"] == true => {
            json!({"result":null,"error":crate::channel_verbs::turn_errors::inspect(&error)})
        }
        Err(error) => {
            let kind = match &error {
                RoomKitError::RemoteTurn { kind, .. } if kind == "PermissionError" => {
                    "builtins:PermissionError".to_owned()
                }
                RoomKitError::RemoteTurn { kind, .. } => kind.clone(),
                RoomKitError::Value(_) => "builtins:ValueError".to_owned(),
                RoomKitError::Runtime(_) => "builtins:RuntimeError".to_owned(),
                _ => return Err(format!("unmapped declared host error: {error:?}")),
            };
            json!({"result": null, "error": {"kind": kind, "message": error.to_string()}})
        }
    })
}
