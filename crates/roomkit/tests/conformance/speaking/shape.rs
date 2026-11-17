//! Fixture controls are checked before policy fallback can hide a malformed script.
use serde_json::Value;

fn object<'a>(
    value: &'a Value,
    allowed: &[&str],
) -> Result<&'a serde_json::Map<String, Value>, String> {
    let object = value.as_object().ok_or("expected an object")?;
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("unknown member {key}"));
    }
    Ok(object)
}

fn strings(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|items| items.iter().all(Value::is_string))
}

pub(super) fn decision(value: &Value) -> Result<(), String> {
    let members = object(value, &["mode", "reason", "judgments", "notes", "final"])?;
    if !matches!(value["mode"].as_str(), Some("speak" | "offer" | "silent")) {
        return Err("decision.mode must be speak, offer or silent".into());
    }
    if members.get("reason").is_some_and(|v| !v.is_string())
        || members.get("final").is_some_and(|v| !v.is_boolean())
        || members.get("notes").is_some_and(|v| !strings(v))
        || members.get("judgments").is_some_and(|v| {
            !v.as_object()
                .is_some_and(|m| m.values().all(Value::is_number))
        })
    {
        return Err("malformed decision field".into());
    }
    if value["final"] == true && value["mode"] != "silent" {
        return Err("only a silent decision can be final".into());
    }
    Ok(())
}

fn thought(value: &Value) -> Result<(), String> {
    let members = object(value, &["text", "want_to_say", "urgent"])?;
    if members.get("text").is_some_and(|v| !v.is_string())
        || members.get("urgent").is_some_and(|v| !v.is_boolean())
        || members.get("want_to_say").is_some_and(|v| !strings(v))
    {
        return Err("malformed thought field".into());
    }
    Ok(())
}

pub(super) fn script(value: &Value, thinking: bool) -> Result<(), String> {
    let (list, payload) = if thinking {
        ("thoughts", "thought")
    } else {
        ("decisions", "decision")
    };
    let members = object(value, &[list])?;
    let entries = members
        .get(list)
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .ok_or("script must be a nonempty array")?;
    for entry in entries {
        let members = object(entry, &[payload, "fail", "gate"])?;
        if members.contains_key(payload) == members.contains_key("fail") {
            return Err(format!("exactly one of {payload} and fail is required"));
        }
        if let Some(value) = members.get(payload) {
            if thinking {
                thought(value)?;
            } else {
                decision(value)?;
            }
        }
        for field in ["fail", "gate"] {
            if members.get(field).is_some_and(|value| !value.is_string()) {
                return Err(format!("{field} must be a string"));
            }
        }
    }
    Ok(())
}

pub(crate) fn check(channel: &Value, path: &str, found: &mut Vec<String>) {
    for (name, thinking) in [("speak_policy", false), ("thinker", true)] {
        if let Some(value) = channel.get(name)
            && let Err(error) = script(value, thinking)
        {
            found.push(format!("{path}.{name}: {error}"));
        }
    }
    for name in ["speak_timeout_ms", "think_wait_ms"] {
        if channel.get(name).is_some_and(|value| !value.is_number()) {
            found.push(format!("{path}.{name}: expected a number"));
        }
    }
}

pub(super) fn patch(value: &Value) -> Result<(), String> {
    let members = object(value, &["decision"])?;
    decision(members.get("decision").ok_or("missing decision")?)
}

pub(crate) fn hook(hook: &Value, path: &str, found: &mut Vec<String>) {
    if hook["trigger"] != "on_speak_decision" {
        return;
    }
    for (index, reaction) in hook["reactions"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let result = &reaction["result"];
        if result["action"] == "modify"
            && let Err(error) = patch(&result["event"])
        {
            found.push(format!("{path}.reactions[{index}].result.event: {error}"));
        }
    }
}
