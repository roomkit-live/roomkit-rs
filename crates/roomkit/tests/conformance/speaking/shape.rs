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
    script_with(value, thinking, &[])
}

pub(super) fn script_with(value: &Value, thinking: bool, beside: &[&str]) -> Result<(), String> {
    let (list, payload) = if thinking {
        ("thoughts", "thought")
    } else {
        ("decisions", "decision")
    };
    let allowed: Vec<&str> = [list].into_iter().chain(beside.iter().copied()).collect();
    let members = object(value, &allowed)?;
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

/// The members of the implementation's classifier policy (README, `speak_policy`).
const CLASSIFIER_POLICY: [&str; 8] = [
    "classifier",
    "agent_name",
    "agent_role",
    "questions",
    "languages",
    "history",
    "proactivity",
    "composition",
];

/// A scripted classifier's controls: each call answers or fails, maybe at a gate.
pub(super) fn classifier(value: &Value) -> Result<(), String> {
    let members = object(value, &["calls"])?;
    let calls = members
        .get("calls")
        .and_then(Value::as_array)
        .filter(|calls| !calls.is_empty())
        .ok_or("classifier.calls must be a nonempty array")?;
    for call in calls {
        let members = object(call, &["answers", "fail", "gate"])?;
        if members.contains_key("answers") == members.contains_key("fail") {
            return Err("exactly one of answers and fail is required".into());
        }
        let readable = members.get("answers").is_none_or(|answers| {
            answers
                .as_object()
                .is_some_and(|m| m.values().all(|v| v.is_number() || v.is_string()))
        });
        if !readable
            || ["fail", "gate"]
                .iter()
                .any(|k| members.get(*k).is_some_and(|v| !v.is_string()))
        {
            return Err("malformed classifier call".into());
        }
    }
    Ok(())
}

fn question(value: &Value) -> Result<(), String> {
    let members = object(value, &["instructions", "yes", "no", "options", "levels"])?;
    let kinds = ["options", "levels"]
        .iter()
        .filter(|k| members.contains_key(**k))
        .count();
    let described = members.contains_key("yes") || members.contains_key("no");
    if !value["instructions"].is_string() || kinds > 1 || (kinds == 1 && described) {
        return Err("a question is a yes/no question, a choice or a score".into());
    }
    let options = members.get("options").is_none_or(|options| {
        options
            .as_object()
            .is_some_and(|m| m.len() >= 2 && m.values().all(Value::is_string))
    });
    let levels = members.get("levels").is_none_or(|levels| {
        levels
            .as_array()
            .is_some_and(|l| l.len() >= 2 && l.iter().all(Value::is_string))
    });
    let told = ["yes", "no"]
        .iter()
        .all(|k| members.get(*k).is_none_or(|v| v.is_string() || v.is_null()));
    if !(options && levels && told) {
        return Err("malformed question member".into());
    }
    Ok(())
}

fn classifier_policy(members: &serde_json::Map<String, Value>) -> Result<(), String> {
    classifier(&members["classifier"])?;
    if members.contains_key("decisions") {
        return Err("decisions and classifier are two policies: give one".into());
    }
    if !members.get("agent_name").is_some_and(Value::is_string)
        || members.get("agent_role").is_some_and(|v| !v.is_string())
        || members.get("history").is_some_and(|v| !v.is_i64())
        || members.get("proactivity").is_some_and(|v| !v.is_number())
        || members.get("languages").is_some_and(|v| {
            !v.as_object()
                .is_some_and(|m| m.values().all(Value::is_string))
        })
    {
        return Err("malformed classifier policy argument".into());
    }
    for asked in members
        .get("questions")
        .map(|q| q.as_object().ok_or("questions: expected an object"))
        .transpose()?
        .into_iter()
        .flatten()
    {
        question(asked.1)?;
    }
    if let Some(composition) = members.get("composition") {
        decision(
            object(composition, &["decision"])?
                .get("decision")
                .ok_or("missing decision")?,
        )?;
    }
    Ok(())
}

fn answer_only(value: &Value) -> Result<(), String> {
    let people = object(value, &["people"])?
        .get("people")
        .and_then(Value::as_array);
    if !people.is_some_and(|people| !people.is_empty() && people.iter().all(Value::is_string)) {
        return Err("answer_only.people must be a nonempty array of strings".into());
    }
    Ok(())
}

/// A speak policy: scripted, or the classifier policy, maybe answer-only.
fn policy(value: &Value) -> Result<(), String> {
    let beside: Vec<&str> = ["answer_only"]
        .into_iter()
        .chain(CLASSIFIER_POLICY)
        .collect();
    let members = object(value, &[&["decisions"], beside.as_slice()].concat())?;
    if let Some(only) = members.get("answer_only") {
        answer_only(only)?;
    }
    if members.contains_key("classifier") {
        return classifier_policy(members);
    }
    if let Some(key) = members
        .keys()
        .find(|key| CLASSIFIER_POLICY.contains(&key.as_str()))
    {
        return Err(format!("{key} needs classifier"));
    }
    script_with(value, false, &["answer_only"])
}

/// A thinker: scripted, or the implementation's LLM thinker over a scripted model.
fn thinker(value: &Value) -> Result<(), String> {
    let Some(llm) = value.get("llm") else {
        return script(value, true);
    };
    object(value, &["llm"])?;
    let members = object(
        llm,
        &[
            "model",
            "instructions",
            "max_tokens",
            "reasoning_effort",
            "timeout_ms",
        ],
    )?;
    if !members.get("model").is_some_and(Value::is_object)
        || ["instructions", "reasoning_effort"]
            .iter()
            .any(|k| members.get(*k).is_some_and(|v| !v.is_string()))
        || members.get("max_tokens").is_some_and(|v| !v.is_u64())
        || members.get("timeout_ms").is_some_and(|v| !v.is_number())
    {
        return Err("malformed thinker.llm".into());
    }
    Ok(())
}

/// A check of one of a channel's speaking members.
type Read = fn(&Value) -> Result<(), String>;

pub(crate) fn check(channel: &Value, path: &str, found: &mut Vec<String>) {
    let reads: [(&str, Read); 2] = [("speak_policy", policy), ("thinker", thinker)];
    for (name, read) in reads {
        if let Some(value) = channel.get(name)
            && let Err(error) = read(value)
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

/// A speak decision's patch: its decision, and the event, room and channel
/// it may name (README, `speak_policy`).
pub(super) fn patch(value: &Value) -> Result<(), String> {
    let members = object(value, &["decision", "event", "room_id", "channel_id"])?;
    for member in ["room_id", "channel_id"] {
        if members.get(member).is_some_and(|value| !value.is_string()) {
            return Err(format!("{member}: expected a text"));
        }
    }
    if members.get("event").is_some_and(|value| !value.is_object()) {
        return Err("event: expected a patch".into());
    }
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
