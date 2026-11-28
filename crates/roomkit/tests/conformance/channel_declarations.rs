//! What a scenario's channel declares beside its reactions (roomkit-specs
//! `conformance/README.md`, "Setup" and "Scripted agents"), as the shape
//! check reads it: a streaming channel's default stream reader and its
//! cut's reactions, a channel's room-turn declarations, an agent's intents
//! lists and how it is served, and a model that streams text alone. Python's
//! `_shape._declarations` and `_shape._model`.

use serde_json::Value;

/// A round's fields only a structured stream writes, `deltas` aside.
const STREAM_ONLY: [&str; 6] = [
    "thinking",
    "stream",
    "done",
    "usage_at_start",
    "failure_after_calls",
    "failure_after_done",
];

/// The problems of `channel`'s declarations, at `path`, pushed on `found`.
pub(crate) fn check(channel: &Value, path: &str, found: &mut Vec<String>) {
    for member in ["default_deliver_stream", "deliver_cut"] {
        if channel.get(member).is_some() && channel["streams"] != true {
            found.push(format!("{path}.{member}: requires streams"));
        }
    }
    if channel
        .get("default_deliver_stream")
        .is_some_and(|flag| flag != true)
    {
        found.push(format!("{path}.default_deliver_stream: true or absent"));
    }
    let steering = ["steers_room_turns", "serializes_room_turns"]
        .iter()
        .filter(|member| channel.get(**member).is_some())
        .count();
    if steering > 0 && (channel.get("model").is_some() || steering != 2) {
        found.push(format!(
            "{path}: steers_room_turns and serializes_room_turns go together, no agent"
        ));
    }
    for flag in [
        "steers_room_turns",
        "serializes_room_turns",
        "tool_call_intents_required",
    ] {
        if channel.get(flag).is_some_and(|value| !value.is_boolean()) {
            found.push(format!("{path}.{flag}: expected a boolean"));
        }
    }
    if let Some(only) = channel.get("tool_call_intents_only")
        && !only
            .as_array()
            .is_some_and(|names| names.iter().all(Value::is_string))
    {
        found.push(format!(
            "{path}.tool_call_intents_only: expected tool names"
        ));
    }
    serving(channel, path, found);
}

/// An agent's `serving` (README, `serving`): `both`, with a router, or
/// `neither`; it and the intents lists are a scripted agent's.
fn serving(channel: &Value, path: &str, found: &mut Vec<String>) {
    match channel.get("serving").and_then(Value::as_str) {
        None if channel.get("serving").is_some() => {
            found.push(format!("{path}.serving: both or neither"));
        }
        Some("both") if channel.get("router").is_none() => {
            found.push(format!("{path}.serving: both needs a router"));
        }
        None | Some("both" | "neither") => {}
        Some(_) => found.push(format!("{path}.serving: both or neither")),
    }
    for member in [
        "tool_call_intents_required",
        "tool_call_intents_only",
        "serving",
    ] {
        if channel.get(member).is_some() && channel.get("model").is_none() {
            found.push(format!("{path}.{member}: a scripted agent's"));
        }
    }
}

/// The problems of a model that streams text alone (README,
/// `model.stream_text_only`), at `path`: it streams no structured stream.
pub(crate) fn text_stream(model: &Value, path: &str, found: &mut Vec<String>) {
    for flag in ["stream_text_only", "refuses_images"] {
        if model.get(flag).is_some_and(|value| !value.is_boolean()) {
            found.push(format!("{path}.{flag}: expected a boolean"));
        }
    }
    if model["stream_text_only"] != true {
        return;
    }
    if model["streaming"] == true {
        found.push(format!(
            "{path}.stream_text_only: streams no structured stream"
        ));
    }
    for (index, round) in model["rounds"].as_array().into_iter().flatten().enumerate() {
        for field in STREAM_ONLY {
            if round.get(field).is_some() {
                found.push(format!(
                    "{path}.rounds[{index}].{field}: requires a structured stream"
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    fn problems(channel: &Value) -> Vec<String> {
        let mut found = Vec::new();
        super::check(channel, "c", &mut found);
        super::text_stream(&channel["model"], "c.model", &mut found);
        found
    }

    // Python: tests/conformance/_shape.py _declarations, _model.
    #[test]
    fn a_channel_s_declarations_are_read_as_python_reads_them() {
        let model = json!({ "rounds": [] });
        for (channel, valid) in [
            (
                json!({ "streams": true, "default_deliver_stream": true }),
                true,
            ),
            (json!({ "default_deliver_stream": true }), false),
            (
                json!({ "streams": true, "default_deliver_stream": false }),
                false,
            ),
            (json!({ "streams": true, "deliver_cut": [] }), true),
            (
                json!({ "steers_room_turns": true, "serializes_room_turns": false }),
                true,
            ),
            (json!({ "steers_room_turns": true }), false),
            (
                json!({ "model": model, "steers_room_turns": true, "serializes_room_turns": true }),
                false,
            ),
            (
                json!({ "model": model, "tool_call_intents_only": ["a"] }),
                true,
            ),
            (
                json!({ "model": model, "tool_call_intents_only": "a" }),
                false,
            ),
            (json!({ "tool_call_intents_required": true }), false),
            (json!({ "model": model, "serving": "neither" }), true),
            (json!({ "model": model, "serving": "both" }), false),
            (json!({ "model": model, "serving": null }), false),
            (
                json!({ "model": { "stream_text_only": true, "rounds": [{ "deltas": ["a"] }] } }),
                true,
            ),
            (
                json!({ "model": { "stream_text_only": true, "streaming": true, "rounds": [] } }),
                false,
            ),
            (
                json!({ "model": { "stream_text_only": true, "rounds": [{ "done": false }] } }),
                false,
            ),
            (
                json!({ "model": { "refuses_images": "yes", "rounds": [] } }),
                false,
            ),
        ] {
            assert_eq!(
                problems(&channel).is_empty(),
                valid,
                "{channel}: {:?}",
                problems(&channel)
            );
        }
    }
}
