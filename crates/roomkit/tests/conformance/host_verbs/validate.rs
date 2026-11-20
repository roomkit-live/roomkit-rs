//! Nested fixture fields are checked before a scenario runs.
use serde_json::Value;

fn members(value: &Value, known: &[&str], path: &str, found: &mut Vec<String>) {
    if let Some(map) = value.as_object() {
        for key in map.keys() {
            if !known.contains(&key.as_str()) {
                found.push(format!("{path}.{key}: unknown member"));
            }
        }
    }
}

pub(crate) fn tool_shape(script: &Value, path: &str, found: &mut Vec<String>) {
    members(script, &["calls", "concurrent", "record_keys"], path, found);
    for (index, call) in script["calls"].as_array().into_iter().flatten().enumerate() {
        members(
            call,
            &["verb", "arguments", "returned", "cancel_after"],
            &format!("{path}.calls[{index}]"),
            found,
        );
    }
}

pub(crate) fn handler_shape(script: &Value, path: &str, found: &mut Vec<String>) {
    members(
        script,
        &[
            "returns",
            "returns_context",
            "returns_arguments",
            "raises_captured",
            "raises",
            "as",
            "gate",
            "writes_record",
            "removes_record",
            "updates_record",
            "removes_entries",
            "record_keys",
            "cancelled",
        ],
        path,
        found,
    );
    members(
        &script["raises"],
        &["kind", "message"],
        &format!("{path}.raises"),
        found,
    );
}

#[cfg(test)]
mod tests {
    use crate::shape::unknown_members;
    use serde_json::json;

    #[test]
    fn unknown_script_and_call_fields_are_refused_for_channels_and_turn_configs() {
        for configured in [false, true] {
            for nested in [false, true] {
                let mut script = json!({"calls": [{"verb": "app.read"}]});
                if nested {
                    script["calls"][0]["argument"] = json!({});
                } else {
                    script["concurrant"] = json!(true);
                }
                let tool = json!({"name": "lookup", "host_calls": script});
                let mut channel = json!({"id": "agent", "type": "ai"});
                if configured {
                    channel["config_provider"] = json!({"r1": {"tool_handler": [tool]}});
                } else {
                    channel["tools"] = json!([tool]);
                }
                assert_ne!(
                    unknown_members(&json!({"setup": {"channels": [channel]}})),
                    Vec::<String>::new()
                );
            }
        }
    }

    #[test]
    fn unknown_handler_and_error_fields_are_refused() {
        for nested in [false, true] {
            let mut handler = json!({"raises": {"kind": "PermissionError", "message": "private"}});
            if nested {
                handler["raises"]["messege"] = json!("private");
            } else {
                handler["writes_records"] = json!({});
            }
            assert_ne!(
                unknown_members(
                    &json!({"steps": [{"call": "declare_host_verb", "name": "app.read", "handler": handler}]})
                ),
                Vec::<String>::new()
            );
        }
    }
}
