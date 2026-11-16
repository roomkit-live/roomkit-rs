//! Strict fixture controls; server-owned JSON stays opaque.
use serde_json::Value;

fn object(value: &Value, fields: &[&str], path: &str, found: &mut Vec<String>) -> bool {
    let Some(map) = value.as_object() else {
        found.push(format!("{path}: expected an object"));
        return false;
    };
    for key in map.keys().filter(|key| !fields.contains(&key.as_str())) {
        found.push(format!("{path}: unknown member {key}"));
    }
    true
}

fn initialization(value: &Value, path: &str, found: &mut Vec<String>) {
    if value.get("server_info").is_some_and(|v| !v.is_object()) {
        found.push(format!("{path}.server_info: expected an object"));
    }
    if value.get("instructions").is_some_and(|v| !v.is_string()) {
        found.push(format!("{path}.instructions: expected a string"));
    }
}

fn replies(value: &Value, path: &str, found: &mut Vec<String>) {
    let Some(replies) = value.as_array() else {
        found.push(format!("{path}: expected replies"));
        return;
    };
    for (i, reply) in replies.iter().enumerate() {
        let path = format!("{path}[{i}]");
        if !object(
            reply,
            &["method", "params", "result", "error", "hold"],
            &path,
            found,
        ) {
            continue;
        }
        if !reply["method"].is_string()
            || reply.get("params").is_some_and(|v| !v.is_object())
            || ["result", "error", "hold"]
                .iter()
                .filter(|k| reply.get(**k).is_some())
                .count()
                != 1
            || reply.get("hold").is_some_and(|v| v != true)
        {
            found.push(format!("{path}: invalid reply"));
        }
        if let Some(error) = reply.get("error")
            && object(error, &["code", "message", "data"], &path, found)
            && (!error["code"].is_i64() || !error["message"].is_string())
        {
            found.push(format!("{path}: invalid protocol error"));
        }
    }
}

fn peer(value: &Value, path: &str, found: &mut Vec<String>) {
    if !object(
        value,
        &[
            "server_info",
            "extensions",
            "instructions",
            "replies",
            "framing",
            "initializations",
        ],
        path,
        found,
    ) {
        return;
    }
    initialization(value, path, found);
    if value
        .get("framing")
        .is_some_and(|v| v != "json" && v != "sse")
    {
        found.push(format!("{path}: unknown framing"));
    }
    if let Some(initializations) = value.get("initializations") {
        if let Some(items) = initializations.as_array().filter(|items| !items.is_empty()) {
            for item in items {
                if object(
                    item,
                    &["server_info", "extensions", "instructions"],
                    path,
                    found,
                ) {
                    initialization(item, path, found);
                }
            }
        } else {
            found.push(format!("{path}: expected nonempty initializations"));
        }
    }
    replies(&value["replies"], path, found);
}

fn operation(op: &Value, path: &str, found: &mut Vec<String>) {
    let fields: &[&str] = match op["method"].as_str() {
        Some("connect" | "close" | "connected" | "tool_names") => &["method"],
        Some("tool_meta" | "server_info" | "server_extensions") => &["method", "mutate"],
        Some("request") => &["method", "name", "params", "timeout_ms"],
        Some("read_resource") => &["method", "uri", "timeout_ms"],
        _ => {
            found.push(format!("{path}: unknown MCP operation"));
            return;
        }
    };
    if !object(op, fields, path, found) {
        return;
    }
    let required = match op["method"].as_str() {
        Some("request") => Some("name"),
        Some("read_resource") => Some("uri"),
        _ => None,
    };
    if required.is_some_and(|key| !op[key].is_string())
        || op
            .get("params")
            .is_some_and(|v| !v.is_null() && !v.is_object())
        || op
            .get("timeout_ms")
            .is_some_and(|v| v.as_u64().is_none_or(|n| n == 0))
    {
        found.push(format!("{path}: invalid operation arguments"));
    }
    if let Some(mutation) = op.get("mutate")
        && object(mutation, &["path", "value"], path, found)
        && (mutation["path"]
            .as_array()
            .is_none_or(|v| v.is_empty() || v.iter().any(|k| !k.is_string()))
            || mutation.get("value").is_none())
    {
        found.push(format!("{path}: invalid mutation"));
    }
}

pub(crate) fn validate(spec: &Value, path: &str, found: &mut Vec<String>) {
    peer(&spec["peer"], path, found);
    if let Some(ops) = spec["operations"].as_array().filter(|v| !v.is_empty()) {
        for op in ops {
            operation(op, path, found);
        }
    } else {
        found.push(format!("{path}: expected nonempty operations"));
    }
    if spec.get("discover").is_some_and(|v| !v.is_boolean()) {
        found.push(format!("{path}: expected boolean discover"));
    }
    if spec.get("tool_filter").is_some_and(|v| {
        v.as_array()
            .is_none_or(|a| a.iter().any(|v| !v.is_string()))
    }) {
        found.push(format!("{path}: expected string tool filter list"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Python: tests/conformance/test_mcp_host_fixture.py::test_malformed_host_controls_are_refused
    #[test]
    fn malformed_host_controls_are_refused() {
        for update in [
            json!({"peer":null}),
            json!({"peer":{"replies":[],"reply":{}}}),
            json!({"peer":{"replies":null}}),
            json!({"peer":{"replies":[{"method":"read","result":{},"error":{}}]}}),
            json!({"peer":{"replies":[{"method":"read","hold":false}]}}),
            json!({"peer":{"replies":[{"method":"read","error":{"code":true,"message":"x"}}]}}),
            json!({"peer":{"replies":[],"framing":"later"}}),
            json!({"peer":{"replies":[],"initializations":[]}}),
            json!({"peer":{"replies":[],"initializations":[{"server_info":false}]}}),
            json!({"peer":{"replies":[],"initializations":[{"instruction":"unknown"}]}}),
            json!({"operations":[]}),
            json!({"operations":[{"method":"unknown"}]}),
            json!({"operations":[{"method":"connect","timeout_ms":10}]}),
            json!({"operations":[{"method":"request"}]}),
            json!({"operations":[{"method":"request","name":"read","params":[]}]}),
            json!({"operations":[{"method":"read_resource","uri":"resource://x","timeout_ms":0}]}),
            json!({"operations":[{"method":"tool_meta","mutate":{"path":[],"value":null}}]}),
            json!({"discover":null}),
            json!({"tool_filter":"all"}),
        ] {
            let mut spec = json!({"peer":{"replies":[]},"operations":[{"method":"connect"}]});
            spec.as_object_mut()
                .unwrap()
                .extend(update.as_object().unwrap().clone());
            let mut found = vec![];
            validate(&spec, "mcp_session", &mut found);
            assert!(!found.is_empty(), "{update}");
        }
    }
}
