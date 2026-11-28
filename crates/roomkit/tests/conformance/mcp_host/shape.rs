//! Strict fixture controls; server-owned JSON stays opaque.
use serde_json::Value;

/// The controls of an MCP session, validated by this module.
pub(crate) fn arguments(call: &str) -> Option<&'static [&'static str]> {
    if call == "mcp_session" {
        return Some(&[
            "peer",
            "operations",
            "discover",
            "tool_filter",
            "constructor",
            "transport",
            "headers",
            "headers_after_construction",
            "label",
            "url",
            "connect_timeout_ms",
        ]);
    }
    None
}

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
            "era",
            "refuse_discovery",
            "initializations",
            "endpoint_on_connections",
        ],
        path,
        found,
    ) {
        return;
    }
    if value.get("era").is_some_and(|era| era != "2026-07-28") {
        found.push(format!("{path}.era: expected 2026-07-28"));
    }
    if value
        .get("refuse_discovery")
        .is_some_and(|refusal| refusal != "http")
    {
        found.push(format!("{path}.refuse_discovery: expected http"));
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
    if value.get("endpoint_on_connections").is_some_and(|values| {
        values
            .as_array()
            .is_none_or(|values| values.is_empty() || values.iter().any(|v| !v.is_boolean()))
    }) {
        found.push(format!(
            "{path}: expected nonempty boolean endpoint announcements"
        ));
    }
}

fn operation(op: &Value, path: &str, found: &mut Vec<String>) {
    let fields: &[&str] = match op["method"].as_str() {
        Some(
            "connect" | "close" | "connected" | "instructions" | "tool_names"
            | "get_tools_as_dicts",
        ) => &["method"],
        Some("tool_meta" | "server_info" | "server_extensions") => &["method", "mutate"],
        Some("request") => &["method", "name", "params", "timeout_ms"],
        Some("read_resource") => &["method", "uri", "timeout_ms"],
        Some("call_tool") => &["method", "name", "arguments", "timeout_ms"],
        _ => {
            found.push(format!("{path}: unknown MCP operation"));
            return;
        }
    };
    if !object(op, fields, path, found) {
        return;
    }
    let required = match op["method"].as_str() {
        Some("request" | "call_tool") => Some("name"),
        Some("read_resource") => Some("uri"),
        _ => None,
    };
    if required.is_some_and(|key| !op[key].is_string())
        || op.get("arguments").is_some_and(|v| !v.is_object())
        || op
            .get("params")
            .is_some_and(|v| !v.is_null() && !v.is_object())
        || op.get("timeout_ms").is_some_and(|v| v.as_u64().is_none())
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
    construction(spec, path, found);
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

fn construction(spec: &Value, path: &str, found: &mut Vec<String>) {
    for (field, choices) in [
        ("constructor", ["direct", "from_url"]),
        ("transport", ["sse", "streamable_http"]),
    ] {
        if spec
            .get(field)
            .is_some_and(|value| value.as_str().is_none_or(|value| !choices.contains(&value)))
        {
            found.push(format!("{path}.{field}: unknown choice"));
        }
    }
    if spec.get("label").is_some_and(|value| !value.is_string())
        || spec.get("url").is_some_and(|value| !value.is_string())
        || spec
            .get("connect_timeout_ms")
            .is_some_and(|value| value.as_u64().is_none_or(|value| value == 0))
    {
        found.push(format!("{path}: invalid constructor settings"));
    }
    for field in ["headers", "headers_after_construction"] {
        if spec.get(field).is_some_and(|value| {
            value
                .as_object()
                .is_none_or(|map| map.values().any(|value| !value.is_string()))
        }) {
            found.push(format!("{path}.{field}: expected a string mapping"));
        }
    }
    if spec.get("headers_after_construction").is_some() && spec.get("headers").is_none() {
        found.push(format!("{path}: headers are required for their mutation"));
    }
    if spec["peer"].get("endpoint_on_connections").is_some() && spec["transport"] != "sse" {
        found.push(format!("{path}: endpoint announcements require legacy SSE"));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn mcp_constructor_controls_reach_the_session_validator() {
        let call = json!({"call":"mcp_session","peer":{"replies":[]},
            "operations":[{"method":"connect"}],"discover":false,"tool_filter":["echo"],
            "constructor":"direct","transport":"sse","headers":{"X-Test":"one"},
            "headers_after_construction":{"X-Test":"two"},"label":"server","connect_timeout_ms":50});
        assert_eq!(
            crate::shape::unknown_members(&json!({"steps":[call.clone()]})),
            Vec::<String>::new()
        );
        let mut misspelled = call.clone();
        misspelled["header"] = json!({});
        assert_eq!(
            crate::shape::unknown_members(&json!({"steps":[misspelled]})),
            ["steps[0]: header"]
        );
        let mut unknown_call = call;
        unknown_call["call"] = json!("mcp_sesion");
        assert!(
            crate::shape::unknown_members(&json!({"steps":[unknown_call]}))
                .iter()
                .any(|message| message.contains("no call"))
        );
    }

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
            json!({"peer":{"replies":[],"era":"2025-11-25"}}),
            json!({"peer":{"replies":[],"refuse_discovery":"jsonrpc"}}),
            json!({"peer":{"replies":[],"initializations":[]}}),
            json!({"peer":{"replies":[],"initializations":[{"server_info":false}]}}),
            json!({"peer":{"replies":[],"initializations":[{"instruction":"unknown"}]}}),
            json!({"operations":[]}),
            json!({"operations":[{"method":"unknown"}]}),
            json!({"operations":[{"method":"connect","timeout_ms":10}]}),
            json!({"operations":[{"method":"request"}]}),
            json!({"operations":[{"method":"request","name":"read","params":[]}]}),
            json!({"operations":[{"method":"read_resource","uri":"resource://x","timeout_ms":-1}]}),
            json!({"operations":[{"method":"tool_meta","mutate":{"path":[],"value":null}}]}),
            json!({"discover":null}),
            json!({"tool_filter":"all"}),
            json!({"constructor":"later"}),
            json!({"constructor":[]}),
            json!({"transport":"legacy"}),
            json!({"label":1}),
            json!({"connect_timeout_ms":0}),
            json!({"connect_timeout_ms":true}),
            json!({"headers":null}),
            json!({"headers":{"X-Test":1}}),
            json!({"headers_after_construction":{}}),
            json!({"headers":{},"headers_after_construction":[]}),
            json!({"peer":{"replies":[],"endpoint_on_connections":[]},"transport":"sse"}),
            json!({"peer":{"replies":[],"endpoint_on_connections":[1]},"transport":"sse"}),
            json!({"peer":{"replies":[],"endpoint_on_connections":[true]}}),
            json!({"operations":[{"method":"call_tool"}]}),
            json!({"operations":[{"method":"call_tool","name":"echo","arguments":[]}]}),
        ] {
            let mut spec = json!({"peer":{"replies":[]},"operations":[{"method":"connect"}]});
            spec.as_object_mut()
                .unwrap()
                .extend(update.as_object().unwrap().clone());
            spec["call"] = json!("mcp_session");
            let found = crate::shape::unknown_members(&json!({"steps":[spec]}));
            assert!(!found.is_empty(), "{update}");
        }
    }
}
