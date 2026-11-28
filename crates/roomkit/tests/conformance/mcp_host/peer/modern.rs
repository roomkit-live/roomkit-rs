//! MCP 2026-07-28 for the scripted peer (roomkit-specs conformance/README.md,
//! `mcp_session`): a peer of that `era` answers discovery, keeps no session,
//! and checks, before it leaves them out of the observation, the protocol's
//! own `_meta` entries and the HTTP headers each request carries; a peer of
//! an earlier revision refuses the probe, by a JSON-RPC error or, with
//! `refuse_discovery: "http"`, by HTTP 400. Python: `_mcp_peer.py`.

use std::collections::HashMap;

use base64::Engine as _;
use serde_json::{Value, json};

/// The MCP revision a peer with this `era` speaks.
pub(super) const MODERN: &str = "2026-07-28";

/// The `_meta` entries every request of a 2026-07-28 connection carries.
const PROTOCOL_KEYS: [&str; 3] = [
    "io.modelcontextprotocol/protocolVersion",
    "io.modelcontextprotocol/clientInfo",
    "io.modelcontextprotocol/clientCapabilities",
];

/// The requests whose `Mcp-Name` header names what they act on, by method:
/// the parameter it repeats.
const NAME_BEARING: [(&str, &str); 3] = [
    ("tools/call", "name"),
    ("resources/read", "uri"),
    ("prompts/get", "name"),
];

/// The arguments each listed tool mirrors into a header, by tool: its
/// top-level properties annotated with `x-mcp-header`.
#[derive(Default)]
pub(super) struct Mirrored(HashMap<String, Vec<(String, String)>>);

impl Mirrored {
    /// Keeps what a listing's page says each tool mirrors.
    pub(super) fn remember(&mut self, result: &Value) {
        for tool in result["tools"].as_array().into_iter().flatten() {
            let Some(properties) = tool["inputSchema"]["properties"].as_object() else {
                continue;
            };
            let mirrored = properties
                .iter()
                .filter_map(|(argument, annotation)| {
                    let header = annotation.get("x-mcp-header")?.as_str()?;
                    Some((argument.clone(), header.to_owned()))
                })
                .collect();
            let name = tool["name"].as_str().unwrap_or_default().to_owned();
            self.0.insert(name, mirrored);
        }
    }
}

/// A request head's headers, their names lowercased and their values
/// trimmed, in order.
pub(super) fn headers(head: &str) -> Vec<(String, String)> {
    head.lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect()
}

/// Checks what a 2026-07-28 request says of itself in its HTTP headers: its
/// revision, its method, the name it bears, and each argument the listed
/// tool mirrors into an `Mcp-Param-*` header.
pub(super) fn check_headers(
    headers: &[(String, String)],
    message: &Value,
    mirrored: &Mirrored,
    problems: &mut Vec<String>,
) {
    let method = message["method"].as_str().unwrap_or_default();
    let mut expected = vec![("mcp-method".to_owned(), Some(method.to_owned()))];
    if message.get("id").is_some() {
        expected.push(("mcp-protocol-version".to_owned(), Some(MODERN.to_owned())));
    }
    let params = &message["params"];
    if let Some((_, key)) = NAME_BEARING.iter().find(|(named, _)| *named == method) {
        expected.push((
            "mcp-name".to_owned(),
            params[*key].as_str().map(str::to_owned),
        ));
    }
    if method == "tools/call" {
        let arguments = &params["arguments"];
        let tool = params["name"].as_str().unwrap_or_default();
        for (argument, header) in mirrored.0.get(tool).into_iter().flatten() {
            let value = &arguments[argument.as_str()];
            if !value.is_null() {
                let name = format!("mcp-param-{}", header.to_ascii_lowercase());
                expected.push((name, Some(rendered(value))));
            }
        }
    }
    for (name, value) in expected {
        let sent = headers
            .iter()
            .find(|(sent, _)| *sent == name)
            .map(|(_, value)| value.as_str());
        if decoded(sent) != value {
            problems.push(format!("MCP request {method} sent {name}: {sent:?}"));
        }
    }
}

/// An argument as MCP 2026-07-28 writes it into its header.
fn rendered(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_)) => other.to_string(),
    }
}

/// A header's value, a value MCP wrapped in base64 unwrapped.
fn decoded(sent: Option<&str>) -> Option<String> {
    let text = sent?;
    if !(text.starts_with("=?base64?") && text.ends_with("?=")) {
        return Some(text.to_owned());
    }
    let inner = text
        .get(9..text.len().saturating_sub(2))
        .unwrap_or_default();
    let bytes = base64::engine::general_purpose::STANDARD.decode(inner);
    Some(bytes.map_or_else(
        |_| "<invalid base64>".to_owned(),
        |bytes| String::from_utf8_lossy(&bytes).into_owned(),
    ))
}

/// `message` without the `_meta` entries every 2026-07-28 request carries,
/// which the peer checks: its revision, the client's identity and
/// capabilities. A `_meta` or `params` left empty goes too.
pub(super) fn without_protocol_meta(message: &Value, problems: &mut Vec<String>) -> Value {
    let mut message = message.clone();
    let method = message["method"].as_str().unwrap_or_default().to_owned();
    let Some(params) = message.get_mut("params").and_then(Value::as_object_mut) else {
        problems.push(format!("MCP request {method} names no 2026-07-28 revision"));
        return message;
    };
    let Some(meta) = params.get_mut("_meta").and_then(Value::as_object_mut) else {
        problems.push(format!("MCP request {method} names no 2026-07-28 revision"));
        return message;
    };
    if meta.get(PROTOCOL_KEYS[0]).and_then(Value::as_str) != Some(MODERN) {
        problems.push(format!("MCP request {method} names no 2026-07-28 revision"));
        return message;
    }
    for key in &PROTOCOL_KEYS[1..] {
        if !meta.get(*key).is_some_and(Value::is_object) {
            problems.push(format!("MCP request {method} carries no {key}"));
        }
    }
    for key in PROTOCOL_KEYS {
        meta.shift_remove(key);
    }
    if meta.is_empty() {
        params.shift_remove("_meta");
    }
    if params.is_empty() {
        message.as_object_mut().unwrap().shift_remove("params");
    }
    message
}

/// A 2026-07-28 discovery result, from what an initialization would say:
/// what the server is in its `_meta`, and no protocol version of its own.
pub(super) fn discovery(mut initialization: Value) -> Value {
    let info = initialization
        .as_object_mut()
        .unwrap()
        .shift_remove("serverInfo");
    initialization
        .as_object_mut()
        .unwrap()
        .shift_remove("protocolVersion");
    initialization["supportedVersions"] = json!([MODERN]);
    initialization["resultType"] = json!("complete");
    initialization["ttlMs"] = json!(0);
    initialization["cacheScope"] = json!("private");
    initialization["_meta"] = json!({"io.modelcontextprotocol/serverInfo": info});
    initialization
}

/// How a server of an earlier revision, which knows no discovery and has
/// opened no session yet, refuses the probe `id`: by a JSON-RPC error over
/// HTTP 200, or with `refuse_discovery: "http"` by HTTP 400, as a stateful
/// mcp 1.x server answers a request without a session.
pub(super) fn refusal(spec: &Value, id: &Value) -> (u16, Value) {
    let (status, code, message) = if spec["refuse_discovery"] == "http" {
        (400, -32600, "Bad Request: Missing session ID")
    } else {
        (200, -32601, "Method not found")
    };
    let body = json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}});
    (status, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modern_request(meta: &Value) -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"vendor/read","params":{"_meta":meta}})
    }

    /// A 2026-07-28 request that leaves out the protocol's own entries, or
    /// names another revision, is a problem, never an observation that
    /// passes.
    #[test]
    fn a_request_without_the_protocol_s_metadata_is_a_problem() {
        let complete = json!({
            PROTOCOL_KEYS[0]: MODERN, PROTOCOL_KEYS[1]: {}, PROTOCOL_KEYS[2]: {}, "custom": 1
        });
        for (meta, faults) in [
            (complete, 0),
            (json!({ PROTOCOL_KEYS[0]: MODERN }), 2),
            (
                json!({ PROTOCOL_KEYS[0]: "2025-11-25", PROTOCOL_KEYS[1]: {} }),
                1,
            ),
            (json!({}), 1),
        ] {
            let mut problems = Vec::new();
            let observed = without_protocol_meta(&modern_request(&meta), &mut problems);
            assert_eq!(problems.len(), faults, "{meta}: {problems:?}");
            if faults == 0 {
                assert_eq!(observed["params"], json!({"_meta":{"custom":1}}));
            }
        }
    }

    /// A tool call's header must mirror each annotated argument, decoded
    /// from base64 where the client wrapped it, and name the tool.
    #[test]
    fn a_tool_call_s_headers_mirror_its_annotated_arguments() {
        let mut mirrored = Mirrored::default();
        mirrored.remember(
            &json!({"tools":[{"name":"routed","inputSchema":{"type":"object",
            "properties":{"region":{"type":"string","x-mcp-header":"Region"}}}}]}),
        );
        let call = json!({"jsonrpc":"2.0","id":2,"method":"tools/call",
            "params":{"name":"routed","arguments":{"region":"é"}}});
        let wrapped = format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode("é")
        );
        for (region, faults) in [(Some(wrapped.as_str()), 0), (Some("eu"), 1), (None, 1)] {
            let mut sent = vec![
                ("mcp-method".to_owned(), "tools/call".to_owned()),
                ("mcp-protocol-version".to_owned(), MODERN.to_owned()),
                ("mcp-name".to_owned(), "routed".to_owned()),
            ];
            if let Some(region) = region {
                sent.push(("mcp-param-region".to_owned(), region.to_owned()));
            }
            let mut problems = Vec::new();
            check_headers(&sent, &call, &mirrored, &mut problems);
            assert_eq!(problems.len(), faults, "{region:?}: {problems:?}");
        }
    }
}
