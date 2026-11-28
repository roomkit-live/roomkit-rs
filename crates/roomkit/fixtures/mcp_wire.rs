//! A scripted MCP server's answers (roomkit-specs conformance/README.md,
//! "Scripted agents", `mcp_servers`), shared by the conformance runner's
//! servers in process over HTTP and its fixture process over stdio.

use serde_json::{Value, json};

/// The JSON-RPC response of the server `spec` describes to `request`, none
/// for a notification: its handshake, its tools as the spec writes them,
/// and each call answered with the tool's `returns` or `content`, and its
/// `structured_content`, a result that says `isError` when the tool's
/// `is_error` is true. It validates nothing it sends.
pub(crate) fn answer(spec: &Value, request: &Value) -> Option<Value> {
    let name = request["method"].as_str().unwrap_or_default();
    if name.starts_with("notifications/") || request.get("id").is_none() {
        return None;
    }
    let tools = spec["tools"].as_array().cloned().unwrap_or_default();
    let result = match name {
        "initialize" => initialized(spec),
        "tools/list" => json!({ "tools": tools.iter().map(listed).collect::<Vec<_>>() }),
        "tools/call" => {
            let called = &request["params"]["name"];
            match tools.iter().find(|tool| &tool["name"] == called) {
                Some(tool) => result(tool),
                None => json!({
                    "content": [{ "type": "text", "text": format!("Unknown tool: {called}") }],
                    "isError": true,
                }),
            }
        }
        _ => json!({}),
    };
    Some(json!({ "jsonrpc": "2.0", "id": request["id"], "result": result }))
}

/// The handshake's result: the server's name for itself (its label when
/// the spec gives no `server_name`) and its `instructions`.
fn initialized(spec: &Value) -> Value {
    let label = spec["label"].as_str().unwrap_or("scripted");
    let mut result = json!({
        "protocolVersion": "2025-03-26",
        "capabilities": { "tools": {} },
        "serverInfo": { "name": spec["server_name"].as_str().unwrap_or(label), "version": "1" },
    });
    if let Some(instructions) = spec.get("instructions") {
        result["instructions"] = instructions.clone();
    }
    result
}

/// A tool of the spec as the server lists it.
fn listed(tool: &Value) -> Value {
    let mut listed = json!({
        "name": tool["name"],
        "inputSchema": tool.get("input_schema").cloned().unwrap_or_else(|| json!({ "type": "object" })),
    });
    if let Some(description) = tool.get("description") {
        listed["description"] = description.clone();
    }
    if let Some(schema) = tool.get("output_schema") {
        listed["outputSchema"] = schema.clone();
    }
    listed
}

/// The result of a call of `tool`.
fn result(tool: &Value) -> Value {
    let content = match tool["content"].as_array() {
        Some(blocks) => blocks.iter().map(block).collect(),
        None => vec![json!({ "type": "text", "text": tool["returns"] })],
    };
    let mut result = json!({ "content": content, "isError": tool["is_error"] == true });
    if let Some(structured) = tool.get("structured_content") {
        result["structuredContent"] = structured.clone();
    }
    result
}

/// A content block as the scenario writes it, its shorthands written out:
/// `text_chars` n characters `x`, `data_chars` n characters `A`.
fn block(block: &Value) -> Value {
    let mut written = block.clone();
    let Some(map) = written.as_object_mut() else {
        return written;
    };
    if let Some(count) = map
        .shift_remove("text_chars")
        .and_then(|count| count.as_u64())
    {
        map.insert("text".to_owned(), json!("x".repeat(length(count))));
    }
    if let Some(count) = map
        .shift_remove("data_chars")
        .and_then(|count| count.as_u64())
    {
        map.insert("data".to_owned(), json!("A".repeat(length(count))));
    }
    written
}

fn length(count: u64) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}
