//! A scripted agent's MCP servers (roomkit-specs conformance/README.md,
//! "Scripted agents", `mcp_servers`): each one served in process over
//! streamable HTTP and reached through roomkit-mcp's own provider,
//! connected before the run, its tools listed then, and closed after it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use roomkit_ai::{AiTool, ToolEntry, ToolHandler};
use roomkit_mcp::{AsToolHandlerOptions, FromUrlOptions, McpToolProvider};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::run::lock;

/// One server: its spec, its provider, and the task serving it.
pub(crate) struct Server {
    spec: Value,
    provider: McpToolProvider,
    task: JoinHandle<()>,
}

/// Every setup channel's MCP servers, by channel id, in chain order.
#[derive(Default)]
pub(crate) struct McpServers {
    by_channel: Mutex<HashMap<String, Vec<Arc<Server>>>>,
}

impl McpServers {
    /// Starts and connects the servers of each of `channels`, in order.
    pub(crate) async fn connect(&self, channels: &Value) -> Result<(), String> {
        for channel in channels.as_array().into_iter().flatten() {
            let id = channel["id"].as_str().unwrap_or_default();
            for spec in channel["mcp_servers"].as_array().into_iter().flatten() {
                let server = serve(spec).await?;
                lock(&self.by_channel)
                    .entry(id.to_owned())
                    .or_default()
                    .push(Arc::new(server));
            }
        }
        Ok(())
    }

    /// The servers of the channel `id`, in chain order.
    pub(crate) fn of(&self, id: &str) -> Vec<Arc<Server>> {
        lock(&self.by_channel).get(id).cloned().unwrap_or_default()
    }

    /// Disconnects every provider and stops its server.
    pub(crate) async fn close(&self) {
        let servers: Vec<Arc<Server>> = lock(&self.by_channel)
            .drain()
            .flat_map(|(_, servers)| servers)
            .collect();
        for server in servers {
            server.provider.close().await;
            server.task.abort();
        }
    }
}

/// The definitions the agent declares for `servers`, as each `declare`
/// says: the provider's own, under their aliases, or none.
pub(crate) fn declared(servers: &[Arc<Server>]) -> Result<Vec<ToolEntry>, String> {
    let mut declared = Vec::new();
    for server in servers {
        let tools = server.provider.get_tools().map_err(|e| e.to_string())?;
        match server.spec["declare"].as_str().unwrap_or("bare") {
            "bare" => declared.extend(tools.into_iter().map(ToolEntry::from)),
            "aliases" => {
                let label = server.spec["label"].as_str().unwrap_or_default();
                for tool in tools {
                    let alias = format!("mcp__{label}__{}", tool.name);
                    let named = AiTool::new(alias, tool.description).map_err(|e| e.to_string())?;
                    declared.push(ToolEntry::from(AiTool {
                        parameters: tool.parameters,
                        tags: tool.tags,
                        ..named
                    }));
                }
            }
            _ => {}
        }
    }
    Ok(declared)
}

/// Each server's tool handler, in the chain's order.
pub(crate) fn handlers(servers: &[Arc<Server>]) -> Result<Vec<ToolHandler>, String> {
    servers
        .iter()
        .map(|server| {
            let options = AsToolHandlerOptions {
                gate_discovery: server.spec["gate_discovery"] != false,
            };
            server
                .provider
                .as_tool_handler(options)
                .map_err(|e| e.to_string())
        })
        .collect()
}

/// `spec`'s server, served at a port of its own, and its provider,
/// connected.
async fn serve(spec: &Value) -> Result<Server, String> {
    let app = Router::new()
        .route("/mcp", any(answer))
        .with_state(Arc::new(spec.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let url = format!(
        "http://{}/mcp",
        listener.local_addr().map_err(|e| e.to_string())?
    );
    let task = tokio::spawn(async move {
        let _served = axum::serve(listener, app).await;
    });
    let options = FromUrlOptions {
        label: spec["label"].as_str().map(str::to_owned),
        ..FromUrlOptions::default()
    };
    let provider = McpToolProvider::from_url(&url, options).map_err(|e| e.to_string())?;
    if let Err(error) = provider.connect().await {
        task.abort();
        return Err(format!("mcp_servers: {error}"));
    }
    Ok(Server {
        spec: spec.clone(),
        provider,
        task,
    })
}

/// The server's answer to one request: its handshake, its tools as the
/// spec writes them, and each call answered with the tool's `returns`, a
/// result that says `isError` when the tool's `is_error` is true.
async fn answer(State(spec): State<Arc<Value>>, method: Method, body: Bytes) -> Response {
    if method == Method::DELETE {
        return StatusCode::NO_CONTENT.into_response();
    }
    if method != Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let name = request["method"].as_str().unwrap_or_default();
    if name.starts_with("notifications/") {
        return StatusCode::ACCEPTED.into_response();
    }
    let tools = spec["tools"].as_array().cloned().unwrap_or_default();
    let result = match name {
        "initialize" => json!({
            "protocolVersion": "2025-03-26",
            "capabilities": { "tools": {} },
            "serverInfo": { "name": spec["label"].as_str().unwrap_or("scripted"), "version": "1" },
        }),
        "tools/list" => json!({ "tools": tools.iter().map(listed).collect::<Vec<_>>() }),
        "tools/call" => {
            let called = &request["params"]["name"];
            match tools.iter().find(|tool| &tool["name"] == called) {
                Some(tool) => json!({
                    "content": [{ "type": "text", "text": tool["returns"] }],
                    "isError": tool["is_error"] == true,
                }),
                None => json!({
                    "content": [{ "type": "text", "text": format!("Unknown tool: {called}") }],
                    "isError": true,
                }),
            }
        }
        _ => json!({}),
    };
    let body = json!({ "jsonrpc": "2.0", "id": request["id"], "result": result }).to_string();
    ([("content-type", "application/json")], body).into_response()
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
    listed
}
