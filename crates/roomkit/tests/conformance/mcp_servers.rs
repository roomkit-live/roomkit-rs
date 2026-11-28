//! A scripted agent's MCP servers (roomkit-specs conformance/README.md,
//! "Scripted agents", `mcp_servers`): each one served in process over
//! streamable HTTP, or as the runner's fixture process over stdio
//! (`stdio`), and reached through roomkit-mcp's own provider, connected
//! before the run, its tools listed then, and closed after it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use std::path::PathBuf;

use roomkit_ai::{AiTool, ToolEntry, ToolHandler};
use roomkit_mcp::{AsToolHandlerOptions, FromCommandOptions, FromUrlOptions, McpToolProvider};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::processes::ProcessReports;
use crate::run::lock;

#[path = "../../fixtures/mcp_wire.rs"]
mod mcp_wire;

/// The `tools/call` requests a server in process received.
type Calls = Arc<Mutex<Vec<Value>>>;

/// Where a server's `tools/call` requests are read: in process, or in its
/// fixture process's report.
enum Recorded {
    Memory(Calls),
    Report(PathBuf),
}

/// One server: its spec, its provider, the task serving it in process, and
/// its calls.
pub(crate) struct Server {
    spec: Value,
    provider: McpToolProvider,
    task: Option<JoinHandle<()>>,
    calls: Recorded,
}

/// Every setup channel's MCP servers, by channel id, in chain order.
#[derive(Default)]
pub(crate) struct McpServers {
    by_channel: Mutex<HashMap<String, Vec<Arc<Server>>>>,
    calls: Mutex<BTreeMap<String, Vec<Value>>>,
}

impl McpServers {
    /// Starts and connects the servers of each of `channels`, in order.
    pub(crate) async fn connect(
        &self,
        channels: &Value,
        reports: &ProcessReports,
    ) -> Result<(), String> {
        for channel in channels.as_array().into_iter().flatten() {
            let id = channel["id"].as_str().unwrap_or_default();
            for spec in channel["mcp_servers"].as_array().into_iter().flatten() {
                let server = serve(spec, reports).await?;
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

    /// Disconnects every provider and stops its server, keeping what each
    /// labelled one was sent: `expect.mcp_calls`.
    pub(crate) async fn close(&self) {
        let servers: Vec<Arc<Server>> = lock(&self.by_channel)
            .drain()
            .flat_map(|(_, servers)| servers)
            .collect();
        for server in servers {
            server.provider.close().await;
            if let Some(task) = &server.task {
                task.abort();
            }
            if let Some(label) = server.spec["label"].as_str() {
                let calls = match &server.calls {
                    Recorded::Memory(calls) => lock(calls).clone(),
                    Recorded::Report(path) => crate::processes::calls(path),
                };
                lock(&self.calls).insert(label.to_owned(), calls);
            }
        }
    }

    /// `expect.mcp_calls`, once the servers are closed.
    pub(crate) fn calls(&self) -> BTreeMap<String, Vec<Value>> {
        lock(&self.calls).clone()
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

/// `spec`'s server, connected: the runner's fixture process when the spec
/// names `stdio` (its report under the server's label), served in process
/// over HTTP at a port of its own otherwise.
async fn serve(spec: &Value, reports: &ProcessReports) -> Result<Server, String> {
    if let Some(stdio) = spec.get("stdio") {
        return serve_stdio(spec, stdio, reports).await;
    }
    let calls = Calls::default();
    let app = Router::new()
        .route("/mcp", any(answer))
        .with_state((Arc::new(spec.clone()), Arc::clone(&calls)));
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
        propagate_trace_context: spec["propagate_trace_context"] == true,
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
        task: Some(task),
        calls: Recorded::Memory(calls),
    })
}

/// `spec`'s server as the runner's fixture process, started by the
/// provider's public command constructor from the fixture's program and
/// its own arguments, then each of `stdio`'s `args`.
async fn serve_stdio(
    spec: &Value,
    stdio: &Value,
    reports: &ProcessReports,
) -> Result<Server, String> {
    let label = spec["label"]
        .as_str()
        .ok_or("mcp_servers: stdio needs a label")?;
    let report = reports.report(label)?;
    let own = json!({ "mode": "mcp", "server": spec, "env": [] });
    let mut args = vec![report.clone().into_os_string(), own.to_string().into()];
    for arg in stdio["args"].as_array().into_iter().flatten() {
        args.push(
            arg.as_str()
                .ok_or("mcp_servers: stdio args are texts")?
                .into(),
        );
    }
    let options = FromCommandOptions {
        args,
        label: Some(label.to_owned()),
        propagate_trace_context: spec["propagate_trace_context"] == true,
        ..FromCommandOptions::default()
    };
    let provider = McpToolProvider::from_command(crate::processes::FIXTURE, options)
        .map_err(|e| e.to_string())?;
    provider
        .connect()
        .await
        .map_err(|error| format!("mcp_servers: {error}"))?;
    Ok(Server {
        spec: spec.clone(),
        provider,
        task: None,
        calls: Recorded::Report(report),
    })
}

/// The server's answer to one request (`mcp_wire::answer`), each
/// `tools/call` recorded first.
async fn answer(
    State((spec, calls)): State<(Arc<Value>, Calls)>,
    method: Method,
    body: Bytes,
) -> Response {
    if method == Method::DELETE {
        return StatusCode::NO_CONTENT.into_response();
    }
    if method != Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if request["method"] == "tools/call" {
        let params = &request["params"];
        lock(&calls).push(json!({
            "name": params["name"],
            "arguments": params.get("arguments").cloned().unwrap_or_else(|| json!({})),
            "_meta": params.get("_meta").cloned().unwrap_or_else(|| json!({})),
        }));
    }
    match mcp_wire::answer(&spec, &request) {
        Some(body) => ([("content-type", "application/json")], body.to_string()).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}
