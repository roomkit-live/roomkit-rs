//! The scripted ACP agent (roomkit-specs conformance/README.md, "Scripted
//! ACP agents"): served in this process over an in-memory pipe, written as
//! raw JSON lines, so that it shares none of the channel's ACP code.

#[path = "acp_agents/control.rs"]
pub(crate) mod control;

#[path = "acp_agents/thinking.rs"]
pub(crate) mod thinking;

#[path = "acp_agents/mcp.rs"]
pub(crate) mod mcp;

#[path = "acp_agents/permissions.rs"]
pub(crate) mod permissions;

#[path = "acp_agents/handler.rs"]
pub(crate) mod handler;
#[path = "acp_agents/refusals.rs"]
mod refusals;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use roomkit::model::ChannelId;
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpError, AcpTransport, ClientSideConnection,
};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::calls::Unreadable;
use crate::run::Run;

/// The members of a channel's `acp` the runner reads.
pub(crate) const ACP: &[&str] = &[
    "cwd",
    "agent",
    "session_config",
    "transport",
    "additional_directories",
    "mcp_servers",
    "publish_thinking_text",
    "external_tool_handler",
];
/// The members of its `agent`.
pub(crate) const AGENT: &[&str] = &[
    "initialize",
    "prompts",
    "new_sessions",
    "set_config_options",
    "close_sessions",
];
/// The members of its `initialize`.
pub(crate) const INITIALIZE: &[&str] = &[
    "protocol_version",
    "closes_sessions",
    "agent_info",
    "mcp_capabilities",
];
/// The members of one of its `prompts`.
pub(crate) const PROMPT: &[&str] = &[
    "gate",
    "updates",
    "after_updates_gate",
    "permission",
    "permission_steps",
    "stop_reason",
    "usage",
    "error",
];

/// Members of a scripted session reply.
pub(crate) const SESSION_REPLY: &[&str] = &["gate", "result", "error"];

/// What the agent received, in order: each `{method, params}`.
pub(crate) type Wire = Arc<Mutex<Vec<Value>>>;

/// The channel a scenario's `acp` member describes, and what its agent
/// receives.
pub(crate) fn scripted_acp_channel(
    channel: &Value,
    id: ChannelId,
    run: &Arc<Run>,
) -> Result<(AcpChannel<ScriptedTransport>, Wire, Arc<AgentPipe>), Unreadable> {
    let spec = &channel["acp"];
    let cwd = spec["cwd"]
        .as_str()
        .ok_or_else(|| "acp.cwd: an absolute path".to_owned())?;
    let wire = Wire::default();
    let pipe = Arc::new(AgentPipe::default());
    let transport = ScriptedTransport {
        agent: spec["agent"].clone(),
        wire: Arc::clone(&wire),
        run: Arc::downgrade(run),
        pipe: Arc::clone(&pipe),
        counts: Arc::default(),
        refusals: Arc::new(Mutex::new(
            spec["transport"]["refuse_prompt"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter(),
        )),
    };
    let options = AcpChannelOptions {
        transport: Some(transport),
        additional_directories: directories(spec)?,
        mcp_servers: mcp::servers(spec, id.as_str(), run)?,
        publish_thinking_text: thinking::publishes(spec)?,
        external_tool_handler: handler::build(spec, id.as_str(), run)?,
        session_config: spec
            .get("session_config")
            .and_then(Value::as_object)
            .cloned(),
        ..AcpChannelOptions::default()
    };
    let channel = AcpChannel::new(id, cwd, options).map_err(|error| format!("acp: {error}"))?;
    Ok((channel, wire, pipe))
}

fn directories(spec: &Value) -> Result<Option<Vec<PathBuf>>, Unreadable> {
    let Some(value) = spec.get("additional_directories") else {
        return Ok(None);
    };
    let values = value
        .as_array()
        .ok_or_else(|| "acp.additional_directories: expected an array".to_owned())?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(PathBuf::from)
                .ok_or_else(|| "acp.additional_directories: expected directory strings".into())
        })
        .collect::<Result<Vec<_>, Unreadable>>()
        .map(Some)
}

/// The transport handing the channel a connection to its scripted agent.
pub(crate) struct ScriptedTransport {
    refusals: Arc<Mutex<std::vec::IntoIter<Value>>>,
    agent: Value,
    wire: Wire,
    run: Weak<Run>,
    pipe: Arc<AgentPipe>,
    /// The sessions and prompts the agent counts, over the whole run: a
    /// reconnect goes on counting (README, "of the run").
    counts: Arc<Counts>,
}

/// The agent's real pipe lifetime, shared with the scenario's fault step.
#[derive(Default)]
pub(crate) struct AgentPipe {
    pub(crate) attempts: Wire,
    pub(crate) permissions: Arc<permissions::Exchanges>,
    serving: Mutex<Option<(JoinHandle<()>, tokio::sync::oneshot::Sender<()>)>>,
}

impl AgentPipe {
    pub(crate) async fn close(&self) {
        let serving = self
            .serving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((serving, stop)) = serving {
            let _ = stop.send(());
            drop(serving.await);
        }
    }

    fn is_alive(&self) -> bool {
        self.serving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|(task, _)| !task.is_finished())
    }
}

#[derive(Default)]
struct Counts {
    sessions: Mutex<u64>,
    prompts: Mutex<usize>,
    replies: Mutex<std::collections::HashMap<String, usize>>,
}

impl AcpTransport for ScriptedTransport {
    type Connection = refusals::Refusing;

    fn name(&self) -> &'static str {
        "scripted"
    }

    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<Self::Connection, AcpError> {
        self.pipe.close().await;
        let (ours, theirs) = tokio::io::duplex(1 << 20);
        let (reader, writer) = tokio::io::split(ours);
        let agent = Agent {
            spec: self.agent.clone(),
            wire: Arc::clone(&self.wire),
            run: Weak::clone(&self.run),
            counts: Arc::clone(&self.counts),
            permissions: Arc::clone(&self.pipe.permissions),
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let serving = tokio::spawn(serve(Arc::new(agent), theirs, stopped));
        *self
            .pipe
            .serving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((serving, stop));
        Ok(refusals::Refusing {
            connection: ClientSideConnection::new(client, reader, writer),
            refusals: Arc::clone(&self.refusals),
            attempts: Arc::clone(&self.pipe.attempts),
        })
    }

    async fn close(&self) {
        self.pipe.close().await;
    }

    fn is_alive(&self) -> bool {
        self.pipe.is_alive()
    }
}

struct Agent {
    spec: Value,
    permissions: Arc<permissions::Exchanges>,
    wire: Wire,
    run: Weak<Run>,
    counts: Arc<Counts>,
}

type Writer = Arc<AsyncMutex<WriteHalf<tokio::io::DuplexStream>>>;

/// Reads the client's messages to the end of its stream, each request
/// answered by a task of its own, so that a prompt held at its gate does
/// not stop the reading.
async fn serve(
    agent: Arc<Agent>,
    stream: tokio::io::DuplexStream,
    mut stopped: tokio::sync::oneshot::Receiver<()>,
) {
    let (reader, writer) = tokio::io::split(stream);
    let writer: Writer = Arc::new(AsyncMutex::new(writer));
    let mut lines = BufReader::new(reader).lines();
    let mut answering = tokio::task::JoinSet::new();
    loop {
        let line = tokio::select! {
            _ = &mut stopped => break,
            line = lines.next_line() => line,
        };
        let Ok(Some(line)) = line else { break };
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = message["method"].as_str().map(str::to_owned) else {
            agent.permissions.accept(&message);
            continue;
        };
        agent
            .wire
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(json!({ "method": method, "params": message["params"] }));
        if message.get("id").is_none() {
            continue;
        }
        let prompt = if method == "session/prompt" {
            Some(agent.next_prompt())
        } else {
            agent.next_reply(&method)
        };
        answering.spawn(answer(
            Arc::clone(&agent),
            message,
            prompt,
            Arc::clone(&writer),
        ));
    }
    answering.shutdown().await;
}

impl Agent {
    fn next_reply(&self, method: &str) -> Option<Value> {
        let kind = match method {
            "session/new" => "new_sessions",
            "session/set_config_option" => "set_config_options",
            "session/close" => "close_sessions",
            _ => return None,
        };
        let mut counts = self
            .counts
            .replies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = counts.entry(kind.to_owned()).or_default();
        let reply = self.spec[kind]
            .as_array()
            .and_then(|replies| replies.get(*count))
            .cloned()
            .unwrap_or_else(|| json!({}));
        *count += 1;
        Some(reply)
    }

    async fn session_reply(&self, method: &str, reply: &Value) -> Result<Value, Value> {
        let mut result = Map::new();
        if method == "session/new" {
            let mut sessions = self
                .counts
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *sessions += 1;
            result.insert("sessionId".to_owned(), json!(format!("session-{sessions}")));
        } else if method == "session/set_config_option" {
            result.insert("configOptions".to_owned(), json!([]));
        }
        if let (Some(gate), Some(run)) = (reply["gate"].as_str(), self.run.upgrade()) {
            run.gates.hold(gate).await;
        }
        if let Some(error) = reply.get("error") {
            return Err(error.clone());
        }
        if let Some(fields) = reply["result"].as_object() {
            result.extend(fields.clone());
        }
        Ok(Value::Object(result))
    }

    fn next_prompt(&self) -> Value {
        let mut prompts = self
            .counts
            .prompts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = *prompts;
        *prompts += 1;
        self.spec["prompts"]
            .as_array()
            .and_then(|prompts| prompts.get(index))
            .cloned()
            .unwrap_or_else(|| json!({}))
    }

    fn initialized(&self, params: &Value) -> Value {
        let spec = &self.spec["initialize"];
        let mut capabilities = Map::new();
        if spec["closes_sessions"].as_bool().unwrap_or(true) {
            capabilities.insert("sessionCapabilities".to_owned(), json!({ "close": {} }));
        }
        if let Some(mcp) = spec.get("mcp_capabilities") {
            capabilities.insert("mcpCapabilities".to_owned(), mcp.clone());
        }
        let version = spec
            .get("protocol_version")
            .cloned()
            .unwrap_or_else(|| params["protocolVersion"].clone());
        let mut result = json!({ "protocolVersion": version, "agentCapabilities": capabilities });
        if let Some(info) = spec.get("agent_info") {
            result["agentInfo"] = info.clone();
        }
        result
    }
}

impl Agent {
    async fn after_updates(&self, prompt: &Value) {
        if let (Some(gate), Some(run)) = (prompt["after_updates_gate"].as_str(), self.run.upgrade())
        {
            run.gates.hold(gate).await;
        }
    }
}

impl Agent {
    async fn prompt_reply(
        &self,
        message: &Value,
        prompt: &Value,
        writer: &Writer,
    ) -> Result<Value, Value> {
        let session = &message["params"]["sessionId"];
        if let (Some(gate), Some(run)) = (prompt["gate"].as_str(), self.run.upgrade()) {
            run.gates.hold(gate).await;
        }
        send_updates(writer, session, &prompt["updates"]).await;
        self.after_updates(prompt).await;
        if let Some(permission) = prompt.get("permission") {
            let mut params = permission.clone();
            params["sessionId"] = session.clone();
            send(
                writer,
                &json!({"jsonrpc":"2.0", "id":format!("permission-{}", message["id"]),
                "method":"session/request_permission", "params":params}),
            )
            .await;
        }
        for step in prompt["permission_steps"].as_array().into_iter().flatten() {
            self.permissions
                .exchange(session, &step["request"], writer)
                .await;
            send_updates(writer, session, &step["updates"]).await;
        }
        if let Some(error) = prompt.get("error") {
            return Err(error.clone());
        }
        let stop = prompt
            .get("stop_reason")
            .cloned()
            .unwrap_or_else(|| json!("end_turn"));
        let mut result = json!({"stopReason": stop});
        if let Some(usage) = prompt.get("usage") {
            result["usage"] = usage.clone();
        }
        Ok(result)
    }
}

async fn send_updates(writer: &Writer, session: &Value, updates: &Value) {
    for update in updates.as_array().into_iter().flatten() {
        send(
            writer,
            &json!({"jsonrpc":"2.0", "method":"session/update",
            "params":{"sessionId":session,"update":update}}),
        )
        .await;
    }
}

async fn answer(agent: Arc<Agent>, message: Value, prompt: Option<Value>, writer: Writer) {
    let method = message["method"].as_str().unwrap_or_default();
    let params = &message["params"];
    let outcome: Result<Value, Value> = match (method, prompt) {
        ("initialize", _) => Ok(agent.initialized(params)),
        ("session/new" | "session/set_config_option" | "session/close", Some(reply)) => {
            agent.session_reply(method, &reply).await
        }
        ("session/prompt", Some(prompt)) => agent.prompt_reply(&message, &prompt, &writer).await,
        (other, _) => Err(json!({
            "code": -32601,
            "message": "Method not found",
            "data": { "method": other },
        })),
    };
    let mut response = json!({ "jsonrpc": "2.0", "id": message["id"] });
    match outcome {
        Ok(result) => response["result"] = result,
        Err(error) => response["error"] = error,
    }
    send(&writer, &response).await;
}

async fn send(writer: &Writer, message: &Value) {
    let mut line = message.to_string();
    line.push('\n');
    // A client that went reads nothing more.
    let _gone = writer.lock().await.write_all(line.as_bytes()).await;
}
