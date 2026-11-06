//! The scripted ACP agent (roomkit-specs conformance/README.md, "Scripted
//! ACP agents"): served in this process over an in-memory pipe, written as
//! raw JSON lines, so that it shares none of the channel's ACP code.

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
pub(crate) const ACP: &[&str] = &["cwd", "agent"];
/// The members of its `agent`.
pub(crate) const AGENT: &[&str] = &["initialize", "prompts"];
/// The members of its `initialize`.
pub(crate) const INITIALIZE: &[&str] = &["protocol_version", "closes_sessions", "agent_info"];
/// The members of one of its `prompts`.
pub(crate) const PROMPT: &[&str] = &["gate", "updates", "stop_reason", "usage", "error"];

/// What the agent received, in order: each `{method, params}`.
pub(crate) type Wire = Arc<Mutex<Vec<Value>>>;

/// The channel a scenario's `acp` member describes, and what its agent
/// receives.
pub(crate) fn scripted_acp_channel(
    channel: &Value,
    id: ChannelId,
    run: &Arc<Run>,
) -> Result<(AcpChannel<ScriptedTransport>, Wire), Unreadable> {
    let spec = &channel["acp"];
    let cwd = spec["cwd"]
        .as_str()
        .ok_or_else(|| "acp.cwd: an absolute path".to_owned())?;
    let wire = Wire::default();
    let transport = ScriptedTransport {
        agent: spec["agent"].clone(),
        wire: Arc::clone(&wire),
        run: Arc::downgrade(run),
        serving: Mutex::new(None),
        counts: Arc::default(),
    };
    let options = AcpChannelOptions {
        transport: Some(transport),
        ..AcpChannelOptions::default()
    };
    let channel = AcpChannel::new(id, cwd, options).map_err(|error| format!("acp: {error}"))?;
    Ok((channel, wire))
}

/// The transport handing the channel a connection to its scripted agent.
pub(crate) struct ScriptedTransport {
    agent: Value,
    wire: Wire,
    run: Weak<Run>,
    serving: Mutex<Option<JoinHandle<()>>>,
    /// The sessions and prompts the agent counts, over the whole run: a
    /// reconnect goes on counting (README, "of the run").
    counts: Arc<Counts>,
}

#[derive(Default)]
struct Counts {
    sessions: Mutex<u64>,
    prompts: Mutex<usize>,
}

impl AcpTransport for ScriptedTransport {
    type Connection = ClientSideConnection;

    fn name(&self) -> &'static str {
        "scripted"
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's open is a future"
    )]
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<ClientSideConnection, AcpError> {
        let (ours, theirs) = tokio::io::duplex(1 << 20);
        let (reader, writer) = tokio::io::split(ours);
        let agent = Agent {
            spec: self.agent.clone(),
            wire: Arc::clone(&self.wire),
            run: Weak::clone(&self.run),
            counts: Arc::clone(&self.counts),
        };
        let serving = tokio::spawn(serve(Arc::new(agent), theirs));
        if let Some(old) = self
            .serving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(serving)
        {
            old.abort();
        }
        Ok(ClientSideConnection::new(client, reader, writer))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's close is a future"
    )]
    async fn close(&self) {
        let serving = self
            .serving
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(serving) = serving {
            serving.abort();
        }
    }
}

struct Agent {
    spec: Value,
    wire: Wire,
    run: Weak<Run>,
    counts: Arc<Counts>,
}

type Writer = Arc<AsyncMutex<WriteHalf<tokio::io::DuplexStream>>>;

/// Reads the client's messages to the end of its stream, each request
/// answered by a task of its own, so that a prompt held at its gate does
/// not stop the reading.
async fn serve(agent: Arc<Agent>, stream: tokio::io::DuplexStream) {
    let (reader, writer) = tokio::io::split(stream);
    let writer: Writer = Arc::new(AsyncMutex::new(writer));
    let mut lines = BufReader::new(reader).lines();
    let mut answering = tokio::task::JoinSet::new();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = message["method"].as_str().map(str::to_owned) else {
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
        let prompt = (method == "session/prompt").then(|| agent.next_prompt());
        answering.spawn(answer(
            Arc::clone(&agent),
            message,
            prompt,
            Arc::clone(&writer),
        ));
    }
}

impl Agent {
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

async fn answer(agent: Arc<Agent>, message: Value, prompt: Option<Value>, writer: Writer) {
    let method = message["method"].as_str().unwrap_or_default();
    let params = &message["params"];
    let outcome: Result<Value, Value> = match (method, prompt) {
        ("initialize", _) => Ok(agent.initialized(params)),
        ("session/new", _) => {
            let mut sessions = agent
                .counts
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *sessions += 1;
            Ok(json!({ "sessionId": format!("session-{sessions}") }))
        }
        ("session/prompt", Some(prompt)) => {
            if let (Some(gate), Some(run)) = (prompt["gate"].as_str(), agent.run.upgrade()) {
                run.gates.hold(gate).await;
            }
            for update in prompt["updates"].as_array().into_iter().flatten() {
                let update = json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": { "sessionId": params["sessionId"], "update": update },
                });
                send(&writer, &update).await;
            }
            if let Some(error) = prompt.get("error") {
                Err(error.clone())
            } else {
                let stop = prompt.get("stop_reason").cloned();
                let mut result = json!({ "stopReason": stop.unwrap_or_else(|| json!("end_turn")) });
                if let Some(usage) = prompt.get("usage") {
                    result["usage"] = usage.clone();
                }
                Ok(result)
            }
        }
        ("session/close", _) => Ok(json!({})),
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
