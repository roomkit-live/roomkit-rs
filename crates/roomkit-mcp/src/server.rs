//! A connected MCP server, served to an agent as a toolset (step 4,
//! decision 7): its process, its connection, its tools, its instructions.

use std::fmt;
use std::sync::Arc;

use rmcp::model::CallToolRequestParams;
use rmcp::service::{Peer, RoleClient, RunningService};
use rmcp::transport::async_rw::AsyncRwTransport;
use rmcp::{ClientCacheConfig, ServiceExt};
use roomkit::model::ToolCall;
use roomkit_ai::{AiTool, ToolContext, ToolError, ToolOutput, Toolset};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OnceCell};

use crate::command::McpCommand;
use crate::error::{McpError, stderr_ending};
use crate::listing;
use crate::process::{ServerProcess, Started, Stderr};
use crate::results;
use crate::texts::bounded_message;

/// The largest message the server may send, one line of JSON-RPC, in
/// bytes: a longer one is dropped unread, and the request it answers is
/// left to its bound.
pub(crate) const MESSAGE_LIMIT: usize = 4 * 1024 * 1024;

/// The kind of error the model reads a call failed with when the
/// connection failed it: the server answered an error, or is gone (RFC
/// §9.3: the observers read the message).
const CALL_FAILED: &str = "McpError";

/// A connected MCP server over stdio or Streamable HTTP (RFC §21.2), serving its
/// tools to an agent as a [`Toolset`], under their bare names by default.
/// [`with_label`](Self::with_label) supplies host-qualified aliases, and
/// [`with_tool_names`](Self::with_tool_names) restricts one handle's inventory.
///
/// [`McpCommand::connect`] starts the command's process, runs the MCP
/// handshake and lists the server's tools once, following the listing's
/// cursor; no call lists them again. A connection that fails part-way
/// stops the process and waits for it before the error is returned. What
/// the server sends is bounded:
/// - a message, one line, at 4 MiB: a longer one is dropped unread, and the
///   call it answers is cut at the agent's bound per call (RFC §21.6);
/// - the listing at 1,000 pages and 512 tools; a tool whose name no vendor
///   accepts or is longer than 128 characters, whose name is listed
///   already, or whose parameters' schema is over 32 KiB is left out with a
///   warning; a description is cut at 4,096 characters, at a word;
/// - a call's result at 100,000 characters, cut with a note saying how long
///   it was.
///
/// A call answers the result's text, a note standing for what is not text
/// (an image, audio, a binary resource, a resource link); a result that
/// says `isError` is a failed call whose words the model reads (RFC
/// §9.3); a call the server answers with an error, or that its connection
/// fails, fails with `McpError`, its message for the call's observers
/// alone. The agent's bound per call applies: a server that never answers
/// is cut at it.
///
/// The server's instructions ([`instructions`](Self::instructions)) are
/// its text, as it sent it: hand them to the agent
/// ([`AiAgent::with_server_instructions`](roomkit_ai::AiAgent::with_server_instructions)),
/// which frames and bounds them, rather than into a prompt.
///
/// [`close`](Self::close) closes the server's input, gives it the
/// command's grace to exit (2 seconds by default), then sends its process
/// group `SIGTERM`, and `SIGKILL` after the grace again, and returns once
/// the server is waited for; an agent serving the tools closes it when its
/// kit shuts down. Dropping the last clone ends the group the same way at
/// once, and a server that exits on its own is waited for as it exits, what
/// it started ended with it: no zombie and no orphan stays. Cloning shares
/// the connection.
#[derive(Clone)]
pub struct McpServer {
    inner: Arc<Inner>,
    label: Option<String>,
    selected: Option<std::collections::BTreeSet<String>>,
}

struct Inner {
    program: String,
    peer: Peer<RoleClient>,
    tools: Vec<AiTool>,
    instructions: Option<String>,
    pid: Option<u32>,
    close_grace: std::time::Duration,
    connection: Mutex<Option<Connection>>,
    closed: OnceCell<()>,
}

/// What a connection holds until it is closed.
struct Connection {
    service: RunningService<RoleClient, ()>,
    process: Option<ServerProcess>,
    #[cfg(feature = "http")]
    http_finished: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl McpServer {
    /// Starts `command`'s server, runs the MCP handshake and lists its
    /// tools, within the command's connect bound ([`McpCommand::connect`]).
    pub(crate) async fn connect(
        command: McpCommand,
        cancelled: impl Future<Output = ()> + Send,
    ) -> Result<Self, McpError> {
        let program = command.program();
        let Started {
            process,
            stdin,
            stdout,
            stderr,
        } = ServerProcess::start(&command)?;
        let connecting = handshake(stdout, stdin, &program);
        let connected = tokio::select! {
            biased;
            () = cancelled => Err(McpError::Cancelled),
            result = tokio::time::timeout(command.connect_timeout, connecting) => match result {
                Ok(connected) => connected,
                Err(_elapsed) => Err(McpError::Timeout {
                    program: program.clone(),
                    after: command.connect_timeout,
                    stderr: Vec::new(),
                }),
            },
        };
        let (service, tools) = match connected {
            Ok(connected) => connected,
            Err(error) => return Err(failed(error, process, stderr).await),
        };
        let instructions = service
            .peer_info()
            .and_then(|info| info.instructions.clone());
        tracing::info!(
            target: "roomkit::mcp",
            server = program,
            tools = tools.len(),
            "connected to an MCP server"
        );
        Ok(Self {
            label: None,
            selected: None,
            inner: Arc::new(Inner {
                program,
                peer: service.peer().clone(),
                tools,
                instructions,
                pid: process.id(),
                close_grace: command.close_grace,
                connection: Mutex::new(Some(Connection {
                    service,
                    process: Some(process),
                    #[cfg(feature = "http")]
                    http_finished: None,
                })),
                closed: OnceCell::new(),
            }),
        })
    }

    #[cfg(feature = "http")]
    pub(crate) fn from_http(
        service: RunningService<RoleClient, ()>,
        tools: Vec<AiTool>,
        finished: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        let instructions = service
            .peer_info()
            .and_then(|info| info.instructions.clone());
        Self {
            label: None,
            selected: None,
            inner: Arc::new(Inner {
                program: "http".into(),
                peer: service.peer().clone(),
                tools,
                instructions,
                pid: None,
                close_grace: std::time::Duration::from_secs(10),
                connection: Mutex::new(Some(Connection {
                    service,
                    process: None,
                    http_finished: Some(finished),
                })),
                closed: OnceCell::new(),
            }),
        }
    }

    /// What the server published, when it connected, on how to use its
    /// tools (its handshake's `instructions`), as it sent it; `None` when
    /// it published none (RFC §21.2).
    pub fn instructions(&self) -> Option<&str> {
        self.inner.instructions.as_deref()
    }

    /// The id the server's process was started with; `None` for HTTP or once
    /// the server is closed.
    pub fn process_id(&self) -> Option<u32> {
        if !self.is_connected() {
            return None;
        }
        self.inner.pid
    }

    /// Whether the transport is still open. An exited server is reported as
    /// disconnected without waiting for an explicit close (RFC §21.2).
    pub fn is_connected(&self) -> bool {
        !self.inner.closed.initialized() && !self.inner.peer.is_transport_closed()
    }

    /// Serves this handle's tools as `mcp__<label>__<tool>` (RFC §21.2).
    /// Other clones keep their own labels and selections, sharing one process.
    /// Vendor name limits still apply to the complete alias.
    ///
    /// # Errors
    ///
    /// Refuses an empty label, more than 64 ASCII identifier characters, `__`,
    /// or a trailing underscore. Letters, digits, `_`, `-` and `.` are accepted.
    pub fn with_label(mut self, label: impl Into<String>) -> Result<Self, McpError> {
        let label = label.into();
        if label.is_empty()
            || label.len() > 64
            || label.contains("__")
            || label.ends_with('_')
            || !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        {
            return Err(McpError::Configuration {
                reason: "invalid server label",
            });
        }
        self.label = Some(label);
        Ok(self)
    }

    /// Limits this handle to the listed bare tool names. An empty selection
    /// exposes no tools. Both discovery and dispatch enforce the selection;
    /// another clone's selection is unchanged (RFC §21.1).
    ///
    /// # Errors
    ///
    /// Refuses names absent from the connection's discovered catalogue.
    pub fn with_tool_names(mut self, names: &[String]) -> Result<Self, McpError> {
        if names
            .iter()
            .any(|name| !self.inner.tools.iter().any(|tool| &tool.name == name))
        {
            return Err(McpError::Configuration {
                reason: "selected tool was not discovered",
            });
        }
        self.selected = Some(names.iter().cloned().collect());
        Ok(self)
    }

    /// Closes the connection (RFC §21.2). HTTP connections drain their worker
    /// and attempt to delete any negotiated session, without replaying tools.
    /// For stdio it stops the server: its input
    /// closed, it is given the command's grace to exit, then its process
    /// group is ended (`SIGTERM`, the grace, `SIGKILL`), and this returns
    /// once it is waited for. A call running fails. Called again, or beside
    /// a close running, it waits for that one's end.
    pub async fn close(&self) {
        self.inner
            .closed
            .get_or_init(|| async {
                let connection = self.inner.connection.lock().await.take();
                let Some(connection) = connection else {
                    return;
                };
                // Ending the service closes the server's input.
                let grace = self.inner.close_grace;
                let _ended = tokio::time::timeout(grace, connection.service.cancel()).await;
                if let Some(process) = connection.process {
                    process.stop(grace).await;
                }
                #[cfg(feature = "http")]
                if let Some(finished) = connection.http_finished {
                    let _finished = finished.await;
                }
                tracing::info!(
                    target: "roomkit::mcp",
                    server = self.inner.program,
                    "an MCP server is closed"
                );
            })
            .await;
    }
}

/// Stops the server of a connection that failed with `error`, and returns
/// the error once its process is waited for (RFC §21.2), with the last
/// lines the server wrote on its standard error: what a server that fails
/// to start says of why. A handshake's or a listing's message ends with
/// them, and a timeout carries them.
async fn failed(error: McpError, process: ServerProcess, stderr: Stderr) -> McpError {
    process.kill().await;
    match error {
        McpError::Handshake { program, message } => McpError::Handshake {
            program,
            message: message + &stderr_ending(&stderr.last_lines().await),
        },
        McpError::Listing { program, message } => McpError::Listing {
            program,
            message: message + &stderr_ending(&stderr.last_lines().await),
        },
        McpError::Timeout { program, after, .. } => McpError::Timeout {
            program,
            after,
            stderr: stderr.last_lines().await,
        },
        error @ (McpError::Http { .. }
        | McpError::Command { .. }
        | McpError::Spawn { .. }
        | McpError::Cancelled
        | McpError::Configuration { .. }) => error,
    }
}

/// The MCP handshake over the server's output and input, then the listing
/// of its tools; the service is ended if the listing fails.
async fn handshake<R, W>(
    output: R,
    input: W,
    program: &str,
) -> Result<(RunningService<RoleClient, ()>, Vec<AiTool>), McpError>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let transport = AsyncRwTransport::new_client(output, input).with_max_line_length(MESSAGE_LIMIT);
    let service =
        ().serve(transport)
            .await
            .map_err(|error| McpError::Handshake {
                program: program.to_owned(),
                message: bounded_message(&error.to_string()),
            })?;
    // rmcp keeps each page a server marks fresh (`ttlMs`) for as long as
    // the connection is open; a connection lists once, and keeps only the
    // tools it takes.
    service
        .peer()
        .set_response_cache_config(ClientCacheConfig::disabled())
        .await;
    match listing::tools(service.peer(), program).await {
        Ok(tools) => Ok((service, tools)),
        Err(error) => {
            let _ended = service.cancel().await;
            Err(error)
        }
    }
}

impl Toolset for McpServer {
    fn tools(&self) -> Vec<AiTool> {
        self.inner
            .tools
            .iter()
            .filter(|tool| {
                self.selected
                    .as_ref()
                    .is_none_or(|names| names.contains(&tool.name))
            })
            .cloned()
            .map(|mut tool| {
                if let Some(label) = &self.label {
                    tool.name = format!("mcp__{label}__{}", tool.name);
                }
                tool
            })
            .collect()
    }

    async fn call(
        &self,
        mut call: ToolCall,
        _context: ToolContext,
    ) -> Result<ToolOutput, ToolError> {
        if let Some(label) = &self.label {
            let prefix = format!("mcp__{label}__");
            let Some(name) = call.name.strip_prefix(&prefix) else {
                return Err(ToolError::raised_as(
                    CALL_FAILED,
                    "tool does not belong to this server",
                ));
            };
            call.name = name.to_owned();
        }
        if self
            .selected
            .as_ref()
            .is_some_and(|names| !names.contains(&call.name))
        {
            return Err(ToolError::raised_as(CALL_FAILED, "tool is not selected"));
        }
        let params = CallToolRequestParams::new(call.name.clone()).with_arguments(call.arguments);
        match self.inner.peer.call_tool(params).await {
            Ok(result) => results::read(&call.name, result),
            Err(error) => Err(ToolError::raised_as(
                CALL_FAILED,
                bounded_message(&error.to_string()),
            )),
        }
    }

    async fn close(&self) {
        Self::close(self).await;
    }
}

impl fmt::Debug for McpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self
            .inner
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        f.debug_struct("McpServer")
            .field("program", &self.inner.program)
            .field("tools", &names)
            .field("closed", &self.inner.closed.initialized())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pretty_assertions::assert_eq;
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::*;

    /// A server on `stream` whose every listing says it stays fresh for a
    /// day (`ttlMs`), counting the listings it is asked for.
    async fn fresh_for_a_day(stream: DuplexStream, listings: Arc<AtomicUsize>) {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let request: Value = serde_json::from_str(&line).unwrap();
            let Some(id) = request.get("id").cloned() else {
                continue;
            };
            let result = match request["method"].as_str() {
                Some("initialize") => json!({
                    "protocolVersion": request["params"]["protocolVersion"],
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "fresh", "version": "0" },
                }),
                Some("tools/list") => {
                    listings.fetch_add(1, Ordering::SeqCst);
                    json!({
                        "tools": [{ "name": "echo", "inputSchema": { "type": "object" } }],
                        "ttlMs": 86_400_000,
                    })
                }
                _ => json!({}),
            };
            let reply = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .unwrap();
        }
    }

    /// The listing keeps nothing in rmcp's response cache, though the server
    /// says it stays fresh: a connection lists once, and a cached page would
    /// stay resident for as long as it is open. Listing again reaches the
    /// server.
    #[tokio::test]
    async fn a_listing_that_says_it_stays_fresh_leaves_nothing_cached() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let listings = Arc::new(AtomicUsize::new(0));
        tokio::spawn(fresh_for_a_day(server, Arc::clone(&listings)));
        let (read, write) = tokio::io::split(client);
        let (service, tools) = handshake(read, write, "fresh").await.unwrap();
        let again = service.peer().list_tools(None).await.unwrap();
        let _ended = service.cancel().await;
        assert_eq!(
            (
                tools.len(),
                again.tools.len(),
                listings.load(Ordering::SeqCst)
            ),
            (1, 1, 2)
        );
    }
}
