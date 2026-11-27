//! A connected MCP server, served to an agent as a toolset (step 4,
//! decision 7): its process, its connection, its tools, its instructions.

mod requests;

use std::fmt;
use std::sync::Arc;

use rmcp::model::{CallToolRequestParams, CallToolResult, ClientConfig};
use rmcp::service::{Peer, RoleClient, RunningService};
use rmcp::{ClientCacheConfig, ServiceExt};
use roomkit::model::Metadata;
use roomkit_ai::AiTool;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OnceCell};

use crate::command::McpCommand;
use crate::error::{McpError, stderr_ending};
use crate::listing::{self, Listed, ToolFilter};
use crate::process::{ServerProcess, Started, Stderr};
use crate::raw::Capture;
use crate::texts::bounded_message;

/// The largest message the server may send, one line of JSON-RPC, in
/// bytes: a longer one is dropped unread, and the request it answers is
/// left to its bound.
pub(crate) const MESSAGE_LIMIT: usize = 4 * 1024 * 1024;

/// How long a closing server is given to end, as Python's MCP SDK gives
/// it: two seconds.
pub(crate) const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// A connected MCP server over stdio or Streamable HTTP (RFC §21.2), behind
/// an [`McpToolProvider`](crate::McpToolProvider): its process, its
/// connection, its tools as it names them, its instructions.
///
/// A connection starts the command's process, runs the MCP handshake and
/// lists the server's tools once, following the listing's cursor; no call
/// lists them again. A connection that fails part-way stops the process
/// and waits for it before the error is returned. What the server sends is
/// bounded:
/// - a message, one line, at 4 MiB: a longer one is dropped unread, and the
///   call it answers is cut at its bound;
/// - the listing at 1,000 pages and 512 tools; a tool whose name no vendor
///   accepts or is longer than 128 characters, whose name is listed
///   already, or whose parameters' schema is over 32 KiB is left out with a
///   warning; a description is cut at 4,096 characters, at a word.
///
/// [`close`](Self::close) closes the server's input, gives it the grace to
/// exit, then sends its process group `SIGTERM`, and `SIGKILL` after the
/// grace again, and returns once the server is waited for. Dropping the
/// last clone ends the group the same way at once, and a server that exits
/// on its own is waited for as it exits, what it started ended with it: no
/// zombie and no orphan stays. Cloning shares the connection.
#[derive(Clone)]
pub(crate) struct McpServer {
    inner: Arc<Inner>,
}

struct Inner {
    program: String,
    /// Where a legacy SSE connection's callers send their cancellations;
    /// `None` on another transport.
    #[cfg(feature = "http")]
    legacy: Option<crate::http::legacy::request::Cancellations>,
    peer: Peer<RoleClient>,
    tools: Listed,
    capture: Capture,
    instructions: Option<String>,
    pid: Option<u32>,
    close_grace: std::time::Duration,
    connection: Mutex<Option<Connection>>,
    closed: OnceCell<()>,
}

/// What a connection holds until it is closed.
struct Connection {
    service: RunningService<RoleClient, ClientConfig>,
    process: Option<ServerProcess>,
    #[cfg(feature = "http")]
    http_finished: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl McpServer {
    /// Starts `command`'s server, runs the MCP handshake and lists its
    /// tools, within the command's connect bound, unless `cancelled` ends
    /// first.
    pub(crate) async fn connect(
        command: McpCommand,
        cancelled: impl Future<Output = ()> + Send,
        filter: Option<ToolFilter>,
        discover: bool,
    ) -> Result<Self, McpError> {
        let program = command.program();
        let Started {
            process,
            stdin,
            stdout,
            stderr,
        } = ServerProcess::start(&command)?;
        let capture = Capture::default();
        let connecting = handshake(
            stdout,
            stdin,
            &program,
            filter.as_ref(),
            discover,
            capture.clone(),
        );
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
            tools = tools.tools.len(),
            "connected to an MCP server"
        );
        Ok(Self {
            inner: Arc::new(Inner {
                program,
                #[cfg(feature = "http")]
                legacy: None,
                peer: service.peer().clone(),
                tools,
                capture,
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
        service: RunningService<RoleClient, ClientConfig>,
        tools: Listed,
        capture: Capture,
        finished: tokio::sync::oneshot::Receiver<()>,
        legacy: Option<crate::http::legacy::request::Cancellations>,
    ) -> Self {
        let instructions = service
            .peer_info()
            .and_then(|info| info.instructions.clone());
        Self {
            inner: Arc::new(Inner {
                program: "http".into(),
                legacy,
                peer: service.peer().clone(),
                tools,
                capture,
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
    pub(crate) fn instructions(&self) -> Option<&str> {
        self.inner.instructions.as_deref()
    }

    /// The id the server's process was started with; `None` for HTTP or once
    /// the server is closed.
    pub(crate) fn process_id(&self) -> Option<u32> {
        if !self.is_connected() {
            return None;
        }
        self.inner.pid
    }

    /// Whether the transport is still open. An exited server is reported as
    /// disconnected without waiting for an explicit close (RFC §21.2).
    pub(crate) fn is_connected(&self) -> bool {
        !self.inner.closed.initialized() && !self.inner.peer.is_transport_closed()
    }

    /// Closes the connection (RFC §21.2). HTTP connections drain their worker
    /// and attempt to delete any negotiated session, without replaying tools.
    /// For stdio it stops the server: its input
    /// closed, it is given the command's grace to exit, then its process
    /// group is ended (`SIGTERM`, the grace, `SIGKILL`), and this returns
    /// once it is waited for. A call running fails. Called again, or beside
    /// a close running, it waits for that one's end.
    pub(crate) async fn close(&self) {
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
        | McpError::Configuration { .. }
        | McpError::NotConnected
        | McpError::AlreadyConnected
        | McpError::Call { .. }
        | McpError::CallTimeout { .. }
        | McpError::Validation { .. }) => error,
    }
}

/// The MCP handshake over the server's output and input, then the listing
/// of its tools; the service is ended if the listing fails.
async fn handshake<R, W>(
    output: R,
    input: W,
    program: &str,
    filter: Option<&ToolFilter>,
    discover: bool,
    capture: Capture,
) -> Result<(RunningService<RoleClient, ClientConfig>, Listed), McpError>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let transport = crate::raw_stdio::transport(output, input, capture);
    let service = crate::protocol::client()
        .serve(transport)
        .await
        .map_err(|error| McpError::Handshake {
            program: program.to_owned(),
            message: bounded_message(&error.to_string()),
        })?;
    if !service
        .peer_info()
        .is_some_and(|info| crate::protocol::supported(&info.protocol_version))
    {
        let _ended = service.cancel().await;
        return Err(McpError::Handshake {
            program: program.to_owned(),
            message: "unsupported MCP protocol version".into(),
        });
    }
    // rmcp keeps each page a server marks fresh (`ttlMs`) for as long as
    // the connection is open; a connection lists once, and keeps only the
    // tools it takes.
    service
        .peer()
        .set_response_cache_config(ClientCacheConfig::disabled())
        .await;
    if !discover {
        return Ok((service, Listed::default()));
    }
    match listing::tools(service.peer(), program, filter).await {
        Ok(tools) => Ok((service, tools)),
        Err(error) => {
            let _ended = service.cancel().await;
            Err(error)
        }
    }
}

impl McpServer {
    pub(crate) fn host_capture(&self) -> &Capture {
        &self.inner.capture
    }

    pub(crate) fn tool_meta(&self) -> std::collections::BTreeMap<String, Metadata> {
        self.inner.tools.metadata.clone()
    }

    /// The tools the server listed, as it names them.
    pub(crate) fn listed(&self) -> Vec<AiTool> {
        self.inner.tools.tools.clone()
    }

    /// Calls the tool `name`, as the server names it, with `arguments`: the
    /// server's result as it sent it.
    pub(crate) async fn call(
        &self,
        name: &str,
        arguments: Metadata,
    ) -> Result<CallToolResult, McpError> {
        let params = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
        self.tool_request(params)
            .await
            .map_err(|error| McpError::Call {
                message: bounded_message(&error.to_string()),
            })
    }
}

impl fmt::Debug for McpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self
            .inner
            .tools
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

/// Refuses a label that is no identifier for the server's tools: letters,
/// digits, `_`, `.` and `-`, 64 at most, with no `__` and no trailing `_`,
/// which would make `mcp__<label>__<tool>` ambiguous.
pub(crate) fn check_label(label: &str) -> Result<(), McpError> {
    let valid = !label.is_empty()
        && label.len() <= 64
        && !label.contains("__")
        && !label.ends_with('_')
        && label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(McpError::Configuration {
            reason: "invalid server label",
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use pretty_assertions::assert_eq;
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::*;

    /// The messages a server was sent, in order.
    type Received = Arc<Mutex<Vec<Value>>>;

    /// A server on `stream` whose every listing says it stays fresh for a
    /// day (`ttlMs`), keeping every message it is sent in `received`.
    async fn fresh_for_a_day(stream: DuplexStream, received: Received) {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let request: Value = serde_json::from_str(&line).unwrap();
            received.lock().unwrap().push(request.clone());
            let Some(id) = request.get("id").cloned() else {
                continue;
            };
            let result = match request["method"].as_str() {
                Some("initialize") => json!({
                    "protocolVersion": request["params"]["protocolVersion"],
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "fresh", "version": "0" },
                }),
                Some("tools/list") => json!({
                    "tools": [{ "name": "echo", "inputSchema": { "type": "object" } }],
                    "ttlMs": 86_400_000,
                }),
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
        let received = Received::default();
        tokio::spawn(fresh_for_a_day(server, Arc::clone(&received)));
        let (read, write) = tokio::io::split(client);
        let (service, tools) = handshake(read, write, "fresh", None, true, Capture::default())
            .await
            .unwrap();
        let again = service.peer().list_tools(None).await.unwrap();
        let _ended = service.cancel().await;
        let listings = received
            .lock()
            .unwrap()
            .iter()
            .filter(|message| message["method"] == "tools/list")
            .count();
        assert_eq!((tools.tools.len(), again.tools.len(), listings), (1, 1, 2));
    }

    /// RFC §21.2: the connection opens with MCP's `initialize` handshake at
    /// protocol 2025-11-25, as Python's SDK opens it, `server/discover`
    /// never sent: the protocol of the MCP revision of 2026-07-28 is not
    /// spoken yet.
    #[tokio::test]
    async fn the_connection_opens_with_initialize_at_2025_11_25() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let received = Received::default();
        tokio::spawn(fresh_for_a_day(server, Arc::clone(&received)));
        let (read, write) = tokio::io::split(client);
        let (service, _tools) = handshake(read, write, "fresh", None, true, Capture::default())
            .await
            .unwrap();
        let _ended = service.cancel().await;
        let methods: Vec<Value> = received
            .lock()
            .unwrap()
            .iter()
            .map(|message| message["method"].clone())
            .collect();
        let version = received.lock().unwrap()[0]["params"]["protocolVersion"].clone();
        assert_eq!(
            (methods, version),
            (
                vec![
                    json!("initialize"),
                    json!("notifications/initialized"),
                    json!("tools/list")
                ],
                json!("2025-11-25")
            )
        );
    }
}
