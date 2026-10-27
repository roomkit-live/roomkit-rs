//! A connected MCP server, served to an agent as a toolset (step 4,
//! decision 7): its process, its connection, its tools, its instructions.

use std::fmt;
use std::sync::Arc;

use rmcp::model::CallToolRequestParams;
use rmcp::service::{Peer, RoleClient, RunningService};
use rmcp::transport::async_rw::AsyncRwTransport;
use rmcp::{ClientCacheConfig, ServiceExt};
use roomkit::model::ToolCall;
use roomkit_ai::{ToolContext, ToolDefinition, ToolError, ToolOutput, Toolset};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OnceCell};

use crate::command::McpCommand;
use crate::error::McpError;
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

/// An MCP server started over stdio and connected (RFC §21.2), serving its
/// tools to an agent as a [`Toolset`], under their bare names.
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
}

struct Inner {
    program: String,
    peer: Peer<RoleClient>,
    tools: Vec<ToolDefinition>,
    instructions: Option<String>,
    pid: Option<u32>,
    close_grace: std::time::Duration,
    connection: Mutex<Option<Connection>>,
    closed: OnceCell<()>,
}

/// What a connection holds until it is closed.
struct Connection {
    service: RunningService<RoleClient, ()>,
    process: ServerProcess,
}

impl McpServer {
    /// Starts `command`'s server, runs the MCP handshake and lists its
    /// tools, within the command's connect bound ([`McpCommand::connect`]).
    pub(crate) async fn connect(command: McpCommand) -> Result<Self, McpError> {
        let program = command.program();
        let Started {
            process,
            stdin,
            stdout,
            stderr,
        } = ServerProcess::start(&command)?;
        let connecting = handshake(stdout, stdin, &program);
        let connected = match tokio::time::timeout(command.connect_timeout, connecting).await {
            Ok(connected) => connected,
            Err(_elapsed) => Err(McpError::Timeout {
                program: program.clone(),
                after: command.connect_timeout,
            }),
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
            inner: Arc::new(Inner {
                program,
                peer: service.peer().clone(),
                tools,
                instructions,
                pid: process.id(),
                close_grace: command.close_grace,
                connection: Mutex::new(Some(Connection { service, process })),
                closed: OnceCell::new(),
            }),
        })
    }

    /// What the server published, when it connected, on how to use its
    /// tools (its handshake's `instructions`), as it sent it; `None` when
    /// it published none (RFC §21.2).
    pub fn instructions(&self) -> Option<&str> {
        self.inner.instructions.as_deref()
    }

    /// The id the server's process was started with; `None` once the
    /// server is closed.
    pub fn process_id(&self) -> Option<u32> {
        if self.inner.closed.initialized() {
            return None;
        }
        self.inner.pid
    }

    /// Closes the connection and stops the server (RFC §21.2): its input
    /// closed, it is given the command's grace to exit, then its process
    /// group is ended (`SIGTERM`, the grace, `SIGKILL`), and this returns
    /// once it is waited for. A call running fails. Called again, or beside
    /// a close running, it waits for that one's end.
    pub async fn close(&self) {
        self.inner
            .closed
            .get_or_init(|| async {
                let connection = self.inner.connection.lock().await.take();
                let Some(Connection { service, process }) = connection else {
                    return;
                };
                // Ending the service closes the server's input.
                let grace = self.inner.close_grace;
                let _ended = tokio::time::timeout(grace, service.cancel()).await;
                process.stop(grace).await;
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
/// the error once its process is waited for (RFC §21.2), the last lines it
/// wrote on its standard error added to a handshake's or a listing's
/// message: what a server that fails to start says of why.
async fn failed(error: McpError, process: ServerProcess, stderr: Stderr) -> McpError {
    process.kill().await;
    match error {
        McpError::Handshake { program, message } => McpError::Handshake {
            program,
            message: with_stderr(message, &stderr.last_lines().await),
        },
        McpError::Listing { program, message } => McpError::Listing {
            program,
            message: with_stderr(message, &stderr.last_lines().await),
        },
        error @ (McpError::Command { .. } | McpError::Spawn { .. } | McpError::Timeout { .. }) => {
            error
        }
    }
}

/// `message`, then the server's last lines of standard error, `stderr`,
/// when it wrote any.
fn with_stderr(message: String, stderr: &str) -> String {
    if stderr.is_empty() {
        return message;
    }
    format!("{message}; its standard error ended with:\n{stderr}")
}

/// The MCP handshake over the server's output and input, then the listing
/// of its tools; the service is ended if the listing fails.
async fn handshake<R, W>(
    output: R,
    input: W,
    program: &str,
) -> Result<(RunningService<RoleClient, ()>, Vec<ToolDefinition>), McpError>
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
    fn tools(&self) -> Vec<ToolDefinition> {
        self.inner.tools.clone()
    }

    async fn call(&self, call: ToolCall, _context: ToolContext) -> Result<ToolOutput, ToolError> {
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
