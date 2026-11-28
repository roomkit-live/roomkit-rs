//! A connected MCP server, served to an agent as a toolset (step 4,
//! decision 7): its process, its connection, its tools, its instructions.

mod requests;

use std::fmt;
use std::sync::Arc;

use rmcp::ClientCacheConfig;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientConfig, MetaObject, RequestMetaObject,
};
use rmcp::service::{Peer, RoleClient, RunningService};
use roomkit::model::Metadata;
use roomkit_ai::AiTool;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OnceCell};

use crate::command::McpCommand;
use crate::error::{McpError, stderr_ending};
use crate::listing::{self, Listed, ToolFilter};
use crate::process::{ServerProcess, Started, Stderr};
use crate::protocol::Protocol;
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
    /// A legacy SSE connection's writer queue, which its callers'
    /// cancellations join; `None` on another transport.
    #[cfg(feature = "http")]
    legacy: Option<crate::http::legacy::Queue>,
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
    /// Starts `command`'s server, agrees on the MCP revision with it as
    /// `protocol` says and lists its tools, within the command's connect
    /// bound, unless `cancelled` ends first.
    pub(crate) async fn connect(
        command: McpCommand,
        cancelled: impl Future<Output = ()> + Send,
        filter: Option<ToolFilter>,
        discover: bool,
        protocol: Protocol,
    ) -> Result<Self, McpError> {
        let program = command.program();
        let Started {
            process,
            stdin,
            stdout,
            stderr,
        } = ServerProcess::start(&command)?;
        let capture = Capture::opening(protocol.propagate_trace_context);
        let connecting = handshake(
            stdout,
            stdin,
            &program,
            filter.as_ref(),
            discover,
            protocol.probe,
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
        legacy: Option<crate::http::legacy::Queue>,
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

/// The MCP negotiation over the server's output and input (discovery
/// first when `probe`, the handshake otherwise), then the listing of its
/// tools when `discover`; the service is ended if the listing fails.
async fn handshake<R, W>(
    output: R,
    input: W,
    program: &str,
    filter: Option<&ToolFilter>,
    discover: bool,
    probe: bool,
    capture: Capture,
) -> Result<(RunningService<RoleClient, ClientConfig>, Listed), McpError>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let refused = |error: String| McpError::Handshake {
        program: program.to_owned(),
        message: bounded_message(&error),
    };
    let mut transport = crate::raw_stdio::transport(output, input, capture.clone());
    if probe {
        let settled = crate::negotiation::negotiate_auto(&mut transport)
            .await
            .map_err(refused)?;
        capture.settle(&settled);
    }
    let service = crate::protocol::serve(transport, capture.modern())
        .await
        .map_err(refused)?;
    if !service
        .peer_info()
        .is_some_and(|info| crate::protocol::supported(&info.protocol_version, capture.modern()))
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
    match listing::tools(service.peer(), &capture, program, filter).await {
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
        let mut params = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
        if self.inner.capture.propagates() {
            let trace = crate::trace::host_context();
            if !trace.is_empty() {
                params.meta = Some(RequestMetaObject(MetaObject(trace)));
            }
        }
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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::*;

    /// The messages a server was sent, in order.
    type Received = Arc<Mutex<Vec<Value>>>;

    /// How a scripted server answers a method.
    #[derive(Clone)]
    enum Answer {
        /// With this result.
        Result(Value),
        /// With this JSON-RPC error.
        Error(Value),
        /// Never.
        Silent,
        /// With this result, once the next request is answered.
        Late(Value),
        /// As the answer, once this long has passed.
        After(Duration, Box<Answer>),
        /// Each call as the next of these, the last one from then on.
        Each(Vec<Answer>),
    }

    /// A server on `stream` answering each method as `plan` says, a method
    /// it does not plan with a JSON-RPC error, and keeping every message it
    /// is sent in `received`: Python's scripted server of
    /// `tests/test_mcp_discovery.py`.
    async fn scripted(
        stream: DuplexStream,
        plan: HashMap<&'static str, Answer>,
        received: Received,
    ) {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        let mut held = Vec::new();
        let mut calls: HashMap<String, usize> = HashMap::new();
        while let Ok(Some(line)) = lines.next_line().await {
            let request: Value = serde_json::from_str(&line).unwrap();
            received.lock().unwrap().push(request.clone());
            let Some(id) = request.get("id").cloned() else {
                continue;
            };
            let method = request["method"].as_str().unwrap_or_default();
            let call = calls.entry(method.to_owned()).or_default();
            let mut answer = plan.get(method).cloned();
            if let Some(Answer::Each(answers)) = answer {
                answer = answers.get(*call).or(answers.last()).cloned();
            }
            *call += 1;
            if let Some(Answer::After(delay, then)) = answer {
                tokio::time::sleep(delay).await;
                answer = Some(*then);
            }
            let reply = match answer {
                Some(Answer::Silent) => continue,
                Some(Answer::Late(result)) => {
                    held.push(json!({ "jsonrpc": "2.0", "id": id, "result": result }));
                    continue;
                }
                Some(Answer::Result(result)) => {
                    json!({ "jsonrpc": "2.0", "id": id, "result": result })
                }
                Some(Answer::Error(error)) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
                Some(Answer::After(..) | Answer::Each(_)) => unreachable!("one level of each"),
                None => json!({ "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32601, "message": "Method not found" } }),
            };
            for reply in std::iter::once(reply).chain(held.drain(..)) {
                write
                    .write_all(format!("{reply}\n").as_bytes())
                    .await
                    .unwrap();
            }
        }
    }

    fn handshake_result(name: &str) -> Answer {
        Answer::Result(json!({
            "protocolVersion": "2025-11-25",
            "capabilities": { "tools": {} },
            "serverInfo": { "name": name, "version": "0" },
            "instructions": "from the handshake",
        }))
    }

    fn discovery_result() -> Value {
        json!({
            "supportedVersions": ["2026-07-28"],
            "capabilities": { "tools": {} },
            "instructions": "from discovery",
            "resultType": "complete", "ttlMs": 0, "cacheScope": "private",
            "_meta": { "io.modelcontextprotocol/serverInfo": { "name": "modern", "version": "2" } },
        })
    }

    /// A listing that says it stays fresh for a day (`ttlMs`).
    fn fresh_listing() -> Answer {
        Answer::Result(json!({
            "tools": [{ "name": "echo", "inputSchema": { "type": "object" } }],
            "ttlMs": 86_400_000,
        }))
    }

    /// The methods `received` holds, in order.
    fn methods(received: &Received) -> Vec<Value> {
        received
            .lock()
            .unwrap()
            .iter()
            .map(|message| message["method"].clone())
            .collect()
    }

    /// A connection to a scripted server following `plan`, probing its
    /// discovery when `probe`: the service, its listing, its capture and
    /// what the server received.
    async fn connected(
        plan: HashMap<&'static str, Answer>,
        probe: bool,
    ) -> (
        RunningService<RoleClient, ClientConfig>,
        Listed,
        Capture,
        Received,
    ) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let received = Received::default();
        tokio::spawn(scripted(server, plan, Arc::clone(&received)));
        let (read, write) = tokio::io::split(client);
        let capture = Capture::default();
        let (service, tools) = handshake(read, write, "fake", None, true, probe, capture.clone())
            .await
            .unwrap();
        (service, tools, capture, received)
    }

    /// The listing keeps nothing in rmcp's response cache, though the server
    /// says it stays fresh: a connection lists once, and a cached page would
    /// stay resident for as long as it is open. Listing again reaches the
    /// server.
    #[tokio::test]
    async fn a_listing_that_says_it_stays_fresh_leaves_nothing_cached() {
        let plan = HashMap::from([
            ("initialize", handshake_result("fresh")),
            ("tools/list", fresh_listing()),
        ]);
        let (service, tools, _capture, received) = connected(plan, false).await;
        let again = service.peer().list_tools(None).await.unwrap();
        let _ended = service.cancel().await;
        let listings = methods(&received)
            .iter()
            .filter(|method| *method == "tools/list")
            .count();
        assert_eq!((tools.tools.len(), again.tools.len(), listings), (1, 1, 2));
    }

    /// RFC §21.2, RK-21.2-056: `protocol="legacy"` opens with MCP's
    /// `initialize` handshake at 2025-11-25, as Python's SDK opens it, and
    /// sends no probe even to a server that would answer it; `"auto"`
    /// probes discovery first, and a server that refuses it is reached by
    /// the same handshake. Python:
    /// `tests/test_mcp_discovery.py::test_the_legacy_protocol_goes_straight_to_the_handshake`
    /// and `test_a_server_that_does_not_discover_is_reached_by_the_handshake[refused]`.
    #[tokio::test]
    async fn the_handshake_opens_at_2025_11_25_straight_or_after_a_refused_probe() {
        for probe in [false, true] {
            let mut plan = HashMap::from([
                ("initialize", handshake_result("legacy")),
                ("tools/list", fresh_listing()),
            ]);
            if !probe {
                plan.insert("server/discover", Answer::Result(discovery_result()));
            }
            let (service, _tools, capture, received) = connected(plan, probe).await;
            let _ended = service.cancel().await;
            let mut expected = vec![
                json!("initialize"),
                json!("notifications/initialized"),
                json!("tools/list"),
            ];
            if probe {
                expected.insert(0, json!("server/discover"));
            }
            let initialize = received.lock().unwrap()[usize::from(probe)].clone();
            assert_eq!(
                (
                    methods(&received),
                    initialize["params"]["protocolVersion"].clone(),
                    capture.modern()
                ),
                (expected, json!("2025-11-25"), false),
                "probe: {probe}"
            );
        }
    }

    /// A probe never answered falls back to the handshake within the probe's
    /// own bound, ten seconds as Python's SDK gives it, well within the
    /// connection's. Python: `test_a_server_that_does_not_discover_is_reached_by_the_handshake[unanswered]`.
    #[tokio::test(start_paused = true)]
    async fn an_unanswered_probe_falls_back_within_its_own_bound() {
        let plan = HashMap::from([
            ("server/discover", Answer::Silent),
            ("initialize", handshake_result("legacy")),
            ("tools/list", fresh_listing()),
        ]);
        let started = tokio::time::Instant::now();
        let (service, tools, capture, received) = connected(plan, true).await;
        let waited = started.elapsed();
        let _ended = service.cancel().await;
        assert_eq!(
            (
                methods(&received)[..2].to_vec(),
                tools.tools.len(),
                capture.server_info()["name"].clone()
            ),
            (
                vec![json!("server/discover"), json!("initialize")],
                1,
                json!("legacy")
            )
        );
        assert!(
            waited >= Duration::from_secs(10) && waited < Duration::from_secs(11),
            "{waited:?}"
        );
    }

    /// A server that answers the probe only after the handshake it fell back
    /// to: the connection speaks the handshake's revision and keeps what the
    /// handshake said, not the late discovery, and goes on serving. Python:
    /// `test_a_late_answer_to_the_probe_leaves_what_the_handshake_said`.
    #[tokio::test(start_paused = true)]
    async fn a_late_answer_to_the_probe_leaves_what_the_handshake_said() {
        let plan = HashMap::from([
            ("server/discover", Answer::Late(discovery_result())),
            ("initialize", handshake_result("legacy")),
            ("tools/list", fresh_listing()),
        ]);
        let (service, tools, capture, _received) = connected(plan, true).await;
        let info = service.peer_info().unwrap();
        let read = (
            info.protocol_version.to_string(),
            info.instructions.clone(),
            capture.server_info()["name"].clone(),
            capture.modern(),
            tools.tools.len(),
        );
        let _ended = service.cancel().await;
        assert_eq!(
            read,
            (
                "2025-11-25".to_owned(),
                Some("from the handshake".to_owned()),
                json!("legacy"),
                false,
                1
            )
        );
    }

    /// `-32022`, its data naming the revisions the server speaks.
    fn unsupported(supported: &[&str]) -> Answer {
        Answer::Error(json!({
            "code": -32022, "message": "unsupported protocol version",
            "data": { "supported": supported, "requested": "2025-11-25" },
        }))
    }

    /// A probe that timed out reached a server slow to start, which then
    /// speaks 2026-07-28 only: its handshake refused with `-32022` naming
    /// that revision, the connection probes again and speaks it, as Python's
    /// SDK does (`negotiate_auto`), the late answer to the first probe left.
    #[tokio::test(start_paused = true)]
    async fn a_handshake_refused_after_a_probe_timed_out_probes_again() {
        let plan = HashMap::from([
            (
                "server/discover",
                Answer::Each(vec![
                    Answer::Late(discovery_result()),
                    Answer::Result(discovery_result()),
                ]),
            ),
            ("initialize", unsupported(&["2026-07-28"])),
            ("tools/list", fresh_listing()),
        ]);
        let (service, tools, capture, received) = connected(plan, true).await;
        let _ended = service.cancel().await;
        assert_eq!(
            (methods(&received), capture.modern(), tools.tools.len()),
            (
                vec![
                    json!("server/discover"),
                    json!("initialize"),
                    json!("server/discover"),
                    json!("tools/list")
                ],
                true,
                1
            )
        );
    }

    /// Each probe of a `-32022` retry has a bound of its own, ten seconds:
    /// a second probe answered at 15 seconds, 9 after it went out, agrees.
    #[tokio::test(start_paused = true)]
    async fn each_probe_of_a_retry_has_a_bound_of_its_own() {
        let plan = HashMap::from([
            (
                "server/discover",
                Answer::Each(vec![
                    Answer::After(
                        Duration::from_secs(6),
                        Box::new(unsupported(&["2026-07-28", "2025-11-25"])),
                    ),
                    Answer::After(
                        Duration::from_secs(9),
                        Box::new(Answer::Result(discovery_result())),
                    ),
                ]),
            ),
            ("initialize", handshake_result("legacy")),
            ("tools/list", fresh_listing()),
        ]);
        let started = tokio::time::Instant::now();
        let (service, _tools, capture, received) = connected(plan, true).await;
        let waited = started.elapsed();
        let _ended = service.cancel().await;
        assert_eq!(
            (methods(&received)[..2].to_vec(), capture.modern()),
            (
                vec![json!("server/discover"), json!("server/discover")],
                true
            )
        );
        assert!(waited >= Duration::from_secs(15), "{waited:?}");
    }

    /// A handshake result that also names `supportedVersions` is read as a
    /// handshake's, as Python's SDK reads it: only a probe's answer is given
    /// a discovery's defaults.
    #[tokio::test]
    async fn a_handshake_that_names_revisions_is_still_a_handshake() {
        let plan = HashMap::from([
            (
                "initialize",
                Answer::Result(json!({
                    "protocolVersion": "2025-11-25",
                    "supportedVersions": ["2025-11-25"],
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "legacy", "version": "0" },
                })),
            ),
            ("tools/list", fresh_listing()),
        ]);
        let (service, tools, capture, _received) = connected(plan, true).await;
        let _ended = service.cancel().await;
        assert_eq!((tools.tools.len(), capture.modern()), (1, false));
    }
}
