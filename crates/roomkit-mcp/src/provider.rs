//! Python's `roomkit.tools.MCPToolProvider`: an MCP server's tools, its
//! connection the host opens and closes (RFC §21.2).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rmcp::model::CallToolResult;
use roomkit::model::Metadata;
use roomkit_ai::{AiTool, ToolError, ToolHandler, ToolResult, alias_server, served_tool_name};
use tokio::sync::RwLock;

use crate::command::McpCommand;
use crate::error::McpError;
use crate::results;
use crate::server::{McpServer, check_label};

/// Which tools of the server a provider keeps, by name.
type ToolFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// What [`McpToolProvider::from_command`] takes beside the command:
/// Python's `args=(), *, env=None, cwd=None, tool_filter=None, label=None,
/// connect_timeout=60.0`.
#[derive(Clone)]
pub struct FromCommandOptions {
    /// The command's arguments.
    pub args: Vec<OsString>,
    /// Variables the server's process gets beside the minimal set it
    /// inherits.
    pub env: BTreeMap<OsString, OsString>,
    /// The directory the server runs in.
    pub cwd: Option<PathBuf>,
    /// Keeps the tools whose names it admits; every tool without.
    #[expect(clippy::type_complexity, reason = "Python's Callable[[str], bool]")]
    pub tool_filter: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// The host's name for the server, the `<server>` of an MCP alias
    /// `mcp__<server>__<tool>`: the tool handler then serves only the
    /// aliases naming this server. Letters, digits and `_ . -`, 64 at most,
    /// no `__` and no final `_`.
    pub label: Option<String>,
    /// How long the start, the handshake and the listing may take.
    pub connect_timeout: Duration,
}

impl Default for FromCommandOptions {
    fn default() -> Self {
        Self {
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
            tool_filter: None,
            label: None,
            connect_timeout: Duration::from_secs(60),
        }
    }
}

impl fmt::Debug for FromCommandOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromCommandOptions")
            .field("args", &self.args)
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .field("cwd", &self.cwd)
            .field("label", &self.label)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

/// What [`McpToolProvider::from_url`] takes beside the URL: Python's
/// `*, tool_filter=None, headers=None, label=None`, over Streamable HTTP.
#[cfg(feature = "http")]
#[derive(Clone, Default)]
pub struct FromUrlOptions {
    /// Keeps the tools whose names it admits; every tool without.
    #[expect(clippy::type_complexity, reason = "Python's Callable[[str], bool]")]
    pub tool_filter: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// Headers every request carries.
    pub headers: BTreeMap<String, String>,
    /// The host's name for the server, the `<server>` of an MCP alias
    /// `mcp__<server>__<tool>`: the tool handler then serves only the
    /// aliases naming this server. Letters, digits and `_ . -`, 64 at most,
    /// no `__` and no final `_`.
    pub label: Option<String>,
}

#[cfg(feature = "http")]
impl fmt::Debug for FromUrlOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromUrlOptions")
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

/// What [`McpToolProvider::as_tool_handler`] takes: Python's
/// `*, gate_discovery=True`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AsToolHandlerOptions {
    /// Serve only the tools the connection listed; `false` forwards every
    /// name to the server, for a gateway serving tools it never listed,
    /// and declines only an alias naming another server than the label.
    pub gate_discovery: bool,
}

impl Default for AsToolHandlerOptions {
    fn default() -> Self {
        Self {
            gate_discovery: true,
        }
    }
}

/// What [`McpToolProvider::call_tool`] takes: Python's `*, timeout=30.0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallToolOptions {
    /// How long the call may take.
    pub timeout: Duration,
}

impl Default for CallToolOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}

/// How a provider reaches its server.
#[derive(Clone)]
enum Target {
    Command(McpCommand),
    #[cfg(feature = "http")]
    Url(crate::http::McpHttp),
}

/// An MCP server's tools (RFC §21.2): Python's `MCPToolProvider`. Built
/// with [`from_command`](Self::from_command) or
/// [`from_url`](Self::from_url), it does nothing until the host
/// [`connect`](Self::connect)s it, Python's `async with`, which starts the
/// server, shakes hands and lists its tools; the host closes it
/// ([`close`](Self::close)), and dropping the last clone of a connected
/// provider stops a command's process group all the same. An AI channel
/// serves its tools through [`get_tools`](Self::get_tools) and
/// [`as_tool_handler`](Self::as_tool_handler); its close does not close the
/// provider.
#[derive(Clone)]
pub struct McpToolProvider {
    inner: Arc<Inner>,
}

struct Inner {
    target: Target,
    tool_filter: Option<ToolFilter>,
    label: Option<String>,
    server: RwLock<Option<McpServer>>,
}

impl McpToolProvider {
    /// The server `command` runs, its tools listed over its standard input
    /// and output once connected: Python's `MCPToolProvider.from_command`.
    ///
    /// The program and its arguments are passed as a list, never through a
    /// shell. The server inherits only `HOME`, `LOGNAME`, `PATH`, `SHELL`,
    /// `TERM` and `USER` of the host's environment (on Windows, the
    /// variables a program needs to run), and the variables of
    /// [`env`](FromCommandOptions::env): pass what it needs, an API key
    /// say, there. On Unix it leads a process group of its own, stopped
    /// whole at the close, so that what it starts (the real server, behind
    /// `npx` or `uvx`) goes with it; in its own group it no longer receives
    /// a terminal's Ctrl+C, which a handler closing the provider covers.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Configuration`] for a label that cannot name the
    /// server in an alias, and [`McpError::Command`] for a command that
    /// cannot start a process, as Python's raises `ValueError`.
    pub fn from_command(
        command: impl Into<OsString>,
        options: FromCommandOptions,
    ) -> Result<Self, McpError> {
        let FromCommandOptions {
            args,
            env,
            cwd,
            tool_filter,
            label,
            connect_timeout,
        } = options;
        let mut built = McpCommand::new(command)
            .args(args)
            .connect_timeout(connect_timeout);
        for (key, value) in env {
            built = built.env(key, value);
        }
        if let Some(cwd) = cwd {
            built = built.current_dir(cwd);
        }
        built.check()?;
        Self::over(Target::Command(built), tool_filter, label)
    }

    /// The server at `url`, over Streamable HTTP: Python's
    /// `MCPToolProvider.from_url`.
    ///
    /// Its connection is unbounded, as Python bounds only a server it
    /// starts: the TCP connection is cut at 10 seconds, but a server that
    /// takes it and never answers holds [`connect`](Self::connect) until it
    /// does. A host bounds it itself:
    /// `tokio::time::timeout(Duration::from_secs(60), provider.connect())`.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Configuration`] for a label that cannot name the
    /// server in an alias, a URL that is no HTTP(S) endpoint or carries
    /// credentials or a fragment, and a header that is invalid or the
    /// transport's own.
    #[cfg(feature = "http")]
    pub fn from_url(url: &str, options: FromUrlOptions) -> Result<Self, McpError> {
        let FromUrlOptions {
            tool_filter,
            headers,
            label,
        } = options;
        let mut http = crate::http::McpHttp::new(url)?;
        for (name, value) in &headers {
            http = http.header(name, value)?;
        }
        Self::over(Target::Url(http), tool_filter, label)
    }

    fn over(
        target: Target,
        tool_filter: Option<ToolFilter>,
        label: Option<String>,
    ) -> Result<Self, McpError> {
        if let Some(label) = &label {
            check_label(label)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                target,
                tool_filter,
                label,
                server: RwLock::new(None),
            }),
        })
    }

    /// Connects the server: starts it, shakes hands and lists its tools,
    /// Python's `__aenter__`. A provider closed connects again.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::AlreadyConnected`] for a provider connected
    /// already, and the [`McpError`] the start, the handshake or the
    /// listing failed with.
    pub async fn connect(&self) -> Result<(), McpError> {
        self.connect_until(std::future::pending()).await
    }

    /// Connects the server, as [`connect`](Self::connect), unless
    /// `cancelled` ends first: what the connection started is then cleaned
    /// up before the call returns [`McpError::Cancelled`], a command's
    /// process stopped, an HTTP session deleted. Where Python's cancelled
    /// task cleans up as it unwinds, a dropped Rust future cannot wait.
    ///
    /// # Errors
    ///
    /// As [`connect`](Self::connect), and [`McpError::Cancelled`].
    #[doc(hidden)]
    pub async fn connect_until(
        &self,
        cancelled: impl Future<Output = ()> + Send,
    ) -> Result<(), McpError> {
        let mut held = self.inner.server.write().await;
        if held.as_ref().is_some_and(McpServer::is_connected) {
            return Err(McpError::AlreadyConnected);
        }
        let server = match &self.inner.target {
            Target::Command(command) => command.clone().connect_until(cancelled).await?,
            #[cfg(feature = "http")]
            Target::Url(http) => http.clone().connect_until(cancelled).await?,
        };
        *held = Some(server);
        Ok(())
    }

    /// Closes the server, Python's `__aexit__`: a command's process stops,
    /// within a grace of two seconds, as Python's SDK gives it. A provider
    /// never connected, or closed already, has nothing to close.
    pub async fn close(&self) {
        let server = self.inner.server.read().await.as_ref().cloned();
        if let Some(server) = server {
            server.close().await;
        }
    }

    /// Whether the server is connected: Python's `connected`.
    pub fn connected(&self) -> bool {
        self.server().is_some_and(|server| server.is_connected())
    }

    /// What the server published, when it connected, on how to use its
    /// tools, as it sent it: Python's `instructions`. Hand it to a
    /// channel's `server_instructions`, which sets it apart and bounds it,
    /// rather than into a prompt.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotConnected`] for a provider not connected.
    pub fn instructions(&self) -> Result<Option<String>, McpError> {
        let server = self.connected_server()?;
        Ok(server.instructions().map(str::to_owned))
    }

    /// The server's tools the filter admits, as the server names them:
    /// Python's `get_tools()`.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotConnected`] for a provider not connected.
    pub fn get_tools(&self) -> Result<Vec<AiTool>, McpError> {
        Ok(self.catalogue(&self.connected_server()?))
    }

    /// The names of [`get_tools`](Self::get_tools): Python's `tool_names`.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotConnected`] for a provider not connected.
    pub fn tool_names(&self) -> Result<Vec<String>, McpError> {
        let tools = self.get_tools()?;
        Ok(tools.into_iter().map(|tool| tool.name).collect())
    }

    /// The handler serving the provider's tools, for an AI channel's
    /// `tool_handler`: Python's `as_tool_handler()`.
    ///
    /// An MCP alias `mcp__<server>__<tool>` runs `<tool>`. A provider with
    /// a label serves only the aliases naming it, and declines one naming
    /// another server ([`ToolError::UnservedToolCall`]), for the next
    /// handler of a channel's chain; one without serves an alias whatever
    /// server it names. With `gate_discovery`, it declines a tool its
    /// connection did not list. A result that says `isError` is a failed
    /// call ([`ToolError::Failed`]) whose words the model reads.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotConnected`] for a provider not connected.
    pub fn as_tool_handler(&self, options: AsToolHandlerOptions) -> Result<ToolHandler, McpError> {
        self.connected_server()?;
        let provider = self.clone();
        Ok(ToolHandler::new(move |name, arguments| {
            let provider = provider.clone();
            async move { provider.serve(&name, arguments, options).await }
        }))
    }

    /// Calls the tool `name`, as the server names it, with `arguments`,
    /// within `options.timeout`: Python's `call_tool`. It calls any tool
    /// the server has: the filter shapes what is offered to a model, not
    /// what the host may call.
    ///
    /// Answers the result's text: one text part as itself, several as a
    /// JSON array, a note for what is not text; a result that says
    /// `isError` answers `{"error": "<its words>"}`.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotConnected`] for a provider not connected,
    /// [`McpError::Call`] when the server answers with an error or the
    /// connection fails, and [`McpError::CallTimeout`] past the bound.
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Metadata,
        options: CallToolOptions,
    ) -> Result<String, McpError> {
        let result = self.call_result(name, arguments, options.timeout).await?;
        Ok(results::text(name, result))
    }

    /// The same provider, its command's server given `grace` to exit at its
    /// close, where Python's SDK gives two seconds: for a test, which does
    /// not wait the seconds. Before it is connected only.
    #[doc(hidden)]
    #[must_use]
    pub fn with_close_grace(self, grace: Duration) -> Self {
        self.retargeted(|target| match target {
            Target::Command(command) => Target::Command(command.close_grace(grace)),
            #[cfg(feature = "http")]
            other @ Target::Url(_) => other,
        })
    }

    /// The same provider, its connection bounded by `bound`, where a
    /// command's takes 60 seconds and an HTTP server's is unbounded, as
    /// Python's: for a test. Before it is connected only.
    #[doc(hidden)]
    #[must_use]
    pub fn with_connect_timeout(self, bound: Duration) -> Self {
        self.retargeted(|target| match target {
            Target::Command(command) => Target::Command(command.connect_timeout(bound)),
            #[cfg(feature = "http")]
            Target::Url(http) => Target::Url(http.connect_timeout(bound)),
        })
    }

    /// The server's process id, while it runs: for a test of its lifetime.
    #[doc(hidden)]
    pub fn process_id(&self) -> Option<u32> {
        self.server().and_then(|server| server.process_id())
    }

    /// The same provider, its target changed by `change`.
    fn retargeted(self, change: impl FnOnce(Target) -> Target) -> Self {
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            unreachable_clone()
        };
        let Inner {
            target,
            tool_filter,
            label,
            server,
        } = inner;
        Self {
            inner: Arc::new(Inner {
                target: change(target),
                tool_filter,
                label,
                server,
            }),
        }
    }

    /// A call of `name` as the handler serves it (Python's `_handler`).
    async fn serve(
        &self,
        name: &str,
        arguments: Metadata,
        options: AsToolHandlerOptions,
    ) -> Result<ToolResult, ToolError> {
        if let (Some(server), Some(label)) = (alias_server(name), &self.inner.label)
            && server != label
        {
            return Err(ToolError::unserved(format!(
                "tool {name:?} names another server"
            )));
        }
        let lookup = served_tool_name(name);
        if options.gate_discovery && !self.listed(lookup) {
            return Err(ToolError::unserved(format!(
                "tool {name:?} is not served here"
            )));
        }
        let timeout = CallToolOptions::default().timeout;
        let result = self
            .call_result(lookup, arguments, timeout)
            .await
            .map_err(|error| raised(&error))?;
        results::read(lookup, result)
    }

    /// The server's answer to a call of `name`, within `timeout`.
    async fn call_result(
        &self,
        name: &str,
        arguments: Metadata,
        timeout: Duration,
    ) -> Result<CallToolResult, McpError> {
        let server = self.connected_server()?;
        match tokio::time::timeout(timeout, server.call(name, arguments)).await {
            Ok(answered) => answered,
            Err(_elapsed) => Err(McpError::CallTimeout {
                tool: name.to_owned(),
                after: timeout,
            }),
        }
    }

    /// Whether the connection listed `name` and the filter admits it, the
    /// listing kept after a close, as Python keeps its tools.
    fn listed(&self, name: &str) -> bool {
        self.server()
            .is_some_and(|server| self.catalogue(&server).iter().any(|tool| tool.name == name))
    }

    /// The tools `server` listed that the filter admits.
    fn catalogue(&self, server: &McpServer) -> Vec<AiTool> {
        server
            .listed()
            .into_iter()
            .filter(|tool| self.admits(&tool.name))
            .collect()
    }

    /// The server, while it is connected.
    fn connected_server(&self) -> Result<McpServer, McpError> {
        self.server()
            .filter(McpServer::is_connected)
            .ok_or(McpError::NotConnected)
    }

    /// The server last connected, when there is one.
    fn server(&self) -> Option<McpServer> {
        self.inner
            .server
            .try_read()
            .ok()
            .and_then(|held| held.as_ref().cloned())
    }

    /// Whether the filter admits the tool `name`.
    fn admits(&self, name: &str) -> bool {
        self.inner
            .tool_filter
            .as_ref()
            .is_none_or(|filter| filter(name))
    }
}

/// `error` as the tool handler raises it, of the kind Python's raises:
/// `RuntimeError` for a provider not connected, `TimeoutError` past the
/// bound, the SDK's `McpError` otherwise.
fn raised(error: &McpError) -> ToolError {
    let kind = match error {
        McpError::NotConnected | McpError::AlreadyConnected => "RuntimeError",
        McpError::CallTimeout { .. } => "TimeoutError",
        McpError::Call { .. }
        | McpError::Http { .. }
        | McpError::Configuration { .. }
        | McpError::Cancelled
        | McpError::Command { .. }
        | McpError::Spawn { .. }
        | McpError::Handshake { .. }
        | McpError::Listing { .. }
        | McpError::Timeout { .. } => "McpError",
    };
    ToolError::raised_as(kind, error.to_string())
}

impl fmt::Debug for McpToolProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpToolProvider")
            .field("label", &self.inner.label)
            .field("connected", &self.connected())
            .finish_non_exhaustive()
    }
}

/// A provider cloned before a test hook changes it: a test's mistake.
#[expect(clippy::panic, reason = "a hidden test hook, misused only by a test")]
fn unreachable_clone() -> ! {
    panic!("a test hook changes the provider before it is cloned")
}
