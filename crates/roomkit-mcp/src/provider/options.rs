//! Public MCP constructor options, mirroring Python's defaults (RFC §21.2).

use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::{McpEnvironment, McpHeaders};

/// Python's common `MCPToolProvider` constructor, before connecting (RFC §21.2).
/// Only the chosen transport's options may be supplied. Environment and
/// nonempty headers handles are retained; arguments are an owned snapshot.
#[derive(Clone)]
pub struct McpToolProviderOptions {
    /// `streamable_http` by default; `sse` and `stdio` are the other choices.
    pub transport: String,
    /// Keeps the tools whose names it admits; every tool without.
    #[expect(clippy::type_complexity, reason = "Python's Callable[[str], bool]")]
    pub tool_filter: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// Headers read when each HTTP connection opens.
    pub headers: Option<McpHeaders>,
    /// The stdio program, passed directly without a shell.
    pub command: Option<OsString>,
    /// The stdio program's arguments, copied at construction.
    pub args: Vec<OsString>,
    /// Environment overrides read when each subprocess starts.
    pub env: Option<McpEnvironment>,
    /// The stdio program's working directory.
    pub cwd: Option<PathBuf>,
    /// List tools while connecting; true by default.
    pub discover: bool,
    /// The host's server label in MCP aliases, checked before other options.
    pub label: Option<String>,
    /// The connection deadline; `None` uses 60 seconds, as Python does.
    pub connect_timeout: Option<Duration>,
    /// How the connection agrees on the MCP revision: `"auto"` (the
    /// default) probes the server's discovery (`server/discover`, MCP
    /// 2026-07-28) and falls back to the `initialize` handshake for a
    /// server of an earlier revision; `"legacy"` goes straight to the
    /// handshake, for a server that fails on the probe or leaves it
    /// unanswered. The `sse` transport always takes the handshake. Any other
    /// value is refused at construction.
    pub protocol: String,
    /// Carry the host's trace context (W3C `traceparent`, `tracestate`,
    /// `baggage`, from the kit's current span) in each request's `_meta`,
    /// unless a [`request`](crate::McpToolProvider::request)'s caller wrote
    /// one of its own there, which goes whole instead; `false` (the default)
    /// sends only the trace keys a caller writes.
    pub propagate_trace_context: bool,
}

impl Default for McpToolProviderOptions {
    fn default() -> Self {
        Self {
            transport: "streamable_http".to_owned(),
            tool_filter: None,
            headers: None,
            command: None,
            args: Vec::new(),
            env: None,
            cwd: None,
            discover: true,
            label: None,
            connect_timeout: None,
            protocol: "auto".to_owned(),
            propagate_trace_context: false,
        }
    }
}

impl fmt::Debug for McpToolProviderOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpToolProviderOptions")
            .field("transport", &self.transport)
            .field("headers", &self.headers)
            .field("command", &self.command)
            .field("args", &self.args.len())
            .field("env", &self.env)
            .field("cwd", &self.cwd)
            .field("discover", &self.discover)
            .field("label", &self.label)
            .field("connect_timeout", &self.connect_timeout)
            .field("protocol", &self.protocol)
            .field("propagate_trace_context", &self.propagate_trace_context)
            .finish_non_exhaustive()
    }
}

/// What [`crate::McpToolProvider::from_command`] takes beside the command:
/// Python's `args=(), *, env=None, cwd=None, tool_filter=None, label=None,
/// connect_timeout=60.0, protocol="auto", propagate_trace_context=False`.
#[derive(Clone)]
pub struct FromCommandOptions {
    /// The command's arguments.
    pub args: Vec<OsString>,
    /// Variables the server's process gets beside the minimal set it
    /// inherits.
    pub env: Option<McpEnvironment>,
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
    /// How long the start, the negotiation and the listing may take.
    pub connect_timeout: Duration,
    /// List tools while connecting; false opens a resource-only connection.
    pub discover: bool,
    /// How the connection agrees on the MCP revision: `"auto"` (the
    /// default) probes discovery first, `"legacy"` goes straight to the
    /// handshake; see [`McpToolProviderOptions::protocol`].
    pub protocol: String,
    /// Carry the host's trace context on each request; see
    /// [`McpToolProviderOptions::propagate_trace_context`].
    pub propagate_trace_context: bool,
}

impl Default for FromCommandOptions {
    fn default() -> Self {
        Self {
            args: Vec::new(),
            env: None,
            cwd: None,
            tool_filter: None,
            label: None,
            connect_timeout: Duration::from_secs(60),
            discover: true,
            protocol: "auto".to_owned(),
            propagate_trace_context: false,
        }
    }
}

impl fmt::Debug for FromCommandOptions {
    /// The names of the variables given and how many arguments, never the
    /// arguments nor the values, which may hold secrets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromCommandOptions")
            .field("args", &self.args.len())
            .field("env", &self.env)
            .field("cwd", &self.cwd)
            .field("label", &self.label)
            .field("discover", &self.discover)
            .field("connect_timeout", &self.connect_timeout)
            .field("protocol", &self.protocol)
            .field("propagate_trace_context", &self.propagate_trace_context)
            .finish_non_exhaustive()
    }
}

/// What [`crate::McpToolProvider::from_url`] takes beside the URL: Python's
/// `*, transport="streamable_http", tool_filter=None, headers=None, label=None,
/// connect_timeout=60.0, protocol="auto", propagate_trace_context=False`, over
/// Streamable HTTP or legacy SSE.
#[cfg(feature = "http")]
#[derive(Clone)]
pub struct FromUrlOptions {
    /// The exact Python transport: `streamable_http` (default), `sse`, or a refused choice.
    pub transport: String,
    /// Keeps the tools whose names it admits; every tool without.
    #[expect(clippy::type_complexity, reason = "Python's Callable[[str], bool]")]
    pub tool_filter: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// Headers every request carries.
    pub headers: Option<McpHeaders>,
    /// The host's name for the server, the `<server>` of an MCP alias
    /// `mcp__<server>__<tool>`: the tool handler then serves only the
    /// aliases naming this server. Letters, digits and `_ . -`, 64 at most,
    /// no `__` and no final `_`.
    pub label: Option<String>,
    /// How long the negotiation and the listing may take.
    pub connect_timeout: Duration,
    /// List tools while connecting; false opens a resource-only connection.
    pub discover: bool,
    /// How the connection agrees on the MCP revision: `"auto"` (the
    /// default) probes discovery first, `"legacy"` goes straight to the
    /// handshake, which the `sse` transport always takes; see
    /// [`McpToolProviderOptions::protocol`].
    pub protocol: String,
    /// Carry the host's trace context on each request; see
    /// [`McpToolProviderOptions::propagate_trace_context`].
    pub propagate_trace_context: bool,
}

#[cfg(feature = "http")]
impl Default for FromUrlOptions {
    fn default() -> Self {
        Self {
            transport: "streamable_http".to_owned(),
            tool_filter: None,
            headers: None,
            label: None,
            connect_timeout: Duration::from_secs(60),
            discover: true,
            protocol: "auto".to_owned(),
            propagate_trace_context: false,
        }
    }
}

#[cfg(feature = "http")]
impl fmt::Debug for FromUrlOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromUrlOptions")
            .field("transport", &self.transport)
            .field("headers", &self.headers)
            .field("label", &self.label)
            .field("discover", &self.discover)
            .field("connect_timeout", &self.connect_timeout)
            .field("protocol", &self.protocol)
            .field("propagate_trace_context", &self.propagate_trace_context)
            .finish_non_exhaustive()
    }
}
