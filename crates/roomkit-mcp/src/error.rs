//! What connecting to or configuring an MCP server fails with.

use std::time::Duration;

/// A connection or host configuration failure. A failed connection stops and
/// waits for its server before returning (RFC §21.2). A configuration error
/// on a cloned handle leaves the other handles' connection open. Server text
/// in an error is bounded and has terminal controls removed (see
/// [`Handshake`](Self::Handshake)); it can still contain private server data,
/// so hosts should classify errors before showing or recording them.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum McpError {
    /// An HTTP transport failure, with no URL, headers or response body.
    #[error("MCP HTTP connection failed: {reason} (status {status:?})")]
    Http {
        /// HTTP status when a response was received.
        status: Option<u16>,
        /// Safe category supplied by the transport, never server text.
        reason: &'static str,
    },
    /// A host label or explicit tool selection is invalid (RFC §21.2).
    #[error("invalid MCP tool configuration: {reason}")]
    Configuration {
        /// The violated rule; never server content or credentials.
        reason: &'static str,
    },
    /// The host cancelled connecting. The process has been stopped and reaped.
    #[error("MCP connection cancelled")]
    Cancelled,
    /// The command cannot start a process: an empty program, or an
    /// environment variable whose name is empty or holds `=` or a NUL, or
    /// whose value holds a NUL.
    #[error("the MCP server's command is invalid: {reason}")]
    Command {
        /// What is wrong with it.
        reason: String,
    },
    /// The program could not be started: no such file, say.
    #[error("the MCP server {program:?} could not be started: {source}")]
    Spawn {
        /// The program, without its arguments, which may hold secrets.
        program: String,
        /// Why.
        #[source]
        source: std::io::Error,
    },
    /// The server did not answer the MCP handshake (`initialize`) as the
    /// protocol says: it exited, closed its output, or answered something
    /// else.
    #[error("the MCP server {program:?} failed its handshake: {message}")]
    Handshake {
        /// The program.
        program: String,
        /// What failed, bounded, then the last lines the server wrote on
        /// its standard error, when it wrote any, each indented on a line
        /// of its own: 20 lines and 4 KiB at most. The server's words are
        /// harmless where they are shown: their control characters, line
        /// separators and bidi controls replaced, a line break they hold
        /// indented.
        message: String,
    },
    /// The server's `tools/list` failed.
    #[error("the MCP server {program:?} could not list its tools: {message}")]
    Listing {
        /// The program.
        program: String,
        /// What failed, bounded, then the last lines the server wrote on
        /// its standard error, as [`Handshake`](Self::Handshake)'s.
        message: String,
    },
    /// The handshake and the listing did not end within the command's
    /// connect bound.
    #[error(
        "the MCP server {program:?} did not connect within {} ms{}",
        .after.as_millis(),
        stderr_ending(.stderr)
    )]
    Timeout {
        /// The program.
        program: String,
        /// The bound.
        after: Duration,
        /// The last lines the server wrote on its standard error before it
        /// was stopped, oldest first, as [`Handshake`](Self::Handshake)'s
        /// message ends with them; empty when it wrote none.
        stderr: Vec<String>,
    },
}

/// What ends the error of a connection whose server wrote `stderr`, its
/// last lines of standard error: nothing when it wrote none, else each
/// line indented on a line of its own, so that none starts a log line.
pub(crate) fn stderr_ending(stderr: &[String]) -> String {
    if stderr.is_empty() {
        return String::new();
    }
    let mut ending = "; its standard error ended with:".to_owned();
    for line in stderr {
        ending.push_str("\n  ");
        ending.push_str(line);
    }
    ending
}
