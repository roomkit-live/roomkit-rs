//! What connecting to an MCP server fails with.

use std::time::Duration;

/// What connecting to an MCP server failed with. Whatever it failed at, the
/// server's process was stopped and waited for before the error is
/// returned (RFC §21.2). A message the server's words make up is bounded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum McpError {
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
        /// its standard error, when it wrote any: 20 lines and 4 KiB at
        /// most, their control characters replaced.
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
    #[error("the MCP server {program:?} did not connect within {} ms", .after.as_millis())]
    Timeout {
        /// The program.
        program: String,
        /// The bound.
        after: Duration,
    },
}
