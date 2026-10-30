//! The tools of MCP servers for a RoomKit AI agent, over stdio or the
//! optional `http` feature for Streamable HTTP (RFC §21.2).
//!
//! - [`McpCommand`]: what starts a server, a program and its arguments as a
//!   list, never through a shell, with an environment of its own.
//! - `McpHttp` (feature `http`): a remote endpoint, optional explicit headers,
//!   JSON/SSE responses, bounded startup and session cleanup. No OAuth or
//!   legacy HTTP+SSE transport is provided.
//! - [`McpServer`]: a connected server, a [`Toolset`](roomkit_ai::Toolset)
//!   an agent serves beside its other tools, under bare names or host-labelled
//!   aliases (`McpServer::with_label`), optionally selecting a subset
//!   (`McpServer::with_tool_names`): a
//!   name another of the agent's toolsets serves, a host tool's or a
//!   skill's, is refused when the agent takes it (RFC §21.1 "One tool per
//!   name"). The agent's tool policy governs its tools as any other (RFC
//!   §21.1), and its bound per call applies (RFC §21.6).
//!
//! A server's process stops, with the processes it started (its process
//! group, on Unix), when its connection closes, when the kit of the agent
//! serving its tools shuts down, or when the last handle on it is dropped;
//! a connection that fails part-way releases it before the error is
//! returned. What a server sends is bounded: its messages, its listing,
//! its tools' names and descriptions, a call's result.
//!
//! ```no_run
//! use roomkit::RoomKit;
//! use roomkit::model::ChannelId;
//! use roomkit_ai::AiAgent;
//! use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
//! use roomkit_mcp::McpCommand;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let files = McpCommand::new("npx")
//!     .args(["-y", "@modelcontextprotocol/server-filesystem", "/srv/shop"])
//!     .connect()
//!     .await?;
//! let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
//! let agent = AiAgent::new(ChannelId::new("assistant")?, model)
//!     .with_server_instructions("files", files.instructions())?
//!     .with_tools(files)?;
//! let kit = RoomKit::new(Default::default());
//! kit.register_channel(agent)?;
//! // ...
//! kit.close().await?; // the server's process stops
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
pub use self::http::McpHttp;

mod command;
mod error;
mod group;
mod listing;
mod process;
mod results;
mod server;
mod texts;

pub use self::command::McpCommand;
pub use self::error::McpError;
pub use self::server::McpServer;
