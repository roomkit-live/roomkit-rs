//! The tools of MCP servers for a RoomKit AI agent, each server started as
//! a process and reached over its stdin and stdout (RFC §21.2; step 4,
//! decision 12: stdio only).
//!
//! - [`McpCommand`]: what starts a server, a program and its arguments as a
//!   list, never through a shell, with an environment of its own.
//! - [`McpServer`]: a connected server, a [`Toolset`](roomkit_ai::Toolset)
//!   an agent serves beside its other tools, under the tools' bare names: a
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
//! kit.shutdown().await; // the server's process stops
//! # Ok(())
//! # }
//! ```

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
