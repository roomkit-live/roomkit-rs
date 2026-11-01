//! The tools of MCP servers for a RoomKit AI channel, over stdio or the
//! optional `http` feature for Streamable HTTP (RFC §21.2), in the Python
//! implementation's shape.
//!
//! - [`McpToolProvider`]: Python's `MCPToolProvider`, a server the host
//!   connects and closes. [`from_command`](McpToolProvider::from_command)
//!   starts a program with its arguments as a list, never through a shell,
//!   with an environment of its own; [`from_url`](McpToolProvider::from_url)
//!   (feature `http`) reaches a remote endpoint, with explicit headers,
//!   JSON or SSE responses, a bounded start and its session deleted at the
//!   close. No OAuth and no legacy HTTP+SSE transport are provided.
//! - A channel serves its tools through
//!   [`get_tools`](McpToolProvider::get_tools) and
//!   [`as_tool_handler`](McpToolProvider::as_tool_handler); its
//!   [`label`](FromCommandOptions::label) is the `<server>` of the MCP
//!   aliases `mcp__<server>__<tool>` its handler serves. The channel's tool
//!   policy governs its tools as any other, the alias and the tool it runs
//!   both (RFC §21.1).
//!
//! A server's process stops, with the processes it started (its process
//! group, on Unix), when the host closes the provider, or when the last
//! handle on it is dropped; neither the channel's close nor the kit's
//! closes it. A connection that fails part-way releases it before the
//! error is returned. What a server sends is bounded: its messages, its
//! listing, its tools' names and descriptions, a call's result.
//!
//! ```no_run
//! use roomkit::RoomKit;
//! use roomkit::model::ChannelId;
//! use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
//! use roomkit_ai::{AiChannel, AiChannelOptions, ToolEntry};
//! use roomkit_mcp::{AsToolHandlerOptions, FromCommandOptions, McpToolProvider};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let options = FromCommandOptions {
//!     args: ["-y", "@modelcontextprotocol/server-filesystem", "/srv/shop"]
//!         .map(Into::into)
//!         .into(),
//!     label: Some("files".into()),
//!     ..FromCommandOptions::default()
//! };
//! let files = McpToolProvider::from_command("npx", options)?;
//! files.connect().await?;
//! let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
//! let options = AiChannelOptions {
//!     tools: files.get_tools()?.into_iter().map(ToolEntry::from).collect(),
//!     tool_handler: Some(files.as_tool_handler(AsToolHandlerOptions::default())?),
//!     server_instructions: vec![("files".into(), files.instructions()?)],
//!     ..AiChannelOptions::default()
//! };
//! let agent = AiChannel::new(ChannelId::new("assistant")?, model, options)?;
//! let kit = RoomKit::new(Default::default());
//! kit.register_channel(agent)?;
//! // ...
//! kit.close().await?;
//! files.close().await; // the server's process stops
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "http")]
mod http;

mod command;
mod error;
mod group;
mod listing;
mod process;
mod provider;
mod results;
mod server;
mod texts;

pub use self::error::McpError;
#[cfg(feature = "http")]
pub use self::provider::FromUrlOptions;
pub use self::provider::{
    AsToolHandlerOptions, CallToolOptions, FromCommandOptions, McpToolProvider,
};
