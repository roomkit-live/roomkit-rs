//! An external coding agent spoken to over the Agent Client Protocol (ACP)
//! (RFC §6.4), in the Python implementation's shape: RoomKit is the ACP
//! client, the agent (Claude Code, Codex, Gemini CLI, any ACP v1 agent) the
//! server.
//!
//! The protocol's types are those of the `agent-client-protocol-schema`
//! crate (its `v1` module), and the JSON-RPC connection is this crate's own,
//! on tokio (HITL 0e261e51):
//!
//! - [`AcpTransport`], Python's `ACPTransport`: how a connection reaches its
//!   agent. [`StdioAcpTransport`], Python's `StdioACPTransport`, spawns it
//!   here, a program and its arguments without a shell, and speaks over its
//!   stdio.
//! - [`AcpConnection`]: what a client asks the agent, the methods of the
//!   Python SDK's `ClientSideConnection`; [`ClientSideConnection`] speaks
//!   them as JSON-RPC 2.0 over a byte stream, one message per line.
//! - [`AcpClient`]: what the agent calls back into, the Python SDK's
//!   `acp.Client`: its session updates and its permission requests.
//! - [`AcpError`]: what they fail with.
//! - [`AcpChannel`], Python's `ACPChannel`: the agent as a room's
//!   intelligence channel, one session per room; [`acp_event_text`], what
//!   the agent reads of an event.
//!
//! ```no_run
//! use std::future::Future;
//! use std::pin::Pin;
//! use std::sync::Arc;
//!
//! use agent_client_protocol_schema::ProtocolVersion;
//! use agent_client_protocol_schema::v1::{
//!     InitializeRequest, RequestPermissionOutcome, RequestPermissionRequest,
//!     RequestPermissionResponse, SessionNotification,
//! };
//! use roomkit_acp::{
//!     AcpClient, AcpConnection, AcpError, AcpTransport, StdioAcpTransport,
//!     StdioAcpTransportOptions,
//! };
//!
//! struct Client;
//!
//! impl AcpClient for Client {
//!     fn session_update(
//!         &self,
//!         notification: SessionNotification,
//!     ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
//!         Box::pin(async move { tracing::info!(?notification.update, "an update") })
//!     }
//!
//!     fn request_permission(
//!         &self,
//!         _request: RequestPermissionRequest,
//!     ) -> Pin<Box<dyn Future<Output = Result<RequestPermissionResponse, AcpError>> + Send + '_>>
//!     {
//!         Box::pin(async {
//!             Ok(RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled))
//!         })
//!     }
//! }
//!
//! # async fn run() -> Result<(), AcpError> {
//! let transport = StdioAcpTransport::new(
//!     vec!["my-agent".into(), "--acp".into()],
//!     "/srv/project",
//!     StdioAcpTransportOptions::default(),
//! )?;
//! let connection = transport.open(Arc::new(Client)).await?;
//! let answer = connection
//!     .initialize(InitializeRequest::new(ProtocolVersion::V1))
//!     .await?;
//! tracing::info!(?answer.agent_info, "connected");
//! transport.close().await; // the agent's process stops
//! # Ok(())
//! # }
//! ```

mod channel;
mod client;
mod client_side;
mod connection;
mod context;
mod env;
mod error;
mod invalidated;
mod mcp;
mod process;
mod stdio;
mod text;
mod transport;

pub use self::channel::{AcpChannel, AcpChannelOptions};

pub use self::client::AcpClient;
pub use self::client_side::ClientSideConnection;
pub use self::connection::AcpConnection;
pub use self::context::AcpContextContributor;
pub use self::error::AcpError;
pub use self::invalidated::{AcpSessionInvalidatedError, AcpSessionInvalidatedErrorOptions};
pub use self::mcp::{McpServers, McpServersForRoom};
pub use self::stdio::{StdioAcpTransport, StdioAcpTransportOptions};
pub use self::text::acp_event_text;
pub use self::transport::AcpTransport;
