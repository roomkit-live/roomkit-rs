//! The integrator's MCP descriptors, resolved for every session opening.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use agent_client_protocol_schema::v1::McpServer;
use roomkit::model::RoomId;

use crate::AcpError;

type ServersFuture = Pin<Box<dyn Future<Output = Result<Vec<McpServer>, AcpError>> + Send>>;

/// Python's `mcp_servers`: a fixed list or a function of the room (RFC §6.4).
/// The channel resolves it each time it opens a session, without caching a
/// callback's result across openings. Descriptors are the ACP SDK's types.
#[derive(Clone)]
pub enum McpServers {
    /// The same descriptors, in their original order, for every room.
    Fixed(Vec<McpServer>),
    /// Resolve the descriptors from the room identifier at each opening.
    ForRoom(McpServersForRoom),
}

impl From<Vec<McpServer>> for McpServers {
    fn from(servers: Vec<McpServer>) -> Self {
        Self::Fixed(servers)
    }
}

impl McpServers {
    pub(crate) async fn resolve(&self, room: &RoomId) -> Result<Vec<McpServer>, AcpError> {
        match self {
            Self::Fixed(servers) => Ok(servers.clone()),
            Self::ForRoom(callback) => (callback.0)(room.clone()).await,
        }
    }
}

impl fmt::Debug for McpServers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Descriptors can hold credentials in URLs, headers and environment values.
        match self {
            Self::Fixed(servers) => f
                .debug_struct("Fixed")
                .field("count", &servers.len())
                .finish(),
            Self::ForRoom(_) => f.write_str("ForRoom(McpServersForRoom)"),
        }
    }
}

/// Python's `MCPServersForRoom`: the room's MCP descriptors, sync or async.
/// Use `std::future::ready` for a synchronous supplier. An error fails the
/// opening before `session/new`; cancellation drops the pending future.
#[derive(Clone)]
pub struct McpServersForRoom(Arc<dyn Fn(RoomId) -> ServersFuture + Send + Sync>);

impl McpServersForRoom {
    /// Wrap a supplier called separately for each new session (RFC §6.4).
    ///
    /// ```
    /// use roomkit_acp::{McpServers, McpServersForRoom};
    /// let servers = McpServers::ForRoom(McpServersForRoom::new(|room_id| async move {
    ///     // Resolve this room's descriptors using the host's own services.
    ///     let _ = room_id;
    ///     Ok(Vec::new())
    /// }));
    /// ```
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(RoomId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<McpServer>, AcpError>> + Send + 'static,
    {
        Self(Arc::new(move |room| Box::pin(callback(room))))
    }
}

impl fmt::Debug for McpServersForRoom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("McpServersForRoom")
    }
}
