//! What an ACP agent calls back into: the client side of the protocol.

use std::future::Future;
use std::pin::Pin;

use agent_client_protocol_schema::v1::{
    RequestPermissionRequest, RequestPermissionResponse, SessionNotification,
};

use crate::error::AcpError;

/// What an ACP agent calls back into: the Python SDK's `acp.Client`, reduced
/// to the two calls RoomKit's client serves (HITL e6f3decd).
///
/// A connection hands each `session/update` notification to
/// [`session_update`](Self::session_update), each session's in the order
/// the agent sent them, one after the other and apart from the other
/// sessions', and each `session/request_permission` request to
/// [`request_permission`](Self::request_permission), the reading going on
/// meanwhile: a callback may ask the connection and wait for its answer. It refuses every other request of the agent with
/// JSON-RPC's `method_not_found`, as the Python implementation's client
/// refuses the file system, the terminals, elicitation and extensions, and
/// ignores every other notification.
///
/// The channel passes its own client to [`AcpTransport::open`]; a test, or a
/// host speaking ACP without a channel, implements its own.
///
/// [`AcpTransport::open`]: crate::AcpTransport::open
pub trait AcpClient: Send + Sync + 'static {
    /// Takes one of the agent's session updates. The session's next update
    /// waits until this returns, and a prompt of the session returns once
    /// the updates the agent sent before its answer were taken.
    fn session_update(
        &self,
        notification: SessionNotification,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Answers the agent's request for permission to run a tool call: an
    /// option it offered, or `cancelled`. An error is the JSON-RPC error the
    /// agent is answered: [`AcpError::Rpc`] as it is, any other as an
    /// internal error.
    fn request_permission(
        &self,
        request: RequestPermissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RequestPermissionResponse, AcpError>> + Send + '_>>;
}
