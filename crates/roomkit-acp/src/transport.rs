//! How an ACP connection reaches its agent.

use std::future::Future;
use std::sync::Arc;

use crate::client::AcpClient;
use crate::connection::AcpConnection;
use crate::error::AcpError;

/// The pipe an ACP channel speaks the protocol over: Python's
/// `ACPTransport`.
///
/// [`StdioAcpTransport`](crate::StdioAcpTransport), the default, spawns the
/// agent as a local process and speaks over its stdio. Implement this trait
/// to reach an agent the channel cannot spawn itself, one running on
/// another machine or behind a relay that carries its stdio: open a byte
/// stream to it and hand it to
/// [`ClientSideConnection::new`](crate::ClientSideConnection::new), or
/// return a connection of your own.
///
/// A transport only opens the pipe, says whether it is still alive and
/// tears it down. Everything the protocol does, `initialize` and its
/// version, `authenticate`, the sessions, the prompts, the permissions,
/// stays in the channel, so that a new transport inherits all of it.
pub trait AcpTransport: Send + Sync + 'static {
    /// The connection [`open`](Self::open) returns.
    type Connection: AcpConnection;

    /// The transport's name, which the channel's `info` reports under
    /// `transport`.
    fn name(&self) -> &str;

    /// Whether the transport supplies validated usage provenance (the
    /// `roomkit.live/usage` envelope of a prompt's answer): false by
    /// default, peer metadata never claiming the transport's identities.
    fn provides_usage_metadata(&self) -> bool {
        false
    }

    /// A live connection to the agent, its callbacks going to `client`, the
    /// object the agent calls back into (its permission requests, its
    /// session updates). Failing is fine: the channel reports the failure
    /// and does not take the transport as connected.
    fn open(
        &self,
        client: Arc<dyn AcpClient>,
    ) -> impl Future<Output = Result<Self::Connection, AcpError>> + Send;

    /// Undoes what [`open`](Self::open) set up. Called at the channel's
    /// teardown and after a failed handshake, so it tolerates being called
    /// when `open` never completed; it never fails.
    fn close(&self) -> impl Future<Output = ()> + Send;

    /// Whether the connection of the last [`open`](Self::open) is still
    /// usable: true by default. The channel reconnects, dropping every
    /// session, when this turns false, so a transport answers only when it
    /// knows: a false `false` throws live sessions away, while a false
    /// `true` merely lets the next request fail on a dead pipe, which the
    /// channel reports.
    fn is_alive(&self) -> bool {
        true
    }
}
