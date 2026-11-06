//! What a client asks an ACP agent: the connection a transport opens.

use std::future::Future;

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse,
};

use crate::error::AcpError;

/// A live ACP connection to an agent, as the client asks it: the methods of
/// the Python SDK's `ClientSideConnection` the channel calls (HITL
/// 3592a76c), each taking and returning the protocol's own types.
///
/// [`ClientSideConnection`](crate::ClientSideConnection) is the connection
/// over a byte stream, the one [`StdioAcpTransport`](crate::StdioAcpTransport)
/// opens. Implement this trait to stand between the channel and an agent:
/// a relay that wraps a [`ClientSideConnection`] to refuse a prompt, or a
/// test's scripted agent with no wire at all, which calls back into the
/// [`AcpClient`](crate::AcpClient) its transport was opened with.
///
/// [`ClientSideConnection`]: crate::ClientSideConnection
pub trait AcpConnection: Send + Sync + 'static {
    /// `initialize`: negotiates the protocol's version and both sides'
    /// capabilities.
    fn initialize(
        &self,
        request: InitializeRequest,
    ) -> impl Future<Output = Result<InitializeResponse, AcpError>> + Send;

    /// `authenticate`, with one of the methods the agent's `initialize`
    /// answer offers.
    fn authenticate(
        &self,
        request: AuthenticateRequest,
    ) -> impl Future<Output = Result<AuthenticateResponse, AcpError>> + Send;

    /// `session/new`: opens a session, whose id the answer gives.
    fn new_session(
        &self,
        request: NewSessionRequest,
    ) -> impl Future<Output = Result<NewSessionResponse, AcpError>> + Send;

    /// `session/prompt`: one prompt turn. It returns once the agent answers
    /// and every session update the agent sent before its answer was taken
    /// by the client, the Python SDK's order.
    fn prompt(
        &self,
        request: PromptRequest,
    ) -> impl Future<Output = Result<PromptResponse, AcpError>> + Send;

    /// `session/cancel`, a notification: the agent stops the session's
    /// prompt turn and answers it `cancelled`.
    fn cancel(
        &self,
        notification: CancelNotification,
    ) -> impl Future<Output = Result<(), AcpError>> + Send;

    /// `session/close`, which only an agent that announces it in its
    /// `initialize` answer takes.
    fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> impl Future<Output = Result<CloseSessionResponse, AcpError>> + Send;

    /// `session/set_config_option`: sets one of the session's options; the
    /// answer holds them all.
    fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
    ) -> impl Future<Output = Result<SetSessionConfigOptionResponse, AcpError>> + Send;
}
