//! The SDK's bounded stdio reader, retaining response JSON before typed decoding.

use rmcp::model::{ClientJsonRpcMessage, JsonRpcMessage, ServerJsonRpcMessage};
use rmcp::service::{RoleClient, ServiceRole};
use rmcp::transport::{Transport, async_rw::AsyncRwTransport};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::raw::Capture;
use crate::server::MESSAGE_LIMIT;

#[derive(Debug, Clone, Copy)]
struct RawClient;

impl ServiceRole for RawClient {
    type Req = crate::wire::Request;
    type Resp = <RoleClient as ServiceRole>::Resp;
    type Not = <RoleClient as ServiceRole>::Not;
    type PeerReq = <RoleClient as ServiceRole>::PeerReq;
    type PeerResp = Value;
    type PeerNot = <RoleClient as ServiceRole>::PeerNot;
    type InitializeError = <RoleClient as ServiceRole>::InitializeError;
    type Info = <RoleClient as ServiceRole>::Info;
    type PeerInfo = <RoleClient as ServiceRole>::PeerInfo;
    const IS_CLIENT: bool = true;
}

pub(crate) fn transport<R, W>(
    output: R,
    input: W,
    capture: Capture,
) -> impl Transport<RoleClient, Error = std::io::Error>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    Stdio {
        inner: AsyncRwTransport::new(output, input).with_max_line_length(MESSAGE_LIMIT),
        capture,
    }
}

struct Stdio<R: AsyncRead, W: AsyncWrite> {
    inner: AsyncRwTransport<RawClient, R, W>,
    capture: Capture,
}

impl<R, W> Transport<RoleClient> for Stdio<R, W>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    type Error = std::io::Error;
    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.capture.outgoing(&message);
        let message = match message {
            ClientJsonRpcMessage::Request(request) => {
                JsonRpcMessage::request(crate::wire::Request(request.request), request.id)
            }
            ClientJsonRpcMessage::Response(response) => JsonRpcMessage::Response(response),
            ClientJsonRpcMessage::Notification(notification) => {
                JsonRpcMessage::Notification(notification)
            }
            ClientJsonRpcMessage::Error(error) => JsonRpcMessage::Error(error),
        };
        self.inner.send(message)
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        loop {
            let message = self.inner.receive().await?;
            let value = serde_json::to_value(message).ok()?;
            if let Ok(message) = serde_json::from_value(self.capture.incoming(value)) {
                return Some(message);
            }
            // As with the SDK reader, an invalid response remains unanswered
            // and its caller's deadline bounds it.
        }
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}
