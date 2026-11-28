//! The SDK's bounded stdio reader, retaining response JSON before typed decoding.
//!
//! roomkit-mcp negotiates the MCP revision on it first ([`Link`]): rmcp's
//! own negotiation request is then answered with what that negotiation
//! settled, never sent, and a late answer to a request of the negotiation
//! never reaches rmcp.

use std::collections::VecDeque;
use std::time::Duration;

use rmcp::model::{
    ClientJsonRpcMessage, ClientNotification, ClientRequest, ClientResult, JsonRpcMessage,
    ServerJsonRpcMessage,
};
use rmcp::service::{RoleClient, ServiceRole};
use rmcp::transport::{Transport, async_rw::AsyncRwTransport};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::negotiation::{Link, is_ours};
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

/// What the bounded stdio writer writes.
type Written = JsonRpcMessage<crate::wire::Request, ClientResult, ClientNotification>;

pub(crate) fn transport<R, W>(output: R, input: W, capture: Capture) -> Stdio<R, W>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    Stdio {
        inner: AsyncRwTransport::new(output, input).with_max_line_length(MESSAGE_LIMIT),
        capture,
        answered: VecDeque::new(),
    }
}

pub(crate) struct Stdio<R: AsyncRead, W: AsyncWrite> {
    inner: AsyncRwTransport<RawClient, R, W>,
    capture: Capture,
    /// The answers to rmcp's requests the transport gives itself.
    answered: VecDeque<ServerJsonRpcMessage>,
}

/// `message` as the bounded stdio writer writes it.
fn written(message: ClientJsonRpcMessage) -> Written {
    match message {
        ClientJsonRpcMessage::Request(request) => {
            JsonRpcMessage::request(crate::wire::Request(request.request), request.id)
        }
        ClientJsonRpcMessage::Response(response) => JsonRpcMessage::Response(response),
        ClientJsonRpcMessage::Notification(notification) => {
            JsonRpcMessage::Notification(notification)
        }
        ClientJsonRpcMessage::Error(error) => JsonRpcMessage::Error(error),
    }
}

/// Whether `message` answers a request of the negotiation.
fn answers_the_negotiation(message: &Value) -> bool {
    message.get("method").is_none()
        && message
            .get("id")
            .and_then(|id| serde_json::from_value(id.clone()).ok())
            .is_some_and(|id| is_ours(&id))
}

impl<R, W> Transport<RoleClient> for Stdio<R, W>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    type Error = std::io::Error;
    fn send(
        &mut self,
        mut message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let replayed = self
            .capture
            .replay(&message)
            .and_then(|(answer, _session)| serde_json::from_value(answer).ok());
        let sending = if let Some(answer) = replayed {
            self.answered.push_back(answer);
            None
        } else {
            self.capture.stamp(&mut message);
            self.capture.outgoing(&message);
            Some(self.inner.send(written(message)))
        };
        async move {
            match sending {
                Some(sending) => sending.await,
                None => Ok(()),
            }
        }
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        if let Some(answer) = self.answered.pop_front() {
            return Some(answer);
        }
        loop {
            let message = self.inner.receive().await?;
            let value = serde_json::to_value(message).ok()?;
            if answers_the_negotiation(&value) {
                // A late answer to a probe the negotiation gave up on.
                continue;
            }
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

impl<R, W> Stdio<R, W>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    /// The answer to the negotiation's request `id`, read off the wire; a
    /// server's `ping` answered while it waits on the handshake, as rmcp
    /// answers it there, and anything else the server sends left.
    async fn answer(&mut self, id: &Value, handshake: bool) -> Result<Value, String> {
        loop {
            let message = self
                .inner
                .receive()
                .await
                .ok_or("connection closed: negotiation response")?;
            let value = serde_json::to_value(message).map_err(|error| error.to_string())?;
            if value.get("method").is_none() && value.get("id") == Some(id) {
                return Ok(value);
            }
            if handshake && value.get("method").and_then(Value::as_str) == Some("ping") {
                let pong: ClientJsonRpcMessage = serde_json::from_value(
                    json!({"jsonrpc":"2.0","id":value.get("id"),"result":{}}),
                )
                .map_err(|error| error.to_string())?;
                self.inner
                    .send(written(pong))
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
    }
}

impl<R, W> Link for Stdio<R, W>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    async fn exchange(
        &mut self,
        mut request: ClientJsonRpcMessage,
        bound: Option<Duration>,
    ) -> Result<Option<Value>, String> {
        self.capture.stamp(&mut request);
        let ClientJsonRpcMessage::Request(sent) = &request else {
            return Err("the negotiation sends requests".to_owned());
        };
        let handshake = matches!(sent.request, ClientRequest::InitializeRequest(_));
        let id = serde_json::to_value(&sent.id).map_err(|error| error.to_string())?;
        self.inner
            .send(written(request))
            .await
            .map_err(|error| error.to_string())?;
        let answer = self.answer(&id, handshake);
        match bound {
            Some(bound) => match tokio::time::timeout(bound, answer).await {
                Ok(answer) => answer.map(Some),
                Err(_elapsed) => Ok(None),
            },
            None => answer.await.map(Some),
        }
    }
}
