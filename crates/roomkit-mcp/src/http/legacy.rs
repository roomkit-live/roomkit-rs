//! Legacy SSE has one GET reader and a separately owned sequential POST writer.

mod endpoint;
mod writer;

use std::sync::Arc;

use futures_util::StreamExt;
use reqwest::{Url, header::HeaderMap};
use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{Transport, common::client_side_sse::BoxedSseResponse},
};
use tokio::sync::Notify;

use super::client::{Client, Failure, HttpFailure};
use crate::server::MESSAGE_LIMIT;
use writer::Writer;

pub(super) async fn connect(
    url: String,
    headers: HeaderMap,
    timeout: Option<std::time::Duration>,
    cancelled: impl Future<Output = ()> + Send,
    filter: Option<crate::listing::ToolFilter>,
    discover: bool,
) -> Result<crate::server::McpServer, crate::McpError> {
    let capture = crate::raw::Capture::default();
    let (client, finished, failure) = Client::new(capture.clone())?;
    let interrupted = Arc::new(Notify::new());
    let writer_stop = Arc::clone(&interrupted);
    let connected = {
        let connecting = async move {
            let transport = Legacy::open(&url, headers, client, failure.clone(), writer_stop)
                .await
                .map_err(|_| failure.error("MCP legacy transport failed"))?;
            super::session::open(transport, &failure, filter.as_ref(), discover).await
        };
        tokio::pin!(connecting);
        let deadline = async {
            if let Some(timeout) = timeout {
                tokio::time::sleep(timeout).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            biased;
            () = cancelled => {
                interrupted.notify_one();
                Err(crate::McpError::Cancelled)
            },
            () = deadline => {
                // Stop the writer before dropping discovery's request handle:
                // the SDK may enqueue a cancellation notification on its drop.
                interrupted.notify_one();
                Err(crate::McpError::Http { status: None, reason: "connection timed out" })
            },
            result = &mut connecting => result,
        }
    };
    match connected {
        Ok((service, tools)) => Ok(crate::server::McpServer::from_http(
            service, tools, capture, finished,
        )),
        Err(error) => {
            let _finished = finished.await;
            Err(error)
        }
    }
}

pub(super) struct Legacy {
    stream: Option<BoxedSseResponse>,
    writer: Writer,
    failure: Failure,
}

impl Legacy {
    pub(super) async fn open(
        url: &str,
        headers: HeaderMap,
        client: Client,
        failure: Failure,
        interrupted: Arc<Notify>,
    ) -> Result<Self, HttpFailure> {
        let parsed = Url::parse(url).map_err(|_| failure.record(None, "invalid MCP endpoint"))?;
        let response = client.legacy_get(parsed, headers.clone()).await?;
        let mut stream =
            super::sse::stream(response, client.clone(), failure.clone(), MESSAGE_LIMIT);
        let target = announced(url, &mut stream, &failure).await?;
        Ok(Self {
            stream: Some(stream),
            writer: Writer::new(client, target, headers, failure.clone(), interrupted),
            failure,
        })
    }
}

async fn announced(
    url: &str,
    stream: &mut BoxedSseResponse,
    failure: &Failure,
) -> Result<Url, HttpFailure> {
    while let Some(event) = stream.next().await {
        let event = event.map_err(|_| failure.record(None, "MCP event stream failed"))?;
        if event.event.as_deref() != Some("endpoint") {
            continue;
        }
        if let Some(target) = endpoint::resolve(url, event.data.as_deref().unwrap_or_default()) {
            return Ok(target);
        }
        failure.record(None, "MCP endpoint announcement changes origin");
        // Python's reader cannot hand its streams over after this refusal.
        // Keep that pending opening under the caller's connection deadline.
        return std::future::pending().await;
    }
    Err(failure.record(None, "MCP event stream closed before its endpoint"))
}

impl Transport<RoleClient> for Legacy {
    type Error = HttpFailure;

    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), HttpFailure>> + Send + 'static {
        self.writer.send(message)
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        let stream = self.stream.as_mut()?;
        while let Some(event) = stream.next().await {
            let Ok(event) = event else {
                self.failure.record(None, "MCP event stream failed");
                return None;
            };
            if event.event.as_deref().unwrap_or("message") != "message" {
                continue;
            }
            let Some(data) = event.data.filter(|data| !data.is_empty()) else {
                continue;
            };
            match serde_json::from_str(&data) {
                Ok(message) => return Some(message),
                Err(_) => {
                    self.failure.record(None, "invalid MCP event message");
                }
            }
        }
        None
    }

    async fn close(&mut self) -> Result<(), HttpFailure> {
        self.stream.take();
        self.writer.close().await;
        Ok(())
    }
}
