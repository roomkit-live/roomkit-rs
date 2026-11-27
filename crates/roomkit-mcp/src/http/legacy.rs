//! Legacy SSE has one GET reader and a separately owned sequential POST writer.

pub(super) mod client;
mod endpoint;
pub(crate) mod request;
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

use super::client::{Failure, HttpFailure};
use client::Client;
use writer::Writer;

/// The largest SSE event the GET stream may carry, in bytes, as Python's SDK
/// reads it, a bound its legacy client does not let Python set: a larger
/// event ends the connection, its one stream carrying every answer.
/// Streamable HTTP reads events up to `MESSAGE_LIMIT`.
const EVENT_LIMIT: usize = 1024 * 1024;

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
    let (cancellations, given_up) = request::cancellations();
    let connected = {
        let connecting = async move {
            let writer = WriterParts {
                interrupted: writer_stop,
                given_up,
            };
            let transport = Legacy::open(&url, headers, client, failure.clone(), writer)
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
            service,
            tools,
            capture,
            finished,
            Some(cancellations),
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

/// What the writer is stopped by and what it takes its callers'
/// cancellations from.
pub(super) struct WriterParts {
    interrupted: Arc<Notify>,
    given_up: tokio::sync::mpsc::UnboundedReceiver<request::Cancellation>,
}

impl Legacy {
    pub(super) async fn open(
        url: &str,
        headers: HeaderMap,
        client: Client,
        failure: Failure,
        writer: WriterParts,
    ) -> Result<Self, HttpFailure> {
        let parsed = Url::parse(url).map_err(|_| failure.record(None, "invalid MCP endpoint"))?;
        let response = client.legacy_get(parsed, headers.clone()).await?;
        let mut stream =
            super::sse::legacy_stream(response, client.clone(), failure.clone(), EVENT_LIMIT);
        let target = announced(url, &mut stream, &failure).await?;
        Ok(Self {
            stream: Some(stream),
            writer: Writer::new(
                client,
                target,
                headers,
                failure.clone(),
                writer.interrupted,
                writer.given_up,
            ),
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
        let event = match event {
            Ok(event) => event,
            Err(sse_stream::Error::Body(cause))
                if cause
                    .downcast_ref::<HttpFailure>()
                    .is_some_and(|error| error.is_response_read()) =>
            {
                failure.record(None, "MCP event stream failed");
                // Before announcing the endpoint, Python's reader waits to
                // hand its IO exception to an unconnected zero-buffer stream.
                // The GET closes; the caller's opening bound still applies.
                return std::future::pending().await;
            }
            Err(_) => return Err(failure.record(None, "MCP event stream failed")),
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_read_failures_wait_for_the_opening_bound() {
        for reason in [
            "HTTP response interrupted",
            "MCP SSE event exceeds its size limit",
        ] {
            let failure = Failure::default();
            let error = sse_stream::Error::Body(Box::new(failure.record(None, reason)));
            let mut stream = futures_util::stream::iter([Err(error)]).boxed();
            let result = announced("http://example.test/mcp", &mut stream, &failure);
            tokio::pin!(result);
            assert_eq!(
                futures_util::poll!(&mut result).is_pending(),
                reason == "HTTP response interrupted"
            );
        }
        let failure = Failure::default();
        let mut stream = futures_util::stream::iter([Err(sse_stream::Error::InvalidLine)]).boxed();
        assert!(
            announced("http://example.test/mcp", &mut stream, &failure)
                .await
                .is_err()
        );
    }
}
