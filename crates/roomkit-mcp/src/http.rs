//! Optional Streamable HTTP and legacy SSE composition (RFC §21.2).
mod client;
pub(crate) mod legacy;
mod session;
mod sse;

use crate::error::McpError;
use crate::listing::ToolFilter;
use crate::raw::Capture;
use crate::server::{MESSAGE_LIMIT, McpServer};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use std::{fmt, future::Future, time::Duration};

/// A remote MCP endpoint using Streamable HTTP or legacy SSE (RFC §21.2).
/// Streamable requests accept JSON or SSE replies, without redirects or replay
/// after expiry. Legacy SSE follows method-preserving redirects within its
/// origin and uses an announced POST address. HTTP(S) is supported, including local
/// endpoints. The caller chooses its credential policy; this type never reads
/// environment variables. Debug and errors omit the URL and header values.
#[derive(Clone)]
pub(crate) struct McpHttp {
    url: String,
    headers: HeaderMap,
    legacy: bool,
    /// The bound on the handshake and the discovery: 60 seconds unless
    /// `FromUrlOptions::connect_timeout` says another, as a command's.
    timeout: Option<Duration>,
}

/// How long an HTTP server's handshake and discovery may take by default, as
/// a started server's: Python's `from_url(connect_timeout=60.0)` (RMK-1108).
#[cfg(test)]
const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// `connecting`, within `bound` when there is one.
async fn bounded<T>(
    bound: Option<Duration>,
    connecting: impl Future<Output = Result<T, McpError>>,
) -> Result<T, McpError> {
    let Some(bound) = bound else {
        return connecting.await;
    };
    tokio::time::timeout(bound, connecting)
        .await
        .unwrap_or(Err(McpError::Http {
            status: None,
            reason: "connection timed out",
        }))
}

impl McpHttp {
    /// Takes a fresh retained-header snapshot at connection entry. RoomKit's
    /// policy was validated at construction; only HTTP encoding is checked here.
    pub(crate) fn configured(
        url: &str,
        headers: Vec<(String, String)>,
        timeout: Duration,
        legacy: bool,
    ) -> Result<Self, McpError> {
        let mut encoded = HeaderMap::new();
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| invalid("invalid header name"))?;
            if !value.is_ascii() {
                return Err(invalid("invalid header value"));
            }
            let mut value =
                HeaderValue::from_str(&value).map_err(|_| invalid("invalid header value"))?;
            value.set_sensitive(true);
            encoded.append(name, value);
        }
        Ok(Self {
            url: url.to_owned(),
            headers: encoded,
            legacy,
            timeout: Some(timeout),
        })
    }

    /// Connects and discovers tools once (RFC §21.2), until cancelled.
    /// Failure or cancellation drains HTTP workers and attempts to delete any
    /// negotiated Streamable HTTP session before returning.
    ///
    /// # Errors
    /// Returns a safe transport failure, a timeout, invalid configuration,
    /// or [`McpError::Cancelled`].
    pub(crate) async fn connect_until(
        self,
        cancelled: impl Future<Output = ()> + Send,
        filter: Option<ToolFilter>,
        discover: bool,
    ) -> Result<McpServer, McpError> {
        if self.legacy {
            return legacy::connect(
                self.url,
                self.headers,
                self.timeout,
                cancelled,
                filter,
                discover,
            )
            .await;
        }
        let capture = Capture::default();
        let (client, finished, failure) = client::Client::new(capture.clone())?;
        let client = client.with_headers(self.headers);
        let config = StreamableHttpClientTransportConfig::with_uri(self.url.as_str())
            .max_sse_event_size(MESSAGE_LIMIT)
            .reinit_on_expired_session(false)
            .control_request_timeout(Duration::from_secs(3));
        let transport = StreamableHttpClientTransport::with_client(client, config);
        let stop = transport.cancel_token();
        let connecting = session::open(transport, &failure, filter.as_ref(), discover);
        let connected = tokio::select! {
            biased;
            () = cancelled => Err(McpError::Cancelled),
            result = bounded(self.timeout, connecting) => result,
        };
        match connected {
            Ok((service, tools)) => Ok(McpServer::from_http(
                service, tools, capture, finished, None,
            )),
            Err(error) => {
                stop.cancel();
                let _finished = finished.await;
                Err(error)
            }
        }
    }
}
fn invalid(reason: &'static str) -> McpError {
    McpError::Configuration { reason }
}
impl fmt::Debug for McpHttp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpHttp")
            .field("headers", &self.headers.len())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An HTTP endpoint's connection is bounded at 60 seconds, as a started
    /// server's (RMK-1108): a slow one within the bound connects, one past
    /// it times out. `discovery_deadline_drains_the_connection` (tests/http.rs)
    /// proves the session's cleanup when a bound ends it.
    #[tokio::test(start_paused = true)]
    async fn an_http_connection_is_bounded_at_sixty_seconds() {
        let taking = |seconds| async move {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            Ok::<_, McpError>("connected")
        };
        let endpoint =
            McpHttp::configured("http://localhost/mcp", Vec::new(), CONNECT_TIMEOUT, false)
                .unwrap();
        let within = bounded(endpoint.timeout, taking(59)).await;
        let past = bounded(endpoint.timeout, taking(61)).await;
        assert!(
            matches!(
                (within, past),
                (
                    Ok("connected"),
                    Err(McpError::Http {
                        reason: "connection timed out",
                        ..
                    })
                )
            ),
            "the bound"
        );
    }
}
