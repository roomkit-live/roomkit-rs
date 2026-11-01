//! Optional Streamable HTTP composition (RFC §21.2).
mod client;
mod sse;

use crate::error::McpError;
use crate::listing;
use crate::server::{MESSAGE_LIMIT, McpServer};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use rmcp::{
    ClientCacheConfig, ServiceExt,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use std::{collections::HashMap, fmt, future::Future, time::Duration};

/// A remote MCP endpoint using Streamable HTTP (RFC §21.2).
/// Requests accept JSON or SSE replies. Redirects and automatic replay after
/// session expiry are disabled. HTTP and HTTPS are supported, including local
/// endpoints. The caller chooses its credential policy; this type never reads
/// environment variables. Debug and errors omit the URL and header values.
#[derive(Clone)]
pub(crate) struct McpHttp {
    url: Url,
    headers: HashMap<HeaderName, HeaderValue>,
    /// The bound on the handshake and the discovery: 60 seconds unless a
    /// test sets another, as a command's (RMK-1108).
    timeout: Option<Duration>,
}

/// How long an HTTP server's handshake and discovery may take, as a started
/// server's: Rust only until Python bounds it too (decided 2026-11-01;
/// RMK-1108).
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
    /// Configures an endpoint without connecting, its connection bounded at
    /// 60 seconds, as a command's.
    ///
    /// # Errors
    /// Refuses non-HTTP(S) URLs, user information, fragments and missing hosts.
    pub(crate) fn new(url: &str) -> Result<Self, McpError> {
        let url = Url::parse(url).map_err(|_| invalid("invalid endpoint URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid(
                "use an HTTP(S) endpoint without URL credentials or a fragment",
            ));
        }
        Ok(Self {
            url,
            headers: HashMap::new(),
            timeout: Some(CONNECT_TIMEOUT),
        })
    }
    /// Adds a header to every request, including session cleanup. For bearer
    /// authentication supply `authorization` and the complete `Bearer ...` value.
    ///
    /// # Errors
    /// Refuses invalid names/values, more than 32 headers, values over 8192 bytes,
    /// and headers owned by HTTP framing or MCP negotiation.
    pub(crate) fn header(mut self, name: &str, value: &str) -> Result<Self, McpError> {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid("invalid header name"))?;
        if name.as_str().starts_with("mcp-")
            || matches!(
                name.as_str(),
                "accept"
                    | "content-type"
                    | "content-length"
                    | "host"
                    | "connection"
                    | "transfer-encoding"
                    | "last-event-id"
                    | "upgrade"
                    | "te"
                    | "trailer"
                    | "proxy-authorization"
            )
        {
            return Err(invalid("header is reserved for the transport"));
        }
        if value.len() > 8192 {
            return Err(invalid("header value exceeds 8192 bytes"));
        }
        let mut value =
            HeaderValue::from_str(value).map_err(|_| invalid("invalid header value"))?;
        value.set_sensitive(true);
        self.headers.insert(name, value);
        if self.headers.len() > 32 {
            return Err(invalid("more than 32 custom headers"));
        }
        Ok(self)
    }
    /// The same endpoint, its handshake and catalogue discovery bounded by
    /// `timeout`.
    #[must_use]
    pub(crate) const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    /// Connects and discovers tools once (RFC §21.2), until cancelled.
    /// Failure or cancellation drains the HTTP worker and attempts to delete
    /// any negotiated session before returning.
    ///
    /// # Errors
    /// Returns a safe transport failure, a timeout, invalid configuration,
    /// or [`McpError::Cancelled`].
    pub(crate) async fn connect_until(
        self,
        cancelled: impl Future<Output = ()> + Send,
    ) -> Result<McpServer, McpError> {
        let (client, finished, failure) = client::Client::new()?;
        let config = StreamableHttpClientTransportConfig::with_uri(self.url.as_str())
            .custom_headers(self.headers)
            .max_sse_event_size(MESSAGE_LIMIT)
            .reinit_on_expired_session(false)
            .control_request_timeout(Duration::from_secs(3));
        let transport = StreamableHttpClientTransport::with_client(client, config);
        let stop = transport.cancel_token();
        let connecting = async {
            let service =
                ().serve(transport)
                    .await
                    .map_err(|_| failure.error("MCP handshake failed"))?;
            service
                .peer()
                .set_response_cache_config(ClientCacheConfig::disabled())
                .await;
            if let Ok(tools) = listing::tools(service.peer(), "http").await {
                Ok((service, tools))
            } else {
                let error = failure.error("MCP tool discovery failed");
                let _closed = service.cancel().await;
                Err(error)
            }
        };
        let connected = tokio::select! {
            biased;
            () = cancelled => Err(McpError::Cancelled),
            result = bounded(self.timeout, connecting) => result,
        };
        match connected {
            Ok((service, tools)) => Ok(McpServer::from_http(service, tools, finished)),
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
        let endpoint = McpHttp::new("http://localhost/mcp").unwrap();
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
