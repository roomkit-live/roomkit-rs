//! Bounded HTTP I/O; the SDK owns MCP negotiation, sessions and cancellation.
use crate::{McpError, server::MESSAGE_LIMIT};
use reqwest::{
    Method, RequestBuilder, Response,
    header::{HeaderName, HeaderValue},
};
use rmcp::{
    model::{ClientJsonRpcMessage, ClientRequest, ErrorData, ServerJsonRpcMessage},
    transport::{
        common::client_side_sse::BoxedSseResponse,
        streamable_http_client::{
            StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

type Error = StreamableHttpError<HttpFailure>;
type Headers = HashMap<HeaderName, HeaderValue>;

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("{reason} (HTTP status {status:?})")]
pub(super) struct HttpFailure {
    status: Option<u16>,
    reason: &'static str,
}

#[derive(Clone, Default)]
pub(super) struct Failure(Arc<Mutex<Option<HttpFailure>>>);
impl Failure {
    pub(super) fn error(&self, fallback: &'static str) -> McpError {
        let detail = self.0.lock().ok().and_then(|v| *v);
        McpError::Http {
            status: detail.and_then(|e| e.status),
            reason: detail.map_or(fallback, |e| e.reason),
        }
    }
    pub(super) fn record(&self, status: Option<u16>, reason: &'static str) -> HttpFailure {
        let error = HttpFailure { status, reason };
        if let Ok(mut value) = self.0.lock() {
            *value = Some(error);
        }
        error
    }
}

#[derive(Clone)]
pub(super) struct Client {
    lease: Arc<Lease>,
    failure: Failure,
}
struct Lease {
    client: reqwest::Client,
    // Closing the last worker/stream drops the sender, joining all HTTP owners.
    _finished: tokio::sync::oneshot::Sender<()>,
}
impl Client {
    pub(super) fn new() -> Result<(Self, tokio::sync::oneshot::Receiver<()>, Failure), McpError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| McpError::Http {
                status: None,
                reason: "HTTP client setup failed",
            })?;
        let (done, finished) = tokio::sync::oneshot::channel();
        let failure = Failure::default();
        Ok((
            Self {
                lease: Arc::new(Lease {
                    client,
                    _finished: done,
                }),
                failure: failure.clone(),
            },
            finished,
            failure,
        ))
    }
    fn request(
        &self,
        method: Method,
        uri: &str,
        session: Option<&str>,
        auth: Option<String>,
        headers: Headers,
    ) -> RequestBuilder {
        let mut request = self
            .lease
            .client
            .request(method, uri)
            .header("accept", "application/json, text/event-stream");
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        request
    }
    async fn send(&self, request: RequestBuilder) -> Result<Response, Error> {
        // A tool may send its headers only when its result is ready. Startup
        // and per-call deadlines belong to the caller; DELETE has its own bound.
        request
            .send()
            .await
            .map_err(|_| self.error(None, "HTTP connection failed"))
    }

    fn error(&self, status: Option<u16>, reason: &'static str) -> Error {
        Error::Client(self.failure.record(status, reason))
    }
    fn check_status(&self, response: &Response) -> Result<(), Error> {
        if response.status().is_success() {
            return Ok(());
        }
        Err(self.error(Some(response.status().as_u16()), "HTTP request rejected"))
    }
    async fn body(&self, mut response: Response) -> Result<Vec<u8>, Error> {
        if response
            .content_length()
            .is_some_and(|n| n > MESSAGE_LIMIT as u64)
        {
            return Err(self.error(None, "HTTP response exceeds 4 MiB"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| self.error(None, "HTTP response interrupted"))?
        {
            if bytes.len().saturating_add(chunk.len()) > MESSAGE_LIMIT {
                return Err(self.error(None, "HTTP response exceeds 4 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    async fn post(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: Headers,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, Error> {
        let response = self
            .send(
                self.request(Method::POST, &uri, session.as_deref(), auth, headers)
                    .json(&message),
            )
            .await?;
        let status = response.status();
        if matches!(status.as_u16(), 202 | 204) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status.as_u16() == 404 && session.is_some() {
            self.failure
                .record(Some(404), "MCP session expired; reconnect explicitly");
            return Err(Error::SessionExpired);
        }
        let is_json = content_type(&response, "application/json");
        // Preserve protocol errors for version negotiation, but never retain an
        // authentication challenge or arbitrary HTTP failure body in diagnostics.
        if !status.is_success() {
            if matches!(status.as_u16(), 401 | 403 | 429) {
                self.check_status(&response)?;
            }
            let body = if is_json {
                self.body(response).await?
            } else {
                Vec::new()
            };
            let parsed = serde_json::from_slice::<ServerJsonRpcMessage>(&body).ok();
            if session.is_none()
                && status.is_client_error()
                && let ClientJsonRpcMessage::Request(request) = &message
                && matches!(request.request, ClientRequest::DiscoverRequest(_))
            {
                let error = match parsed {
                    Some(ServerJsonRpcMessage::Error(e)) => e.error,
                    _ => ErrorData::invalid_request("Discovery unsupported", None),
                };
                return Ok(StreamableHttpPostResponse::Json(
                    ServerJsonRpcMessage::error(error, Some(request.id.clone())),
                    None,
                ));
            }
            if let Some(reply @ ServerJsonRpcMessage::Error(_)) = parsed {
                return Ok(StreamableHttpPostResponse::Json(reply, None));
            }
            return Err(self.error(Some(status.as_u16()), "HTTP request rejected"));
        }
        let session = response
            .headers()
            .get("mcp-session-id")
            .map(|v| {
                v.to_str()
                    .ok()
                    .filter(|s| !s.is_empty() && s.len() <= 1024)
                    .map(str::to_owned)
                    .ok_or_else(|| self.error(None, "Invalid MCP session header"))
            })
            .transpose()?;
        let notification = !matches!(message, ClientJsonRpcMessage::Request(_));
        if notification && response.content_length() == Some(0) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if content_type(&response, "text/event-stream") {
            return Ok(StreamableHttpPostResponse::Sse(
                super::sse::stream(
                    response,
                    self.clone(),
                    self.failure.clone(),
                    limit.min(MESSAGE_LIMIT),
                ),
                session,
            ));
        }
        if !is_json {
            return Err(self.error(None, "Expected an MCP JSON or SSE response"));
        }
        let body = self.body(response).await?;
        match serde_json::from_slice(&body) {
            Ok(message) => Ok(StreamableHttpPostResponse::Json(message, session)),
            Err(_) if notification && body.is_empty() => Ok(StreamableHttpPostResponse::Accepted),
            Err(_) => Err(self.error(None, "Invalid MCP JSON response")),
        }
    }
}
fn content_type(response: &Response, expected: &str) -> bool {
    response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case(expected))
}
impl StreamableHttpClient for Client {
    type Error = HttpFailure;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: Headers,
    ) -> Result<StreamableHttpPostResponse, Error> {
        self.post(uri, message, session, auth, headers, MESSAGE_LIMIT)
            .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: Headers,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, Error> {
        self.post(uri, message, session, auth, headers, limit).await
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: Headers,
    ) -> Result<(), Error> {
        // A separate short deadline also bounds shutdown when the peer is gone.
        let response = self
            .request(Method::DELETE, &uri, Some(&session), auth, headers)
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .map_err(|_| {
                Error::Client(HttpFailure {
                    status: None,
                    reason: "MCP session cleanup failed",
                })
            })?;
        if response.status().is_success() || matches!(response.status().as_u16(), 404 | 405) {
            return Ok(());
        }
        Err(Error::Client(HttpFailure {
            status: Some(response.status().as_u16()),
            reason: "MCP session cleanup rejected",
        }))
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        auth: Option<String>,
        headers: Headers,
    ) -> Result<BoxedSseResponse, Error> {
        self.get_stream_with_max_sse_event_size(uri, session, last_id, auth, headers, MESSAGE_LIMIT)
            .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        auth: Option<String>,
        headers: Headers,
        limit: usize,
    ) -> Result<BoxedSseResponse, Error> {
        let mut request = self.request(Method::GET, &uri, session.as_deref(), auth, headers);
        if let Some(last_id) = last_id {
            request = request.header("last-event-id", last_id);
        }
        let response = self.send(request).await?;
        if response.status().as_u16() == 405 {
            return Err(Error::ServerDoesNotSupportSse);
        }
        self.check_status(&response)?;
        if !content_type(&response, "text/event-stream") {
            return Err(self.error(None, "Expected an MCP SSE response"));
        }
        Ok(super::sse::stream(
            response,
            self.clone(),
            self.failure.clone(),
            limit.min(MESSAGE_LIMIT),
        ))
    }
}
