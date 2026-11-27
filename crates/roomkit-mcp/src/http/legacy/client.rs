//! Legacy SSE owns bounded socket operations; Streamable HTTP keeps reqwest.

mod body;
mod connection;
mod io;
mod request;
pub(in crate::http) mod response;

use connection::{Connection, Finished};
use response::Response;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, oneshot};

use reqwest::{
    Method, Url,
    header::{HeaderMap, HeaderValue},
};
use serde_json::Value;

use crate::http::client::{Failure, HttpFailure};
use crate::server::MESSAGE_LIMIT;

const REDIRECT_LIMIT: usize = 20;

#[derive(Clone)]
pub(in crate::http) struct Client {
    lease: Arc<Lease>,
    failure: Failure,
    pub(in crate::http) capture: crate::raw::Capture,
}

struct Lease {
    idle: Mutex<Vec<Connection>>,
    tls: Arc<rustls::ClientConfig>,
    // Build requests with the established URL/auth/header encoding. This
    // client never sends: all legacy socket I/O goes through Connection.
    encoder: reqwest::Client,
    finished: Finished,
}

impl Client {
    pub(super) fn new(
        capture: crate::raw::Capture,
    ) -> Result<(Self, oneshot::Receiver<()>, Failure), crate::McpError> {
        let tls = crate::http::client::tls_config().ok_or(crate::McpError::Http {
            status: None,
            reason: "HTTP client setup failed",
        })?;
        let encoder = reqwest::Client::builder()
            .tls_backend_preconfigured(tls.clone())
            .no_gzip()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| crate::McpError::Http {
                status: None,
                reason: "HTTP client setup failed",
            })?;
        let (done, finished) = oneshot::channel();
        let failure = Failure::default();
        Ok((
            Self {
                lease: Arc::new(Lease {
                    idle: Mutex::new(Vec::new()),
                    tls: Arc::new(tls),
                    encoder,
                    finished: Arc::new(done),
                }),
                failure: failure.clone(),
                capture,
            },
            finished,
            failure,
        ))
    }

    async fn checkout(&self, url: &Url) -> Result<Connection, HttpFailure> {
        let origin = url.origin().ascii_serialization();
        let mut idle = self.lease.idle.lock().await;
        idle.retain(|connection| {
            !connection.sender.is_closed()
                && connection.idle_since.elapsed() < Duration::from_secs(5)
        });
        let found = idle
            .iter()
            .position(|connection| connection.origin == origin)
            .map(|index| idle.swap_remove(index));
        drop(idle);
        if let Some(mut connection) = found
            && connection.sender.ready().await.is_ok()
        {
            return Ok(connection);
        }
        Connection::open(
            url,
            Arc::clone(&self.lease.tls),
            Arc::clone(&self.lease.finished),
            &self.failure,
        )
        .await
    }

    async fn request(
        &self,
        method: Method,
        url: &Url,
        headers: HeaderMap,
        message: Option<&Value>,
    ) -> Result<Response, HttpFailure> {
        let fail = || self.failure.record(None, "HTTP connection failed");
        let mut connection = self.checkout(url).await?;
        let request = request::build(
            &self.lease.encoder,
            method,
            url,
            headers,
            message,
            Arc::clone(&connection.operations),
            &self.failure,
        )?;
        let response = connection
            .sender
            .send_request(request)
            .await
            .map_err(|_| fail())?;
        Ok(Response {
            incoming: response,
            connection: Some(connection),
            client: self.clone(),
        })
    }

    pub(in crate::http) async fn legacy_get(
        &self,
        url: Url,
        mut headers: HeaderMap,
    ) -> Result<Response, HttpFailure> {
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        headers.insert("cache-control", HeaderValue::from_static("no-store"));
        let response = self
            .legacy_redirects(Method::GET, url, headers, None)
            .await?;
        self.legacy_status(&response)?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        // A media type, as Python's SDK reads it: the part before its
        // parameters, trimmed, compared without case.
        let media_type = content_type.split(';').next().unwrap_or_default().trim();
        if !media_type.eq_ignore_ascii_case("text/event-stream") {
            return Err(self.failure.record(None, "expected an SSE response"));
        }
        Ok(response)
    }

    pub(in crate::http) async fn legacy_post(
        &self,
        url: Url,
        headers: HeaderMap,
        message: Value,
    ) -> Result<(), HttpFailure> {
        let response = self
            .legacy_redirects(Method::POST, url, headers, Some(&message))
            .await?;
        self.legacy_status(&response)?;
        self.legacy_drain(response).await
    }

    async fn legacy_redirects(
        &self,
        method: Method,
        mut url: Url,
        headers: HeaderMap,
        message: Option<&Value>,
    ) -> Result<Response, HttpFailure> {
        let mut followed = 0;
        loop {
            let response = self
                .request(method.clone(), &url, headers.clone(), message)
                .await?;
            let Some(next) = redirect(&method, &url, &response) else {
                return Ok(response);
            };
            if followed == REDIRECT_LIMIT {
                return Ok(response);
            }
            self.legacy_drain(response).await?;
            url = next;
            followed += 1;
        }
    }

    fn legacy_status(&self, response: &Response) -> Result<(), HttpFailure> {
        if response.status().is_success() {
            return Ok(());
        }
        Err(self
            .failure
            .record(Some(response.status().as_u16()), "HTTP request rejected"))
    }

    async fn legacy_drain(&self, mut response: Response) -> Result<(), HttpFailure> {
        if response
            .content_length()
            .is_some_and(|n| n > MESSAGE_LIMIT as u64)
        {
            return Err(self.failure.record(None, "HTTP response exceeds 4 MiB"));
        }
        let mut size = 0usize;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| self.failure.record(None, "HTTP response interrupted"))?
        {
            size = size.saturating_add(chunk.len());
            if size > MESSAGE_LIMIT {
                return Err(self.failure.record(None, "HTTP response exceeds 4 MiB"));
            }
        }
        Ok(())
    }
}

fn redirect(method: &Method, sent: &Url, response: &Response) -> Option<Url> {
    let allowed = if method == Method::GET {
        matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308)
    } else {
        matches!(response.status().as_u16(), 307 | 308)
    };
    if !allowed {
        return None;
    }
    let location = response.headers().get("location")?.to_str().ok()?;
    let next = sent.join(location).ok()?;
    if (!next.username().is_empty() || next.password().is_some())
        && (next.username(), next.password()) != (sent.username(), sent.password())
    {
        return None;
    }
    same_origin(sent, &next).then_some(next)
}

fn same_origin(sent: &Url, next: &Url) -> bool {
    sent.origin() == next.origin()
        || (sent.host_str() == next.host_str()
            && sent.scheme() == "http"
            && next.scheme() == "https"
            && sent.port().is_none()
            && next.port().is_none())
}
