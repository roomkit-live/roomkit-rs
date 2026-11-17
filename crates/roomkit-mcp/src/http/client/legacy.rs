//! Legacy SSE uses the same HTTP pool and TLS, with explicit redirect rules.

use reqwest::{
    Method, Response, Url,
    header::{HeaderMap, HeaderValue},
};
use serde_json::Value;

use super::{Client, HttpFailure};
use crate::server::MESSAGE_LIMIT;

const REDIRECT_LIMIT: usize = 20;

impl Client {
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
        if content_type.split(';').next().map(str::trim) != Some("text/event-stream") {
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
            let mut request = self
                .lease
                .client
                .request(method.clone(), url.clone())
                .headers(headers.clone());
            if let Some(message) = message {
                request = request.json(message);
            }
            let response = request
                .send()
                .await
                .map_err(|_| self.failure.record(None, "HTTP connection failed"))?;
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
