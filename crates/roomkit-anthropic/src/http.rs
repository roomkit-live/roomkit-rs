//! Bounded HTTP requests; retry only before the response stream starts.
use crate::{AnthropicAiProvider, error, retry};
use roomkit_ai::ProviderError;
use serde_json::Value;

const BODY_LIMIT: usize = 4 * 1024 * 1024;

impl AnthropicAiProvider {
    pub(crate) async fn send(
        &self,
        body: Option<&Value>,
        key: &str,
    ) -> Result<reqwest::Response, ProviderError> {
        for tried in 0..=self.config.max_retries {
            let response = self.request(body, key)?.send().await;
            match response {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let retry = tried < self.config.max_retries
                        && retry::should_retry(response.status(), response.headers());
                    let delay = retry::delay(tried, Some(response.headers()));
                    let status = response.status().as_u16();
                    if retry {
                        drop(response);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    let bytes = read_body(response).await?;
                    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                    return Err(error::response(&body, Some(status), &self.secrets(key)));
                }
                Err(cause) => {
                    let failure = error::transport(cause);
                    if tried == self.config.max_retries || !failure.retryable {
                        return Err(failure);
                    }
                    tokio::time::sleep(retry::delay(tried, None)).await;
                }
            }
        }
        Err(error::failed("the request exhausted its retries"))
    }

    pub(crate) fn secrets<'a>(&'a self, key: &'a str) -> Vec<&'a str> {
        let mut values = vec![key, self.config.api_key.as_str()];
        if let Some(headers) = &self.config.extra_headers {
            values.extend(headers.values().map(String::as_str));
        }
        values
    }

    pub(crate) fn request(
        &self,
        body: Option<&Value>,
        key: &str,
    ) -> Result<reqwest::RequestBuilder, ProviderError> {
        let request = match body {
            Some(body) => self.client.post(self.messages.clone()).json(body),
            None => self.client.get(self.models.clone()),
        };
        let mut key = reqwest::header::HeaderValue::from_str(key)
            .map_err(|_| error::failed("the API key is not a valid HTTP header"))?;
        key.set_sensitive(true);
        let mut defaults = reqwest::header::HeaderMap::new();
        defaults.insert("x-api-key", key);
        defaults.insert(
            "anthropic-version",
            reqwest::header::HeaderValue::from_static("2023-06-01"),
        );
        defaults.insert(
            "accept-encoding",
            reqwest::header::HeaderValue::from_static("identity"),
        );
        if let Some(headers) = &self.config.extra_headers {
            for (name, value) in headers {
                let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| error::failed("an extra header name is invalid"))?;
                let mut value = reqwest::header::HeaderValue::from_str(value)
                    .map_err(|_| error::failed("an extra header value is invalid"))?;
                value.set_sensitive(true);
                defaults.insert(name, value);
            }
        }
        Ok(request.headers(defaults))
    }
}

pub(crate) async fn read_body(mut response: reqwest::Response) -> Result<Vec<u8>, ProviderError> {
    let too_long = || error::failed("the response exceeds the 4 MiB body limit");
    if response
        .content_length()
        .is_some_and(|size| size > BODY_LIMIT as u64)
    {
        return Err(too_long());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(error::transport)? {
        if bytes.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(too_long());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
