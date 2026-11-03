//! The provider: Python's `OpenAIAIProvider`, an [`AiProvider`] over an
//! OpenAI-compatible Chat Completions or Responses endpoint.

use std::fmt;
use std::sync::Arc;

use roomkit_ai::{AiContext, AiProvider, AiResponse, ProviderError, ProviderStream, TextStream};

use crate::config::{OpenAiApi, OpenAiConfig};
use crate::error::OpenAiError;
use crate::request::Request;
use crate::stream;
use crate::wire::{self, CompletionRequest};

/// The longest answer a generation reads, in bytes: far more than a
/// completion's text and calls take.
pub(crate) const ANSWER_LIMIT: usize = 4 * 1024 * 1024;

/// A model behind an OpenAI-compatible endpoint (RFC §6.7): Python's
/// `OpenAIAIProvider(config)`. A generation is one streamed completion,
/// read as it arrives; [`generate`](AiProvider::generate) asks Chat
/// Completions for one completion whole, as Python's does, and reads a
/// Responses stream to its end.
/// Each request declares the turn's tools, their names checked against
/// OpenAI's rule when the endpoint is OpenAI's own, and fails before
/// anything is sent for one it refuses.
///
/// Its `Debug` never prints the API key, nor the base URL's credentials.
#[derive(Clone)]
pub struct OpenAiAiProvider {
    config: Arc<OpenAiConfig>,
    endpoint: reqwest::Url,
    client: reqwest::Client,
}

impl OpenAiAiProvider {
    /// The provider `config` configures.
    ///
    /// # Errors
    ///
    /// Fails, a [`ProviderError`] not retryable, for a base URL that is no
    /// HTTP URL or carries a query or a fragment, and for an HTTP client
    /// that cannot be built.
    pub fn new(config: OpenAiConfig) -> Result<Self, ProviderError> {
        let endpoint = endpoint(config.base_url(), config.api)?;
        // An API endpoint has no use for a redirect, which could carry the
        // key to another scheme of the same host. The timeouts are Python's
        // httpx `Timeout(timeout, connect=connect_timeout)`: between two
        // reads, never the whole streamed answer.
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(tls_config()?)
            // Answers come uncompressed, as the bounds on their declared
            // length expect, whatever reqwest features the host enables.
            .no_gzip()
            .read_timeout(config.timeout)
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(OpenAiError::client)?;
        Ok(Self {
            config: Arc::new(config),
            endpoint,
            client,
        })
    }

    pub(crate) fn api(&self) -> OpenAiApi {
        self.config.api
    }

    pub(crate) fn namespace(&self) -> Option<&str> {
        self.config
            .tool_namespace
            .as_ref()
            .map(|(name, _)| name.as_str())
    }

    /// Sends `request`: its body as the configured API writes it, the
    /// bearer token the configuration gives, its headers; `streamed` false
    /// asks Chat Completions for the completion whole.
    pub(crate) async fn ask(
        &self,
        request: &Request,
        streamed: bool,
    ) -> Result<(reqwest::Response, String), OpenAiError> {
        crate::names::check(self.config.base_url(), &request.tools)?;
        let secret = match &self.config.token_source {
            Some(source) => source.token().await?,
            None => self.config.api_key.clone(),
        };
        let body = match self.config.api {
            OpenAiApi::ChatCompletions => {
                let completion = CompletionRequest::new(&self.config, request, streamed);
                let mut value =
                    serde_json::to_value(completion).map_err(|_| OpenAiError::Malformed {
                        reason: "could not encode request".into(),
                    })?;
                if let Some(effort) = &self.config.reasoning_effort
                    && let Some(fields) = value.as_object_mut()
                {
                    fields.insert("reasoning_effort".into(), effort.clone().into());
                }
                value
            }
            OpenAiApi::Responses => crate::responses::request(&self.config, request)?,
        };
        let body = with_extra_body(body, self.config.extra_body.as_ref());
        let mut call = self.client.post(self.endpoint.clone()).json(&body);
        let mut authorized = false;
        for (name, value) in self.config.default_headers.iter().flatten() {
            authorized |= name.eq_ignore_ascii_case("authorization");
            call = call.header(name, value);
        }
        // An `Authorization` the host's headers carry replaces the key's
        // bearer, as Python's client lets `default_headers` override it.
        if !secret.is_empty() && !authorized {
            call = call.bearer_auth(&secret);
        }
        let response = call.send().await.map_err(OpenAiError::request)?;
        let status = response.status();
        if !status.is_success() {
            // An error body past what a turn reads still keeps its status,
            // as Python's client reads the status whatever the body.
            let body = match read_body(response).await {
                Ok(body) => body,
                Err(OpenAiError::AnswerTooLong { .. }) => Vec::new(),
                Err(error) => return Err(error),
            };
            return Err(wire::status_error(status, &body, &secret));
        }
        Ok((response, secret))
    }
}

/// `body` with the members of `extra` too, which win over RoomKit's own,
/// as Python's client merges `extra_body` last.
fn with_extra_body(
    mut body: serde_json::Value,
    extra: Option<&roomkit::model::Metadata>,
) -> serde_json::Value {
    if let (Some(fields), Some(extra)) = (body.as_object_mut(), extra) {
        fields.extend(
            extra
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
    }
    body
}

impl AiProvider for OpenAiAiProvider {
    /// Python's class name, `OpenAIAIProvider`, as Python's provider
    /// names itself.
    fn name(&self) -> &'static str {
        "OpenAIAIProvider"
    }

    fn model_name(&self) -> &str {
        &self.config.model
    }

    fn supports_vision(&self) -> bool {
        self.config.vision
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_structured_streaming(&self) -> bool {
        true
    }

    /// The whole answer to `context`: on Chat Completions, one completion
    /// not streamed, as Python's `generate` sends it; on Responses, its
    /// stream read to its end.
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let events = match Request::of(context) {
            Ok(request) => stream::answer(self, request, false),
            Err(error) => stream::refused(error),
        };
        stream::whole(events).await
    }

    fn generate_stream<'a>(&'a self, context: &'a AiContext) -> TextStream<'a> {
        stream::text(self.generate_structured_stream(context))
    }

    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        match Request::of(context) {
            Ok(request) => stream::answer(self, request, true),
            Err(error) => stream::refused(error),
        }
    }
}

impl fmt::Debug for OpenAiAiProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiAiProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The body of `response`, refused past [`ANSWER_LIMIT`]: as its head
/// declares it, then as it arrives, so that an endpoint that streams
/// without end fails the turn instead of filling the memory.
pub(crate) async fn read_body(mut response: reqwest::Response) -> Result<Vec<u8>, OpenAiError> {
    let too_long = OpenAiError::AnswerTooLong {
        limit: ANSWER_LIMIT,
    };
    let declared = response.content_length().unwrap_or_default();
    if usize::try_from(declared).map_or(true, |declared| declared > ANSWER_LIMIT) {
        return Err(too_long);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(OpenAiError::request)? {
        if body.len().saturating_add(chunk.len()) > ANSWER_LIMIT {
            return Err(too_long);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The selected API endpoint under `base_url`, appended after its path.
/// A base URL with a query or a fragment is refused, since the
/// path would follow them instead.
fn endpoint(base_url: &str, api: OpenAiApi) -> Result<reqwest::Url, OpenAiError> {
    let refused = |reason: &str| OpenAiError::BaseUrl {
        reason: reason.to_owned(),
    };
    let not_http = || refused("it is not an http or https URL");
    let base = reqwest::Url::parse(base_url).map_err(|_| not_http())?;
    if base.query().is_some() || base.fragment().is_some() {
        return Err(refused(
            "it has a query or a fragment, which the API path cannot follow",
        ));
    }
    let joined = format!("{}{}", base_url.trim_end_matches('/'), api.path());
    let url = reqwest::Url::parse(&joined).map_err(|_| not_http())?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(not_http()),
    }
}

/// TLS with rustls's ring provider and the webpki roots (step 4, decision 4):
/// the same trust on every host, and no system TLS library. HTTP/1.1 is
/// offered by ALPN, as reqwest's own rustls configuration does.
fn tls_config() -> Result<rustls::ClientConfig, OpenAiError> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| OpenAiError::Client {
        reason: error.to_string(),
    })?
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn the_endpoint_is_the_base_url_s_chat_completions() {
        assert_eq!(
            endpoint("https://api.openai.com/v1/", OpenAiApi::ChatCompletions).map(String::from),
            Ok("https://api.openai.com/v1/chat/completions".to_owned())
        );
    }

    #[test]
    fn a_base_url_that_is_no_http_url_is_refused() {
        for base_url in ["api.openai.com/v1", "ftp://example.com", ""] {
            assert!(
                matches!(
                    endpoint(base_url, OpenAiApi::ChatCompletions),
                    Err(OpenAiError::BaseUrl { .. })
                ),
                "{base_url}"
            );
        }
    }

    #[test]
    fn a_base_url_with_a_query_or_a_fragment_is_refused() {
        for base_url in [
            "https://example.openai.azure.com/openai/deployments/d?api-version=2024-10-21",
            "https://api.openai.com/v1#chat",
        ] {
            let refused = endpoint(base_url, OpenAiApi::ChatCompletions);
            assert!(
                matches!(refused, Err(OpenAiError::BaseUrl { .. })),
                "{base_url}: {refused:?}"
            );
        }
    }

    #[test]
    fn a_refused_base_url_is_not_repeated() {
        let error = endpoint(
            "ftp://user:pass@example.com/?token=abc",
            OpenAiApi::ChatCompletions,
        )
        .unwrap_err();
        let printed = format!("{error} {error:?}");
        assert!(
            !printed.contains("pass") && !printed.contains("abc"),
            "{printed}"
        );
    }

    #[test]
    fn the_provider_s_debug_form_hides_the_api_key_and_the_base_url_s_credentials() {
        let config = OpenAiConfig {
            base_url: Some("https://user:pass@example.com:8443/v1".to_owned()),
            ..OpenAiConfig::new("sk-secret", "gpt-6-luna")
        };
        let provider = OpenAiAiProvider::new(config).unwrap();
        let printed = format!("{provider:?}");
        for secret in ["sk-secret", "user", "pass", "/v1"] {
            assert!(!printed.contains(secret), "{secret} in {printed}");
        }
    }
}
