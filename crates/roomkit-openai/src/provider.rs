//! The provider: Python's `OpenAIAIProvider`, an [`AiProvider`] over an
//! OpenAI-compatible Chat Completions or Responses endpoint.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use roomkit_ai::{
    AiContext, AiProvider, AiResponse, ModelInfo, ProviderError, ProviderStream, TextStream,
};

use crate::config::{OpenAiApi, OpenAiConfig};
use crate::error::OpenAiError;
use crate::reasoning;
use crate::request::Request;
use crate::retry;
use crate::stream;
use crate::wire::{self, CompletionRequest};

/// The longest answer a generation reads, in bytes: far more than a
/// completion's text and calls take.
pub(crate) const ANSWER_LIMIT: usize = 4 * 1024 * 1024;

/// Where an endpoint lists its models, under its base URL.
const MODELS_PATH: &str = "/models";

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
    /// Where the endpoint lists its models.
    models_endpoint: reqwest::Url,
    client: reqwest::Client,
    /// Whether the provider warned of an effort a turn with tools left out
    /// unsaid, which it does once (Python's `_reasoning_left_out_logged`).
    left_out_warned: Arc<AtomicBool>,
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
        let models_endpoint = joined(config.base_url(), MODELS_PATH)?;
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
            models_endpoint,
            client,
            left_out_warned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// The catalogue's entry for the configured model where the endpoint
    /// is OpenAI's own, which the catalogue speaks for: behind a base URL
    /// the server decides what its model takes (RFC §6.7).
    fn official_entry(&self) -> Option<&'static ModelInfo> {
        if crate::names::is_openai_endpoint(self.config.base_url()) {
            crate::models::entry(&self.config.model)
        } else {
            None
        }
    }

    /// The `reasoning_effort` a Chat Completions request carries, the
    /// turn's `turn` and a turn with tools when `tools` (RFC §6.7, Python's
    /// `_apply_sampling_kwargs`): the turn's effort over the configured
    /// one, the model's floor where it states off (`none`) and the
    /// catalogue knows the model's least effort; then, on a turn with
    /// tools, what the endpoint accepts there ([`Self::tool_turn_effort`]).
    pub(crate) fn reasoning_effort(
        &self,
        turn: Option<&str>,
        off: bool,
        tools: bool,
    ) -> Option<String> {
        let effort = self.floored_effort(turn, off);
        if tools {
            self.tool_turn_effort(self.official_entry(), effort)
        } else {
            effort
        }
    }

    /// The effort a request asks for before any tool-turn rule: the turn's
    /// `turn` over the configured one, and the model's floor where it
    /// states off (`none`) and OpenAI's catalogue knows the model's least
    /// effort (Python's `_turn_effort`). The Responses API sends it as it
    /// is: the tool-turn rule was measured on Chat Completions (HITL
    /// 78d888bf, option 1).
    fn floored_effort(&self, turn: Option<&str>, off: bool) -> Option<String> {
        let configured = self.config.reasoning_effort.as_deref();
        let floor = reasoning::reasoning_floor(self.official_entry());
        reasoning::floored_effort(turn, configured, floor, off)
    }

    /// The effort a turn with tools sends (Python's `_tool_turn_effort`):
    /// on OpenAI's own endpoint, a model the catalogue tags
    /// `tools_reasoning_none` takes function tools only with `none`, sent
    /// even unset, and one tagged `tools_reasoning_effort` takes `effort`;
    /// any other model, and any behind a base URL, sends what the config
    /// states of the server ([`Self::tool_turn_setting`]).
    fn tool_turn_effort(
        &self,
        entry: Option<&ModelInfo>,
        effort: Option<String>,
    ) -> Option<String> {
        if reasoning::tagged(entry, reasoning::TOOLS_REASONING_NONE) {
            return Some("none".to_owned());
        }
        if reasoning::tagged(entry, reasoning::TOOLS_REASONING_EFFORT) {
            return effort;
        }
        self.tool_turn_setting(effort)
    }

    /// The setting a turn with tools carries where no catalogue speaks for
    /// the model (Python's `_tool_turn_setting`): kept where
    /// `supports_reasoning_effort_with_tools` says the server takes it,
    /// else left out, an unsaid one warned of once per provider.
    fn tool_turn_setting(&self, setting: Option<String>) -> Option<String> {
        let stated = self.config.supports_reasoning_effort_with_tools;
        if let (None, Some(setting)) = (stated, &setting) {
            self.warn_reasoning_left_out(setting);
        }
        setting.filter(|_| stated == Some(true))
    }

    /// Warns, once per provider, that a turn with tools left out `setting`
    /// because the config does not say the server takes it there (Python's
    /// `_warn_reasoning_left_out`).
    fn warn_reasoning_left_out(&self, setting: &str) {
        if self.left_out_warned.swap(true, Ordering::Relaxed) {
            return;
        }
        tracing::warn!(
            target: "roomkit::openai",
            "openai: reasoning setting '{setting}' left out of a turn with tools, since \
             nothing says the server behind model '{}' takes it there; set \
             supports_reasoning_effort_with_tools to Some(true) on the config if it \
             does, or Some(false) to leave it out without this warning (RFC 6.7)",
            self.config.model,
        );
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

    /// The body of `request` as the configured API writes it, `streamed`
    /// false asking Chat Completions for the completion whole: on Chat
    /// Completions, refused first for a model OpenAI's endpoint does not
    /// serve so (Python's `_check_model_serves`), then carrying the effort
    /// the turn sends ([`Self::reasoning_effort`]).
    pub(crate) fn body(
        &self,
        request: &Request,
        streamed: bool,
    ) -> Result<serde_json::Value, OpenAiError> {
        if self.config.api == OpenAiApi::Responses {
            let mut value = crate::responses::request(&self.config, request)?;
            let turn = request.reasoning_effort.as_deref();
            if let Some(effort) = self.floored_effort(turn, request.thinking_off)
                && let Some(fields) = value.as_object_mut()
            {
                fields.insert("reasoning".into(), serde_json::json!({ "effort": effort }));
            }
            return Ok(value);
        }
        let tools = !request.tools.is_empty();
        reasoning::check_model_serves(&self.config.model, self.official_entry(), tools)?;
        let completion = CompletionRequest::new(&self.config, request, streamed);
        let mut value = serde_json::to_value(completion).map_err(|_| OpenAiError::Malformed {
            reason: "could not encode request".into(),
        })?;
        let turn = request.reasoning_effort.as_deref();
        if let Some(effort) = self.reasoning_effort(turn, request.thinking_off, tools)
            && let Some(fields) = value.as_object_mut()
        {
            fields.insert("reasoning_effort".into(), effort.into());
        }
        Ok(value)
    }

    /// Sends `request`: its body as the configured API writes it, under
    /// [`Self::send`]; `streamed` false asks Chat Completions for the
    /// completion whole.
    pub(crate) async fn ask(
        &self,
        request: &Request,
        streamed: bool,
    ) -> Result<(Answered, String), OpenAiError> {
        // Python's order: the model's refusal, then the effort (and its
        // one warning), then the tools' names.
        let body = self.body(request, streamed)?;
        crate::names::check(self.config.base_url(), &request.tools)?;
        let body = with_extra_body(body, self.config.extra_body.as_ref());
        let call = || self.client.post(self.endpoint.clone()).json(&body);
        self.send(&call, !streamed).await
    }

    /// Sends what `call` builds, tried again up to the configuration's
    /// `max_retries` as Python's SDK does ([`crate::retry`]): its answer and
    /// the credential it was sent with, or the last try's failure. A JSON
    /// answer to a call asked `whole` is read inside the try, as the SDK
    /// reads a request it does not stream.
    async fn send(
        &self,
        call: &(dyn Fn() -> reqwest::RequestBuilder + Sync),
        whole: bool,
    ) -> Result<(Answered, String), OpenAiError> {
        let mut retries_taken = 0;
        loop {
            let left = retries_taken < self.config.max_retries;
            match self.attempt(call, whole, (retries_taken, left)).await {
                Ok(answered) => return Ok(answered),
                Err(Failed::Final(error)) => return Err(error),
                Err(Failed::Again(wait)) => {
                    tracing::info!(
                        target: "roomkit::openai",
                        retries_taken,
                        ?wait,
                        "a request is tried again"
                    );
                    tokio::time::sleep(wait).await;
                    retries_taken += 1;
                }
            }
        }
    }

    /// One try of what `call` builds, the bearer token the configuration
    /// gives and its headers on it, after `retries_taken` tries, with
    /// another one `left` or not.
    async fn attempt(
        &self,
        call: &(dyn Fn() -> reqwest::RequestBuilder + Sync),
        whole: bool,
        (retries_taken, left): (u32, bool),
    ) -> Result<(Answered, String), Failed> {
        let secret = match &self.config.token_source {
            Some(source) => source.token().await.map_err(OpenAiError::from)?,
            None => self.config.api_key.clone(),
        };
        let again = || Failed::Again(retry::delay(retries_taken, None));
        let response = match self.authorized(call(), &secret).send().await {
            Ok(response) => response,
            // A request that could not be built never left: it fails the same
            // however many times it is tried.
            Err(error) if left && !error.is_builder() => return Err(again()),
            Err(error) => return Err(OpenAiError::request(error).into()),
        };
        let status = response.status();
        if !status.is_success() {
            if left && retry::should_retry(status, response.headers()) {
                let wait = retry::delay(retries_taken, Some(response.headers()));
                return Err(Failed::Again(wait));
            }
            // An error body past what a turn reads still keeps its status,
            // as Python's client reads the status whatever the body.
            let body = match read_body(response).await {
                Ok(body) => body,
                Err(OpenAiError::AnswerTooLong { .. }) => Vec::new(),
                Err(OpenAiError::Request { .. }) if left && whole => return Err(again()),
                Err(error) => return Err(error.into()),
            };
            return Err(wire::status_error(status, &body, &secret).into());
        }
        if !whole || !stream::is_json(&response) {
            return Ok((Answered::Head(response), secret));
        }
        match read_body(response).await {
            Ok(body) => Ok((Answered::Body(body), secret)),
            Err(OpenAiError::Request { .. }) if left => Err(again()),
            Err(error) => Err(error.into()),
        }
    }

    /// The models `GET /models` answers.
    async fn live_models(&self) -> Result<Vec<ModelInfo>, OpenAiError> {
        let call = || self.client.get(self.models_endpoint.clone());
        let (answered, _) = self.send(&call, true).await?;
        let body = match answered {
            Answered::Body(body) => body,
            Answered::Head(response) => read_body(response).await?,
        };
        crate::models::listed(&body)
    }

    /// `call` with the configuration's headers and the bearer token
    /// `secret`.
    fn authorized(
        &self,
        mut call: reqwest::RequestBuilder,
        secret: &str,
    ) -> reqwest::RequestBuilder {
        let mut authorized = false;
        for (name, value) in self.config.default_headers.iter().flatten() {
            authorized |= name.eq_ignore_ascii_case("authorization");
            call = call.header(name, value);
        }
        // An `Authorization` the host's headers carry replaces the key's
        // bearer, as Python's client lets `default_headers` override it.
        if !secret.is_empty() && !authorized {
            call = call.bearer_auth(secret);
        }
        call
    }
}

/// A request's answer: its head, its body to read as it arrives, or its
/// body, read whole inside the try.
pub(crate) enum Answered {
    Head(reqwest::Response),
    Body(Vec<u8>),
}

/// What a try that did not answer leaves its request.
enum Failed {
    /// Its failure, the request's.
    Final(OpenAiError),
    /// Another try, after this wait.
    Again(Duration),
}

impl From<OpenAiError> for Failed {
    fn from(error: OpenAiError) -> Self {
        Self::Final(error)
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

    /// OpenAI's curated catalogue, Python's (`catalog/models.json`).
    fn available_models() -> Vec<ModelInfo> {
        crate::models::models().to_vec()
    }

    fn catalog_entry(&self) -> Option<ModelInfo> {
        crate::models::entry(&self.config.model).cloned()
    }

    /// Every model id the configured endpoint exposes, `GET /models`, as
    /// Python's: the response carries only ids, so the metadata of the
    /// models the catalogue knows is backfilled from it, and the non-chat
    /// models (embeddings, audio) pass through, its speech models tagged.
    async fn list_models(&self) -> Result<Vec<ModelInfo>, ProviderError> {
        let live = self.live_models().await.map_err(OpenAiError::of_listing)?;
        Ok(roomkit_ai::listing(live, crate::models::models()))
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
        let events = match Request::of(context, self.config.api) {
            Ok(request) => stream::answer(self, request, false),
            Err(error) => stream::refused(error),
        };
        stream::whole(events).await
    }

    fn generate_stream<'a>(&'a self, context: &'a AiContext) -> TextStream<'a> {
        stream::text(self.generate_structured_stream(context))
    }

    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        match Request::of(context, self.config.api) {
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
fn endpoint(base_url: &str, api: OpenAiApi) -> Result<reqwest::Url, OpenAiError> {
    joined(base_url, api.path())
}

/// `path` under `base_url`, appended after its path. A base URL with a
/// query or a fragment is refused, since the path would follow them instead.
fn joined(base_url: &str, path: &str) -> Result<reqwest::Url, OpenAiError> {
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
    let joined = format!("{}{path}", base_url.trim_end_matches('/'));
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

    /// The effort a turn with tools sends, `base_url` the endpoint and
    /// `stated` the switch.
    fn tool_turn_effort(base_url: Option<&str>, stated: Option<bool>) -> Option<String> {
        let config = OpenAiConfig {
            base_url: base_url.map(str::to_owned),
            reasoning_effort: Some("low".to_owned()),
            supports_reasoning_effort_with_tools: stated,
            ..OpenAiConfig::new("sk-test", "gpt-5")
        };
        OpenAiAiProvider::new(config)
            .unwrap()
            .reasoning_effort(None, false, true)
    }

    /// On OpenAI's own endpoint, however written, the switch does not
    /// decide for a model the catalogue tags: GPT-5's
    /// `tools_reasoning_effort` keeps the configured effort; a base URL
    /// carrying credentials is another endpoint, which the switch decides.
    #[test]
    fn on_openai_s_own_endpoint_a_tool_turn_keeps_its_effort() {
        let low = Some("low".to_owned());
        for base_url in [
            None,
            Some("https://api.openai.com/v1"),
            Some("https://api.openai.com/v1/"),
        ] {
            for stated in [None, Some(false)] {
                assert_eq!(
                    tool_turn_effort(base_url, stated),
                    low,
                    "{base_url:?} {stated:?}"
                );
            }
        }
        assert_eq!(
            tool_turn_effort(Some("https://user:key@api.openai.com/v1"), None),
            None
        );
    }

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
