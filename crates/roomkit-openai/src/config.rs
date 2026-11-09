//! What an [`OpenAiAiProvider`](crate::OpenAiAiProvider) is configured
//! with: Python's `OpenAIConfig`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::Metadata;

/// OpenAI's own endpoint, which a configuration with no base URL calls.
pub(crate) const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// The models that take their token bound as `max_completion_tokens` on
/// OpenAI's own endpoint (Python's `_MAX_COMPLETION_TOKEN_MODEL_PREFIXES`).
const MAX_COMPLETION_TOKEN_MODELS: [&str; 6] = ["gpt-4.1", "gpt-5", "gpt-6", "o1", "o3", "o4"];

/// The models that take no temperature on OpenAI's own endpoint (Python's
/// `_FIXED_TEMPERATURE_MODEL_PREFIXES`).
const FIXED_TEMPERATURE_MODELS: [&str; 5] = ["gpt-5", "gpt-6", "o1", "o3", "o4"];

/// The HTTP API spoken by the provider. Selection is explicit; no fallback
/// changes protocols or repeats a failed generation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenAiApi {
    /// OpenAI-compatible Chat Completions, including local endpoints.
    #[default]
    ChatCompletions,
    /// Stateless Responses with streamed text, function tools and reasoning replay.
    Responses,
}

impl OpenAiApi {
    pub(crate) fn path(self) -> &'static str {
        match self {
            Self::ChatCompletions => "/chat/completions",
            Self::Responses => "/responses",
        }
    }
}

/// The endpoint a provider calls and how: Python's `OpenAIConfig(*, api_key,
/// base_url=None, model, max_tokens=1024, timeout=30.0, connect_timeout=5.0,
/// max_retries=0, include_stream_usage=True, use_max_completion_tokens=False,
/// supports_custom_temperature=True, reasoning_effort=None,
/// default_headers=None, extra_body=None,
/// supports_reasoning_effort_with_tools=None)`, built with
/// [`OpenAiConfig::new`] and struct update.
///
/// Its `Debug` never prints the API key, nor the base URL's user
/// information, path or query, which may carry credentials.
#[derive(Clone)]
pub struct OpenAiConfig {
    /// The key a request carries as its bearer token; empty sends none.
    pub api_key: String,
    /// The endpoint's base URL; `None` calls OpenAI's own.
    pub base_url: Option<String>,
    /// The model asked.
    pub model: String,
    /// The most tokens a generation writes, unless its context says;
    /// `None` sends no bound, and the endpoint decides (Python's
    /// `max_tokens=None`).
    pub max_tokens: Option<u32>,
    /// How long a request waits for the endpoint between two reads, the
    /// whole answer unbounded, as Python's `httpx.Timeout(timeout)` bounds
    /// each read; a request's writes are not bounded, which reqwest does
    /// not offer.
    pub timeout: Duration,
    /// How long a request waits for its connection.
    pub connect_timeout: Duration,
    /// How many times a request is tried again on its own, as Python's
    /// openai SDK does (`max_retries`): a connection failure, a timeout, or
    /// an answer the endpoint's `x-should-retry` or its status says is worth
    /// it (408, 409, 429, 5xx); each after the wait its `retry-after-ms` or
    /// `retry-after` asks, within two minutes, past which the answer is
    /// final, else 0.5 s doubling to 8 s, up to a quarter less. A stream is
    /// tried again until its head came, never after. 0 by default, since a
    /// channel's [`retry_policy`](roomkit_ai::AiChannelOptions::retry_policy)
    /// is the layer that retries, with the fallback beyond it.
    pub max_retries: u32,
    /// Whether a streamed answer reports its usage
    /// (`stream_options.include_usage`).
    pub include_stream_usage: bool,
    /// Whether the token bound is sent as `max_completion_tokens`, as
    /// OpenAI's reasoning models take it, rather than `max_tokens`. `None`,
    /// a switch left unset, is Python's profile: on OpenAI's own endpoint,
    /// a model it names (`gpt-4.1`, `gpt-5`, `gpt-6`, `o1`, `o3`, `o4`)
    /// takes it, any other does not.
    pub use_max_completion_tokens: Option<bool>,
    /// Whether the model takes a temperature: one that refuses it is sent
    /// none. `None`, a switch left unset, is Python's profile: on OpenAI's
    /// own endpoint, a model it names (`gpt-5`, `gpt-6`, `o1`, `o3`, `o4`)
    /// takes none, any other does.
    pub supports_custom_temperature: Option<bool>,
    /// How hard a reasoning model thinks (`reasoning_effort`), when set
    /// (RFC §6.7), under the turn's own. On OpenAI's own endpoint, `none`
    /// is sent as the least effort the catalogue says the model takes
    /// (`minimal` on GPT-5, `low` on o3), on either API. On a Chat
    /// Completions turn with tools, a model the
    /// catalogue tags decides there (`none` for GPT-5.4 and later, sent
    /// even unset); any other model, and any behind a `base_url`, sends it
    /// only where `supports_reasoning_effort_with_tools` says the server
    /// takes it there.
    pub reasoning_effort: Option<String>,
    /// Whether the server takes `reasoning_effort` on a Chat Completions
    /// turn with tools, where OpenAI's catalogue does not tag the model
    /// (RFC §6.7): `Some(true)` sends it as on any turn, `Some(false)`
    /// leaves it out, and `None` leaves it out with one warning per
    /// provider. Python's `supports_reasoning_effort_with_tools`. On the
    /// Responses API, which Python's OpenAI text provider does not speak,
    /// a turn with tools sends its effort as any turn does: the rule was
    /// measured on Chat Completions (HITL 78d888bf).
    pub supports_reasoning_effort_with_tools: Option<bool>,
    /// Headers every request carries; an `Authorization` among them
    /// replaces the key's bearer, as Python's client sends it.
    pub default_headers: Option<BTreeMap<String, String>>,
    /// Members every request's body carries, which win over RoomKit's own,
    /// as Python's client merges them last.
    pub extra_body: Option<Metadata>,
    /// Whether the server honours a `json_schema` response format (RFC
    /// §6.7, Python's `supports_response_schema`): `None` leaves it to the
    /// provider's default, which a server behind a base URL may not keep.
    pub supports_response_schema: Option<bool>,
    /// Whether the server takes a `json_schema` response format beside
    /// function tools (RFC §6.7): `None` leaves it on OpenAI's own endpoint
    /// alone, a grammar-constrained server (vLLM, llama.cpp) unable to call
    /// a tool under the constraint.
    pub supports_response_schema_with_tools: Option<bool>,
    /// Where the bearer token comes from before each request, each try of
    /// it under `max_retries` included, in place of `api_key`: a renewable
    /// credential.
    pub token_source: Option<Arc<dyn crate::TokenSource>>,
    /// Whether the configured model takes images (RFC §6.4).
    pub vision: bool,
    /// The HTTP API spoken: Chat Completions by default.
    pub api: OpenAiApi,
    /// The namespace, its name and description, under which the Responses
    /// API declares the tools.
    pub tool_namespace: Option<(String, String)>,
    /// The assistant message field this server reads a model's earlier
    /// reasoning from, for a server whose documentation asks for it back
    /// (`reasoning_content` on Z.ai's GLM). A round or an answer that
    /// reasoned carries it there, and one that did not carries nothing.
    /// `None` sends no earlier reasoning (RFC §6.4): strict servers refuse a
    /// field they do not know, so it is never guessed. Chat Completions only.
    pub thinking_field: Option<String>,
}

impl OpenAiConfig {
    /// The configuration calling OpenAI's own endpoint with `api_key` for
    /// `model`, with Python's defaults.
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: None,
            model: model.into(),
            max_tokens: Some(1024),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            max_retries: 0,
            include_stream_usage: true,
            use_max_completion_tokens: None,
            supports_custom_temperature: None,
            reasoning_effort: None,
            supports_reasoning_effort_with_tools: None,
            default_headers: None,
            extra_body: None,
            supports_response_schema: None,
            supports_response_schema_with_tools: None,
            token_source: None,
            vision: false,
            api: OpenAiApi::default(),
            tool_namespace: None,
            thinking_field: None,
        }
    }

    /// The base URL the configuration calls.
    pub(crate) fn base_url(&self) -> &str {
        self.base_url.as_deref().unwrap_or(OPENAI_BASE_URL)
    }

    /// Whether the token bound goes as `max_completion_tokens`: the switch
    /// when set, else Python's profile of a modern model on OpenAI's own
    /// endpoint (`OpenAIConfig.model_post_init`).
    pub(crate) fn takes_max_completion_tokens(&self) -> bool {
        self.use_max_completion_tokens
            .unwrap_or_else(|| self.profiled(&MAX_COMPLETION_TOKEN_MODELS))
    }

    /// Whether a temperature is sent: the switch when set, else unless
    /// Python's profile fixes the model's temperature on OpenAI's own
    /// endpoint.
    pub(crate) fn takes_temperature(&self) -> bool {
        self.supports_custom_temperature
            .unwrap_or_else(|| !self.profiled(&FIXED_TEMPERATURE_MODELS))
    }

    /// Whether the model starts with one of `prefixes` on OpenAI's own
    /// endpoint.
    fn profiled(&self, prefixes: &[&str]) -> bool {
        crate::names::is_openai_endpoint(self.base_url())
            && prefixes.iter().any(|prefix| self.model.starts_with(prefix))
    }
}

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let api_key = if self.token_source.is_some() {
            "dynamic"
        } else if self.api_key.is_empty() {
            "none"
        } else {
            "hidden"
        };
        let headers: Option<Vec<&String>> = self
            .default_headers
            .as_ref()
            .map(|headers| headers.keys().collect());
        f.debug_struct("OpenAiConfig")
            .field("base_url", &shown_url(self.base_url()))
            .field("api_key", &api_key)
            .field("model", &self.model)
            .field("max_tokens", &self.max_tokens)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_retries", &self.max_retries)
            .field("api", &self.api)
            .field("vision", &self.vision)
            .field("tool_namespace", &self.tool_namespace)
            .field("thinking_field", &self.thinking_field)
            .field("reasoning_effort", &self.reasoning_effort)
            .field(
                "supports_reasoning_effort_with_tools",
                &self.supports_reasoning_effort_with_tools,
            )
            .field("default_headers", &headers)
            .finish_non_exhaustive()
    }
}

/// `url` as it may be shown: its scheme, host and port, never its user
/// information, path or query, which may carry credentials.
pub(crate) fn shown_url(url: &str) -> String {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return "(not a URL)".to_owned();
    };
    let host = parsed.host_str().unwrap_or_default();
    match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    }
}
