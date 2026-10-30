//! What an [`OpenAiAgent`](crate::OpenAiAgent) is configured with.

use std::fmt;
use std::time::Duration;

/// How many of the room's latest messages a turn sends by default: as many
/// as a kit hands a turn, 50 messages.
const DEFAULT_HISTORY: usize = 50;

/// How long a completion may take by default, from its request to the end
/// of its answer.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

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

/// The endpoint an agent calls, the model it asks, its system prompt, how
/// much of the room it sends and how long it waits.
///
/// The configuration's `Debug` never prints the API key, nor the base URL's
/// user information, path or query, which may carry credentials.
#[derive(Clone)]
pub struct OpenAiConfig {
    pub(crate) base_url: String,
    pub(crate) api_key: String,
    pub(crate) token_source: Option<std::sync::Arc<dyn crate::TokenSource>>,
    pub(crate) model: String,
    pub(crate) vision: bool,
    pub(crate) api: OpenAiApi,
    pub(crate) tool_namespace: Option<(String, String)>,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) system_prompt: Option<String>,
    pub(crate) history: usize,
    pub(crate) timeout: Duration,
}

impl OpenAiConfig {
    /// Declare that the configured model accepts image input (RFC §6.4).
    /// Disabled by default; this does not infer capabilities from a model name.
    #[must_use]
    pub fn with_vision(mut self, enabled: bool) -> Self {
        self.vision = enabled;
        self
    }

    /// A configuration calling `{base_url}/chat/completions` with `api_key`
    /// as its bearer token, asking `model`. The base URL is the API's root,
    /// `https://api.openai.com/v1` for OpenAI's, with no query or fragment;
    /// an empty key sends no `Authorization` header, for a local server
    /// that asks for none.
    ///
    /// It has no system prompt, sends every message the turn's context
    /// holds, and waits 120 seconds for a completion.
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            token_source: None,
            model: model.into(),
            vision: false,
            api: OpenAiApi::default(),
            reasoning_effort: None,
            tool_namespace: None,
            system_prompt: None,
            history: DEFAULT_HISTORY,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Resolve credentials through a host-owned source before every generation.
    /// This replaces the static API key, which is cleared. The source owns
    /// refresh, account selection and storage; its Debug is never called here.
    #[must_use]
    pub fn with_token_source(mut self, source: std::sync::Arc<dyn crate::TokenSource>) -> Self {
        self.api_key.clear();
        self.token_source = Some(source);
        self
    }

    /// Select Chat Completions or Responses, keeping the same API root and
    /// bearer credential. Responses uses `store: false` and `stream: true`.
    #[must_use]
    pub fn with_api(mut self, api: OpenAiApi) -> Self {
        self.api = api;
        self
    }

    /// Group Responses function tools under one namespace. Required by some
    /// account-based routes; Chat Completions ignores this setting. Names use
    /// ASCII letters, digits, underscores or hyphens (1–64 characters).
    #[must_use]
    pub fn with_tool_namespace(
        mut self,
        name: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.tool_namespace = Some((name.into(), description.into()));
        self
    }

    /// Set the model's reasoning effort. Responses writes `reasoning.effort`;
    /// Chat Completions writes `reasoning_effort`. Omit this setting to keep
    /// the provider's default; the endpoint validates supported effort names.
    #[must_use]
    pub fn with_reasoning_effort(mut self, effort: impl Into<String>) -> Self {
        self.reasoning_effort = Some(effort.into());
        self
    }

    /// The base URL as it may be shown, in a log or on a screen: its
    /// scheme, host and port, never its user information, path, query or
    /// fragment, which may carry credentials; `(not a URL)` when it does
    /// not parse.
    pub fn shown_base_url(&self) -> String {
        shown_url(&self.base_url)
    }

    /// The same configuration, with the system prompt every turn opens
    /// with. It stays the same from turn to turn, so a provider that caches
    /// a request's prefix keeps it (RFC §6.4).
    #[must_use]
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    /// The same configuration, sending at most the room's latest
    /// `messages` messages before the turn's input.
    ///
    /// A turn reads only the history the kit hands it: the room's latest 50
    /// messages, and the agent's latest 60 tool-call rows, whose rounds it
    /// replays. A value above what that window holds sends no more: it can
    /// lower the history, never raise it.
    #[must_use]
    pub fn with_history(mut self, messages: usize) -> Self {
        self.history = messages;
        self
    }

    /// The same configuration, failing a turn whose completion takes longer
    /// than `timeout`.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
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
        f.debug_struct("OpenAiConfig")
            .field("base_url", &self.shown_base_url())
            .field("api_key", &api_key)
            .field("model", &self.model)
            .field("api", &self.api)
            .field("vision", &self.vision)
            .field("tool_namespace", &self.tool_namespace)
            .field("reasoning_effort", &self.reasoning_effort)
            .field("system_prompt", &self.system_prompt)
            .field("history", &self.history)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// `url` as it may be shown: its scheme, host and port, never its user
/// information, path or query, which may carry credentials.
fn shown_url(url: &str) -> String {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return "(not a URL)".to_owned();
    };
    let host = parsed.host_str().unwrap_or_default();
    match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    }
}
