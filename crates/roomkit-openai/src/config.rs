//! What an [`OpenAiAgent`](crate::OpenAiAgent) is configured with.

use std::fmt;
use std::time::Duration;

/// How many of the room's latest messages a turn sends by default: as many
/// as a kit's context holds.
const DEFAULT_HISTORY: usize = 50;

/// How long a completion may take by default, from its request to the end
/// of its answer.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

/// The endpoint an agent calls, the model it asks, its system prompt, how
/// much of the room it sends and how long it waits.
///
/// The configuration's `Debug` never prints the API key, nor the base URL's
/// user information, path or query, which may carry credentials.
#[derive(Clone)]
pub struct OpenAiConfig {
    pub(crate) base_url: String,
    pub(crate) api_key: String,
    pub(crate) model: String,
    pub(crate) system_prompt: Option<String>,
    pub(crate) history: usize,
    pub(crate) timeout: Duration,
}

impl OpenAiConfig {
    /// A configuration calling `{base_url}/chat/completions` with `api_key`
    /// as its bearer token, asking `model`. The base URL is the API's root,
    /// `https://api.openai.com/v1` for OpenAI's, with no query or fragment;
    /// an empty key sends no `Authorization` header, for a local server
    /// that asks for none.
    ///
    /// It has no system prompt, sends the room's latest 50 messages, and
    /// waits 120 seconds for a completion.
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            model: model.into(),
            system_prompt: None,
            history: DEFAULT_HISTORY,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// The same configuration, with the system prompt every turn opens
    /// with. It stays the same from turn to turn, so a provider that caches
    /// a request's prefix keeps it (RFC §6.4).
    #[must_use]
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    /// The same configuration, sending the room's latest `messages`
    /// messages before the turn's input, at most: no more than the kit's
    /// context holds (50 events by default).
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
        let api_key = if self.api_key.is_empty() {
            "none"
        } else {
            "hidden"
        };
        f.debug_struct("OpenAiConfig")
            .field("base_url", &shown_url(&self.base_url))
            .field("api_key", &api_key)
            .field("model", &self.model)
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
