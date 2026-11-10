//! Configuration and model defaults, matching Python's `AnthropicConfig`.
use std::{collections::BTreeMap, fmt, time::Duration};

pub(crate) const BASE_URL: &str = "https://api.anthropic.com";

/// Anthropic Messages configuration, with Python's defaults.
/// An explicit model prevents an upgrade from silently changing its cost.
/// `Debug` omits credentials, headers and the base URL.
#[derive(Clone)]
pub struct AnthropicConfig {
    /// The configured API key; a turn's `metadata["api_key"]` overrides it.
    pub api_key: String,
    /// The model identifier.
    pub model: String,
    /// The output token limit, unless the turn supplies one (1024).
    pub max_tokens: u32,
    /// The timeout between reads (60 seconds).
    pub timeout: Duration,
    /// The connection timeout (5 seconds).
    pub connect_timeout: Duration,
    /// Transport retries before a response starts (0, leaving retries to the channel).
    pub max_retries: u32,
    /// A custom API base URL. A pasted `/v1/messages` suffix is removed.
    pub base_url: Option<String>,
    /// Headers included in each request; values are never logged.
    pub extra_headers: Option<BTreeMap<String, String>>,
    /// Cache the stable prefix and the last two eligible messages (true).
    pub enable_prompt_caching: bool,
    /// Use adaptive thinking. `None` applies the official model's profile;
    /// an explicit value wins, including on custom endpoints.
    pub use_adaptive_thinking: Option<bool>,
    /// Send temperature outside thinking turns. `None` applies the model's
    /// profile; an explicit value wins, including on custom endpoints.
    pub supports_custom_temperature: Option<bool>,
}

impl AnthropicConfig {
    /// Configures `model` with `api_key` and Python's default settings.
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            model: model.into(),
            max_tokens: 1024,
            timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(5),
            max_retries: 0,
            base_url: None,
            extra_headers: None,
            enable_prompt_caching: true,
            use_adaptive_thinking: None,
            supports_custom_temperature: None,
        }
    }

    pub(crate) fn normalized(mut self) -> Self {
        self.base_url = self.base_url.map(|base| {
            let trimmed = base.trim().trim_end_matches('/');
            let suffix = "/v1/messages";
            if trimmed.to_ascii_lowercase().ends_with(suffix) {
                trimmed
                    .get(..trimmed.len() - suffix.len())
                    .unwrap_or(trimmed)
                    .to_owned()
            } else {
                trimmed.to_owned()
            }
        });
        self
    }

    pub(crate) fn base(&self) -> &str {
        self.base_url.as_deref().unwrap_or(BASE_URL)
    }

    pub(crate) fn official(&self) -> bool {
        let Ok(url) = reqwest::Url::parse(self.base().trim()) else {
            return false;
        };
        url.scheme() == "https"
            && url.host_str() == Some("api.anthropic.com")
            && url.port_or_known_default() == Some(443)
            && url.path().trim_end_matches('/').is_empty()
            && url.username().is_empty()
            && url.password().is_none()
    }

    pub(crate) fn adaptive(&self) -> bool {
        self.use_adaptive_thinking
            .unwrap_or_else(|| self.official() && modern(&self.model))
    }

    pub(crate) fn temperature(&self) -> bool {
        self.supports_custom_temperature
            .unwrap_or_else(|| !(self.official() && modern(&self.model)))
    }
}

/// Python's closed legacy set: everything after it uses the modern contract.
fn modern(model: &str) -> bool {
    let Some(id) = model.strip_prefix("claude-") else {
        return false;
    };
    if id.starts_with('2') || id.starts_with('3') || id.starts_with("instant") {
        return false;
    }
    for family in ["opus-4-", "sonnet-4-", "haiku-4-"] {
        if let Some(version) = id.strip_prefix(family) {
            let first = version.chars().next();
            let next = version.chars().nth(1);
            let old_version = first.is_some_and(|c| ('0'..='6').contains(&c))
                && next.is_none_or(|c| !c.is_alphanumeric() && c != '_');
            let dated = version.chars().take(8).count() == 8
                && version.chars().take(8).all(|c| c.is_ascii_digit());
            if old_version || dated {
                return false;
            }
        }
    }
    true
}

impl fmt::Debug for AnthropicConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicConfig")
            .field("model", &self.model)
            .field("max_tokens", &self.max_tokens)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_retries", &self.max_retries)
            .field("enable_prompt_caching", &self.enable_prompt_caching)
            .field("use_adaptive_thinking", &self.use_adaptive_thinking)
            .field(
                "supports_custom_temperature",
                &self.supports_custom_temperature,
            )
            .finish_non_exhaustive()
    }
}
