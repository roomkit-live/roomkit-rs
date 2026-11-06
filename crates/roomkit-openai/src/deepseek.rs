//! DeepSeek: Python's `roomkit.providers.deepseek`, its provider over DeepSeek's
//! OpenAI-compatible Chat Completions API and its configuration.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use roomkit::model::Metadata;

use crate::vendor::{Vendor, into_openai, vendor_provider};

/// DeepSeek's endpoint and how it is called: Python's `DeepSeekConfig`, its
/// fields `OpenAIConfig`'s and `enable_thinking`, built with
/// [`DeepSeekConfig::new`] and struct update.
///
/// Its `Debug` never prints the API key, nor the base URL's user
/// information, path or query, which may carry credentials.
#[derive(Clone)]
pub struct DeepSeekConfig {
    /// The key a request carries as its bearer token; empty sends none.
    pub api_key: String,
    /// DeepSeek's endpoint; another URL points at a proxy, where the server
    /// decides the tool names.
    pub base_url: String,
    /// The model asked: required, so that no upgrade changes it.
    pub model: String,
    /// The token bound; `None` sends none.
    pub max_tokens: Option<u32>,
    /// The longest wait between two reads of an answer.
    pub timeout: Duration,
    /// The longest wait for a connection.
    pub connect_timeout: Duration,
    /// How many times a failed request is tried again.
    pub max_retries: u32,
    /// Whether a streamed answer asks for its usage
    /// (`stream_options.include_usage`).
    pub include_stream_usage: bool,
    /// Whether the bound goes as `max_completion_tokens` rather than `max_tokens`.
    pub use_max_completion_tokens: bool,
    /// Whether the turn's temperature is sent.
    pub supports_custom_temperature: bool,
    /// DeepSeek's reasoning depth, `low`, `high` or `max`, nested in its
    /// `thinking` object; the turn's own outranks it.
    pub reasoning_effort: Option<String>,
    /// Refused when set, as Python's config refuses it: the provider applies
    /// its vendor's own rule on a turn with tools (RFC §6.7).
    pub supports_reasoning_effort_with_tools: Option<bool>,
    /// Whether the server honours a `json_schema` response format (RFC
    /// §6.7, Python's `supports_response_schema`): `None` leaves it to the
    /// provider's default, which a server behind a base URL may not keep.
    pub supports_response_schema: Option<bool>,
    /// Whether the server takes a `json_schema` response format beside
    /// function tools (RFC §6.7): `None` leaves it on OpenAI's own endpoint
    /// alone, a grammar-constrained server (vLLM, llama.cpp) unable to call
    /// a tool under the constraint.
    pub supports_response_schema_with_tools: Option<bool>,
    /// The headers every request carries beside the bearer token.
    pub default_headers: Option<BTreeMap<String, String>>,
    /// Members merged into every request's body, the turn's own winning.
    pub extra_body: Option<Metadata>,
    /// DeepSeek's thinking mode forced on or off for every turn; `None` leaves
    /// the model's default, on. The turn's switch outranks it (RFC §6.7).
    pub enable_thinking: Option<bool>,
}

impl DeepSeekConfig {
    /// The vendor whose rules the provider applies.
    pub(crate) const VENDOR: Vendor = Vendor::DeepSeek {
        enable_thinking: None,
    };

    /// The configuration calling DeepSeek's own endpoint with `api_key`
    /// for `model`, with Python's defaults.
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.deepseek.com/v1".to_owned(),
            model: model.into(),
            max_tokens: Some(1024),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            max_retries: 0,
            include_stream_usage: true,
            use_max_completion_tokens: false,
            supports_custom_temperature: true,
            reasoning_effort: None,
            supports_reasoning_effort_with_tools: None,
            supports_response_schema: None,
            supports_response_schema_with_tools: None,
            default_headers: None,
            extra_body: None,
            enable_thinking: None,
        }
    }

    /// The vendor, with what of its rules the configuration states.
    pub(crate) fn vendor(&self) -> Vendor {
        Vendor::DeepSeek {
            enable_thinking: self.enable_thinking,
        }
    }

    /// The OpenAI configuration the provider runs on.
    pub(crate) fn into_openai(self) -> crate::OpenAiConfig {
        into_openai!(self)
    }
}

impl fmt::Debug for DeepSeekConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let api_key = if self.api_key.is_empty() {
            "none"
        } else {
            "hidden"
        };
        f.debug_struct("DeepSeekConfig")
            .field("base_url", &crate::config::shown_url(&self.base_url))
            .field("api_key", &api_key)
            .field("model", &self.model)
            .field("max_tokens", &self.max_tokens)
            .field("reasoning_effort", &self.reasoning_effort)
            .field("enable_thinking", &self.enable_thinking)
            .finish_non_exhaustive()
    }
}

vendor_provider!(
    /// A DeepSeek model (RFC §6.7): Python's `DeepSeekAIProvider(config)`. A
    /// round's reasoning goes back in `reasoning_content`, on every round that
    /// called tools, empty for one that did not reason (RFC §6.4); a reasoning
    /// setting goes in DeepSeek's `thinking` object; its cache hits are read
    /// as DeepSeek reports them.
    DeepSeekAiProvider,
    DeepSeekConfig
);

#[cfg(test)]
mod tests;
