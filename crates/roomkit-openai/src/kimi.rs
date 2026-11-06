//! Kimi: Python's `roomkit.providers.kimi`, its provider over Kimi's
//! OpenAI-compatible Chat Completions API, or a server of your own, and its
//! configuration.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use roomkit::model::Metadata;

use crate::vendor::{Vendor, into_openai, vendor_provider};

/// Kimi's endpoint, or a server serving its weights, and how it is called:
/// Python's `KimiConfig`, its fields `OpenAIConfig`'s, built with
/// [`KimiConfig::new`] and struct update.
///
/// Its `Debug` never prints the API key, nor the base URL's user
/// information, path or query, which may carry credentials.
#[derive(Clone)]
pub struct KimiConfig {
    /// The key a request carries as its bearer token; empty sends none.
    pub api_key: String,
    /// Kimi's endpoint; another URL points at a vLLM or a gateway serving
    /// Kimi's weights, where the server decides the tool names.
    pub base_url: String,
    /// The model asked: required, so that no upgrade changes it.
    pub model: String,
    /// The token bound; `None`, the default, sends none and leaves K3's own:
    /// reasoning counts toward it.
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
    /// Whether the bound goes as `max_completion_tokens`, since Kimi deprecates
    /// `max_tokens`; false for a server that reads `max_tokens` alone.
    pub use_max_completion_tokens: bool,
    /// Whether the turn's temperature is sent: K3 fixes it; true for a server
    /// of your own that honours it.
    pub supports_custom_temperature: bool,
    /// `low`, `high` or `max`; the turn's own outranks it.
    pub reasoning_effort: Option<String>,
    /// Refused when set, as Python's config refuses it: the provider applies
    /// its vendor's own rule on a turn with tools (RFC §6.7).
    pub supports_reasoning_effort_with_tools: Option<bool>,
    /// The headers every request carries beside the bearer token.
    pub default_headers: Option<BTreeMap<String, String>>,
    /// Members merged into every request's body, the turn's own winning.
    pub extra_body: Option<Metadata>,
}

impl KimiConfig {
    /// The vendor whose rules the provider applies.
    pub(crate) const VENDOR: Vendor = Vendor::Kimi;

    /// The configuration calling Kimi's own endpoint with `api_key`
    /// for `model`, with Python's defaults.
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.moonshot.ai/v1".to_owned(),
            model: model.into(),
            max_tokens: None,
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            max_retries: 0,
            include_stream_usage: true,
            use_max_completion_tokens: true,
            supports_custom_temperature: false,
            reasoning_effort: None,
            supports_reasoning_effort_with_tools: None,
            default_headers: None,
            extra_body: None,
        }
    }

    /// The vendor, with what of its rules the configuration states.
    #[expect(
        clippy::unused_self,
        reason = "the providers' macro asks every vendor's configuration, DeepSeek's reading its own"
    )]
    pub(crate) fn vendor(&self) -> Vendor {
        Vendor::Kimi
    }

    /// The OpenAI configuration the provider runs on.
    pub(crate) fn into_openai(self) -> crate::OpenAiConfig {
        into_openai!(self)
    }
}

impl fmt::Debug for KimiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let api_key = if self.api_key.is_empty() {
            "none"
        } else {
            "hidden"
        };
        f.debug_struct("KimiConfig")
            .field("base_url", &crate::config::shown_url(&self.base_url))
            .field("api_key", &api_key)
            .field("model", &self.model)
            .field("max_tokens", &self.max_tokens)
            .field("reasoning_effort", &self.reasoning_effort)
            .finish_non_exhaustive()
    }
}

vendor_provider!(
    /// A Kimi model (RFC §6.7): Python's `KimiAIProvider(config)`, on Kimi's API
    /// or a server of your own. A round's reasoning goes back in
    /// `reasoning_content` for as long as its turn lasts, a single space on a
    /// round that called tools without reasoning (RFC §6.4).
    KimiAiProvider,
    KimiConfig
);

#[cfg(test)]
mod tests;
