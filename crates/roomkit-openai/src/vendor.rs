//! The vendors whose providers speak OpenAI's Chat Completions with rules of
//! their own (RFC §6.4, §6.7): where a vendor reads a model's earlier
//! reasoning, how it takes a reasoning setting, the tool names it accepts,
//! what its usage reports. Each is Python's provider class of that vendor;
//! [`crate::OpenAiAiProvider`] is OpenAI's.

use roomkit::model::Metadata;
use roomkit_ai::{AiTool, ModelInfo};
use serde_json::{Value, json};

use crate::dialect::{ChatDialect, DEEPSEEK_CHAT, KIMI_CHAT, MINIMAX_CHAT, OPENAI_CHAT};
use crate::error::OpenAiError;
use crate::reasoning::{floored_effort, nearest_level, reasoning_floor};
use crate::request::Request;

/// DeepSeek's endpoint, under the two bases its documentation gives.
pub(crate) const DEEPSEEK_BASE_URLS: [&str; 2] =
    ["https://api.deepseek.com/v1", "https://api.deepseek.com"];
/// MiniMax's OpenAI-compatible endpoint.
pub(crate) const MINIMAX_BASE_URL: &str = "https://api.minimax.io/v1";
/// Kimi's OpenAI-compatible endpoint.
pub(crate) const KIMI_BASE_URL: &str = "https://api.moonshot.ai/v1";

/// The efforts on the scale a vendor's levels are read on, least to most.
const SCALE: [&str; 6] = ["minimal", "low", "medium", "high", "xhigh", "max"];
/// The efforts MiniMax-M3.1-Flash takes (MiniMax's API reference).
const MINIMAX_LEVELS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];
/// The efforts K3 takes (Kimi's API reference).
const KIMI_LEVELS: [&str; 3] = ["low", "high", "max"];

/// The vendor a provider applies the rules of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Vendor {
    /// OpenAI, or any server behind a base URL: Python's `OpenAIAIProvider`.
    OpenAi,
    /// Python's `DeepSeekAIProvider`, with its configuration's
    /// `enable_thinking`.
    DeepSeek { enable_thinking: Option<bool> },
    /// Python's `MiniMaxAIProvider`.
    MiniMax,
    /// Python's `KimiAIProvider`.
    Kimi,
}

/// A rule on the tool names a vendor accepts, as Python's `ToolNameRule`.
pub(crate) struct NameRule {
    vendor: &'static str,
    pattern: &'static str,
    accepted: fn(&str) -> bool,
}

/// OpenAI's rule, which DeepSeek's endpoint applies too (measured by Python
/// on 2026-10-02).
fn letters_digits_dashes(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
}

/// Kimi's rule, from its API reference: a letter or `_` first.
fn kimi_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') && letters_digits_dashes(name)
}

const DEEPSEEK_NAMES: NameRule = NameRule {
    vendor: "deepseek",
    pattern: "[A-Za-z0-9_-]{1,128}",
    accepted: letters_digits_dashes,
};

const KIMI_NAMES: NameRule = NameRule {
    vendor: "kimi",
    pattern: "[a-zA-Z_][a-zA-Z0-9_-]{0,127}",
    accepted: kimi_name,
};

impl NameRule {
    /// The first of `tools` the rule refuses, as an error naming the tool
    /// and the rule.
    pub(crate) fn check(&self, tools: &[AiTool]) -> Result<(), OpenAiError> {
        match tools.iter().find(|tool| !(self.accepted)(&tool.name)) {
            Some(tool) => Err(OpenAiError::ToolName {
                name: tool.name.clone(),
                vendor: self.vendor.to_owned(),
                rule: self.pattern.to_owned(),
            }),
            None => Ok(()),
        }
    }
}

/// What a request carries of the turn's reasoning beside OpenAI's fields:
/// its `reasoning_effort`, and the vendor's own members, which win over a
/// configuration's `extra_body` as Python merges the turn's last.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Sampling {
    pub(crate) effort: Option<String>,
    pub(crate) extra: Metadata,
}

impl Vendor {
    /// Where the vendor reads a replayed round's reasoning.
    pub(crate) fn dialect(self) -> ChatDialect {
        match self {
            Self::OpenAi => OPENAI_CHAT,
            Self::DeepSeek { .. } => DEEPSEEK_CHAT,
            Self::MiniMax => MINIMAX_CHAT,
            Self::Kimi => KIMI_CHAT,
        }
    }

    /// The provider's name, as Python's class names itself.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::OpenAi => "OpenAIAIProvider",
            Self::DeepSeek { .. } => "DeepSeekAIProvider",
            Self::MiniMax => "minimax",
            Self::Kimi => "kimi",
        }
    }

    /// The provider an error names (Python's `_provider_name`).
    pub(crate) fn provider(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::DeepSeek { .. } => "deepseek",
            Self::MiniMax => "minimax",
            Self::Kimi => "kimi",
        }
    }

    /// The vendor's curated catalogue, Python's (`catalog/models.json`).
    pub(crate) fn models(self) -> &'static [ModelInfo] {
        crate::models::catalogue(self.provider())
    }

    /// Whether `base_url` is the vendor's own endpoint, whose rules the
    /// provider knows (Python's `is_vendor_endpoint`).
    pub(crate) fn is_own_endpoint(self, base_url: &str) -> bool {
        let official: &[&str] = match self {
            Self::OpenAi => &[crate::config::OPENAI_BASE_URL],
            Self::DeepSeek { .. } => &DEEPSEEK_BASE_URLS,
            Self::MiniMax => &[MINIMAX_BASE_URL],
            Self::Kimi => &[KIMI_BASE_URL],
        };
        official
            .iter()
            .any(|url| crate::names::same_endpoint(base_url, url))
    }

    /// The tool names the vendor's own endpoint accepts; none where the
    /// vendor states no rule, or behind another base URL, where the server
    /// decides (RFC §6.7). OpenAI's is [`crate::names::check`]'s.
    pub(crate) fn name_rule(self, base_url: &str) -> Option<&'static NameRule> {
        let rule = match self {
            Self::DeepSeek { .. } => &DEEPSEEK_NAMES,
            Self::Kimi => &KIMI_NAMES,
            Self::OpenAi | Self::MiniMax => return None,
        };
        self.is_own_endpoint(base_url).then_some(rule)
    }

    /// Whether the vendor reports its cache hits as DeepSeek does, apart
    /// from OpenAI's `prompt_tokens_details`.
    pub(crate) fn reads_cache_hits(self) -> bool {
        matches!(self, Self::DeepSeek { .. })
    }

    /// Whether the configured model reads images, for a vendor's provider:
    /// as its catalogue states it, else, behind a base URL, the server's to
    /// decide (RFC §6.7).
    pub(crate) fn sees_images(self, entry: Option<&ModelInfo>, base_url: &str) -> bool {
        match entry.and_then(|entry| entry.supports_vision) {
            Some(vision) => vision,
            None => !self.is_own_endpoint(base_url),
        }
    }

    /// What a vendor's request carries of the turn's reasoning, on a turn
    /// with tools as on any other, `entry` the model's catalogue entry and
    /// `configured` the configuration's effort (Python's
    /// `_apply_sampling_kwargs` of each vendor's provider). OpenAI's is the
    /// provider's own ([`crate::OpenAiAiProvider`]).
    pub(crate) fn sampling(
        self,
        entry: Option<&ModelInfo>,
        request: &Request,
        configured: Option<&str>,
    ) -> Sampling {
        match self {
            Self::OpenAi => Sampling::default(),
            Self::DeepSeek { enable_thinking } => deepseek(request, configured, enable_thinking),
            Self::MiniMax => minimax(entry, request, configured),
            Self::Kimi => Sampling {
                effort: levelled(entry, request, configured, &KIMI_LEVELS),
                extra: Metadata::new(),
            },
        }
    }
}

/// The effort on a vendor's catalogued model: the turn's over the
/// configured one, its floor where the turn states off, else the nearest
/// of the `levels` it takes; on a model the catalogue does not name, as
/// stated.
fn levelled(
    entry: Option<&ModelInfo>,
    request: &Request,
    configured: Option<&str>,
    levels: &[&str],
) -> Option<String> {
    let floor = reasoning_floor(entry);
    let effort = floored_effort(
        request.reasoning_effort.as_deref(),
        configured,
        floor,
        request.thinking_off,
    );
    if entry.is_none() {
        return effort;
    }
    nearest_level(effort.as_deref(), levels, &SCALE).map(str::to_owned)
}

/// MiniMax's: M3, whose floor is `none`, stops reasoning through its own
/// `thinking` switch, its `reasoning_effort` being M3.1-Flash's alone.
fn minimax(entry: Option<&ModelInfo>, request: &Request, configured: Option<&str>) -> Sampling {
    let floor = reasoning_floor(entry);
    let effort = floored_effort(
        request.reasoning_effort.as_deref(),
        configured,
        floor,
        request.thinking_off,
    );
    if effort.as_deref() == Some("none") && floor == Some("none") {
        let mut extra = Metadata::new();
        extra.insert("thinking".to_owned(), json!({ "type": "disabled" }));
        return Sampling {
            effort: None,
            extra,
        };
    }
    Sampling {
        effort: levelled(entry, request, configured, &MINIMAX_LEVELS),
        extra: Metadata::new(),
    }
}

/// DeepSeek's `thinking` object, its effort nested in it, never a top-level
/// `reasoning_effort`, which DeepSeek ignores (Python's
/// `DeepSeekAIProvider._resolve_thinking`): the turn's switch over the
/// configured `enable_thinking`, off as `disabled`; nothing when neither the
/// switch nor an effort is stated, so that the model's default applies.
fn deepseek(
    request: &Request,
    configured: Option<&str>,
    enable_thinking: Option<bool>,
) -> Sampling {
    let enabled = request.thinking_switch.or(enable_thinking);
    let effort = request.reasoning_effort.as_deref().or(configured);
    let thinking = match (enabled, effort) {
        (Some(false), _) => Some(json!({ "type": "disabled" })),
        (None, None) => None,
        (_, effort) => {
            let mut object = serde_json::Map::new();
            object.insert("type".to_owned(), Value::from("enabled"));
            if let Some(effort) = effort {
                object.insert("reasoning_effort".to_owned(), Value::from(effort));
            }
            Some(Value::Object(object))
        }
    };
    let mut extra = Metadata::new();
    if let Some(thinking) = thinking {
        extra.insert("thinking".to_owned(), thinking);
    }
    Sampling {
        effort: None,
        extra,
    }
}

#[cfg(test)]
mod tests;

/// The error a vendor's configuration is refused with: Python's validator
/// on a field the vendor's provider would ignore, since it applies its
/// vendor's own rule on a turn with tools (RFC §6.7).
pub(crate) fn refused_tool_turn_setting(config: &str) -> roomkit_ai::ProviderError {
    roomkit_ai::ProviderError::from(format!(
        "{config} takes no supports_reasoning_effort_with_tools: its provider applies its \
         vendor's own rule on a turn with tools"
    ))
}

/// A vendor's provider over [`crate::OpenAiAiProvider`]: its rules, its
/// name, its catalogue; every request and answer as OpenAI's wire carries
/// them.
macro_rules! vendor_provider {
    ($(#[$doc:meta])* $provider:ident, $config:ident) => {
        $(#[$doc])*
        #[derive(Clone, Debug)]
        pub struct $provider {
            inner: $crate::OpenAiAiProvider,
        }

        impl $provider {
            /// The provider `config` configures.
            ///
            /// # Errors
            ///
            /// Fails, a [`roomkit_ai::ProviderError`] not retryable, for a
            /// configuration stating `supports_reasoning_effort_with_tools`,
            /// which the vendor's own rule makes moot, and as
            /// [`crate::OpenAiAiProvider::new`] does.
            pub fn new(config: $config) -> Result<Self, roomkit_ai::ProviderError> {
                if config.supports_reasoning_effort_with_tools.is_some() {
                    return Err($crate::vendor::refused_tool_turn_setting(stringify!($config)));
                }
                let vendor = config.vendor();
                let inner = $crate::OpenAiAiProvider::for_vendor(config.into_openai(), vendor)?;
                Ok(Self { inner })
            }
        }

        impl roomkit_ai::AiProvider for $provider {
            fn name(&self) -> &str {
                self.inner.name()
            }

            fn model_name(&self) -> &str {
                self.inner.model_name()
            }

            fn supports_vision(&self) -> bool {
                self.inner.supports_vision()
            }

            fn available_models() -> Vec<roomkit_ai::ModelInfo> {
                $config::VENDOR.models().to_vec()
            }

            fn catalog_entry(&self) -> Option<roomkit_ai::ModelInfo> {
                self.inner.catalog_entry()
            }

            async fn list_models(
                &self,
            ) -> Result<Vec<roomkit_ai::ModelInfo>, roomkit_ai::ProviderError> {
                self.inner.list_models().await
            }

            fn supports_streaming(&self) -> bool {
                true
            }

            fn supports_structured_streaming(&self) -> bool {
                true
            }

            async fn generate(
                &self,
                context: &roomkit_ai::AiContext,
            ) -> Result<roomkit_ai::AiResponse, roomkit_ai::ProviderError> {
                self.inner.generate(context).await
            }

            fn generate_stream<'a>(
                &'a self,
                context: &'a roomkit_ai::AiContext,
            ) -> roomkit_ai::TextStream<'a> {
                self.inner.generate_stream(context)
            }

            fn generate_structured_stream<'a>(
                &'a self,
                context: &'a roomkit_ai::AiContext,
            ) -> roomkit_ai::ProviderStream<'a> {
                self.inner.generate_structured_stream(context)
            }
        }
    };
}

pub(crate) use vendor_provider;

/// The fields every vendor's configuration shares with
/// [`crate::OpenAiConfig`], as Python's derivative configs inherit them,
/// carried into the OpenAI configuration the provider runs on.
macro_rules! into_openai {
    ($config:expr) => {{
        let config = $config;
        $crate::OpenAiConfig {
            base_url: Some(config.base_url),
            max_tokens: config.max_tokens,
            timeout: config.timeout,
            connect_timeout: config.connect_timeout,
            max_retries: config.max_retries,
            include_stream_usage: config.include_stream_usage,
            use_max_completion_tokens: Some(config.use_max_completion_tokens),
            supports_custom_temperature: Some(config.supports_custom_temperature),
            reasoning_effort: config.reasoning_effort,
            default_headers: config.default_headers,
            extra_body: config.extra_body,
            ..$crate::OpenAiConfig::new(config.api_key, config.model)
        }
    }};
}

pub(crate) use into_openai;
