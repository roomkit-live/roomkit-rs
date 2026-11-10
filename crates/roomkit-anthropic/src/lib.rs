//! Native Anthropic Messages support for RoomKit (RFC §§6.4, 6.7).
//!
//! [`AnthropicAiProvider`] implements [`roomkit_ai::AiProvider`] for use in
//! the same [`roomkit_ai::AiChannel`] as any other model provider. Text, tools,
//! images, signed reasoning, prompt caching and constrained JSON are carried
//! on the Messages API; a generation is always streamed, even when collected
//! by [`roomkit_ai::AiProvider::generate`].
//!
//! ```
//! use roomkit_anthropic::{AnthropicAiProvider, AnthropicConfig};
//! let provider = AnthropicAiProvider::new(AnthropicConfig::new("key", "claude-sonnet-5-5"))?;
//! # let _ = provider;
//! # Ok::<(), roomkit_ai::ProviderError>(())
//! ```
mod config;
mod error;
mod folding;
mod http;
mod models;
mod provider;
mod reasoning;
mod request;
mod retry;
mod stream;

pub use config::AnthropicConfig;
pub use provider::AnthropicAiProvider;

#[cfg(test)]
mod tests;
