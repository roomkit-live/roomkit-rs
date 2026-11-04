//! A model provider for RoomKit, answering through an OpenAI-compatible
//! Chat Completions endpoint or a Responses endpoint: Python's
//! `roomkit.providers.openai`.
//!
//! [`OpenAiAiProvider`] is Python's `OpenAIAIProvider`, an
//! [`AiProvider`](roomkit_ai::AiProvider) that `roomkit-ai`'s
//! [`AiChannel`](roomkit_ai::AiChannel) runs its turns through: each
//! generation is a streamed completion (server-sent events), the text
//! reaching a streaming channel of the room as it is written. A request
//! carries the channel's system prompt and the conversation RFC §6.4
//! requires, declares the turn's tools, their names checked against
//! OpenAI's rule on OpenAI's own endpoint, and Python's sampling keywords:
//! the turn's temperature unless [`OpenAiConfig::supports_custom_temperature`]
//! is off or Python's model profile leaves it out, the token bound ([`OpenAiConfig::max_tokens`], 1024; none sent for `None`). A failure
//! reaches the loop as a [`ProviderError`](roomkit_ai::ProviderError), by
//! Python's rule of what a retry may get past. A stream is bounded: a line
//! or an event over 1 MiB, or an answer over 4 MiB, fails the turn.
//!
//! ```
//! use roomkit::RoomKit;
//! use roomkit::model::ChannelId;
//! use roomkit_ai::{AiChannel, AiChannelOptions};
//! use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
//!
//! let provider = OpenAiAiProvider::new(OpenAiConfig::new("sk-...", "gpt-6-luna"))?;
//! let options = AiChannelOptions {
//!     system_prompt: Some("You answer a bakery's customers, in a few words.".into()),
//!     max_context_events: 20,
//!     ..AiChannelOptions::default()
//! };
//! let agent = AiChannel::new(ChannelId::new("assistant")?, provider, options)?;
//! let kit = RoomKit::new(Default::default());
//! kit.register_channel(agent)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Chat Completions is the default. To use stateless Responses, select it
//! explicitly; the same base URL and bearer credential are used:
//!
//! ```
//! use roomkit_openai::{OpenAiApi, OpenAiConfig};
//! let config = OpenAiConfig {
//!     api: OpenAiApi::Responses,
//!     reasoning_effort: Some("medium".into()),
//!     ..OpenAiConfig::new("sk-...", "gpt-6-luna")
//! };
//! # let _ = config;
//! ```
//!
//! Responses uses `store: false` and replays supported output items in order
//! inside each tool loop, including encrypted reasoning and assistant message
//! phases. That provider state is separate from room history; later user turns
//! rebuild the ordinary transcript, including earlier calls and their results.
//! This path supports text and function
//! tools and opt-in author image input, not hosted tools. OAuth login, token refresh and
//! credential persistence are host responsibilities.

mod config;
mod credentials;
mod error;
mod models;
mod names;
mod provider;
mod reasoning;
mod request;
mod responses;
mod retry;
mod sse;
mod stream;
mod think_tags;
mod wire;

pub use self::config::{OpenAiApi, OpenAiConfig};
pub use self::credentials::{TokenError, TokenFuture, TokenSource};
pub use self::provider::OpenAiAiProvider;
