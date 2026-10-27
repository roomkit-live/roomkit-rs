//! An AI agent channel for RoomKit, answering through an OpenAI-compatible
//! Chat Completions endpoint: OpenAI's, or vLLM's, OpenRouter's, DeepSeek's,
//! Mistral's, a local server's.
//!
//! [`OpenAiAgent`] is an intelligence channel (RFC §6.4), `roomkit-ai`'s
//! [`AiAgent`](roomkit_ai::AiAgent) over the Chat Completions wire: each
//! event the room hands it is a turn, one tool loop whose generations are
//! streamed completions (server-sent events, `stream_options.include_usage`),
//! the text reaching a streaming channel of the room as it is written. The
//! turn sends the agent's system prompt,
//! then the room's recent messages as the conversation RFC §6.4 requires
//! ([`roomkit::transcript`]): each in the role of whoever wrote it, labelled
//! by its speaker once several speak, an instruction marked as the
//! application's. It declares the tools the agent serves
//! ([`OpenAiAgent::with_tools`]), runs the calls the model makes and sends
//! their results back, and the room stores each row of the turn as it
//! ends: each round's text, each call's start and end. A provider error
//! fails the turn, the text it produced kept: the room keeps the event,
//! `ON_ERROR` fires, and the caller reads the error on its result. A
//! stream is bounded: a line or an event over 1 MiB, or an answer over 4
//! MiB, fails the turn.
//!
//! ```
//! use roomkit::RoomKit;
//! use roomkit::model::ChannelId;
//! use roomkit_openai::{OpenAiAgent, OpenAiConfig};
//!
//! let config = OpenAiConfig::new("https://api.openai.com/v1", "sk-...", "gpt-6-luna")
//!     .with_system_prompt("You answer a bakery's customers, in a few words.")
//!     .with_history(20);
//! let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?;
//! let kit = RoomKit::new(Default::default());
//! kit.register_channel(agent)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod agent;
mod config;
mod error;
mod names;
mod sse;
mod stream;
mod wire;

pub use self::agent::OpenAiAgent;
pub use self::config::OpenAiConfig;
pub use self::error::OpenAiError;
