//! An AI agent channel for RoomKit, answering through an OpenAI-compatible
//! Chat Completions endpoint: OpenAI's, or vLLM's, OpenRouter's, DeepSeek's,
//! Mistral's, a local server's.
//!
//! [`OpenAiAgent`] is an intelligence channel (RFC §6.4): each event the
//! room hands it is a turn, one completion that is not streamed and calls
//! no tool. The turn sends the agent's system prompt, then the room's
//! recent messages as the conversation RFC §6.4 requires
//! ([`roomkit::transcript`]): each in the role of whoever wrote it, labelled
//! by its speaker once several speak, an instruction marked as the
//! application's. The answer's text re-enters the room as the agent's
//! response. A provider error fails the turn: the room keeps the event,
//! `ON_ERROR` fires, and the caller reads the error on its result.
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
//! let kit = RoomKit::new();
//! kit.register_channel(agent)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod agent;
mod config;
mod error;
mod wire;

pub use self::agent::OpenAiAgent;
pub use self::config::OpenAiConfig;
pub use self::error::OpenAiError;
