//! An AI agent channel for RoomKit, answering through an OpenAI-compatible
//! Chat Completions endpoint: OpenAI's, or vLLM's, OpenRouter's, DeepSeek's,
//! Mistral's, a local server's.
//!
//! [`OpenAiAgent`] is an intelligence channel (RFC §6.4), `roomkit-ai`'s
//! [`AiAgent`](roomkit_ai::AiAgent) over the Chat Completions wire: each
//! event the room hands it is a turn, one tool loop whose generations are
//! completions, not streamed. The turn sends the agent's system prompt,
//! then the room's recent messages as the conversation RFC §6.4 requires
//! ([`roomkit::transcript`]): each in the role of whoever wrote it, labelled
//! by its speaker once several speak, an instruction marked as the
//! application's. It declares the tools the agent serves
//! ([`OpenAiAgent::with_tools`]), runs the calls the model makes and sends
//! their results back, and the turn's rows re-enter the room as the agent's
//! response: each round's text, each call's start and end. A provider error
//! before any round fails the turn: the room keeps the event, `ON_ERROR`
//! fires, and the caller reads the error on its result.
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
