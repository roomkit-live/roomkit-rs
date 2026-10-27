//! The AI channel of RoomKit, over any model provider (RFC §6.4, §6.7,
//! Appendix A.9).
//!
//! - [`AiAgent`]: an intelligence channel whose turn runs one tool loop,
//!   the same for every provider (step 4, decision 6): generate, run the
//!   tools the model calls, hand back their results, generate again.
//! - [`AiProvider`]: a model, reached with one call, a request in and the
//!   generation's events out (text, calls, a done event). `roomkit-openai`
//!   implements it for OpenAI-compatible endpoints;
//!   [`testing::ScriptedProvider`] answers from a script.
//! - [`Toolset`] and [`Tools`]: the tools an agent serves, many behind one
//!   handler (step 4, decision 7), and the host's closures.
//! - [`ToolPolicy`]: which tools the agent may call (RFC §21.1).
//! - [`SkillRegistry`]: the skills of the host's directories (RFC §24),
//!   which the agent lists in its system prompt and the model activates
//!   ([`AiAgent::with_skills`]).
//! - The instructions an MCP server publishes, which the system prompt
//!   carries under the host's label for the server, in a block they cannot
//!   close ([`AiAgent::with_server_instructions`], RFC §21.2); the server's
//!   tools are a [`Toolset`] (`roomkit-mcp`).
//!
//! A call runs through its gate (RFC §21.1), then the kit's tool hooks
//! (RFC §9.3), which the agent reaches through its turn
//! ([`roomkit::current_turn`]): `BEFORE_TOOL_USE` before it runs,
//! `ON_TOOL_CALL` once it ended.
//!
//! ```
//! use roomkit::RoomKit;
//! use roomkit::model::ChannelId;
//! use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
//! use roomkit_ai::{AiAgent, ToolDefinition, ToolOutput, ToolPolicy, Tools};
//! use serde_json::json;
//!
//! let lookup = ToolDefinition::new(
//!     "lookup_order",
//!     "Find an order by its number",
//!     json!({ "type": "object", "properties": { "order": { "type": "string" } } }),
//! );
//! let tools = Tools::new().tool(lookup, |_call, _context| async {
//!     Ok(ToolOutput::Json(json!({ "status": "shipped" })))
//! });
//! let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
//! let agent = AiAgent::new(ChannelId::new("assistant")?, model)
//!     .with_system_prompt("You answer a shop's customers.")
//!     .with_tools(tools)?
//!     .with_tool_policy(ToolPolicy::new().deny("admin_*"));
//! let kit = RoomKit::new();
//! kit.register_channel(agent)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod agent;
mod error;
mod provider;
mod rows;
mod server_instructions;
mod skills;
mod stream;
pub mod testing;
mod texts;
mod tool_loop;
mod tools;

pub use self::agent::AiAgent;
pub use self::error::AiError;
pub use self::provider::{
    AiProvider, AiRequest, Done, ProviderError, ProviderEvent, ProviderResponse, ProviderStream,
    ProviderToolCall,
};
pub use self::skills::{Skill, SkillError, SkillRegistry};
pub use self::tools::{
    ToolContext, ToolDefinition, ToolError, ToolOutput, ToolPolicy, Tools, Toolset, arguments,
};
