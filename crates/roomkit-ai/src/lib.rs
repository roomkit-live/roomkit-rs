//! The AI channel of RoomKit, over any model provider (RFC §6.4, §6.7,
//! Appendix A.9), in the Python implementation's shape.
//!
//! - [`AiChannel`]: Python's `AIChannel`, an intelligence channel whose
//!   turn runs one tool loop, the same for every provider (step 4,
//!   decision 6): generate, run the tools the model calls, hand back their
//!   results, generate again. [`AiChannelOptions`] holds Python's keywords.
//! - [`AiProvider`]: a model, Python's provider contract: an
//!   [`AiContext`] in, an [`AiResponse`] out, or its structured stream of
//!   [`StreamEvent`]s (text, calls, the end), which the tool loop reads.
//!   `roomkit-openai` implements it for OpenAI-compatible endpoints;
//!   [`MockAiProvider`], Python's `MockAIProvider`, answers from a list.
//! - [`AiTool`], [`ToolHandler`], [`Tool`] and [`ToolEntry`]: the tools a
//!   channel declares, the handler serving them by name, and the tools that
//!   serve themselves; an MCP server gives its tools and its handler
//!   (`roomkit-mcp`).
//! - [`ToolPolicy`]: which tools the model may call (RFC §21.1).
//! - [`memory`]: where a turn reads its history (RFC §20), Python's
//!   `roomkit.memory`: [`MemoryProvider`](memory::MemoryProvider), and
//!   [`SlidingWindowMemory`](memory::SlidingWindowMemory), the default.
//! - [`SkillRegistry`]: the skills of the host's directories (RFC §24),
//!   which the channel lists in its system prompt and the model activates.
//! - The instructions an MCP server publishes, which the system prompt
//!   carries under the host's label for the server, in a block they cannot
//!   close ([`AiChannelOptions::server_instructions`], RFC §21.2).
//!
//! A call runs through its gate (RFC §21.1), then the kit's tool hooks
//! (RFC §9.3), which the channel reaches through its turn
//! ([`roomkit::current_turn_host`]): `BEFORE_TOOL_USE` before it runs,
//! `ON_TOOL_CALL` once it ended. A handler reads the call it serves with
//! [`current_tool_call`].
//!
//! ```
//! use roomkit::RoomKit;
//! use roomkit::model::ChannelId;
//! use roomkit_ai::{
//!     AiChannel, AiChannelOptions, AiTool, MockAiProvider, MockAiProviderOptions, ToolHandler,
//!     ToolPolicy, ToolResult,
//! };
//! use serde_json::json;
//!
//! let lookup = AiTool {
//!     parameters: json!({ "type": "object", "properties": { "order": { "type": "string" } } }),
//!     ..AiTool::new("lookup_order", "Find an order by its number")?
//! };
//! let handler = ToolHandler::new(|_name, _arguments| async {
//!     Ok(ToolResult::Json(json!({ "status": "shipped" })))
//! });
//! let model = MockAiProvider::new(MockAiProviderOptions {
//!     responses: vec!["Hello.".into()],
//!     ..MockAiProviderOptions::default()
//! });
//! let options = AiChannelOptions {
//!     system_prompt: Some("You answer a shop's customers.".into()),
//!     tools: vec![lookup.into()],
//!     tool_handler: Some(handler),
//!     tool_policy: Some(ToolPolicy {
//!         deny: vec!["admin_*".into()],
//!         ..ToolPolicy::default()
//!     }),
//!     ..AiChannelOptions::default()
//! };
//! let agent = AiChannel::new(ChannelId::new("assistant")?, model, options)?;
//! let kit = RoomKit::new(Default::default());
//! kit.register_channel(agent)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod channel;
mod error;
mod human_input;
mod images;
pub mod memory;
mod mock;
mod provider;
pub use images::ImageResolver;
mod rows;
mod server_instructions;
mod skills;
mod steering;
mod stream;
#[doc(hidden)]
pub mod testing;
mod texts;
mod tool_loop;
mod tools;

pub use self::channel::{AiChannel, AiChannelOptions};
pub use self::error::AiError;
pub use self::human_input::{AskOptions, HumanInputToolHandler, HumanInputToolHandlerOptions};
pub use self::mock::{MockAiProvider, MockAiProviderOptions};
#[doc(hidden)]
pub use self::provider::read_tool_calls;
pub use self::provider::{
    AiContentPart, AiContext, AiImagePart, AiMessage, AiMessageContent, AiProvider, AiResponse,
    AiRole, AiTextPart, AiToolCall, AiToolCallPart, AiToolResultPart, ProviderError,
    ProviderErrorOptions, ProviderStream, StreamDone, StreamEvent, StreamTextDelta, StreamToolCall,
    TextStream, tool_arguments,
};
#[doc(hidden)]
pub use self::skills::dumps;
pub use self::skills::{DiscoverOptions, Skill, SkillError, SkillMetadata, SkillRegistry};
pub use self::steering::SteerOptions;
pub use self::tools::{
    AiTool, Tool, ToolCallContext, ToolEntry, ToolError, ToolFuture, ToolHandler, ToolPolicy,
    ToolResult, compose_tool_handlers, current_tool_call,
};
#[doc(hidden)]
pub use self::tools::{ToolContext, Tools, alias_server, arguments, served_tool_name};
