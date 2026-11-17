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
//! - [`AiRouter`]: what picks each turn's provider, for a channel built with
//!   [`AiChannel::routed`] (RFC §6.4); [`FallbackRouter`] hands a
//!   generation whose retries are spent to another provider.
//! - [`knowledge`]: the knowledge sources a retrieval memory searches
//!   (RFC §20.2), Python's `roomkit.knowledge`.
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
//!     tool_policy: Some(ToolPolicy::new(roomkit_ai::ToolPolicyOptions {
//!         deny: vec!["admin_*".into()],
//!         ..Default::default()
//!     })?),
//!     ..AiChannelOptions::default()
//! };
//! let agent = AiChannel::new(ChannelId::new("assistant")?, model, options)?;
//! let kit = RoomKit::new(Default::default());
//! kit.register_channel(agent)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod agent;
mod author_images;
mod channel;
mod continuation;
mod error;
mod human_input;
pub mod knowledge;
mod loop_events;
pub mod memory;
mod mock;
pub mod orchestration;
mod plan_event;
mod plan_task;
mod planning;
mod provider;
mod router;
pub mod tasks;
pub use author_images::ImageResolver;
mod handoff;
mod rows;
pub mod sandbox;
mod server_instructions;
mod shutdown;
mod skills;
mod steering;
mod stream;
#[doc(hidden)]
pub mod testing;
mod texts;
mod tool_eviction;
mod tool_loop;
mod tool_search;
mod tool_usage;
mod tools;
mod transcript_content;
mod turn_budget;
mod turn_config;
mod turn_settings;

pub use self::agent::{Agent, AgentOptions, BuildIdentityBlockOptions};
pub use self::channel::{AiChannel, AiChannelOptions};
pub use self::continuation::ContinuationPolicy;
pub use self::error::AiError;
pub use self::handoff::{SetupHandoffOptions, setup_handoff};
pub use self::human_input::{AskOptions, HumanInputToolHandler, HumanInputToolHandlerOptions};
pub use self::loop_events::{
    AiGenerationEvent, AiResponseEvent, DeclaredTool, FromToolOptions, GenerationPurpose,
    ThinkingEvent, ToolDeclarationOrigin, ToolRoundEvent, response_transcript,
};
pub use self::mock::{MockAiProvider, MockAiProviderOptions};
pub use self::plan_event::PlanUpdatedEvent;
pub use self::plan_task::PlanTaskHandle;
pub use self::provider::{
    API_KEY_METADATA_KEY, AiContentPart, AiContext, AiImagePart, AiMessage, AiMessageContent,
    AiProvider, AiResponse, AiRole, AiTextPart, AiThinkingPart, AiToolCall, AiToolCallPart,
    AiToolResultPart, CallIds, Date, DynAiProvider, ModelInfo, ModelPricing, ProviderError,
    ProviderErrorOptions, ProviderStream, RoundTranscript, SPEECH_CAPABILITY, ServedCall,
    StreamDone, StreamEvent, StreamTextDelta, StreamThinkingDelta, StreamToolCall,
    StreamToolCallDelta, StreamUsage, TRANSCRIPTION_CAPABILITY, TextStream, ThinkingBlocks,
    is_context_overflow_message, round_parts, speech_tags, thinking_parts_of, tool_arguments,
    with_speech_tags,
};
#[doc(hidden)]
pub use self::provider::{FirstToken, listing, read_tool_calls};
pub use self::provider::{
    ResponseSchemaError, ResponseSchemaErrorOptions, ResponseSchemaFailure, SchemaMismatchOptions,
    check_portable_schema, schema_mismatch,
};
#[doc(hidden)]
pub use self::provider::{check_schema_answer, checked_stream, schema_for_generate};
pub use self::router::{
    AiRouter, AiRouterFuture, FallbackRouter, RouteReason, RouteRequest, Routed,
};
#[doc(hidden)]
pub use self::skills::dumps;
pub use self::skills::{
    AddMcpServerOptions, ApproveExecution, CopyOptions, DiscoverOptions, LoadedSkill,
    MAX_RESOURCES_PER_SKILL, MAX_TOTAL_SIZE, McpSkill, McpSkillOptions, McpSkillServer,
    McpSkillServerError, McpSkillServerFuture, RequiresMatch, SKILLS_EXTENSION, ScriptExecutor,
    ScriptResult, ServedOrigin, Skill, SkillEntry, SkillEntryKeyError, SkillError,
    SkillExecutionRequest, SkillFileError, SkillMetadata, SkillPathError, SkillRegistry,
    SkillRegistryOptions, SkillResource, SkillResourceSize, SkillResources, SkillToolNames,
    missing_required_tools, parse_entry, parse_tool_names, safe_join_filename, serves_exactly,
    verify_frontmatter, verify_skill_resource,
};
pub use self::steering::SteerOptions;
pub use self::tool_search::{TOOL_FIND_TOOLS, TOOL_LIST_TOOLS, TOOL_SEARCH_INFRA_TOOL_NAMES};
pub use self::tools::{
    AiTool, AiToolHandle, BeforeToolDecision, ExternalToolFuture, ExternalToolHandler,
    ExternalToolHandlerState, FireBeforeHookOptions, FireOnToolHookOptions, OnToolCancelledOptions,
    OnToolRefusedOptions, OnToolResultOptions, PolicyExternalToolHandler,
    PolicyExternalToolHandlerOptions, ProcessToolCallOptions, RoleOverride, RoleOverrideMode,
    RoleOverrideOptions, Tool, ToolCallContext, ToolDecision, ToolEntry, ToolError, ToolFuture,
    ToolHandler, ToolPolicy, ToolPolicyOptions, ToolResult, ToolResultPart, ToolStructuredContent,
    ToolTurnContextOptions, compose_tool_handlers, current_response_metadata,
    current_tool_actor_id, current_tool_allowed_names, current_tool_call, current_tool_chain_depth,
    current_tool_requester, current_tool_room, current_tool_room_id, extract_tools,
    tool_turn_context, turn_report_claim,
};
#[doc(hidden)]
pub use self::tools::{
    ToolContext, Tools, alias_server, arguments, served_tool_name, turn_admits_alias,
};
pub use self::turn_config::AiChannelTurnConfig;
#[doc(inline)]
pub use sandbox::{SandboxExecutor, SandboxResult};
