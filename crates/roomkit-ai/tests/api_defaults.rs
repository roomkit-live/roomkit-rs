//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_ai::AiChannelOptions;
use roomkit_ai::DiscoverOptions;
use roomkit_ai::MockAiProviderOptions;
use roomkit_ai::knowledge::{IndexOptions, MockKnowledgeSourceOptions, SearchOptions};
use roomkit_ai::memory::{
    BudgetAwareMemoryOptions, CompactOptions, CompactingMemoryOptions, EstimateEventTokensOptions,
    HistoryBudgetOptions, IngestOptions, MockMemoryProviderOptions, RetrievalMemoryOptions,
    RetrieveOptions, SlidingWindowMemoryOptions, SummarizingMemoryOptions,
};
use roomkit_ai::{AgentOptions, BuildIdentityBlockOptions};
use roomkit_ai::{FromToolOptions, ToolDeclarationOrigin};
use std::time::Duration;

#[test]
fn mock_memory_and_knowledge_options_defaults_as_python() {
    let memory = MockMemoryProviderOptions::default();
    // roomkit.memory.MockMemoryProvider.__init__(messages) = None
    assert_eq!(memory.messages.len(), 0);
    // roomkit.memory.MockMemoryProvider.__init__(events) = None
    assert_eq!(memory.events.len(), 0);
    // roomkit.memory.MockMemoryProvider.__init__(notes) = None
    assert_eq!(memory.notes.len(), 0);
    // roomkit.knowledge.MockKnowledgeSource.__init__(results) = None
    assert_eq!(MockKnowledgeSourceOptions::default().results.len(), 0);
}

#[test]
fn schema_mismatch_options_defaults_as_python() {
    // roomkit.providers.ai.schema_mismatch(path) = '$'
    assert_eq!(roomkit_ai::SchemaMismatchOptions::default().path, "$");
}

#[test]
fn from_tool_options_defaults_as_python() {
    // roomkit.DeclaredTool.from_tool(origin) = 'always'
    assert_eq!(
        FromToolOptions::default().origin,
        ToolDeclarationOrigin::Always
    );
}

#[test]
fn agent_options_defaults_as_python() {
    let defaults = AgentOptions::default();
    // roomkit.Agent.__init__(name) = None
    assert!(defaults.name.is_none());
    // roomkit.Agent.__init__(role) = None
    assert!(defaults.role.is_none());
    // roomkit.Agent.__init__(description) = None
    assert!(defaults.description.is_none());
    // roomkit.Agent.__init__(purpose) = None
    assert!(defaults.purpose.is_none());
    // roomkit.Agent.__init__(scope) = None
    assert!(defaults.scope.is_none());
    // roomkit.Agent.__init__(voice) = None
    assert!(defaults.voice.is_none());
    // roomkit.Agent.__init__(greeting) = None
    assert!(defaults.greeting.is_none());
    // roomkit.Agent.__init__(language) = None
    assert!(defaults.language.is_none());
    // roomkit.Agent.__init__(auto_greet) = True
    assert!(defaults.auto_greet);
    // roomkit.Agent.__init__(identity_in_prompt) = True
    assert!(defaults.identity_in_prompt);
    // roomkit.Agent.build_identity_block(language) = None
    assert!(BuildIdentityBlockOptions::default().language.is_none());
}

#[test]
fn ai_channel_options_defaults_as_python() {
    let defaults = AiChannelOptions::default();
    // roomkit.AIChannel.__init__(tool_search) = None
    assert_eq!(defaults.tool_search, None);
    // roomkit.AIChannel.__init__(tool_search_pinned) = None
    assert_eq!(defaults.tool_search_pinned.len(), 0);
    // roomkit.AIChannel.__init__(tool_search_threshold) = 20
    assert_eq!(defaults.tool_search_threshold, 20);
    // roomkit.AIChannel.__init__(tool_search_threshold_pct) = 10.0
    assert!((defaults.tool_search_threshold_pct - 10.0).abs() < f64::EPSILON);
    // roomkit.AIChannel.__init__(tool_search_miss_hint) = None
    assert_eq!(defaults.tool_search_miss_hint, None);
    // roomkit.AIChannel.__init__(tool_search_threshold_tokens) = 8000
    assert_eq!(defaults.tool_search_threshold_tokens, Some(8000));
    // roomkit.AIChannel.__init__(max_context_events) = 50
    assert_eq!(defaults.max_context_events, 50);
    // roomkit.AIChannel.__init__(max_empty_retries) = 1
    assert_eq!(defaults.max_empty_retries, 1);
    // roomkit.AIChannel.__init__(max_tokens) = None
    assert!(defaults.max_tokens.is_none());
    // roomkit.AIChannel.__init__(max_tool_rounds) = 50
    assert_eq!(defaults.max_tool_rounds, 50);
    // roomkit.AIChannel.__init__(turn_budget_tokens) = None
    assert!(defaults.turn_budget_tokens.is_none());
    // roomkit.AIChannel.__init__(turn_budget_usd) = None
    assert!(defaults.turn_budget_usd.is_none());
    // roomkit.AIChannel.__init__(reasoning_effort) = None
    assert!(defaults.reasoning_effort.is_none());
    // roomkit.AIChannel.__init__(response_schema) = None
    assert!(defaults.response_schema.is_none());
    // roomkit.AIChannel.__init__(thinking_budget) = None
    assert!(defaults.thinking_budget.is_none());
    // roomkit.AIChannel.__init__(enable_thinking) = None
    assert!(defaults.enable_thinking.is_none());
    // roomkit.AIChannel.__init__(thinking_coalesce_ms) = 80.0
    assert!((defaults.thinking_coalesce_ms - 80.0).abs() < f64::EPSILON);
    // roomkit.AIChannel.__init__(thinking_coalesce_chars) = 256
    assert_eq!(defaults.thinking_coalesce_chars, 256);
    // roomkit.AIChannel.__init__(text_coalesce_ms) = 80.0
    assert!((defaults.text_coalesce_ms - 80.0).abs() < f64::EPSILON);
    // roomkit.AIChannel.__init__(text_coalesce_chars) = 256
    assert_eq!(defaults.text_coalesce_chars, 256);
    // roomkit.AIChannel.__init__(publish_text_deltas) = False
    assert!(!defaults.publish_text_deltas);
    // roomkit.AIChannel.__init__(publish_thinking_text) = False
    assert!(!defaults.publish_thinking_text);
    // roomkit.AIChannel.__init__(config_provider) = None
    assert!(defaults.config_provider.is_none());
    // roomkit.AIChannel.__init__(retry_policy) = None
    assert!(defaults.retry_policy.is_none());
    // roomkit.AIChannel.__init__(tool_timeouts) = None
    assert!(defaults.tool_timeouts.is_empty());
    // roomkit.AIChannel.__init__(describe_empty_event) = None
    assert!(defaults.describe_empty_event.is_none());
    // roomkit.AIChannel.__init__(max_author_images) = 4
    assert_eq!(defaults.max_author_images, Some(4));
    // roomkit.AIChannel.__init__(evict_threshold_tokens) = 5000
    assert_eq!(defaults.evict_threshold_tokens, 5000);
    // roomkit.AIChannel.__init__(memory) = None
    assert!(defaults.memory.is_none());
    // roomkit.AIChannel.__init__(server_instructions) = None
    assert_eq!(defaults.server_instructions, Vec::new());
    // roomkit.AIChannel.__init__(skills) = None
    assert!(defaults.skills.is_none());
    // roomkit.AIChannel.__init__(skills_in_prompt) = True
    assert!(defaults.skills_in_prompt);
    // roomkit.AIChannel.__init__(system_prompt) = None
    assert!(defaults.system_prompt.is_none());
    // roomkit.AIChannel.__init__(temperature) = 0.7
    assert!((defaults.temperature - 0.7).abs() < f64::EPSILON);
    // roomkit.AIChannel.__init__(tool_call_intents) = False
    assert!(!defaults.tool_call_intents);
    // roomkit.AIChannel.__init__(tool_call_intents_required) = False
    assert!(!defaults.tool_call_intents_required);
    // roomkit.AIChannel.__init__(tool_call_intents_only) = None
    assert_eq!(defaults.tool_call_intents_only, None);
    // roomkit.AIChannel.__init__(tool_handler) = None
    assert!(defaults.tool_handler.is_none());
    // roomkit.AIChannel.__init__(tool_loop_timeout_seconds) = 300.0
    assert_eq!(defaults.tool_loop_timeout, Some(Duration::from_secs(300)));
    // roomkit.AIChannel.__init__(tool_loop_warn_after) = 25
    assert_eq!(defaults.tool_loop_warn_after, 25);
    // roomkit.AIChannel.__init__(tool_policy) = None
    assert!(defaults.tool_policy.is_none());
    // roomkit.AIChannel.__init__(tool_timeout_seconds) = 30.0
    assert_eq!(defaults.tool_timeout, Some(Duration::from_secs(30)));
    // roomkit.AIChannel.__init__(tools) = None
    assert!(defaults.tools.is_empty());
    // roomkit.AIChannel.__init__(human_input_handler) = None
    assert!(defaults.human_input_handler.is_none());
}

#[test]
fn human_input_tool_handler_options_defaults_as_python() {
    let defaults = roomkit_ai::HumanInputToolHandlerOptions::default();
    // roomkit.HumanInputToolHandler.__init__(timeout) = 300
    assert_eq!(defaults.timeout, Duration::from_secs(300));
    // roomkit.HumanInputToolHandler.__init__(handler) = None
    assert!(defaults.handler.is_none());
    // roomkit.HumanInputToolHandler.__init__(tool_definitions) = None
    assert_eq!(defaults.tool_definitions, Vec::<roomkit_ai::AiTool>::new());
    // roomkit.HumanInputToolHandler.ask(channel_type) = <ChannelType.AI: 'ai'>
    assert_eq!(
        roomkit_ai::AskOptions::default().channel_type,
        roomkit::model::ChannelType::AI
    );
}

#[test]
fn memory_options_defaults_as_python() {
    // roomkit.memory.SlidingWindowMemory.__init__(max_events) = 50
    assert_eq!(SlidingWindowMemoryOptions::default().max_events, 50);
    // roomkit.MemoryProvider.retrieve(channel_id) = None
    assert!(RetrieveOptions::default().channel_id.is_none());
    // roomkit.MemoryProvider.ingest(channel_id) = None
    assert!(IngestOptions::default().channel_id.is_none());
    // roomkit.MemoryProvider.compact(channel_id) = None
    assert!(CompactOptions::default().channel_id.is_none());
    let budget = BudgetAwareMemoryOptions::default();
    // roomkit.memory.BudgetAwareMemory.__init__(safety_margin_ratio) = 0.15
    assert!((budget.safety_margin_ratio - 0.15).abs() < f64::EPSILON);
    // roomkit.memory.BudgetAwareMemory.__init__(min_events) = 3
    assert_eq!(budget.min_events, 3);
    // roomkit.memory.BudgetAwareMemory.__init__(reserved_tokens) = 0
    assert_eq!(budget.reserved_tokens, 0);
    // roomkit.memory.estimate_event_tokens(text_only) = False
    assert!(!EstimateEventTokensOptions::default().text_only);
    let history = HistoryBudgetOptions::default();
    // roomkit.memory.history_budget(safety_margin_ratio) = 0.15
    assert!((history.safety_margin_ratio - 0.15).abs() < f64::EPSILON);
    // roomkit.memory.history_budget(reserved_tokens) = 0
    assert_eq!(history.reserved_tokens, 0);
    // roomkit.memory.history_budget(messages) = None
    assert!(history.messages.is_none());
    // roomkit.memory.history_budget(current_event) = None
    assert!(history.current_event.is_none());
    // roomkit.memory.history_budget(reply_tokens) = 0
    assert_eq!(history.reply_tokens, 0);
    let compacting = CompactingMemoryOptions::default();
    // roomkit.memory.CompactingMemory.__init__(summary_ratio) = 0.1
    assert!((compacting.summary_ratio - 0.1).abs() < f64::EPSILON);
    // roomkit.memory.CompactingMemory.__init__(safety_margin_ratio) = 0.15
    assert!((compacting.safety_margin_ratio - 0.15).abs() < f64::EPSILON);
    // roomkit.memory.CompactingMemory.__init__(min_events) = 5
    assert_eq!(compacting.min_events, 5);
    // roomkit.memory.CompactingMemory.__init__(summary_cache_ttl_seconds) = 300.0
    assert_eq!(compacting.summary_cache_ttl, Duration::from_mins(5));
    let summarizing = SummarizingMemoryOptions::default();
    // roomkit.memory.SummarizingMemory.__init__(tier1_ratio) = 0.5
    assert!((summarizing.tier1_ratio - 0.5).abs() < f64::EPSILON);
    // roomkit.memory.SummarizingMemory.__init__(tier2_ratio) = 0.85
    assert!((summarizing.tier2_ratio - 0.85).abs() < f64::EPSILON);
    // roomkit.memory.SummarizingMemory.__init__(truncate_chars) = 2000
    assert_eq!(summarizing.truncate_chars, 2000);
    // roomkit.memory.SummarizingMemory.__init__(summary_max_tokens) = 1000
    assert_eq!(summarizing.summary_max_tokens, 1000);
    // roomkit.memory.SummarizingMemory.__init__(min_events) = 5
    assert_eq!(summarizing.min_events, 5);
    // roomkit.memory.SummarizingMemory.__init__(summary_cache_ttl_seconds) = 1800.0
    assert_eq!(summarizing.summary_cache_ttl, Duration::from_mins(30));
    let retrieval = RetrievalMemoryOptions::default();
    // roomkit.memory.RetrievalMemory.__init__(max_results) = 5
    assert_eq!(retrieval.max_results, 5);
    // roomkit.memory.RetrievalMemory.__init__(min_query_length) = 3
    assert_eq!(retrieval.min_query_length, 3);
}

#[test]
fn knowledge_options_defaults_as_python() {
    let search = SearchOptions::default();
    // roomkit.knowledge.KnowledgeSource.search(room_id) = None
    assert!(search.room_id.is_none());
    // roomkit.knowledge.KnowledgeSource.search(limit) = 5
    assert_eq!(search.limit, 5);
    // roomkit.knowledge.KnowledgeSource.index(metadata) = None
    assert!(IndexOptions::default().metadata.is_none());
}

#[test]
fn discover_options_defaults_as_python() {
    let defaults = DiscoverOptions::default();
    // roomkit.SkillRegistry.discover(strict) = True
    assert!(defaults.strict);
}

#[test]
fn mock_ai_provider_options_defaults_as_python() {
    let defaults = MockAiProviderOptions::default();
    // roomkit.providers.ai.MockAIProvider.__init__(tool_call_delta_chunks) = 0
    assert_eq!(defaults.tool_call_delta_chunks, 0);
    // roomkit.providers.ai.MockAIProvider.__init__(ai_responses) = None
    assert!(defaults.ai_responses.is_none());
    // roomkit.providers.ai.MockAIProvider.__init__(responses) = None
    assert_eq!(defaults.responses, Vec::<String>::new());
    // roomkit.providers.ai.MockAIProvider.__init__(streaming) = False
    assert!(!defaults.streaming);
    // roomkit.providers.ai.MockAIProvider.__init__(response_schema) = False
    assert!(!defaults.response_schema);
    // roomkit.providers.ai.MockAIProvider.__init__(response_schema_with_tools) = False
    assert!(!defaults.response_schema_with_tools);
    // roomkit.providers.ai.MockAIProvider.__init__(vision) = False
    assert!(!defaults.vision);
}

#[test]
fn steer_options_defaults_as_python() {
    let defaults = roomkit_ai::SteerOptions::default();
    // roomkit.AIChannel.steer(loop_id) = None
    assert!(defaults.loop_id.is_none());
    // roomkit.AIChannel.steer(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.AIChannel.steer(turn_id) = None
    assert!(defaults.turn_id.is_none());
    // roomkit.Agent.steer(loop_id) = None
    assert!(defaults.loop_id.is_none());
    // roomkit.Agent.steer(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.Agent.steer(turn_id) = None
    assert!(defaults.turn_id.is_none());
}

#[test]
fn ai_channel_room_turn_options_defaults_as_python() {
    let defaults = AiChannelOptions::default();
    // roomkit.AIChannel.__init__(serialize_room_turns) = False
    assert!(!defaults.serialize_room_turns);
    // roomkit.AIChannel.__init__(steer_room_turns) = False
    assert!(!defaults.steer_room_turns);
}

#[test]
fn task_tool_options_defaults_as_python() {
    let status = roomkit_ai::tasks::TaskStatusToolOptions::default();
    // roomkit.tasks.TaskStatusTool.__init__(window) = 500
    assert_eq!(status.window, 500);
    // roomkit.tasks.TaskStatusTool.__init__(limit) = 10
    assert_eq!(status.limit, 10);
    let cancel = roomkit_ai::tasks::CancelTaskToolOptions::default();
    // roomkit.tasks.CancelTaskTool.__init__(window) = 500
    assert_eq!(cancel.window, 500);
}

#[test]
fn model_catalogue_defaults_as_python() {
    let entry = roomkit_ai::ModelInfo::new("m");
    // roomkit.providers.ai.ModelInfo.__init__(display_name) = None
    assert_eq!(entry.display_name, None);
    // roomkit.providers.ai.ModelInfo.__init__(context_window) = None
    assert_eq!(entry.context_window, None);
    // roomkit.providers.ai.ModelInfo.__init__(supports_vision) = None
    assert_eq!(entry.supports_vision, None);
    // roomkit.providers.ai.ModelInfo.__init__(deprecated) = False
    assert!(!entry.deprecated);
    // roomkit.providers.ai.ModelInfo.__init__(capabilities) = <factory>
    assert_eq!(entry.capabilities, Vec::<String>::new());
    // roomkit.providers.ai.ModelInfo.__init__(pricing) = None
    assert_eq!(entry.pricing, None);
    let day = roomkit_ai::Date::new(2026, 8, 5).unwrap();
    let pricing = roomkit_ai::ModelPricing::new(1.0, 2.0, day).unwrap();
    // roomkit.ModelPricing.__init__(cache_read_per_million) = None
    assert_eq!(pricing.cache_read_per_million, None);
    // roomkit.ModelPricing.__init__(cache_write_per_million) = None
    assert_eq!(pricing.cache_write_per_million, None);
    // roomkit.ModelPricing.__init__(image_input_per_million) = None
    assert_eq!(pricing.image_input_per_million, None);
    // roomkit.ModelPricing.__init__(image_output_per_million) = None
    assert_eq!(pricing.image_output_per_million, None);
    // roomkit.ModelPricing.__init__(long_context_threshold_tokens) = None
    assert_eq!(pricing.long_context_threshold_tokens, None);
    // roomkit.ModelPricing.__init__(long_context_input_multiplier) = 1.0
    assert!((pricing.long_context_input_multiplier - 1.0).abs() < f64::EPSILON);
    // roomkit.ModelPricing.__init__(long_context_output_multiplier) = 1.0
    assert!((pricing.long_context_output_multiplier - 1.0).abs() < f64::EPSILON);
    // roomkit.ModelPricing.__init__(currency) = 'USD'
    assert_eq!(pricing.currency, "USD");
}

#[test]
fn setup_handoff_options_defaults_as_python() {
    let defaults = roomkit_ai::SetupHandoffOptions::default();
    // roomkit.setup_handoff(tool) = None
    assert!(defaults.tool.is_none());
    // roomkit.setup_handoff(room_id) = None
    assert!(defaults.room_id.is_none());
}

#[test]
fn setup_delegation_options_defaults_as_python() {
    let defaults = roomkit_ai::tasks::SetupDelegationOptions::default();
    // roomkit.tasks.setup_delegation(tool) = None
    assert!(defaults.tool.is_none());
}

#[test]
fn external_handler_keyword_defaults_as_python() {
    let defaults = roomkit_ai::ProcessToolCallOptions::default();
    // roomkit.tools.ExternalToolHandler.process_tool_call(tool_call_id) = ''
    // roomkit.tools.PolicyExternalToolHandler.process_tool_call(tool_call_id) = ''
    assert_eq!(defaults.tool_call_id, "");
    // roomkit.tools.ExternalToolHandler.process_tool_call(job_id) = None
    // roomkit.tools.PolicyExternalToolHandler.process_tool_call(job_id) = None
    assert!(defaults.job_id.is_none());
    // roomkit.tools.ExternalToolHandler.process_tool_call(session_id) = None
    // roomkit.tools.PolicyExternalToolHandler.process_tool_call(session_id) = None
    assert!(defaults.session_id.is_none());
    // roomkit.tools.ExternalToolHandler.process_tool_call(tenant_id) = None
    // roomkit.tools.PolicyExternalToolHandler.process_tool_call(tenant_id) = None
    assert!(defaults.tenant_id.is_none());
    // roomkit.tools.ExternalToolHandler.process_tool_call(room_id) = None
    // roomkit.tools.PolicyExternalToolHandler.process_tool_call(room_id) = None
    assert!(defaults.room_id.is_none());
    let defaults = roomkit_ai::OnToolResultOptions::default();
    // roomkit.tools.ExternalToolHandler.on_tool_result(is_error) = False
    // roomkit.tools.PolicyExternalToolHandler.on_tool_result(is_error) = False
    assert!(!defaults.is_error);
    // roomkit.tools.ExternalToolHandler.on_tool_result(tool_call_id) = ''
    // roomkit.tools.PolicyExternalToolHandler.on_tool_result(tool_call_id) = ''
    assert_eq!(defaults.tool_call_id, "");
    // roomkit.tools.ExternalToolHandler.on_tool_result(job_id) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_result(job_id) = None
    assert!(defaults.job_id.is_none());
    // roomkit.tools.ExternalToolHandler.on_tool_result(room_id) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_result(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.tools.ExternalToolHandler.on_tool_result(refused_but_ran) = False
    // roomkit.tools.PolicyExternalToolHandler.on_tool_result(refused_but_ran) = False
    assert!(!defaults.refused_but_ran);
    // roomkit.tools.ExternalToolHandler.on_tool_result(error_detail) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_result(error_detail) = None
    assert!(defaults.error_detail.is_none());
    let defaults = roomkit_ai::OnToolCancelledOptions::default();
    // roomkit.tools.ExternalToolHandler.on_tool_cancelled(tool_call_id) = ''
    // roomkit.tools.PolicyExternalToolHandler.on_tool_cancelled(tool_call_id) = ''
    assert_eq!(defaults.tool_call_id, "");
    // roomkit.tools.ExternalToolHandler.on_tool_cancelled(job_id) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_cancelled(job_id) = None
    assert!(defaults.job_id.is_none());
    // roomkit.tools.ExternalToolHandler.on_tool_cancelled(room_id) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_cancelled(room_id) = None
    assert!(defaults.room_id.is_none());
    let defaults = roomkit_ai::OnToolRefusedOptions::default();
    // roomkit.tools.ExternalToolHandler.on_tool_refused(tool_call_id) = ''
    // roomkit.tools.PolicyExternalToolHandler.on_tool_refused(tool_call_id) = ''
    assert_eq!(defaults.tool_call_id, "");
    // roomkit.tools.ExternalToolHandler.on_tool_refused(job_id) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_refused(job_id) = None
    assert!(defaults.job_id.is_none());
    // roomkit.tools.ExternalToolHandler.on_tool_refused(room_id) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_refused(room_id) = None
    assert!(defaults.room_id.is_none());
    // roomkit.tools.ExternalToolHandler.on_tool_refused(detail) = None
    // roomkit.tools.PolicyExternalToolHandler.on_tool_refused(detail) = None
    assert!(defaults.detail.is_none());
    let decision = roomkit_ai::ToolDecision::new(true);
    // roomkit.tools.ToolDecision.__init__(modified_input) = None
    assert!(decision.modified_input.is_none());
    // roomkit.tools.ToolDecision.__init__(result) = None
    assert!(decision.result.is_none());
    // roomkit.tools.ToolDecision.__init__(reason) = ''
    assert_eq!(decision.reason, "");
    // roomkit.tools.ToolDecision.__init__(detail) = None
    assert!(decision.detail.is_none());
}

#[test]
fn external_policy_default_as_python() {
    // roomkit.tools.PolicyExternalToolHandler.__init__(policy) = None
    assert!(
        roomkit_ai::PolicyExternalToolHandlerOptions::default()
            .policy
            .is_none()
    );
}
