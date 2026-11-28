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
fn a_turn_has_no_tool_timeout_overrides_by_default() {
    // roomkit.AIChannelTurnConfig.__init__(tool_timeouts) = None
    assert!(
        roomkit_ai::AiChannelTurnConfig::default()
            .tool_timeouts
            .is_none()
    );
}

#[test]
fn sandbox_result_defaults_as_python() {
    let result = roomkit_ai::SandboxResult::new(7);
    // roomkit.SandboxResult.__init__(output) = ''
    assert_eq!(result.output, "");
    // roomkit.SandboxResult.__init__(error) = ''
    assert_eq!(result.error, "");
    // roomkit.SandboxResult.__init__(metadata) = <factory>
    assert!(result.metadata.is_empty());
    assert_eq!(result.exit_code, 7);
    assert!(!result.success());
}

#[test]
fn script_result_defaults_as_python() {
    let result = roomkit_ai::ScriptResult::new(7);
    // roomkit.skills.ScriptResult.__init__(stdout) = ''
    assert_eq!(result.stdout, "");
    // roomkit.skills.ScriptResult.__init__(stderr) = ''
    assert_eq!(result.stderr, "");
    // roomkit.skills.ScriptResult.__init__(success) = True
    assert!(result.success);
    // roomkit.skills.ScriptResult.__init__(metadata) = <factory>
    assert!(result.metadata.is_empty());
    assert_eq!(result.exit_code, 7);
}

struct ScriptArguments;
impl roomkit_ai::ScriptExecutor for ScriptArguments {
    fn execute<'a>(
        &'a self,
        _: &'a roomkit_ai::Skill,
        _: &'a str,
        arguments: Option<&'a roomkit::model::Metadata>,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<roomkit_ai::ScriptResult, roomkit_ai::ToolError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            assert!(arguments.is_none());
            Ok(roomkit_ai::ScriptResult::new(0))
        })
    }
}

#[tokio::test]
async fn script_arguments_default_as_python() {
    use roomkit_ai::ScriptExecutor;
    let skill = roomkit_ai::Skill {
        metadata: roomkit_ai::SkillMetadata::new("local", ""),
        instructions: String::new(),
        path: std::path::PathBuf::new(),
    };
    // roomkit.ScriptExecutor.execute(arguments) = None
    assert_eq!(
        ScriptArguments
            .execute(&skill, "run", None)
            .await
            .unwrap()
            .exit_code,
        0
    );
}

#[test]
fn standalone_skill_helpers_default_as_python() {
    let mut metadata = roomkit_ai::SkillMetadata::new("local", "");
    metadata
        .extra_metadata
        .insert("requires".into(), serde_json::json!("lookup"));
    // roomkit.skills.missing_required_tools(closed) = ()
    assert_eq!(
        roomkit_ai::missing_required_tools(&metadata, ["lookup"], &[], None),
        Ok(Vec::new())
    );
    // roomkit.skills.missing_required_tools(match) = <function serves_exactly>
    assert_eq!(
        roomkit_ai::missing_required_tools(&metadata, ["lookup_extra"], &[], None),
        Ok(vec!["lookup".to_owned()])
    );
    // roomkit.skills.safe_join_filename(kind) = 'file'
    assert!(
        roomkit_ai::safe_join_filename(std::path::Path::new("references"), "../outside", None)
            .is_err_and(|error| error.to_string().starts_with("Invalid file filename"))
    );
}

#[test]
fn skill_registry_options_default_as_python() {
    let options = roomkit_ai::SkillRegistryOptions::default();
    // roomkit.SkillRegistry.__init__(requires_match) = <function serves_exactly>
    assert_eq!(options.requires_match, roomkit_ai::RequiresMatch::default());
    let names = std::collections::HashSet::from(["lookup".to_owned()]);
    assert!(matches!(
        options.requires_match.call("lookup", &names),
        Ok(true)
    ));
    assert!(matches!(
        options.requires_match.call("look*", &names),
        Ok(false)
    ));
    // roomkit.SkillRegistry.copy(marks) = True
    assert!(roomkit_ai::CopyOptions::default().marks);
}

#[test]
fn tool_turn_context_defaults_as_python() {
    let options = roomkit_ai::ToolTurnContextOptions::default();
    // roomkit.tools.tool_turn_context(room_id) = None
    assert!(options.room_id.is_none());
    // roomkit.tools.tool_turn_context(room) = None
    assert!(options.room.is_none());
    // roomkit.tools.tool_turn_context(actor_id) = None
    assert!(options.actor_id.is_none());
    // roomkit.tools.tool_turn_context(tools) = None
    assert!(options.tools.is_none());
    // roomkit.tools.tool_turn_context(chain_depth) = 0
    assert_eq!(options.chain_depth, 0);
    // roomkit.tools.tool_turn_context(call) = None
    assert!(options.call.is_none());
    // roomkit.tools.tool_turn_context(requester) = None
    assert!(options.requester.is_none());
}

#[test]
fn tool_call_structured_content_defaults_as_python() {
    // roomkit.tools.ToolCallContext.__init__(structured_content) = None
    assert!(
        roomkit_ai::ToolCallContext::default()
            .structured_content
            .get()
            .is_none()
    );
}

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
    // roomkit.AIChannel.__init__(speak_policy) = None
    assert!(defaults.speak_policy.is_none());
    // roomkit.AIChannel.__init__(speak_timeout) = 2.0
    assert_eq!(defaults.speak_timeout, Duration::from_secs(2));
    // roomkit.AIChannel.__init__(thinker) = None
    assert!(defaults.thinker.is_none());
    // roomkit.AIChannel.__init__(think_wait) = 1.5
    assert_eq!(defaults.think_wait, Duration::from_millis(1500));
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
    // roomkit.AIChannel.__init__(external_tool_handler) = None
    assert!(defaults.external_tool_handler.is_none());
    // roomkit.AIChannel.__init__(continuation) = None
    assert!(defaults.continuation.is_none());
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
    // roomkit.AIChannel.__init__(enable_planning) = False
    assert!(!defaults.enable_planning);
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
    // roomkit.AIChannel.__init__(publish_tool_call_text) = False
    assert!(!defaults.publish_tool_call_text);
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
    // roomkit.AIChannel.__init__(script_executor) = None
    assert!(defaults.script_executor.is_none());
    // roomkit.AIChannel.__init__(sandbox) = None
    assert!(defaults.sandbox.is_none());
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

#[test]
fn role_policy_defaults_as_python() {
    let role = roomkit_ai::RoleOverrideOptions::default();
    // roomkit.RoleOverride.__init__(allow) = <factory>
    assert_eq!(role.allow, Vec::<String>::new());
    // roomkit.RoleOverride.__init__(deny) = <factory>
    assert_eq!(role.deny, Vec::<String>::new());
    // roomkit.RoleOverride.__init__(mode) = 'restrict'
    assert_eq!(role.mode, roomkit_ai::RoleOverrideMode::Restrict);
    let policy = roomkit_ai::ToolPolicyOptions::default();
    // roomkit.ToolPolicy.__init__(allow) = <factory>
    assert_eq!(policy.allow, Vec::<String>::new());
    // roomkit.ToolPolicy.__init__(deny) = <factory>
    assert_eq!(policy.deny, Vec::<String>::new());
    // roomkit.ToolPolicy.__init__(role_overrides) = <factory>
    assert!(policy.role_overrides.is_empty());
    let policy = roomkit_ai::ToolPolicy::default();
    // roomkit.ToolPolicy.resolve(role) = None
    assert!(
        matches!(policy.resolve(None).unwrap(), std::borrow::Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
    );
}

#[test]
fn served_skill_defaults_as_python() {
    let server = roomkit_ai::AddMcpServerOptions::default();
    // roomkit.SkillRegistry.add_mcp_server(allow_dynamic) = False
    assert!(!server.allow_dynamic);
    // roomkit.SkillRegistry.add_mcp_server(approve_execution) = None
    assert!(server.approve_execution.is_none());
    // roomkit.SkillRegistry.add_mcp_server(execution_tools) = ()
    assert_eq!(server.execution_tools, Vec::<String>::new());
    let skill = roomkit_ai::McpSkillOptions::default();
    // roomkit.MCPSkill.__init__(origin) = ''
    assert_eq!(skill.origin, "");
    // roomkit.MCPSkill.__init__(entry) = None
    assert!(skill.entry.is_none());
    // roomkit.MCPSkill.__init__(files) = <factory>
    assert!(skill.files.is_empty());
    // roomkit.MCPSkill.__init__(read_total) = 0
    assert_eq!(skill.read_total, 0);
    // roomkit.MCPSkill.__init__(retirements) = 0
    assert_eq!(skill.retirements, 0);
}

#[test]
fn speaking_carriers_default_as_python() {
    let decision = roomkit_ai::SpeakDecisionOptions::default();
    // roomkit.SpeakDecision.__init__(reason) = ''
    assert_eq!(decision.reason, "");
    // roomkit.SpeakDecision.__init__(judgments) = <factory>
    assert!(decision.judgments.is_empty());
    // roomkit.SpeakDecision.__init__(notes) = ()
    assert_eq!(decision.notes, Vec::<String>::new());
    // roomkit.SpeakDecision.__init__(final) = False
    assert!(!decision.r#final);
    let thought = roomkit_ai::Thought::default();
    // roomkit.Thought.__init__(text) = ''
    assert_eq!(thought.text, "");
    // roomkit.Thought.__init__(want_to_say) = ()
    assert_eq!(thought.want_to_say, Vec::<String>::new());
    // roomkit.Thought.__init__(urgent) = False
    assert!(!thought.urgent);
}

#[test]
fn speaking_mock_options_default_as_python() {
    let policy = roomkit_ai::MockSpeakPolicyOptions::default();
    // roomkit.MockSpeakPolicy.__init__(decisions) = None
    assert_eq!(policy.decisions, Vec::<roomkit_ai::SpeakDecision>::new());
    // roomkit.MockSpeakPolicy.__init__(delay) = 0.0
    assert!(policy.delay.abs() < f64::EPSILON);
    // roomkit.MockSpeakPolicy.__init__(error) = None
    assert!(policy.error.is_none());
    let thinker = roomkit_ai::MockThinkerOptions::default();
    // roomkit.MockThinker.__init__(thoughts) = None
    assert_eq!(thinker.thoughts, Vec::<roomkit_ai::Thought>::new());
    // roomkit.MockThinker.__init__(delay) = 0.0
    assert!(thinker.delay.abs() < f64::EPSILON);
    // roomkit.MockThinker.__init__(error) = None
    assert!(thinker.error.is_none());
    let classifier = roomkit_ai::classifiers::MockClassifierOptions::default();
    // roomkit.MockClassifier.__init__(answers) = None
    assert_eq!(classifier.answers.len(), 0);
    // roomkit.MockClassifier.__init__(error) = None
    assert!(classifier.error.is_none());
}

#[test]
fn classifier_speaking_options_default_as_python() {
    let options = roomkit_ai::ClassifierSpeakPolicyOptions::default();
    // roomkit.ClassifierSpeakPolicy.__init__(agent_role) = ''
    assert_eq!(options.agent_role, "");
    // roomkit.ClassifierSpeakPolicy.__init__(questions) = None
    assert!(options.questions.is_none());
    // roomkit.ClassifierSpeakPolicy.__init__(languages) = None
    assert!(options.languages.is_none());
    // roomkit.ClassifierSpeakPolicy.__init__(history) = 6
    assert_eq!(options.history, 6);
    // roomkit.ClassifierSpeakPolicy.__init__(proactivity) = 0.5
    assert!((options.proactivity - 0.5).abs() < f64::EPSILON);
    let compose = roomkit_ai::speaking::ComposeOptions::default();
    // roomkit.speaking.compose(alone) = False
    assert!(!compose.alone);
    // roomkit.speaking.compose(proactivity) = 0.5
    assert!((compose.proactivity - 0.5).abs() < f64::EPSILON);
    // roomkit.speaking.compose(urgent) = False
    assert!(!compose.urgent);
    // roomkit.speaking.addressed(alone) = False
    assert!(!roomkit_ai::speaking::AddressedOptions::default().alone);
}

#[test]
fn llm_thinker_options_default_as_python() {
    let options = roomkit_ai::LlmThinkerOptions::default();
    // roomkit.LLMThinker.__init__(instructions) = None
    assert!(options.instructions.is_none());
    // roomkit.LLMThinker.__init__(max_tokens) = 600
    assert_eq!(options.max_tokens, Some(600));
    // roomkit.LLMThinker.__init__(reasoning_effort) = None
    assert!(options.reasoning_effort.is_none());
    // roomkit.LLMThinker.__init__(timeout) = 10.0
    assert!(
        options
            .timeout
            .is_some_and(|value| (value - 10.0).abs() < f64::EPSILON)
    );
}

#[test]
fn classifier_answer_and_question_defaults_as_python() {
    let question = roomkit_ai::classifiers::YesNoQuestion::new("Is it so?");
    // roomkit.YesNoQuestion.__init__(yes) = None
    assert!(question.yes.is_none());
    // roomkit.YesNoQuestion.__init__(no) = None
    assert!(question.no.is_none());
    // roomkit.ChoiceAnswer.__init__(probabilities) = <factory>
    assert!(
        roomkit_ai::classifiers::ChoiceAnswer::new("first")
            .probabilities
            .is_empty()
    );
    // roomkit.ScoreAnswer.__init__(probabilities) = ()
    assert_eq!(
        roomkit_ai::classifiers::ScoreAnswer::new(0.0).probabilities,
        Vec::<f64>::new()
    );
}

#[test]
fn speaking_turn_and_report_fields_default_as_python() {
    use roomkit_ai::speaking::SpeakMode;
    use roomkit_ai::{SpeakDecisionEvent, SpeakTurn, Thought, ThoughtEvent};
    let room = roomkit::model::RoomId::new("room").unwrap();
    let source = roomkit::model::EventSource::new(
        roomkit::model::ChannelId::new("sms").unwrap(),
        roomkit::model::ChannelType::SMS,
    );
    let event =
        roomkit::model::RoomEvent::new(room, source, roomkit::model::TextContent::new("Hello"));
    let turn = SpeakTurn::new(event.clone());
    // roomkit.SpeakTurn.__init__(recent) = ()
    assert_eq!(turn.recent.len(), 0);
    // roomkit.SpeakTurn.__init__(people) = ()
    assert_eq!(turn.people, Vec::<String>::new());
    // roomkit.SpeakTurn.__init__(channel_id) = ''
    assert_eq!(turn.channel_id, "");
    // roomkit.SpeakTurn.__init__(speakers) = <factory>
    assert!(turn.speakers.is_empty());
    // roomkit.SpeakTurn.__init__(thought) = None
    assert!(turn.thought.is_none());
    // roomkit.SpeakTurn.__init__(cut) = None
    assert!(turn.cut.is_none());
    let report = SpeakDecisionEvent::new("", "", event, SpeakMode::Speak.into());
    // roomkit.SpeakDecisionEvent.__init__(duration_ms) = 0
    assert!(report.duration_ms.abs() < f64::EPSILON);
    // roomkit.SpeakDecisionEvent.__init__(asked_again) = False
    assert!(!report.asked_again);
    // roomkit.SpeakDecisionEvent.__init__(thought) = None
    assert!(report.thought.is_none());
    // roomkit.SpeakDecisionEvent.__init__(late) = False
    assert!(!report.late);
    let thought = ThoughtEvent::new("", "", Thought::default());
    // roomkit.ThoughtEvent.__init__(previous) = <factory>
    assert_eq!(thought.previous, Thought::default());
    // roomkit.ThoughtEvent.__init__(duration_ms) = None
    assert!(thought.duration_ms.is_none());
}

#[test]
fn default_chat_dialect_matches_the_standard_openai_dialect() {
    let options = roomkit_ai::ChatDialectOptions::default();
    // roomkit.providers.ai.ChatDialect.__init__(thinking_field) = None
    assert!(options.thinking_field.is_none());
    // roomkit.providers.ai.ChatDialect.__init__(names_tool_results) = False
    assert!(!options.names_tool_results);
    // roomkit.providers.ai.ChatDialect.__init__(flattens_text) = False
    assert!(!options.flattens_text);
    // roomkit.providers.ai.ChatDialect.__init__(round_thinking_required) = False
    assert!(!options.round_thinking_required);
    // roomkit.providers.ai.ChatDialect.__init__(round_thinking_placeholder) = ''
    assert_eq!(options.round_thinking_placeholder, "");
    assert_eq!(
        roomkit_ai::ChatDialect::new(options).unwrap(),
        roomkit_ai::OPENAI_CHAT
    );
}

#[test]
fn a_single_provider_call_is_the_last_by_default() {
    let raw = serde_json::Value::String("{".into());
    // roomkit.providers.ai.call_cut(last) = True
    assert!(roomkit_ai::call_cut(
        &raw,
        None,
        roomkit_ai::CallCutOptions::default()
    ));
    // roomkit.providers.ai.call_partial(last) = True
    assert!(roomkit_ai::CallPartialOptions::default().last);
    // roomkit.providers.ai.call_garbled(last) = True
    assert!(!roomkit_ai::call_garbled(
        &raw,
        None,
        roomkit_ai::CallGarbledOptions::default()
    ));
}

#[test]
fn an_image_s_media_type_falls_back_to_png_by_default() {
    use roomkit_ai::image::{
        ParseDataUriOptions, PayloadMimeTypeOptions, SniffMimeTypeOptions, parse_data_uri,
    };
    // roomkit.providers.image.sniff_mime_type(fallback) = 'image/png'
    assert_eq!(SniffMimeTypeOptions::default().fallback, "image/png");
    // roomkit.providers.image.payload_mime_type(fallback) = 'image/png'
    assert_eq!(PayloadMimeTypeOptions::default().fallback, "image/png");
    // roomkit.providers.image.parse_data_uri(fallback_mime) = None
    assert!(ParseDataUriOptions::default().fallback_mime.is_none());
    let kind = parse_data_uri("data:;base64,QUJD", ParseDataUriOptions::default());
    assert_eq!(kind.map(|(kind, _)| kind), Ok("image/png".to_owned()));
}

#[test]
fn an_image_generation_asks_one_image_of_the_model_s_size_by_default() {
    use roomkit_ai::image::{GenerateOptions, GenerateWithOptionsOptions};
    let generate = GenerateOptions::default();
    // roomkit.ImageProvider.generate(size) = None
    assert!(generate.size.is_none());
    // roomkit.ImageProvider.generate(n) = 1
    assert_eq!(generate.n, 1);
    // roomkit.ImageProvider.generate(reference_images) = None
    assert_eq!(generate.reference_images, Vec::new());
    let with_options = GenerateWithOptionsOptions::default();
    // roomkit.ImageProvider.generate_with_options(size) = None
    assert!(with_options.size.is_none());
    // roomkit.ImageProvider.generate_with_options(n) = 1
    assert_eq!(with_options.n, 1);
    // roomkit.ImageProvider.generate_with_options(reference_images) = None
    assert_eq!(with_options.reference_images, Vec::new());
    // roomkit.ImageProvider.generate_with_options(options) = None
    assert!(with_options.options.is_none());
    // roomkit.ImageProvider.generate_with_options(mask) = None
    assert!(with_options.mask.is_none());
    // roomkit.ImageProvider.generate_with_options(on_progress) = None
    assert!(with_options.on_progress.is_none());
}

#[test]
fn the_mock_image_provider_draws_a_png_with_mock_image_by_default() {
    let options = roomkit_ai::image::MockImageProviderOptions::default();
    // roomkit.MockImageProvider.__init__(images) = None
    assert_eq!(options.images, Vec::<Vec<u8>>::new());
    // roomkit.MockImageProvider.__init__(model) = 'mock-image'
    assert_eq!(options.model, "mock-image");
    // roomkit.MockImageProvider.__init__(mime_type) = 'image/png'
    assert_eq!(options.mime_type, "image/png");
    // roomkit.MockImageProvider.__init__(supports_editing) = True
    assert!(options.supports_editing);
    // roomkit.MockImageProvider.__init__(revised_prompt) = None
    assert!(options.revised_prompt.is_none());
}

#[test]
fn image_models_default_as_python() {
    use roomkit_ai::image::{
        ImageAttempt, ImageAttemptStatus, ImageCapabilities, ImageModelInfo, ImageOptions,
        ImageResult,
    };
    let verified = roomkit_ai::Date::new(2026, 11, 28).unwrap();
    for capabilities in [
        ImageCapabilities::new(verified),
        serde_json::from_value(serde_json::json!({"verified": "2026-11-28"})).unwrap(),
    ] {
        // roomkit.ImageCapabilities.__init__(max_images) = 10
        assert_eq!(capabilities.max_images, Some(10));
        // roomkit.ImageCapabilities.__init__(max_references) = 0
        assert_eq!(capabilities.max_references, 0);
        // roomkit.ImageCapabilities.__init__(flexible_size) = False
        assert!(!capabilities.flexible_size);
        // roomkit.ImageCapabilities.__init__(mask) = False
        assert!(!capabilities.mask);
        // roomkit.ImageCapabilities.__init__(continuity) = False
        assert!(!capabilities.continuity);
        // roomkit.ImageCapabilities.__init__(streaming) = False
        assert!(!capabilities.streaming);
        // roomkit.ImageCapabilities.__init__(retirement_date) = None
        assert!(capabilities.retirement_date.is_none());
        // roomkit.ImageCapabilities.__init__(options) = <factory>
        assert_eq!(capabilities.options, Vec::<String>::new());
        assert_eq!(capabilities.sizes, Vec::<String>::new());
        assert_eq!(capabilities.search_types, Vec::<String>::new());
    }
    let attempt = ImageAttempt::default();
    // roomkit.ImageAttempt.__init__(index) = 0
    assert_eq!(attempt.index, 0);
    // roomkit.ImageAttempt.__init__(status) = 'started'
    assert_eq!(attempt.status, ImageAttemptStatus::Started);
    // roomkit.ImageAttempt.__init__(provider_request_id) = None
    assert!(attempt.provider_request_id.is_none());
    // roomkit.ImageAttempt.__init__(results) = <factory>
    assert_eq!(attempt.results, Vec::new());
    // roomkit.ImageAttempt.__init__(error) = None
    assert!(attempt.error.is_none());
    // roomkit.ImageAttempt.__init__(status_code) = None
    assert!(attempt.status_code.is_none());
    assert!(attempt.usage.is_empty());
    let entry = ImageModelInfo::new("gpt-image-2", ImageCapabilities::new(verified));
    // roomkit.ImageModelInfo.__init__(deprecated) = False
    assert!(!entry.deprecated);
    // roomkit.ImageModelInfo.__init__(aliases) = <factory>
    assert_eq!(entry.aliases, Vec::<String>::new());
    // roomkit.ImageModelInfo.__init__(pricing) = None
    assert!(entry.pricing.is_none());
    assert!(entry.display_name.is_none() && entry.context_window.is_none());
    assert!(entry.supports_vision.is_none());
    assert_eq!(entry.capabilities, Vec::<String>::new());
    let result = ImageResult::new("data:image/png;base64,QUJD", "image/png").unwrap();
    // roomkit.ImageResult.__init__(revised_prompt) = None
    assert!(result.revised_prompt.is_none());
    // roomkit.ImageResult.__init__(usage) = <factory>
    assert!(result.usage.is_empty());
    // roomkit.ImageResult.__init__(attempt_id) = None
    assert!(result.attempt_id.is_none());
    // roomkit.ImageResult.__init__(width) = None
    assert!(result.width.is_none() && result.height.is_none());
    // roomkit.ImageOptions.__init__(quality) = None, and every other control
    assert_eq!(ImageOptions::default().supplied(), Vec::<&str>::new());
}
