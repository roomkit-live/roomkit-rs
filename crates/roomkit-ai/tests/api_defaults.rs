//! Python's defaults, each Rust option's (step 5, lot 6): rustdoc shows no
//! default, so each line asserts the default of the Python parameter its
//! comment names, which `make api-parity` checks every defaulted parameter
//! in scope has.

use roomkit_ai::AiChannelOptions;
use roomkit_ai::DiscoverOptions;
use roomkit_ai::MockAiProviderOptions;
use roomkit_ai::knowledge::{IndexOptions, SearchOptions};
use roomkit_ai::memory::{
    BudgetAwareMemoryOptions, CompactingMemoryOptions, EstimateEventTokensOptions,
    HistoryBudgetOptions, IngestOptions, RetrievalMemoryOptions, RetrieveOptions,
    SlidingWindowMemoryOptions, SummarizingMemoryOptions,
};
use std::time::Duration;

#[test]
fn ai_channel_options_defaults_as_python() {
    let defaults = AiChannelOptions::default();
    // roomkit.AIChannel.__init__(max_context_events) = 50
    assert_eq!(defaults.max_context_events, 50);
    // roomkit.AIChannel.__init__(max_empty_retries) = 1
    assert_eq!(defaults.max_empty_retries, 1);
    // roomkit.AIChannel.__init__(max_tokens) = None
    assert!(defaults.max_tokens.is_none());
    // roomkit.AIChannel.__init__(max_tool_rounds) = 50
    assert_eq!(defaults.max_tool_rounds, 50);
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
    // roomkit.providers.ai.MockAIProvider.__init__(ai_responses) = None
    assert!(defaults.ai_responses.is_none());
    // roomkit.providers.ai.MockAIProvider.__init__(responses) = None
    assert_eq!(defaults.responses, Vec::<String>::new());
    // roomkit.providers.ai.MockAIProvider.__init__(streaming) = False
    assert!(!defaults.streaming);
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
}

#[test]
fn ai_channel_room_turn_options_defaults_as_python() {
    let defaults = AiChannelOptions::default();
    // roomkit.AIChannel.__init__(serialize_room_turns) = False
    assert!(!defaults.serialize_room_turns);
    // roomkit.AIChannel.__init__(steer_room_turns) = False
    assert!(!defaults.steer_room_turns);
}
