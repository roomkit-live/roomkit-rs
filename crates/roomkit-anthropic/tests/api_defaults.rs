//! Defaults of the Python Anthropic provider and its neutral tool fields.
use roomkit_ai::{AiTool, AiToolResultPart, StreamToolCallDelta};
use roomkit_anthropic::AnthropicConfig;
use std::time::Duration;

#[test]
fn anthropic_defaults_match_python() {
    let config = AnthropicConfig::new("key", "model");
    // roomkit.providers.anthropic.AnthropicConfig.__init__(max_tokens) = 1024
    assert_eq!(config.max_tokens, 1024);
    // roomkit.providers.anthropic.AnthropicConfig.__init__(timeout) = 60.0
    assert_eq!(config.timeout, Duration::from_secs(60));
    // roomkit.providers.anthropic.AnthropicConfig.__init__(connect_timeout) = 5.0
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
    // roomkit.providers.anthropic.AnthropicConfig.__init__(max_retries) = 0
    assert_eq!(config.max_retries, 0);
    // roomkit.providers.anthropic.AnthropicConfig.__init__(base_url) = None
    assert_eq!(config.base_url, None);
    // roomkit.providers.anthropic.AnthropicConfig.__init__(extra_headers) = None
    assert_eq!(config.extra_headers, None);
    // roomkit.providers.anthropic.AnthropicConfig.__init__(enable_prompt_caching) = True
    assert!(config.enable_prompt_caching);
    // None means the caller left the default unset; the model profile applies.
    // roomkit.providers.anthropic.AnthropicConfig.__init__(use_adaptive_thinking) = False
    assert_eq!(config.use_adaptive_thinking, None);
    // roomkit.providers.anthropic.AnthropicConfig.__init__(supports_custom_temperature) = True
    assert_eq!(config.supports_custom_temperature, None);
}

#[test]
fn deferred_tools_and_fragments_match_python_defaults() {
    // roomkit.providers.ai.AITool.__init__(defer_loading) = False
    assert!(!AiTool::new("lookup", "look up").unwrap().defer_loading);
    // roomkit.providers.ai.AIToolResultPart.__init__(references) = []
    assert_eq!(
        AiToolResultPart::new("c", "lookup", "found")
            .references
            .len(),
        0
    );
    let delta = StreamToolCallDelta::new("c", "lookup");
    // roomkit.providers.ai.StreamToolCallDelta.__init__(index) = 0
    assert_eq!(delta.index, 0);
    // roomkit.providers.ai.StreamToolCallDelta.__init__(arguments_delta) = ''
    assert_eq!(delta.arguments_delta, "");
}
