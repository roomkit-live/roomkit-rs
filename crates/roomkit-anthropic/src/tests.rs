//! Python's provider tests translated at the wire boundary; live.rs exercises the API.
use crate::{AnthropicAiProvider, AnthropicConfig, error, folding::Folding, models, request};
use roomkit_ai::{
    AiContentPart, AiContext, AiImagePart, AiMessage, AiMessageContent, AiProvider, AiRole,
    AiThinkingPart, AiTool, AiToolResultPart, StreamEvent, ToolResult, ToolResultPart,
};
use serde_json::{Value, json};
use std::collections::VecDeque;

fn config(model: &str) -> AnthropicConfig {
    AnthropicConfig {
        enable_prompt_caching: false,
        ..AnthropicConfig::new("secret", model)
    }
}

fn context() -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "hello")],
        ..AiContext::default()
    }
}

// Python: test_anthropic.py::test_config_preserves_legacy_custom_and_explicit_settings
#[test]
fn official_profiles_preserve_legacy_custom_and_explicit_settings() {
    for (model, adaptive, temperature) in [
        ("claude-sonnet-4-6", false, true),
        ("claude-sonnet-4-20250514", false, true),
        ("claude-opus-5-5", true, false),
        ("claude-future-8", true, false),
        ("custom-model", false, true),
    ] {
        let config = config(model);
        assert_eq!(
            (config.adaptive(), config.temperature()),
            (adaptive, temperature)
        );
        let custom = AnthropicConfig {
            base_url: Some("https://gateway.example".into()),
            ..config.clone()
        };
        assert_eq!((custom.adaptive(), custom.temperature()), (false, true));
        let explicit = AnthropicConfig {
            use_adaptive_thinking: Some(false),
            supports_custom_temperature: Some(true),
            ..config
        };
        assert_eq!((explicit.adaptive(), explicit.temperature()), (false, true));
    }
}

// Python: test_anthropic.py::test_the_appended_path_is_dropped
#[test]
fn a_pasted_messages_endpoint_is_normalized_without_dropping_a_bare_v1() {
    for (base, expected) in [
        (
            " https://host/anthropic/v1/messages/ ",
            "https://host/anthropic/v1/messages",
        ),
        ("https://host/v1", "https://host/v1/v1/messages"),
    ] {
        let provider = AnthropicAiProvider::new(AnthropicConfig {
            base_url: Some(base.into()),
            ..config("custom")
        })
        .unwrap();
        assert_eq!(provider.messages.as_str(), expected);
    }
}

// Python: test_anthropic.py::test_thinking_shape_follows_adaptive_flag
#[test]
fn thinking_shapes_and_temperature_follow_the_model_and_turn() {
    let mut context = context();
    context.thinking_budget = Some(1024);
    let legacy = request::body(&config("claude-sonnet-4-6"), &context).unwrap();
    assert_eq!(
        legacy["thinking"],
        json!({"type":"enabled", "budget_tokens":1024})
    );
    assert!(legacy.get("temperature").is_none());
    let modern = request::body(&config("claude-opus-5-5"), &context).unwrap();
    assert_eq!(
        modern["thinking"],
        json!({"type":"adaptive", "display":"summarized"})
    );
    context.thinking_budget = Some(0);
    context.reasoning_effort = Some("max".into());
    let off = request::body(&config("claude-sonnet-5-5"), &context).unwrap();
    assert_eq!(off["thinking"], json!({"type":"between_tools"}));
    assert_eq!(off["output_config"]["effort"], "high");
    let floor = request::body(&config("claude-opus-5-5"), &context).unwrap();
    assert_eq!(floor["output_config"]["effort"], "low");
}

// Python: test_anthropic.py::test_the_effort_shares_output_config_with_the_schema
#[test]
fn schema_and_effort_share_output_config_without_erasing_tools() {
    let mut context = context();
    context.reasoning_effort = Some("minimal".into());
    context.response_schema = Some(
        json!({"type":"object", "properties":{}})
            .as_object()
            .unwrap()
            .clone(),
    );
    context
        .tools
        .push(AiTool::new("lookup", "Look up.").unwrap());
    let body = request::body(&config("claude-opus-5-5"), &context).unwrap();
    assert_eq!(
        body["output_config"],
        json!({"effort":"low", "format":{"type":"json_schema", "schema":{"type":"object", "properties":{}}}})
    );
    assert_eq!(
        body["tools"][0]["input_schema"],
        json!({"type":"object", "properties":{}})
    );
}

// Python: test_anthropic.py::test_image_message_part_still_renders_base64
// Python: test_anthropic.py::test_tool_result_image_renders_content_blocks
#[test]
fn images_and_failed_tool_results_keep_their_native_blocks() {
    let image = AiImagePart::new("data:;base64,aGk");
    let mut result = AiToolResultPart::new(
        "c1",
        "screen",
        ToolResult::Parts(vec![ToolResultPart::Image(image.clone())]),
    );
    result.is_error = true;
    let messages = vec![
        AiMessage::new(
            AiRole::User,
            AiMessageContent::Parts(vec![AiContentPart::Image(image)]),
        ),
        AiMessage::new(
            AiRole::Tool,
            AiMessageContent::Parts(vec![AiContentPart::ToolResult(result)]),
        ),
    ];
    let wire = request::messages(&messages).unwrap();
    let expected = json!({"type":"image", "source":{"type":"base64", "media_type":"image/png", "data":"aGk="}});
    assert_eq!(wire[0]["content"][0], expected);
    assert_eq!(wire[1]["role"], "user");
    assert_eq!(
        wire[1]["content"][0],
        json!({"type":"tool_result", "tool_use_id":"c1", "content":[expected], "is_error":true})
    );
}

// Python: test_anthropic.py::test_thinking_part_round_trip
#[test]
fn signed_and_redacted_blocks_replay_but_unsigned_blocks_do_not() {
    let parts = vec![
        AiContentPart::Thinking(AiThinkingPart::new("unsigned")),
        AiContentPart::Thinking(AiThinkingPart {
            thinking: "signed".into(),
            signature: Some("S".into()),
            redacted: None,
        }),
        AiContentPart::Thinking(AiThinkingPart {
            redacted: Some("opaque".into()),
            ..AiThinkingPart::new("")
        }),
    ];
    let wire = request::messages(&[AiMessage::new(
        AiRole::Assistant,
        AiMessageContent::Parts(parts),
    )])
    .unwrap();
    assert_eq!(
        wire[0]["content"],
        json!([{"type":"thinking", "thinking":"signed", "signature":"S"}, {"type":"redacted_thinking", "data":"opaque"}])
    );
}

// Python: providers/anthropic/request.py::_apply_cache_control
#[test]
fn cache_markers_cover_tools_system_and_two_eligible_messages() {
    let mut context = context();
    context.system_prompt = Some("system".into());
    context
        .messages
        .push(AiMessage::new(AiRole::Assistant, "answer"));
    context.messages.push(AiMessage::new(
        AiRole::Assistant,
        AiMessageContent::Parts(vec![AiContentPart::Thinking(AiThinkingPart {
            signature: Some("S".into()),
            ..AiThinkingPart::new("thought")
        })]),
    ));
    context
        .tools
        .push(AiTool::new("lookup", "Look up.").unwrap());
    let config = AnthropicConfig::new("secret", "claude-sonnet-5-5");
    let body = request::body(&config, &context).unwrap();
    assert_eq!(body.to_string().matches("cache_control").count(), 4);
    assert!(
        body["messages"][2]["content"][0]
            .get("cache_control")
            .is_none()
    );
}

fn feed(folding: &mut Folding, events: &[Value]) -> Vec<StreamEvent> {
    let mut ready = VecDeque::new();
    for event in events {
        folding.event(event, &mut ready, &["secret"]).unwrap();
    }
    ready.into()
}

// Python: test_anthropic.py::test_thinking_signature_captured_from_stream
// Python: test_thinking_blocks.py::test_each_block_keeps_its_text_and_signature
#[test]
fn thinking_deltas_and_signatures_keep_their_block_identity() {
    let events = feed(
        &mut Folding::new("asked"),
        &[
            json!({"type":"message_start", "message":{}}),
            json!({"type":"content_block_start", "index":0, "content_block":{"type":"thinking"}}),
            json!({"type":"content_block_delta", "index":0, "delta":{"type":"thinking_delta", "thinking":"first"}}),
            json!({"type":"content_block_delta", "index":0, "delta":{"type":"signature_delta", "signature":"S0"}}),
            json!({"type":"content_block_start", "index":2, "content_block":{"type":"redacted_thinking", "data":"opaque"}}),
        ],
    );
    let mut blocks = roomkit_ai::ThinkingBlocks::new();
    for event in events {
        if let StreamEvent::ThinkingDelta(delta) = event {
            blocks.add(&delta);
        }
    }
    assert_eq!(blocks.parts().len(), 2);
    assert_eq!(blocks.parts()[0].signature.as_deref(), Some("S0"));
    assert_eq!(blocks.parts()[1].redacted.as_deref(), Some("opaque"));
}

// Python: text_conformance/anthropic_wire.py and tool_blocks.py::remaining
#[test]
fn fragmented_calls_are_not_executable_when_the_output_cap_cuts_them() {
    let events = feed(
        &mut Folding::new("asked"),
        &[
            json!({"type":"message_start", "message":{"usage":{"input_tokens":3, "output_tokens":1}}}),
            json!({"type":"content_block_start", "index":0, "content_block":{"type":"tool_use", "id":"c", "name":"lookup"}}),
            json!({"type":"content_block_delta", "index":0, "delta":{"type":"input_json_delta", "partial_json":"{\"x\":"}}),
            json!({"type":"message_delta", "delta":{"stop_reason":"max_tokens"}, "usage":{"output_tokens":10}}),
            json!({"type":"message_stop"}),
        ],
    );
    let StreamEvent::ToolCall(call) = &events[2] else {
        panic!("missing call")
    };
    assert!(call.partial && !call.garbled);
    let StreamEvent::Done(done) = &events[3] else {
        panic!("missing done")
    };
    assert_eq!(done.usage["output_tokens"], 10);
    assert_eq!(done.usage["input_tokens"], 3);
}

// Python: test_anthropic.py::test_sdk_error_wrapped_in_provider_error
#[test]
fn stream_errors_retain_retryability_and_redact_the_active_key() {
    let mut folding = Folding::new("asked");
    let refused = folding.event(&json!({"type":"error", "error":{"type":"overloaded_error", "message":"secret overloaded"}}), &mut VecDeque::new(), &["secret"]).unwrap_err();
    assert_eq!(refused.status_code, Some(529));
    assert!(refused.retryable);
    assert!(!format!("{refused:?}").contains("secret"));
    let auth = error::response(
        &json!({"error":{"type":"authentication_error", "message":"bad key"}}),
        Some(401),
        &[],
    );
    assert!(!auth.retryable);
}

#[test]
fn the_catalogue_is_complete_and_configuration_debug_is_redacted() {
    assert!(models::all().len() > 10);
    assert!(models::all().iter().all(|model| !model.id.is_empty()));
    let provider = AnthropicAiProvider::new(AnthropicConfig {
        base_url: Some("https://user:password@host/private".into()),
        ..config("claude-sonnet-5-5")
    })
    .unwrap();
    for secret in ["secret", "password", "private", "user"] {
        assert!(!format!("{provider:?}").contains(secret));
    }
    assert!(provider.supports_response_schema_with_tools());
}

mod wire;
