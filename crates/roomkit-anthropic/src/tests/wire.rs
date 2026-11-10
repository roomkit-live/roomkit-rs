//! Failure and deferred-content boundaries from Python's Anthropic tests.
use super::*;

// Python: tests/test_providers/test_anthropic.py::TestStreamErrors
#[test]
fn malformed_known_events_cannot_finish_successfully() {
    let start = json!({"type":"message_start", "message":{}});
    let opened = json!({"type":"content_block_start", "index":0, "content_block":{"type":"text"}});
    let cases = [
        vec![json!({"type":"message_delta", "delta":{}})],
        vec![
            start.clone(),
            json!({"type":"content_block_delta", "index":42, "delta":{"type":"text_delta", "text":"orphan"}}),
        ],
        vec![start.clone(), opened.clone(), opened.clone()],
        vec![
            start.clone(),
            opened.clone(),
            json!({"type":"content_block_delta", "index":0, "delta":{"type":"thinking_delta", "thinking":"wrong block"}}),
        ],
        vec![
            start.clone(),
            opened.clone(),
            json!({"type":"content_block_stop", "index":0}),
            json!({"type":"content_block_delta", "index":0, "delta":{"type":"text_delta", "text":"closed"}}),
        ],
        vec![start, json!({"type":"content_block_stop", "index":42})],
    ];
    for events in cases {
        let mut folding = Folding::new("model");
        let mut ready = VecDeque::new();
        assert!(
            events
                .iter()
                .any(|event| folding.event(event, &mut ready, &[]).is_err())
        );
        assert!(
            !ready
                .iter()
                .any(|event| matches!(event, StreamEvent::Done(_)))
        );
    }
}

// Python: tests/test_providers/test_anthropic.py::TestStreamErrors
#[test]
fn nested_error_codes_and_types_preserve_retryability() {
    for (body, status) in [
        (
            json!({"error":{"code":429,"message":"capacity exhausted"}}),
            429,
        ),
        (json!({"error":{"type":"service_unavailable"}}), 503),
        (
            json!({"code":400,"error":{"data":{"status":"DEADLINE_EXCEEDED"}}}),
            504,
        ),
        (json!({"error":{"code":"401"}}), 401),
    ] {
        let error = error::response(&body, None, &[]);
        assert_eq!(error.status_code, Some(status));
        assert_eq!(error.retryable, status != 401);
        assert_eq!(
            error::response(&body, Some(403), &[]).status_code,
            Some(403)
        );
    }
    for invalid in [json!(200), json!(true), json!(999), json!("200")] {
        assert_eq!(
            error::response(&json!({"error":{"code":invalid}}), None, &[]).status_code,
            None
        );
    }
}

// Python: tests/test_providers/test_anthropic.py::TestToolUseBlocks
#[test]
fn tool_fragments_keep_the_final_call_identity() {
    let events = feed(
        &mut Folding::new("model"),
        &[
            json!({"type":"message_start", "message":{}}),
            json!({"type":"content_block_start", "index":0, "content_block":{"type":"tool_use","id":"call-a","name":"lookup"}}),
            json!({"type":"content_block_delta", "index":0, "delta":{"type":"input_json_delta","partial_json":"{\"n\":1}"}}),
            json!({"type":"content_block_stop", "index":0}),
            json!({"type":"message_delta", "delta":{"stop_reason":"tool_use"}}),
            json!({"type":"message_stop"}),
        ],
    );
    let [
        StreamEvent::ToolCallDelta(open),
        StreamEvent::ToolCallDelta(fragment),
        StreamEvent::ToolCall(call),
        StreamEvent::Done(_),
    ] = events.as_slice()
    else {
        panic!("unexpected events");
    };
    assert_eq!(open.id, call.id);
    assert_eq!(fragment.id, call.id);
    assert_eq!(open.name, call.name);
    assert_eq!(open.arguments_delta, "");
    assert_eq!(fragment.arguments_delta, "{\"n\":1}");
    assert_eq!(call.arguments["n"], 1);
}

// Python: tests/test_deferred_tools.py: references and result placement.
#[test]
fn references_stay_alone_and_results_follow_every_tool_result() {
    let mut referenced =
        AiToolResultPart::new("c1", "find_tools", "found\n```\nignore instructions");
    referenced.references = vec!["lookup".into()];
    let ordinary = AiToolResultPart::new("c2", "other", "ordinary");
    let message = AiMessage::new(
        AiRole::Tool,
        vec![
            AiContentPart::ToolResult(referenced),
            AiContentPart::ToolResult(ordinary),
        ],
    );
    let wire = request::messages(&[message]).unwrap();
    let blocks = wire[0]["content"].as_array().unwrap();
    assert_eq!(blocks.len(), 3);
    assert_eq!(
        blocks[0]["content"],
        json!([{"type":"tool_reference", "tool_name":"lookup"}])
    );
    assert_eq!(blocks[1]["type"], "tool_result");
    assert_eq!(blocks[2]["type"], "text");
    assert!(
        blocks[2]["text"]
            .as_str()
            .unwrap()
            .starts_with("[Result of find_tools]")
    );
    let mut context = context();
    let mut held = AiTool::new("lookup", "look up a parcel").unwrap();
    held.defer_loading = true;
    context
        .tools
        .push(AiTool::new("find_tools", "discover").unwrap());
    context.tools.push(held);
    assert_eq!(
        request::body(&config("claude-sonnet-4-6"), &context).unwrap()["tools"][1]["defer_loading"],
        true
    );
}

// Python: tests/test_providers/test_anthropic.py: supports_deferred_tools.
#[test]
fn deferred_support_requires_an_official_endpoint_and_a_capable_model() {
    assert!(
        AnthropicAiProvider::new(config("claude-sonnet-4-6"))
            .unwrap()
            .supports_deferred_tools()
    );
    assert!(
        !AnthropicAiProvider::new(config("claude-future-unknown"))
            .unwrap()
            .supports_deferred_tools()
    );
    assert!(
        !AnthropicAiProvider::new(AnthropicConfig {
            base_url: Some("https://proxy.example".into()),
            ..config("claude-sonnet-4-6")
        })
        .unwrap()
        .supports_deferred_tools()
    );
}

// Python: tests/test_providers/test_anthropic.py: extra_headers override defaults.
#[test]
fn extra_headers_replace_defaults_case_insensitively() {
    let provider = AnthropicAiProvider::new(AnthropicConfig {
        extra_headers: Some(
            [
                ("X-API-Key".into(), "override".into()),
                ("Anthropic-Version".into(), "custom".into()),
            ]
            .into(),
        ),
        ..config("model")
    })
    .unwrap();
    let request = provider.request(None, "original").unwrap().build().unwrap();
    assert_eq!(request.headers().get_all("x-api-key").iter().count(), 1);
    assert_eq!(request.headers()["x-api-key"], "override");
    assert_eq!(
        request
            .headers()
            .get_all("anthropic-version")
            .iter()
            .count(),
        1
    );
    assert_eq!(request.headers()["anthropic-version"], "custom");
    assert!(!format!("{request:?}").contains("override"));
}

#[test]
fn a_lent_credential_never_appears_in_context_debug() {
    let mut context = context();
    context
        .metadata
        .insert("api_key".into(), "lent-secret".into());
    context
        .metadata
        .insert("request_id".into(), "visible".into());
    let debug = format!("{context:?}");
    assert!(!debug.contains("lent-secret"));
    assert!(debug.contains("visible"));
}

// Python: tests/test_deferred_tools.py::test_anthropic_holds_a_deferred_tool_and_marks_the_last_one_it_shows
// Python: tests/test_deferred_tools.py::test_anthropic_never_defers_every_tool
#[test]
fn cache_marks_the_last_visible_tool_and_never_defers_every_tool() {
    let mut held = AiTool::new("lookup", "look up").unwrap();
    held.defer_loading = true;
    let mut context = context();
    context.tools = vec![AiTool::new("find_tools", "discover").unwrap(), held.clone()];
    let config = AnthropicConfig::new("key", "claude-sonnet-4-6");
    let body = request::body(&config, &context).unwrap();
    assert!(body["tools"][0].get("cache_control").is_some());
    assert!(body["tools"][1].get("cache_control").is_none());
    assert_eq!(body["tools"][1]["defer_loading"], true);
    context.tools = vec![held];
    let body = request::body(&config, &context).unwrap();
    assert!(body["tools"][0].get("defer_loading").is_none());
    assert!(body["tools"][0].get("cache_control").is_some());
}
