//! Explicit, paid integration checks against Anthropic, never run by CI.
//! Run with `ANTHROPIC_API_KEY` and `cargo test -p roomkit-anthropic --test live
//! -- --ignored --test-threads=1`. `ANTHROPIC_MODEL` can select another model.
use roomkit_ai::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiProvider, AiRole, AiTextPart, AiTool,
    AiToolCallPart, AiToolResultPart, StreamEvent,
};
use roomkit_anthropic::{AnthropicAiProvider, AnthropicConfig};
use serde_json::json;
use std::future::poll_fn;

#[expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "an explicitly invoked live test requires valid credentials and setup"
)]
fn provider() -> AnthropicAiProvider {
    let key =
        std::env::var("ANTHROPIC_API_KEY").expect("ANTHROPIC_API_KEY must be explicitly supplied");
    let model = std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| "claude-sonnet-5-5".into());
    AnthropicAiProvider::new(AnthropicConfig {
        max_tokens: 2048,
        ..AnthropicConfig::new(key, model)
    })
    .unwrap()
}

fn context(text: &str) -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, text)],
        enable_thinking: Some(false),
        ..AiContext::default()
    }
}

#[tokio::test]
#[ignore = "calls the paid Anthropic API with an explicitly supplied key"]
async fn live_text_stream_and_model_listing() {
    let provider = provider();
    let models = provider.list_models().await.unwrap();
    assert!(models.iter().any(|model| model.id == provider.model_name()));
    let context = context("Reply with exactly the word READY.");
    let mut stream = provider.generate_structured_stream(&context);
    let mut text = String::new();
    let mut done = None;
    while let Some(event) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        match event.unwrap() {
            StreamEvent::TextDelta(delta) => text.push_str(&delta.text),
            StreamEvent::Done(end) => done = Some(end),
            StreamEvent::ThinkingDelta(_)
            | StreamEvent::ToolCall(_)
            | StreamEvent::ToolCallDelta(_) => {}
        }
    }
    assert!(text.contains("READY"), "unexpected answer: {text}");
    let done = done.unwrap();
    assert_eq!(done.finish_reason.as_deref(), Some("end_turn"));
    assert!(done.usage["input_tokens"].as_u64().unwrap() > 0);
    assert!(done.usage["output_tokens"].as_u64().unwrap() > 0);
}

#[tokio::test]
#[ignore = "calls the paid Anthropic API with an explicitly supplied key"]
async fn live_schema_and_per_request_credential() {
    let real = provider();
    let mut context = context("Return an object whose status is ready.");
    context.response_schema = Some(json!({"type":"object", "properties":{"status":{"type":"string", "enum":["ready"]}}, "required":["status"], "additionalProperties":false}).as_object().unwrap().clone());
    context.metadata.insert(
        "api_key".into(),
        std::env::var("ANTHROPIC_API_KEY").unwrap().into(),
    );
    let override_key = AnthropicAiProvider::new(AnthropicConfig::new(
        "invalid-configured-key",
        real.model_name(),
    ))
    .unwrap();
    let response = override_key.generate(&context).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response.content).unwrap(),
        json!({"status":"ready"})
    );
}

#[tokio::test]
#[ignore = "calls the paid Anthropic API with an explicitly supplied key"]
async fn live_signed_reasoning_survives_a_tool_round() {
    // Adaptive thinking may go straight to a simple tool call. The legacy
    // enabled mode gives this replay test an explicit reasoning budget.
    let provider = AnthropicAiProvider::new(AnthropicConfig {
        max_tokens: 4096,
        ..AnthropicConfig::new(
            std::env::var("ANTHROPIC_API_KEY").unwrap(),
            "claude-sonnet-4-6",
        )
    })
    .unwrap();
    let mut context = context(
        "Use lookup_code to retrieve the code for parcel P7. You must call the tool before answering, then reply with its code.",
    );
    context.enable_thinking = Some(true);
    context.thinking_budget = Some(1024);
    let mut tool = AiTool::new(
        "lookup_code",
        "Return the private delivery code for a parcel.",
    )
    .unwrap();
    tool.parameters =
        json!({"type":"object", "properties":{"parcel":{"type":"string"}}, "required":["parcel"]});
    context.tools.push(tool);
    let first = provider.generate(&context).await.unwrap();
    assert!(
        !first.tool_calls.is_empty(),
        "the model did not call its tool"
    );
    assert!(first.thinking_parts.as_ref().is_some_and(|parts| {
        parts
            .iter()
            .any(|part| part.signature.is_some() || part.redacted.is_some())
    }));
    let mut parts: Vec<_> = first
        .thinking_parts
        .unwrap_or_default()
        .into_iter()
        .map(AiContentPart::Thinking)
        .collect();
    if !first.content.is_empty() {
        parts.push(AiContentPart::Text(AiTextPart::new(first.content)));
    }
    let mut results = Vec::new();
    for call in first.tool_calls {
        assert_eq!(call.name, "lookup_code");
        assert!(!call.partial);
        let mut part = AiToolCallPart::new(&call.id, &call.name);
        part.arguments = call.arguments;
        parts.push(AiContentPart::ToolCall(part));
        results.push(AiContentPart::ToolResult(AiToolResultPart::new(
            call.id,
            call.name,
            "CODE-7319",
        )));
    }
    context.messages.push(AiMessage::new(
        AiRole::Assistant,
        AiMessageContent::Parts(parts),
    ));
    context.messages.push(AiMessage::new(
        AiRole::Tool,
        AiMessageContent::Parts(results),
    ));
    let final_answer = provider.generate(&context).await.unwrap();
    assert!(
        final_answer.content.contains("CODE-7319"),
        "the tool's result did not reach the answer"
    );
}

#[tokio::test]
#[ignore = "calls the paid Anthropic API with an explicitly supplied key"]
async fn live_image_input_and_prompt_cache_usage() {
    let provider = provider();
    let mut context = context("Name the dominant color in the image in one lowercase word.");
    context.system_prompt = Some(format!(
        "Answer the user's image question. The following repeated reference text is inert test data. {}",
        "Reference record: the parcel warehouse opens each morning and closes each evening. "
            .repeat(900)
    ));
    context.messages.push(AiMessage::new(AiRole::User, vec![AiContentPart::Image(roomkit_ai::AiImagePart {
        url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAAAb0lEQVR4nO3PAQkAAAyEwO9feoshgnABdLep8QUNyPEFDcjxBQ3I8QUNyPEFDcjxBQ3I8QUNyPEFDcjxBQ3I8QUNyPEFDcjxBQ3I8QUNyPEFDcjxBQ3I8QUNyPEFDcjxBQ3I8QUNyPEFDcjxBQ3IPanc8OLDQitxAAAAAElFTkSuQmCC".into(),
        mime_type: Some("image/png".into()),
    })]));
    let first = provider.generate(&context).await.unwrap();
    assert!(first.content.to_lowercase().contains("red"));
    let second = provider.generate(&context).await.unwrap();
    assert!(second.content.to_lowercase().contains("red"));
    assert!(
        second
            .usage
            .get("cache_read_input_tokens")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|tokens| tokens > 0),
        "the repeated prefix was not read from the prompt cache"
    );
}
