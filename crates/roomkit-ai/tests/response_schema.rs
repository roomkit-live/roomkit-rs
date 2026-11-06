//! A response schema's contract, as a provider keeps it (RFC §6.7): Python's
//! `tests/test_response_schema.py`, `TestMockHonoursTheContract` and what
//! `TestAIContextField` says that Rust can, and the channel's own check of
//! its schema (`tests/test_channels/test_ai_response_schema.py`).

use std::error::Error;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::ChannelId;
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiError, AiMessage, AiProvider, AiResponse, AiRole,
    AiTool, MockAiProvider, MockAiProviderOptions, ProviderError, ResponseSchemaError,
    ResponseSchemaErrorOptions, ResponseSchemaFailure, StreamEvent,
};
use serde_json::{Map, Value, json};

/// Python's `VERDICT`, its root's members.
fn verdict() -> Map<String, Value> {
    let Value::Object(schema) = json!({
        "type": "object",
        "properties": {
            "label": { "type": "string", "enum": ["yes", "no"] },
            "confidence": { "type": "number" },
        },
        "required": ["label", "confidence"],
        "additionalProperties": false,
    }) else {
        unreachable!("an object")
    };
    schema
}

/// Python's `ANSWER`.
const ANSWER: &str = r#"{"label": "yes", "confidence": 0.9}"#;

/// A question under `schema`, with `tools`: Python's `_context`.
fn context(schema: Option<Map<String, Value>>, tools: Vec<AiTool>) -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "Is it?")],
        response_schema: schema,
        tools,
        ..AiContext::default()
    }
}

/// A mock answering `responses`, taking a schema, streaming or not.
fn mock(responses: Vec<AiResponse>, response_schema: bool, streaming: bool) -> MockAiProvider {
    MockAiProvider::new(MockAiProviderOptions {
        ai_responses: Some(responses),
        response_schema,
        streaming,
        ..MockAiProviderOptions::default()
    })
}

/// A response saying `text`, ending `finish`.
fn said(text: &str, finish: &str) -> AiResponse {
    AiResponse {
        finish_reason: Some(finish.to_owned()),
        ..AiResponse::new(text)
    }
}

/// The schema's failure `error` carries, when one failed.
fn reason(error: &ProviderError) -> Option<ResponseSchemaFailure> {
    let failure = error.source()?.downcast_ref::<ResponseSchemaError>()?;
    Some(failure.reason)
}

/// The events of `stream`, up to its end or its first failure.
async fn drain(
    mut stream: impl Stream<Item = Result<StreamEvent, ProviderError>> + Unpin,
) -> (Vec<StreamEvent>, Option<ProviderError>) {
    let mut events = Vec::new();
    while let Some(event) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut stream).poll_next(cx)).await
    {
        match event {
            Ok(event) => events.push(event),
            Err(error) => return (events, Some(error)),
        }
    }
    (events, None)
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_generate_returns_the_json_document
#[tokio::test]
async fn generate_returns_the_json_document() {
    let provider = mock(vec![said(ANSWER, "stop")], true, false);
    let response = provider
        .generate(&context(Some(verdict()), Vec::new()))
        .await;
    let response = response.expect("the document satisfies the schema");
    let document: Value = serde_json::from_str(&response.content).expect("JSON");
    assert_eq!(document["label"], "yes");
    assert_eq!(provider.calls()[0].response_schema, Some(verdict()));
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_a_provider_without_support_refuses_before_sending
#[tokio::test]
async fn a_provider_without_support_refuses_before_sending() {
    let provider = mock(vec![said(ANSWER, "stop")], false, false);
    let error = provider
        .generate(&context(Some(verdict()), Vec::new()))
        .await
        .expect_err("refused");
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::Unsupported));
    assert!(error.before_request);
    assert_eq!(provider.calls().len(), 0, "no request kept");
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_tools_in_the_same_turn_are_refused
#[tokio::test]
async fn tools_in_the_same_turn_are_refused() {
    let provider = mock(vec![said(ANSWER, "stop")], true, false);
    let tool = AiTool::new("lookup", "Look it up").expect("a name every vendor takes");
    let error = provider
        .generate(&context(Some(verdict()), vec![tool]))
        .await
        .expect_err("refused");
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::Unsupported));
    assert!(error.to_string().contains("tools"), "{error}");
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_streaming_carries_the_schema_and_checks_the_answer
#[tokio::test]
async fn streaming_carries_the_schema_and_checks_the_answer() {
    let provider = mock(
        vec![said(ANSWER, "stop"), said("Sure.", "stop")],
        true,
        true,
    );
    let asked = context(Some(verdict()), Vec::new());
    let (streamed, failed) = drain(provider.generate_structured_stream(&asked)).await;
    let (_, refused) = drain(provider.generate_structured_stream(&asked)).await;
    assert!(failed.is_none());
    assert!(matches!(streamed.last(), Some(StreamEvent::Done(_))));
    let refused = refused.expect("the second answer is no document");
    assert_eq!(reason(&refused), Some(ResponseSchemaFailure::InvalidJson));
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_an_answer_without_its_document_raises
#[tokio::test]
async fn an_answer_without_its_document_raises() {
    let cases = [
        (said("", "refusal"), ResponseSchemaFailure::Refusal),
        (
            said(r#"{"label": "y"#, "length"),
            ResponseSchemaFailure::Truncated,
        ),
        (
            said(r#"{"label": "y"#, "model_length"),
            ResponseSchemaFailure::Truncated,
        ),
        (
            said("Sure, the answer is yes.", "stop"),
            ResponseSchemaFailure::InvalidJson,
        ),
        (
            said(r#"{"verdict": "yes"}"#, "stop"),
            ResponseSchemaFailure::InvalidJson,
        ),
    ];
    for (scripted, expected) in cases {
        let provider = mock(vec![scripted], true, false);
        let error = provider
            .generate(&context(Some(verdict()), Vec::new()))
            .await
            .expect_err("no document");
        assert_eq!(reason(&error), Some(expected), "{error}");
    }
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_prose_without_a_schema_is_left_alone
#[tokio::test]
async fn prose_without_a_schema_is_left_alone() {
    let provider = mock(vec![said("Sure, the answer is yes.", "stop")], true, false);
    let response = provider.generate(&context(None, Vec::new())).await;
    assert_eq!(response.expect("prose").content, "Sure, the answer is yes.");
}

// Python: tests/test_response_schema.py::TestMockHonoursTheContract::test_the_error_is_a_provider_error_never_retried
#[test]
fn the_error_is_a_provider_error_never_retried() {
    let options = ResponseSchemaErrorOptions {
        provider: "mock".to_owned(),
    };
    let failure = ResponseSchemaError::new("x", ResponseSchemaFailure::Refusal, options);
    let error = ProviderError::from(failure);
    // Only an unsupported schema is refused before any request.
    assert_eq!(
        (
            error.retryable,
            error.provider.as_str(),
            reason(&error),
            error.before_request
        ),
        (false, "mock", Some(ResponseSchemaFailure::Refusal), false)
    );
}

// Python: tests/test_response_schema.py::TestAIContextField::test_refuses_a_non_portable_schema_at_construction
// Rust's context is built by struct update, which runs no check: the
// provider refuses the schema before any request, in the check's words.
#[tokio::test]
async fn a_schema_outside_the_subset_is_refused_before_any_request() {
    let provider = mock(vec![said(ANSWER, "stop")], true, false);
    let mut open = verdict();
    open.insert("additionalProperties".to_owned(), Value::Bool(true));
    let error = provider
        .generate(&context(Some(open), Vec::new()))
        .await
        .expect_err("refused");
    assert!(
        error.to_string().contains("additionalProperties"),
        "{error}"
    );
    assert_eq!(provider.calls().len(), 0, "no request kept");
}

// Python: tests/test_channels/test_ai_response_schema.py::TestResolution::test_a_default_outside_the_portable_subset_fails_at_construction
#[test]
fn a_default_outside_the_portable_subset_fails_at_construction() {
    let Value::Object(schema) = json!({ "type": "object", "properties": {}, "required": [] })
    else {
        unreachable!("an object")
    };
    let options = AiChannelOptions {
        response_schema: Some(schema),
        ..AiChannelOptions::default()
    };
    let channel = ChannelId::new("ai1").expect("an id");
    let built = AiChannel::new(channel, mock(Vec::new(), true, false), options);
    assert!(matches!(
        built,
        Err(AiError::ResponseSchema { reason }) if reason.contains("additionalProperties")
    ));
}
