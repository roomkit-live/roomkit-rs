//! The provider called alone, as a host calls `generate` and
//! `generate_structured_stream` (RFC §6.7): the request each sends, and the
//! `ProviderError` a failure reads as, translated from Python's provider
//! tests.

use std::pin::Pin;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit_ai::{
    AiContentPart, AiContext, AiMessage, AiProvider, AiRole, AiTextPart, ProviderError,
};
use roomkit_openai::{OpenAiAiProvider, OpenAiApi, OpenAiConfig};
use serde_json::{Value, json};

use super::fake::{FakeOpenAi, Reply};

/// Python's `OVERLOAD`, the words of an overloaded server.
const OVERLOAD: &str = "The server is overloaded, please try again later";

async fn provider(replies: Vec<Reply>) -> (OpenAiAiProvider, FakeOpenAi) {
    on(OpenAiApi::ChatCompletions, replies).await
}

async fn on(api: OpenAiApi, replies: Vec<Reply>) -> (OpenAiAiProvider, FakeOpenAi) {
    let fake = FakeOpenAi::start(replies).await;
    let config = OpenAiConfig {
        base_url: Some(fake.base_url().to_owned()),
        api,
        ..OpenAiConfig::new("sk-test", "fake-model")
    };
    (OpenAiAiProvider::new(config).unwrap(), fake)
}

fn hello() -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "Hi")],
        ..AiContext::default()
    }
}

/// The error one generation fails with, `generate` or streamed.
async fn failure(replies: Vec<Reply>, streamed: bool) -> ProviderError {
    failure_on(OpenAiApi::ChatCompletions, replies, streamed).await
}

async fn failure_on(api: OpenAiApi, replies: Vec<Reply>, streamed: bool) -> ProviderError {
    let (provider, _fake) = on(api, replies).await;
    if !streamed {
        return provider.generate(&hello()).await.unwrap_err();
    }
    let context = hello();
    let mut events = provider.generate_structured_stream(&context);
    while let Some(event) = std::future::poll_fn(|cx| Pin::new(&mut events).poll_next(cx)).await {
        if let Err(error) = event {
            return error;
        }
    }
    unreachable!("the stream ended without a failure");
}

fn read(error: &ProviderError) -> (bool, Option<u16>) {
    (error.retryable, error.status_code)
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_generate_success
/// `generate` sends one completion, not streamed: no `stream` nor
/// `stream_options` in its body, as Python's `_complete`
/// (`tasks/probes/step5-review/mcp-openai/py_openai_wire.out`).
#[tokio::test]
async fn generate_success() {
    let (provider, fake) = provider(vec![Reply::completion("Hi there!")]).await;
    let response = provider.generate(&hello()).await.unwrap();
    assert_eq!(
        (
            response.content.as_str(),
            response.finish_reason.as_deref(),
            fake.requests()[0].body.clone()
        ),
        (
            "Hi there!",
            Some("stop"),
            json!({
                "model": "fake-model",
                "messages": [{ "role": "user", "content": "Hi" }],
                "temperature": 0.7,
                "max_tokens": 1024,
            })
        )
    );
}

// Python: tests/text_conformance/test_failures.py::test_a_200_json_error_object_reads_as_its_status
/// A gateway answers a completion with 200 and an error object: the status
/// it names decides (Python's `error_body_failure`).
#[tokio::test]
async fn a_200_json_error_object_reads_as_its_status() {
    let body = json!({ "error": { "message": OVERLOAD, "type": "server_error", "code": 503 } });
    let error = failure(vec![Reply::status(200, &body.to_string())], false).await;
    assert_eq!(read(&error), (true, Some(503)));
}

// Python: tests/text_conformance/test_failures.py::test_an_overload_reads_the_same_as_a_status_and_inside_a_200_stream
/// An overload reads the same as its status and as an error object inside
/// a stream that answered 200: retried, with the status the object names.
#[tokio::test]
async fn an_overload_reads_the_same_as_a_status_and_inside_a_200_stream() {
    let status_body =
        json!({ "error": { "message": "status 503", "type": "server_error", "code": null } });
    let as_status = failure(vec![Reply::status(503, &status_body.to_string())], true).await;
    let event = json!({ "error": { "message": OVERLOAD, "type": "server_error", "code": 503 } });
    let in_stream = failure(vec![Reply::events(&[event])], true).await;
    assert_eq!(
        (read(&as_status), read(&in_stream)),
        ((true, Some(503)), (true, Some(503)))
    );
}

// Python: tests/text_conformance/test_failures.py::test_an_error_in_a_stream_that_names_no_status_is_final
/// Without a status, an error inside a stream is final unless its own
/// words name a transient failure: one that brings no message reads as
/// Python's client words it, "An error occurred during streaming", never
/// as its JSON (a bare `"rate limited"`, a `rate_limit` code no table
/// names).
#[tokio::test]
async fn an_error_in_a_stream_that_names_no_status_is_final() {
    let unnamed = json!({ "error": { "message": "Failed to generate a reply" } });
    let bare = json!({ "error": "rate limited" });
    let coded = json!({ "error": { "code": "rate_limit" } });
    let (unnamed, bare, coded) = (
        failure(vec![Reply::events(&[unnamed])], true).await,
        failure(vec![Reply::events(&[bare])], true).await,
        failure(vec![Reply::events(&[coded])], true).await,
    );
    assert_eq!(
        (read(&unnamed), read(&bare), read(&coded)),
        ((false, None), (false, None), (false, None))
    );
}

/// An error inside a stream whose object names a status, by its type alone
/// or by digits, reads as that status (Python's `_ERROR_TYPE_STATUS`:
/// `server_error` is 500, and `_status_in` reads `"429"`).
#[tokio::test]
async fn an_error_in_a_stream_reads_as_the_status_it_names() {
    let typed =
        json!({ "error": { "message": "Failed to generate a reply", "type": "server_error" } });
    let numbered = json!({ "error": { "message": "Failed", "code": "429" } });
    let (typed, numbered) = (
        failure(vec![Reply::events(&[typed])], true).await,
        failure(vec![Reply::events(&[numbered])], true).await,
    );
    assert_eq!(
        (read(&typed), read(&numbered)),
        ((true, Some(500)), (true, Some(429)))
    );
}

/// A streamed request answered 200 with a JSON error object is final with
/// no status, as Python's client, which reads server-sent events off the
/// body, finds none in it: the status the object names decides only for a
/// completion asked whole.
#[tokio::test]
async fn a_stream_answered_with_a_json_error_object_is_final() {
    let body = json!({ "error": { "message": OVERLOAD, "type": "server_error", "code": 503 } });
    let error = failure(vec![Reply::status(200, &body.to_string())], true).await;
    assert_eq!(read(&error), (false, None));
}

/// A Responses stream that fails reads as the status its error's code
/// names, as every provider reads one (RFC §6.7): `server_error` is a
/// retried 500, a code no table names stays final.
#[tokio::test]
async fn a_failed_responses_stream_reads_as_the_status_its_code_names() {
    let failed = |code: &str| {
        Reply::events(&[
            json!({ "type": "response.failed", "response": { "status": "failed",
            "error": { "code": code, "message": "The server had an error." } } }),
        ])
    };
    let (server, quota) = (
        failure_on(OpenAiApi::Responses, vec![failed("server_error")], true).await,
        failure_on(OpenAiApi::Responses, vec![failed("quota_exceeded")], true).await,
    );
    assert_eq!(
        (read(&server), read(&quota)),
        ((true, Some(500)), (false, None))
    );
}

/// An error answer whose body is past what a turn reads keeps its status:
/// a 503 is still retried, as Python's client reads the status whatever
/// the body (RMK-1087).
#[tokio::test]
async fn an_error_answer_past_the_answer_s_bound_keeps_its_status() {
    let body = "x".repeat(5 * 1024 * 1024);
    let (generated, streamed) = (
        failure(vec![Reply::status(503, &body)], false).await,
        failure(vec![Reply::status(503, &body)], true).await,
    );
    assert_eq!(
        (read(&generated), read(&streamed)),
        ((true, Some(503)), (true, Some(503)))
    );
}

/// The messages a request carries, its system prompt first.
async fn sent(messages: Vec<AiMessage>) -> Vec<Value> {
    let (provider, fake) = provider(vec![Reply::completion("Yes.")]).await;
    let context = AiContext {
        messages,
        ..AiContext::default()
    };
    provider.generate(&context).await.unwrap();
    fake.requests()[0].body["messages"]
        .as_array()
        .unwrap()
        .clone()
}

/// A message of the conversation keeps its role, the system's included,
/// and a message of parts goes as its blocks whatever its role (Python's
/// `chat_request._rendered` and `_plain`, `{"role": message.role, ...}`).
#[tokio::test]
async fn each_message_keeps_its_role_and_its_parts() {
    let parts = |text: &str| {
        AiMessage::new(
            AiRole::Assistant,
            vec![AiContentPart::Text(AiTextPart::new(text))],
        )
    };
    let messages = sent(vec![
        AiMessage::new(AiRole::System, "Answer in French."),
        AiMessage::new(AiRole::User, "Hi"),
        parts("Bonjour."),
    ])
    .await;
    assert_eq!(
        messages,
        [
            json!({ "role": "system", "content": "Answer in French." }),
            json!({ "role": "user", "content": "Hi" }),
            json!({ "role": "assistant", "content": [{ "type": "text", "text": "Bonjour." }] }),
        ]
    );
}
