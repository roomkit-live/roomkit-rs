//! A request's failures on the wire: what each reads as for a channel's
//! retry policy, translated from Python's
//! `tests/test_provider_transport_retryable.py`, its OpenAI rows, and what
//! `OpenAiConfig::max_retries` tries again, named after what Python's openai
//! SDK does (`openai._base_client`, 2.54).

use std::pin::Pin;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit_ai::{AiContext, AiMessage, AiProvider, AiRole, ProviderError, StreamEvent};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::json;

use super::fake::{FakeOpenAi, Reply};

/// A wait a retried answer asks for, so that a test takes no backoff.
const SOON: (&str, &str) = ("retry-after-ms", "1");

fn soon(reply: Reply) -> Reply {
    reply.with_header(SOON.0, SOON.1)
}

fn config(base_url: &str, max_retries: u32) -> OpenAiConfig {
    OpenAiConfig {
        base_url: Some(base_url.to_owned()),
        max_retries,
        ..OpenAiConfig::new("sk-test", "fake-model")
    }
}

async fn on(replies: Vec<Reply>, max_retries: u32) -> (OpenAiAiProvider, FakeOpenAi) {
    let fake = FakeOpenAi::start(replies).await;
    let provider = OpenAiAiProvider::new(config(fake.base_url(), max_retries)).unwrap();
    (provider, fake)
}

fn hello() -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "Hi")],
        ..AiContext::default()
    }
}

/// The text a generation answered, or its failure, through `generate` or
/// its stream.
async fn answer(provider: &OpenAiAiProvider, streamed: bool) -> Result<String, ProviderError> {
    let context = hello();
    if !streamed {
        return provider
            .generate(&context)
            .await
            .map(|response| response.content);
    }
    let mut events = provider.generate_structured_stream(&context);
    let mut said = String::new();
    while let Some(event) = std::future::poll_fn(|cx| Pin::new(&mut events).poll_next(cx)).await {
        if let StreamEvent::TextDelta(delta) = event? {
            said.push_str(&delta.text);
        }
    }
    Ok(said)
}

fn events(text: &str) -> Reply {
    Reply::events(&[json!({
        "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": null }],
    })])
}

/// What answers a generation asked whole or streamed.
fn answering(streamed: bool, text: &str) -> Reply {
    if streamed {
        events(text)
    } else {
        Reply::completion(text)
    }
}

fn status_of(error: &ProviderError) -> (bool, Option<u16>) {
    (error.retryable, error.status_code)
}

// Python: tests/test_provider_transport_retryable.py::test_a_transport_failure_is_retryable
#[tokio::test]
async fn a_transport_failure_is_retryable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refused = format!("http://{}/v1", listener.local_addr().unwrap());
    drop(listener);
    for streamed in [false, true] {
        let provider = OpenAiAiProvider::new(config(&refused, 0)).unwrap();
        let error = answer(&provider, streamed).await.unwrap_err();
        assert_eq!(status_of(&error), (true, None), "refused {streamed}");

        let (provider, _fake) = on(vec![Reply::dropped_before_its_first_event()], 0).await;
        let error = answer(&provider, streamed).await.unwrap_err();
        assert_eq!(status_of(&error), (true, None), "dropped {streamed}");
    }
}

// Python: tests/test_provider_transport_retryable.py::test_a_client_error_stays_final
#[tokio::test]
async fn a_client_error_stays_final() {
    for streamed in [false, true] {
        let reply = Reply::status(400, r#"{"error": {"message": "bad request"}}"#);
        let (provider, _fake) = on(vec![reply], 0).await;
        let error = answer(&provider, streamed).await.unwrap_err();
        assert_eq!(status_of(&error), (false, Some(400)), "{streamed}");
    }
}

/// Python's `OpenAIConfig.max_retries = 0`: no try of the provider's own,
/// the channel's retry policy being the layer that retries.
#[tokio::test]
async fn by_default_a_request_is_tried_once() {
    assert_eq!(OpenAiConfig::new("k", "m").max_retries, 0);
    for streamed in [false, true] {
        let (provider, fake) = on(vec![soon(Reply::status(503, "{}"))], 0).await;
        let error = answer(&provider, streamed).await.unwrap_err();
        assert_eq!(status_of(&error), (true, Some(503)));
        assert_eq!(fake.requests().len(), 1);
    }
}

#[tokio::test]
async fn a_status_worth_a_retry_is_tried_again_until_it_answers() {
    for streamed in [false, true] {
        let replies = vec![
            soon(Reply::status(429, "{}")),
            soon(Reply::status(503, "{}")),
            answering(streamed, "Recovered"),
        ];
        let (provider, fake) = on(replies, 2).await;
        assert_eq!(answer(&provider, streamed).await.unwrap(), "Recovered");
        assert_eq!(fake.requests().len(), 3, "{streamed}");
    }
}

#[tokio::test]
async fn retries_spent_the_last_try_s_failure_is_the_request_s() {
    let first = Reply::status(503, r#"{"error": {"message": "first"}}"#);
    let last = Reply::status(500, r#"{"error": {"message": "last"}}"#);
    let replies = vec![soon(first), soon(last)];
    let (provider, fake) = on(replies, 1).await;
    let error = answer(&provider, true).await.unwrap_err();
    assert_eq!(status_of(&error), (true, Some(500)));
    assert!(error.to_string().contains("last"), "{error}");
    assert_eq!(fake.requests().len(), 2);
}

#[tokio::test]
async fn a_status_not_worth_a_retry_is_final() {
    let (provider, fake) = on(vec![soon(Reply::status(400, "{}"))], 3).await;
    let error = answer(&provider, true).await.unwrap_err();
    assert_eq!(status_of(&error), (false, Some(400)));
    assert_eq!(fake.requests().len(), 1);
}

/// The SDK's `_should_retry`: the endpoint's `x-should-retry` over the
/// status, and a wait past two minutes over both.
#[tokio::test]
async fn the_endpoint_s_headers_decide_over_the_status() {
    let replies = vec![
        soon(Reply::status(400, "{}")).with_header("x-should-retry", "true"),
        answering(true, "Recovered"),
    ];
    let (provider, fake) = on(replies, 1).await;
    assert_eq!(answer(&provider, true).await.unwrap(), "Recovered");
    assert_eq!(fake.requests().len(), 2);

    for final_answer in [
        Reply::status(503, "{}").with_header("x-should-retry", "false"),
        Reply::status(429, "{}").with_header("retry-after", "121"),
    ] {
        let (provider, fake) = on(vec![final_answer, answering(true, "never")], 3).await;
        assert!(answer(&provider, true).await.is_err());
        assert_eq!(fake.requests().len(), 1);
    }
}

/// The SDK's `except Exception` around its send: a connection that closed
/// with no answer is tried again, after its backoff.
#[tokio::test]
async fn a_connection_that_closed_is_tried_again() {
    let fake = FakeOpenAi::start(vec![Reply::closed(), answering(true, "Recovered")]).await;
    let provider = OpenAiAiProvider::new(config(fake.base_url(), 1)).unwrap();
    assert_eq!(answer(&provider, true).await.unwrap(), "Recovered");
    assert_eq!(fake.requests().len(), 2);
}

/// The SDK's `send` reads a request it does not stream inside its try; a
/// stream's body it reads after, where no retry reaches it.
#[tokio::test]
async fn a_whole_completion_s_body_counts_in_the_try_a_stream_s_does_not() {
    let whole = vec![Reply::dropped_in_its_body(), Reply::completion("Recovered")];
    let stream = vec![
        Reply::dropped_before_its_first_event(),
        answering(true, "never"),
    ];
    let (whole, whole_fake) = on(whole, 1).await;
    let (stream, stream_fake) = on(stream, 1).await;
    let (whole, stream) = tokio::join!(answer(&whole, false), answer(&stream, true));
    assert_eq!(whole.unwrap(), "Recovered");
    assert_eq!(whole_fake.requests().len(), 2);
    assert_eq!(status_of(&stream.unwrap_err()), (true, None));
    assert_eq!(stream_fake.requests().len(), 1);
}
