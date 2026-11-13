//! A request's failures on the wire: what each reads as for a channel's
//! retry policy, translated from Python's
//! `tests/test_provider_transport_retryable.py`, its OpenAI rows, and what
//! `OpenAiConfig::max_retries` tries again, named after what Python's openai
//! SDK does (`openai._base_client`, 2.54).

use std::pin::Pin;

use futures_core::Stream;
use std::collections::BTreeMap;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_ai::{AiContext, AiMessage, AiProvider, AiRole, ProviderError, StreamEvent};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::json;
use tokio::time::Instant;

use super::fake::{FakeOpenAi, Reply, refused_address};

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
    let (_refusing, address) = refused_address();
    let refused = format!("http://{address}/v1");
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

/// HITL 08856192: a try again carries no `x-stainless-retry-count`, which the SDK
/// sends, nor any `x-stainless-*` header, written in docs/architecture.md.
#[tokio::test]
async fn a_try_again_carries_no_sdk_header() {
    let replies = vec![
        soon(Reply::status(503, "{}")),
        answering(false, "Recovered"),
    ];
    let (provider, fake) = on(replies, 1).await;
    assert_eq!(answer(&provider, false).await.unwrap(), "Recovered");
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            !request
                .header_names
                .iter()
                .any(|name| name.starts_with("x-stainless")),
            "{:?}",
            request.header_names
        );
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
/// with no answer is tried again, each try after its backoff, which doubles:
/// at least 0.375 s then 0.75 s.
#[tokio::test]
async fn a_connection_that_closed_is_tried_again_after_its_backoff() {
    let replies = vec![
        Reply::closed(),
        Reply::closed(),
        answering(true, "Recovered"),
    ];
    let (provider, fake) = on(replies, 2).await;
    let started = Instant::now();
    assert_eq!(answer(&provider, true).await.unwrap(), "Recovered");
    assert!(
        started.elapsed() >= Duration::from_millis(1125),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(fake.requests().len(), 3);
}

/// The SDK's `except timeout_exceptions()`: a try that timed out is tried
/// again.
#[tokio::test]
async fn a_timeout_is_tried_again() {
    let fake = FakeOpenAi::start(vec![
        Reply::silent_for(Duration::from_millis(100)),
        answering(true, "Recovered"),
    ])
    .await;
    let timed = OpenAiConfig {
        timeout: Duration::from_millis(50),
        ..config(fake.base_url(), 1)
    };
    let provider = OpenAiAiProvider::new(timed).unwrap();
    assert_eq!(answer(&provider, true).await.unwrap(), "Recovered");
    assert_eq!(fake.requests().len(), 2);
}

/// The SDK's `_calculate_retry_timeout`: the wait the endpoint asks replaces
/// the backoff, whose first wait is 0.5 s at most.
#[tokio::test]
async fn the_wait_an_endpoint_asks_is_taken() {
    let asking = Reply::status(429, "{}").with_header("retry-after-ms", "700");
    let (provider, fake) = on(vec![asking, answering(true, "Recovered")], 1).await;
    let started = Instant::now();
    assert_eq!(answer(&provider, true).await.unwrap(), "Recovered");
    assert!(
        started.elapsed() >= Duration::from_millis(700),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(fake.requests().len(), 2);
}

/// The SDK builds its request outside its try: one that cannot be built
/// never leaves, and fails at once, with no backoff (at least 2.6 s here).
#[tokio::test]
async fn a_request_that_cannot_be_built_is_not_tried_again() {
    let (_, fake) = on(vec![answering(true, "never")], 0).await;
    let unbuildable = OpenAiConfig {
        default_headers: Some(BTreeMap::from([(
            "x-broken".to_owned(),
            "new\nline".to_owned(),
        )])),
        ..config(fake.base_url(), 3)
    };
    let provider = OpenAiAiProvider::new(unbuildable).unwrap();
    let started = Instant::now();
    assert!(answer(&provider, true).await.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(fake.requests().len(), 0);
}

/// The SDK's `send` reads a request it does not stream inside its try,
/// whatever its status; a stream's body it reads after, where no retry
/// reaches it.
#[tokio::test]
async fn a_whole_completion_s_body_counts_in_the_try_a_stream_s_does_not() {
    let answered = vec![
        Reply::dropped_in_its_body(200),
        Reply::completion("Recovered"),
    ];
    let refused = vec![
        Reply::dropped_in_its_body(400),
        Reply::completion("Recovered"),
    ];
    let stream = vec![
        Reply::dropped_before_its_first_event(),
        answering(true, "never"),
    ];
    let stream_refused = vec![Reply::dropped_in_its_body(400), answering(true, "never")];
    let (answered, answered_fake) = on(answered, 1).await;
    let (refused, refused_fake) = on(refused, 1).await;
    let (stream, stream_fake) = on(stream, 1).await;
    let (stream_refused, stream_refused_fake) = on(stream_refused, 1).await;
    let (answered, refused, stream, stream_refused) = tokio::join!(
        answer(&answered, false),
        answer(&refused, false),
        answer(&stream, true),
        answer(&stream_refused, true)
    );
    assert_eq!(answered.unwrap(), "Recovered");
    assert_eq!(answered_fake.requests().len(), 2);
    assert_eq!(refused.unwrap(), "Recovered");
    assert_eq!(refused_fake.requests().len(), 2);
    assert_eq!(status_of(&stream.unwrap_err()), (true, None));
    assert_eq!(stream_fake.requests().len(), 1);
    assert!(stream_refused.is_err());
    assert_eq!(stream_refused_fake.requests().len(), 1);
}

// Python: tests/test_providers/test_ai_models.py::test_openai_list_models_maps_and_merges
#[tokio::test]
async fn openai_list_models_maps_and_merges() {
    let reply = Reply::models(&[
        "gpt-4o",
        "gpt-6-luna",
        "text-embedding-3",
        "whisper-1",
        "gpt-4o-mini-tts",
    ]);
    let (provider, fake) = on(vec![reply], 0).await;
    let models = provider.list_models().await.unwrap();
    let known = models.first().unwrap();
    assert_eq!(known.display_name.as_deref(), Some("GPT-4o"));
    assert_eq!(known.supports_vision, Some(true));
    let read: Vec<(&str, Option<&str>, &[String])> = models
        .iter()
        .map(|model| {
            (
                model.id.as_str(),
                model.display_name.as_deref(),
                &model.capabilities[..],
            )
        })
        .skip(2)
        .collect();
    assert_eq!(
        read,
        [
            ("text-embedding-3", None, &[][..]),
            ("whisper-1", None, &["transcription".to_owned()][..]),
            ("gpt-4o-mini-tts", None, &["speech".to_owned()][..]),
        ]
    );
    // A catalogue's flag (gpt-6-luna's tools_reasoning_none) stays out.
    let luna = models.get(1).unwrap();
    assert_eq!(luna.capabilities, Vec::<String>::new());
    let request = fake.requests().remove(0);
    assert_eq!(
        (
            request.method.as_str(),
            request.path.as_str(),
            request.authorization.as_deref()
        ),
        ("GET", "/v1/models", Some("Bearer sk-test"))
    );
}

/// Python reads `page.data` of what the SDK gave: a body that is no page of
/// models with their ids raises, it is no empty listing, and the error does
/// not repeat it.
#[tokio::test]
async fn a_model_listing_that_is_no_page_of_models_fails() {
    for body in [
        "<html>sk-test Sign in</html>",
        "{}",
        r#"{"data": null}"#,
        r#"{"data": [{"object": "model"}]}"#,
        r#"{"data": [{"id": 5}]}"#,
        r#"{"data": [["gpt-4o"]]}"#,
        r#"[[["gpt-4o"]]]"#,
    ] {
        let (provider, _fake) = on(vec![Reply::status(200, body)], 0).await;
        let error = provider.list_models().await.unwrap_err();
        assert_eq!(status_of(&error), (false, None), "{body}");
        assert!(!error.to_string().contains("sk-test"), "{error}");
    }
}

#[tokio::test]
async fn a_model_listing_refused_fails_with_its_status_and_is_tried_again_as_any_request() {
    let (provider, _fake) = on(vec![Reply::status(401, "{}")], 0).await;
    let error = provider.list_models().await.unwrap_err();
    assert_eq!(status_of(&error), (false, Some(401)));

    let (provider, _fake) = on(vec![Reply::closed()], 0).await;
    let error = provider.list_models().await.unwrap_err();
    assert_eq!(status_of(&error), (true, None));
    assert!(
        error
            .to_string()
            .contains("the model listing request failed"),
        "{error}"
    );

    let replies = vec![soon(Reply::status(503, "{}")), Reply::models(&["a"])];
    let (provider, fake) = on(replies, 1).await;
    let models = provider.list_models().await.unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(fake.requests().len(), 2);
}
