//! A response schema on OpenAI's wire (RFC §6.7): a strict `json_schema`
//! response format on Chat Completions, the answer checked whole or before
//! its stream's end, and what each vendor takes by default; Python's
//! `tests/test_providers/test_openai.py` `TestOpenAIResponseSchema` and
//! `TestOpenAIResponseSchemaWithTools`, and the vendors' defaults.

use std::error::Error;
use std::pin::Pin;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit_ai::{
    AiContext, AiMessage, AiProvider, AiResponse, AiRole, AiTool, ProviderError,
    ResponseSchemaError, ResponseSchemaFailure, StreamEvent,
};
use roomkit_openai::deepseek::{DeepSeekAiProvider, DeepSeekConfig};
use roomkit_openai::kimi::{KimiAiProvider, KimiConfig};
use roomkit_openai::minimax::{MiniMaxAiProvider, MiniMaxConfig};
use roomkit_openai::{OpenAiAiProvider, OpenAiApi, OpenAiConfig};
use serde_json::{Map, Value, json};

use super::fake::{FakeOpenAi, Reply};

/// Python's `_VERDICT`.
fn verdict() -> Map<String, Value> {
    let Value::Object(schema) = json!({
        "type": "object",
        "properties": { "label": { "type": "string" } },
        "required": ["label"],
        "additionalProperties": false,
    }) else {
        unreachable!("an object")
    };
    schema
}

/// A question under the verdict's schema, with `tools`.
fn asked(tools: Vec<AiTool>) -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "Is it?")],
        response_schema: Some(verdict()),
        tools,
        ..AiContext::default()
    }
}

fn lookup() -> AiTool {
    AiTool::new("lookup", "Look it up").unwrap()
}

/// The provider against a fake answering `replies`, its configuration
/// adjusted by `adjust`.
async fn provider(
    replies: Vec<Reply>,
    adjust: impl FnOnce(OpenAiConfig) -> OpenAiConfig,
) -> (OpenAiAiProvider, FakeOpenAi) {
    let fake = FakeOpenAi::start(replies).await;
    let config = adjust(OpenAiConfig {
        base_url: Some(fake.base_url().to_owned()),
        ..OpenAiConfig::new("sk-test", "fake-model")
    });
    (OpenAiAiProvider::new(config).unwrap(), fake)
}

/// A whole completion whose message says `text` and ends `finish`, its
/// `refusal` when it gives one.
fn answered(text: &str, finish: &str, refusal: Option<&str>) -> Reply {
    Reply::whole(&json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "fake-model",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": text, "refusal": refusal },
            "finish_reason": finish,
        }],
    }))
}

/// A streamed completion: `text` in one delta, its `refusal` when it gives
/// one, then the stop (Python's `_openai_chunks`).
fn streamed(text: &str, refusal: Option<&str>) -> Reply {
    let content = (!text.is_empty()).then_some(text);
    Reply::events(&[
        json!({ "choices": [{ "index": 0, "delta": { "content": content, "refusal": refusal } }] }),
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
    ])
}

/// The schema's failure `error` carries, when one failed.
fn reason(error: &ProviderError) -> Option<ResponseSchemaFailure> {
    let failure = error.source()?.downcast_ref::<ResponseSchemaError>()?;
    Some(failure.reason)
}

/// The events of a stream to its end or its failure.
async fn drain(
    mut events: impl Stream<Item = Result<StreamEvent, ProviderError>> + Unpin,
) -> (Vec<StreamEvent>, Option<ProviderError>) {
    let mut read = Vec::new();
    while let Some(event) = std::future::poll_fn(|cx| Pin::new(&mut events).poll_next(cx)).await {
        match event {
            Ok(event) => read.push(event),
            Err(error) => return (read, Some(error)),
        }
    }
    (read, None)
}

/// The text a stream's deltas wrote.
fn text_of(events: &[StreamEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::TextDelta(delta) => Some(delta.text.as_str()),
            StreamEvent::ToolCall(_) | StreamEvent::Done(_) | StreamEvent::ThinkingDelta(_) => None,
        })
        .collect()
}

/// The strict response format the verdict rides.
fn strict_format() -> Value {
    json!({
        "type": "json_schema",
        "json_schema": { "name": "response", "schema": verdict(), "strict": true },
    })
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_the_schema_rides_a_strict_json_schema_response_format
#[tokio::test]
async fn the_schema_rides_a_strict_json_schema_response_format() {
    let replies = vec![answered(r#"{"label": "yes"}"#, "stop", None)];
    let (provider, fake) = provider(replies, |config| config).await;
    let response: AiResponse = provider.generate(&asked(Vec::new())).await.unwrap();
    assert_eq!(response.content, r#"{"label": "yes"}"#);
    assert_eq!(fake.requests()[0].body["response_format"], strict_format());
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_no_schema_sends_no_response_format
#[tokio::test]
async fn no_schema_sends_no_response_format() {
    let (provider, fake) = provider(vec![Reply::completion("Hi")], |config| config).await;
    let context = AiContext {
        response_schema: None,
        ..asked(Vec::new())
    };
    provider.generate(&context).await.unwrap();
    assert_eq!(fake.requests()[0].body.get("response_format"), None);
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_think_tags_are_split_off_before_the_document_is_checked
#[tokio::test]
async fn think_tags_are_split_off_before_the_document_is_checked() {
    let replies = vec![answered(
        r#"<think>easy</think>{"label": "no"}"#,
        "stop",
        None,
    )];
    let (provider, _fake) = provider(replies, |config| config).await;
    let response = provider.generate(&asked(Vec::new())).await.unwrap();
    assert_eq!(
        (response.content.as_str(), response.thinking.as_deref()),
        (r#"{"label": "no"}"#, Some("easy"))
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_a_refusal_field_raises_refusal
#[tokio::test]
async fn a_refusal_field_raises_refusal() {
    let replies = vec![answered("", "stop", Some("I can't help with that."))];
    let (provider, _fake) = provider(replies, |config| config).await;
    let error = provider.generate(&asked(Vec::new())).await.unwrap_err();
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::Refusal));
    assert!(error.to_string().contains("can't help"), "{error}");
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_an_answer_without_its_document_raises
#[tokio::test]
async fn an_answer_without_its_document_raises() {
    let cases = [
        ("", "content_filter", ResponseSchemaFailure::Refusal),
        (
            r#"{"label": "y"#,
            "length",
            ResponseSchemaFailure::Truncated,
        ),
        ("Yes, it is.", "stop", ResponseSchemaFailure::InvalidJson),
        (
            r#"{"dept": "billing"}"#,
            "stop",
            ResponseSchemaFailure::InvalidJson,
        ),
    ];
    for (text, finish, expected) in cases {
        let (provider, _fake) = provider(vec![answered(text, finish, None)], |config| config).await;
        let error = provider.generate(&asked(Vec::new())).await.unwrap_err();
        assert_eq!(
            (reason(&error), error.provider.as_str()),
            (Some(expected), "openai"),
            "{error}"
        );
    }
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_a_server_declared_without_support_is_refused_before_the_call
#[tokio::test]
async fn a_server_declared_without_support_is_refused_before_the_call() {
    let replies = vec![answered(r#"{"label": "yes"}"#, "stop", None)];
    let (provider, fake) = provider(replies, |config| OpenAiConfig {
        supports_response_schema: Some(false),
        ..config
    })
    .await;
    assert!(!provider.supports_response_schema());
    let error = provider.generate(&asked(Vec::new())).await.unwrap_err();
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::Unsupported));
    assert_eq!(fake.requests().len(), 0, "no request is sent");
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_a_streamed_answer_is_checked_before_its_done_event
#[tokio::test]
async fn a_streamed_answer_is_checked_before_its_done_event() {
    let replies = vec![streamed(r#"{"label": "yes"}"#, None)];
    let (provider, fake) = provider(replies, |config| config).await;
    let context = asked(Vec::new());
    let (events, error) = drain(provider.generate_structured_stream(&context)).await;
    assert!(error.is_none(), "{error:?}");
    assert!(matches!(events.last(), Some(StreamEvent::Done(_))));
    assert_eq!(text_of(&events), r#"{"label": "yes"}"#);
    let format = &fake.requests()[0].body["response_format"];
    assert_eq!(format["json_schema"]["schema"], Value::Object(verdict()));
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_a_streamed_answer_that_is_not_the_document_raises_instead_of_done
#[tokio::test]
async fn a_streamed_answer_that_is_not_the_document_raises_instead_of_done() {
    let (provider, _fake) = provider(vec![streamed("Yes.", None)], |config| config).await;
    let context = asked(Vec::new());
    let (events, error) = drain(provider.generate_structured_stream(&context)).await;
    assert_eq!(
        reason(&error.unwrap()),
        Some(ResponseSchemaFailure::InvalidJson)
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StreamEvent::Done(_)))
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchema::test_a_streamed_refusal_is_a_refusal
#[tokio::test]
async fn a_streamed_refusal_is_a_refusal() {
    let replies = vec![streamed("", Some("I can't help with that."))];
    let (provider, _fake) = provider(replies, |config| config).await;
    let context = asked(Vec::new());
    let (_, error) = drain(provider.generate_structured_stream(&context)).await;
    let error = error.unwrap();
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::Refusal));
    assert!(error.to_string().contains("can't help"), "{error}");
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchemaWithTools::test_on_for_the_official_endpoint_off_behind_a_base_url
#[test]
fn on_for_the_official_endpoint_off_behind_a_base_url() {
    let with_tools = |base_url: Option<&str>, stated: Option<bool>| {
        let config = OpenAiConfig {
            base_url: base_url.map(str::to_owned),
            supports_response_schema_with_tools: stated,
            ..OpenAiConfig::new("sk-test", "gpt-6-luna")
        };
        OpenAiAiProvider::new(config)
            .unwrap()
            .supports_response_schema_with_tools()
    };
    assert_eq!(
        (
            with_tools(None, None),
            with_tools(Some("http://vllm:8000/v1"), None),
            with_tools(Some("http://proxy/v1"), Some(true)),
        ),
        (true, false, true)
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchemaWithTools::test_a_tool_round_is_not_checked_and_the_final_answer_is
#[tokio::test]
async fn a_tool_round_is_not_checked_and_the_final_answer_is() {
    let replies = vec![
        Reply::calling("", "call_1", "lookup", "{}"),
        answered("Yes.", "stop", None),
    ];
    let (provider, fake) = provider(replies, |config| OpenAiConfig {
        supports_response_schema_with_tools: Some(true),
        ..config
    })
    .await;
    let context = asked(vec![lookup()]);
    let first = provider.generate(&context).await.unwrap();
    let error = provider.generate(&context).await.unwrap_err();
    let called: Vec<&str> = first
        .tool_calls
        .iter()
        .map(|call| call.name.as_str())
        .collect();
    assert_eq!(called, ["lookup"]);
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::InvalidJson));
    let body = &fake.requests()[1].body;
    assert!(body.get("tools").is_some() && body.get("response_format").is_some());
}

// Python: tests/test_providers/test_openai.py::TestOpenAIResponseSchemaWithTools::test_a_server_behind_base_url_refuses_the_pair_before_the_call
#[tokio::test]
async fn a_server_behind_base_url_refuses_the_pair_before_the_call() {
    let (provider, fake) = provider(vec![Reply::completion("{}")], |config| config).await;
    let error = provider.generate(&asked(vec![lookup()])).await.unwrap_err();
    assert_eq!(reason(&error), Some(ResponseSchemaFailure::Unsupported));
    assert_eq!(fake.requests().len(), 0, "no request is sent");
}

/// A Responses stream ending `status`, its message saying `parts` (each an
/// `output_text` or a `refusal`), cut for `incomplete` when it gives why.
fn responded(parts: &[(&str, &str)], status: &str, incomplete: Option<&str>) -> Reply {
    let content: Vec<Value> = parts
        .iter()
        .map(|(kind, said)| match *kind {
            "refusal" => json!({ "type": "refusal", "refusal": said }),
            _ => json!({ "type": "output_text", "text": said, "annotations": [] }),
        })
        .collect();
    let mut response = json!({
        "id": "resp_1", "status": status, "model": "fake-model",
        "output": [{ "type": "message", "role": "assistant", "content": content }],
    });
    if let Some(reason) = incomplete {
        response["incomplete_details"] = json!({ "reason": reason });
    }
    Reply::events(&[json!({ "type": format!("response.{status}"), "response": response })])
}

/// The provider on the Responses API against a fake answering `replies`,
/// its configuration adjusted by `adjust`.
async fn on_responses(
    replies: Vec<Reply>,
    adjust: impl FnOnce(OpenAiConfig) -> OpenAiConfig,
) -> (OpenAiAiProvider, FakeOpenAi) {
    provider(replies, |config| {
        adjust(OpenAiConfig {
            api: OpenAiApi::Responses,
            ..config
        })
    })
    .await
}

// The Responses API carries a response schema as Chat does (HITL 78f2cb96):
// strictly, in its `text.format`, the answer checked before its end.
#[tokio::test]
async fn the_responses_api_carries_the_schema_as_its_text_format() {
    let replies = vec![responded(
        &[("text", r#"{"label": "yes"}"#)],
        "completed",
        None,
    )];
    let (provider, fake) = on_responses(replies, |config| config).await;
    let response = provider.generate(&asked(Vec::new())).await.unwrap();
    assert_eq!(response.content, r#"{"label": "yes"}"#);
    assert_eq!(
        fake.requests()[0].body["text"],
        json!({ "format": {
            "type": "json_schema", "name": "response", "schema": verdict(), "strict": true } })
    );
    let context = AiContext {
        response_schema: None,
        ..asked(Vec::new())
    };
    let (provider, fake) = on_responses(
        vec![responded(&[("text", "Hi")], "completed", None)],
        |config| config,
    )
    .await;
    provider.generate(&context).await.unwrap();
    assert_eq!(fake.requests()[0].body.get("text"), None);
}

// The Responses API fails an answer without its document as Chat does
// (HITL 78f2cb96): a refusal part, an answer cut for its output cap or by a
// content filter, prose, streamed or whole.
#[tokio::test]
async fn a_responses_answer_without_its_document_fails_as_on_chat() {
    let cases = [
        (
            responded(&[("refusal", "I can't help with that.")], "completed", None),
            ResponseSchemaFailure::Refusal,
        ),
        (
            responded(
                &[("text", r#"{"label": "y"#)],
                "incomplete",
                Some("max_output_tokens"),
            ),
            ResponseSchemaFailure::Truncated,
        ),
        (
            responded(&[("text", "")], "incomplete", Some("content_filter")),
            ResponseSchemaFailure::Refusal,
        ),
        (
            responded(&[("text", "Yes, it is.")], "completed", None),
            ResponseSchemaFailure::InvalidJson,
        ),
    ];
    for (reply, expected) in cases {
        let (provider, _fake) = on_responses(vec![reply.clone(), reply], |config| config).await;
        let error = provider.generate(&asked(Vec::new())).await.unwrap_err();
        assert_eq!(reason(&error), Some(expected), "{error}");
        let (events, error) = drain(provider.generate_structured_stream(&asked(Vec::new()))).await;
        assert_eq!(reason(&error.unwrap()), Some(expected));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, StreamEvent::Done(_))),
            "the failure replaces the end"
        );
    }
}

// Without a schema, a refusal on the Responses API stays the answer's text,
// and its end names it, as on Chat.
#[tokio::test]
async fn a_responses_refusal_without_a_schema_is_text_its_end_naming_it() {
    let reply = responded(&[("refusal", "Cannot help.")], "completed", None);
    let (provider, _fake) = on_responses(vec![reply], |config| config).await;
    let context = AiContext {
        response_schema: None,
        ..asked(Vec::new())
    };
    let (events, error) = drain(provider.generate_structured_stream(&context)).await;
    assert!(error.is_none(), "{error:?}");
    let refusal = events.iter().find_map(|event| match event {
        StreamEvent::Done(done) => done.metadata.get("refusal").cloned(),
        StreamEvent::TextDelta(_) | StreamEvent::ToolCall(_) | StreamEvent::ThinkingDelta(_) => {
            None
        }
    });
    assert_eq!(
        (text_of(&events), refusal),
        ("Cannot help.".to_owned(), Some(json!("Cannot help.")))
    );
}

// The Responses API takes Chat's defaults (HITL 78f2cb96): a schema by
// default whatever the URL, beside tools only on OpenAI's own endpoint, the
// configuration deciding when it says.
#[tokio::test]
async fn the_responses_api_takes_chat_s_defaults() {
    let read = |stated: Option<bool>, with_tools: Option<bool>| {
        let provider = OpenAiAiProvider::new(OpenAiConfig {
            api: OpenAiApi::Responses,
            base_url: Some("http://127.0.0.1:9/v1".to_owned()),
            supports_response_schema: stated,
            supports_response_schema_with_tools: with_tools,
            ..OpenAiConfig::new("sk-test", "fake-model")
        })
        .unwrap();
        (
            provider.supports_response_schema(),
            provider.supports_response_schema_with_tools(),
        )
    };
    let official = OpenAiAiProvider::new(OpenAiConfig {
        api: OpenAiApi::Responses,
        ..OpenAiConfig::new("sk-test", "gpt-6-luna")
    })
    .unwrap();
    assert_eq!(
        [
            read(None, None),
            read(Some(false), None),
            read(None, Some(true)),
            (
                official.supports_response_schema(),
                official.supports_response_schema_with_tools()
            ),
        ],
        [(true, false), (false, false), (true, true), (true, true)]
    );
    // Beside tools behind a base URL, refused before any request.
    let (provider, fake) = on_responses(
        vec![responded(&[("text", "{}")], "completed", None)],
        |config| config,
    )
    .await;
    let (_, error) = drain(provider.generate_structured_stream(&asked(vec![lookup()]))).await;
    assert_eq!(
        reason(&error.unwrap()),
        Some(ResponseSchemaFailure::Unsupported)
    );
    assert_eq!(fake.requests().len(), 0, "no request is sent");
}

// Python: tests/test_providers/test_minimax_kimi.py::test_a_response_schema_is_taken_where_the_vendor_documents_it
// and tests/test_providers/test_deepseek.py::TestResponseSchemaDefault::test_off_unless_the_config_turns_it_on
#[test]
fn a_response_schema_is_taken_where_the_vendor_documents_it() {
    let minimax = |stated: Option<bool>| {
        let config = MiniMaxConfig {
            supports_response_schema: stated,
            ..MiniMaxConfig::new("key", "MiniMax-M3")
        };
        MiniMaxAiProvider::new(config)
            .unwrap()
            .supports_response_schema()
    };
    let kimi = |stated: Option<bool>| {
        let config = KimiConfig {
            supports_response_schema: stated,
            ..KimiConfig::new("key", "kimi-k3")
        };
        KimiAiProvider::new(config)
            .unwrap()
            .supports_response_schema()
    };
    let deepseek = |stated: Option<bool>| {
        let config = DeepSeekConfig {
            supports_response_schema: stated,
            ..DeepSeekConfig::new("key", "deepseek-v4-flash")
        };
        DeepSeekAiProvider::new(config)
            .unwrap()
            .supports_response_schema()
    };
    assert_eq!(
        [
            (minimax(None), minimax(Some(true))),
            (kimi(None), kimi(Some(false))),
            (deepseek(None), deepseek(Some(true))),
        ],
        [(false, true), (true, false), (false, true)]
    );
}

// Python: tests/text_conformance/test_responses.py::TestResponseSchema::test_entries_with_no_function_are_no_tool_round
// Under a schema, a round is a step of the loop only when calls reach it:
// an entry with no function hands none, so the answer is due and checked,
// whole and streamed.
#[tokio::test]
async fn entries_with_no_function_are_no_tool_round() {
    let entry = json!({ "id": "c1", "type": "custom", "custom": { "name": "x", "input": "" } });
    let whole = Reply::whole(&json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "fake-model",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": "", "tool_calls": [entry] },
            "finish_reason": "tool_calls",
        }],
    }));
    let streamed = Reply::events(&[
        json!({ "choices": [{ "index": 0, "delta": { "tool_calls": [{ "index": 0, "id": "c1" }] } }] }),
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] }),
    ]);
    let (provider, _fake) = provider(vec![whole, streamed], |config| config).await;
    let context = asked(Vec::new());
    let answered = provider.generate(&context).await.unwrap_err();
    let (_, read) = drain(provider.generate_structured_stream(&context)).await;
    assert_eq!(
        (reason(&answered), reason(&read.unwrap())),
        (
            Some(ResponseSchemaFailure::InvalidJson),
            Some(ResponseSchemaFailure::InvalidJson)
        )
    );
}

// Python: tests/text_conformance/test_refusal.py::test_a_refusal_reads_the_same_streamed_or_not
#[tokio::test]
async fn a_refusal_reads_the_same_streamed_or_not() {
    const REFUSAL: &str = "I cannot help with that.";
    let whole = answered("", "stop", Some(REFUSAL));
    let (provider, _fake) =
        provider(vec![whole, streamed("", Some(REFUSAL))], |config| config).await;
    let context = AiContext {
        response_schema: None,
        ..asked(Vec::new())
    };
    let response = provider.generate(&context).await.unwrap();
    let (events, error) = drain(provider.generate_structured_stream(&context)).await;
    assert!(error.is_none(), "{error:?}");
    let Some(StreamEvent::Done(done)) = events.last() else {
        panic!("the stream ends with its done event: {events:?}");
    };
    assert_eq!(
        (
            response.content.as_str(),
            response.metadata.get("refusal"),
            text_of(&events).as_str(),
            done.metadata.get("refusal"),
        ),
        ("", Some(&json!(REFUSAL)), "", Some(&json!(REFUSAL)))
    );
}

// Python: tests/text_conformance/test_refusal.py::test_a_refusal_that_is_not_text_is_not_reported
#[tokio::test]
async fn a_refusal_that_is_not_text_is_not_reported() {
    let reply = Reply::whole(&json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "fake-model",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": null, "refusal": true },
            "finish_reason": "stop",
        }],
    }));
    let (provider, _fake) = provider(vec![reply], |config| config).await;
    let context = AiContext {
        response_schema: None,
        ..asked(Vec::new())
    };
    let response = provider.generate(&context).await.unwrap();
    assert_eq!(response.metadata.get("refusal"), None);
}
