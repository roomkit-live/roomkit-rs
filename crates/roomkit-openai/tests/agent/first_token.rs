//! The time to first token, the metric `roomkit.llm.ttfb_ms`: Python's
//! `tests/text_conformance/test_responses.py`, `TestModesAgree`.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelType, HookResult, InboundMessage, Metadata, RoomEvent, TextContent};
use roomkit::telemetry::{MockTelemetryProvider, TelemetryConfig};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookOptions, HookTrigger, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};
use roomkit_ai::{AiContext, AiMessage, AiProvider, AiRole};
use roomkit_openai::{OpenAiAiProvider, OpenAiApi, OpenAiConfig};
use serde_json::json;

use super::fake::{FakeOpenAi, Reply};
use super::{channel, room_id};

/// How a generation is read.
#[derive(Clone, Copy, Debug)]
enum Mode {
    Generate,
    Stream,
}

fn hello() -> AiContext {
    AiContext {
        messages: vec![AiMessage::new(AiRole::User, "Hi")],
        ..AiContext::default()
    }
}

/// Each `roomkit.llm.ttfb_ms` one generation records, its value, unit and
/// attributes: asked as `mode`, answered `reply`, inside a hook of a kit
/// reporting to a mock, as a turn's generation runs under its kit.
async fn recorded(mode: Mode, api: OpenAiApi, reply: Reply) -> Vec<(f64, String, Metadata)> {
    let fake = FakeOpenAi::start(vec![reply]).await;
    let config = OpenAiConfig {
        base_url: Some(fake.base_url().to_owned()),
        api,
        ..OpenAiConfig::new("sk-test", "fake-model")
    };
    let provider = Arc::new(OpenAiAiProvider::new(config).unwrap());
    let mock = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig::from(Arc::clone(&mock))),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room_id()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| {
            let provider = Arc::clone(&provider);
            async move {
                generate(mode, &provider).await;
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "generates".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("Hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    kit.close().await.unwrap();
    mock.metrics()
        .into_iter()
        .filter(|(name, ..)| name == "roomkit.llm.ttfb_ms")
        .map(|(_, value, unit, attributes)| (value, unit, attributes))
        .collect()
}

/// One generation, read to its end whatever it comes to.
async fn generate(mode: Mode, provider: &OpenAiAiProvider) {
    let context = hello();
    match mode {
        Mode::Generate => {
            drop(provider.generate(&context).await);
        }
        Mode::Stream => {
            let mut events = provider.generate_structured_stream(&context);
            while poll_fn(|cx| Pin::new(&mut events).poll_next(cx))
                .await
                .is_some()
            {}
        }
    }
}

/// A stream whose one chunk's delta is `delta`, then its end.
fn delta(delta: &serde_json::Value) -> Reply {
    Reply::events(&[
        json!({ "choices": [{ "index": 0, "delta": delta }] }),
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
    ])
}

fn labels() -> Metadata {
    let mut labels = Metadata::new();
    labels.insert("provider".to_owned(), json!("OpenAIAIProvider"));
    labels.insert("model".to_owned(), json!("fake-model"));
    labels
}

// Python: tests/test_telemetry.py::TestAIProviderTTFB::test_ai_provider_records_ttfb_metric,
// tests/text_conformance/test_responses.py::TestModesAgree::test_time_to_first_token_is_labelled_alike_on_both_modes
#[tokio::test]
async fn the_time_to_first_token_is_labelled_alike_on_both_modes() {
    for (mode, reply) in [
        (Mode::Generate, Reply::completion("Ok.")),
        (Mode::Stream, delta(&json!({ "content": "Ok." }))),
    ] {
        let metrics = recorded(mode, OpenAiApi::ChatCompletions, reply).await;
        let [(value, unit, attributes)] = &metrics[..] else {
            panic!("one metric in {metrics:?} for {mode:?}");
        };
        assert!(*value >= 0.0);
        assert_eq!(unit, "ms");
        assert_eq!(attributes, &labels());
    }
}

// Python: tests/text_conformance/test_responses.py::TestModesAgree::test_a_stream_of_calls_alone_records_no_first_token
#[tokio::test]
async fn a_stream_of_calls_alone_records_no_first_token() {
    let call = json!({ "tool_calls": [{
        "index": 0,
        "id": "c1",
        "function": { "name": "lookup", "arguments": "{}" },
    }]});
    let metrics = recorded(Mode::Stream, OpenAiApi::ChatCompletions, delta(&call)).await;
    assert_eq!(metrics, vec![]);
}

// Python: tests/text_conformance/test_responses.py::TestModesAgree::test_text_held_back_to_the_end_records_its_first_token
#[tokio::test]
async fn text_held_back_to_the_end_records_its_first_token() {
    let held = delta(&json!({ "content": "<" }));
    let metrics = recorded(Mode::Stream, OpenAiApi::ChatCompletions, held).await;
    assert_eq!(metrics.len(), 1, "{metrics:?}");
}

// Python: providers/ai/base.py `FirstToken`: "at its first text or reasoning".
#[tokio::test]
async fn a_stream_of_reasoning_alone_records_its_first_token() {
    let chunks = Reply::events(&[
        json!({ "choices": [{ "index": 0, "delta": { "reasoning_content": "Hm." } }] }),
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
    ]);
    let metrics = recorded(Mode::Stream, OpenAiApi::ChatCompletions, chunks).await;
    assert_eq!(metrics.len(), 1, "{metrics:?}");
}

// Python: providers/ai/base.py `FirstToken`: recorded once, its reasoning first.
#[tokio::test]
async fn a_stream_records_its_first_token_once_at_its_reasoning() {
    let chunks = Reply::events(&[
        json!({ "choices": [{ "index": 0, "delta": { "reasoning_content": "Hm." } }] }),
        json!({ "choices": [{ "index": 0, "delta": { "content": "Ok." } }] }),
        json!({ "choices": [{ "index": 0, "delta": { "content": "Yes." } }] }),
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
    ]);
    let metrics = recorded(Mode::Stream, OpenAiApi::ChatCompletions, chunks).await;
    assert_eq!(metrics.len(), 1, "{metrics:?}");
}

// Python: providers/openai/ai.py `_complete`: a completion asked whole records
// as it arrives, whatever it holds; one that fails records nothing.
#[tokio::test]
async fn a_completion_asked_whole_records_as_it_arrives_a_failure_nothing() {
    let call = Reply::calling("", "c1", "lookup", "{}");
    let metrics = recorded(Mode::Generate, OpenAiApi::ChatCompletions, call).await;
    assert_eq!(metrics.len(), 1, "{metrics:?}");
    let refused = Reply::status(500, r#"{"error":{"message":"down"}}"#);
    let metrics = recorded(Mode::Generate, OpenAiApi::ChatCompletions, refused).await;
    assert_eq!(metrics, vec![]);
}

// Rust alone: Python's provider has no Responses API (a feature only one
// implementation has). As on Chat Completions: once, at the first text, on
// both modes.
#[tokio::test]
async fn on_responses_the_first_text_records_it_once_on_both_modes() {
    for mode in [Mode::Generate, Mode::Stream] {
        let reply = Reply::events(&[
            json!({ "type": "response.output_text.delta", "delta": "Ready " }),
            json!({ "type": "response.output_text.delta", "delta": "at noon." }),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_1",
                    "status": "completed",
                    "model": "fake-model",
                    "output": [{
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{ "type": "output_text", "text": "Ready at noon." }],
                    }],
                },
            }),
        ]);
        let metrics = recorded(mode, OpenAiApi::Responses, reply).await;
        assert_eq!(metrics.len(), 1, "{mode:?}: {metrics:?}");
    }
}
