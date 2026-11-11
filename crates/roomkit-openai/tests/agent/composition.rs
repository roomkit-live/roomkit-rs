//! Python: `tests/test_providers/test_openai_composition.py`. Real Chat
//! Completions decoding, paired native wire cases (RFC §6.4, RMK-1119).

use std::future::poll_fn;

use roomkit_ai::{AiContext, AiProvider, ProviderError, StreamEvent};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::{Value, json};

use super::fake::{FakeOpenAi, Reply};
use super::{chunk, ending};

async fn provider(chunks: &[Value]) -> (OpenAiAiProvider, FakeOpenAi) {
    let fake = FakeOpenAi::start(vec![Reply::events(chunks)]).await;
    let config = OpenAiConfig {
        base_url: Some(fake.base_url().to_owned()),
        max_retries: 0,
        ..OpenAiConfig::new("test", "fake")
    };
    (OpenAiAiProvider::new(config).unwrap(), fake)
}

async fn read(chunks: &[Value]) -> Vec<Result<StreamEvent, ProviderError>> {
    let (provider, _fake) = provider(chunks).await;
    let context = AiContext::default();
    let mut stream = provider.generate_structured_stream(&context);
    let mut events = Vec::new();
    while let Some(event) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        events.push(event);
    }
    events
}

#[tokio::test]
async fn wire_fragments_keep_each_calls_final_id() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("composition_cases.json")).unwrap();
    for case in cases {
        let mut chunks: Vec<_> = case[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|fragment| chunk(&json!({"tool_calls": [fragment]})))
            .collect();
        chunks.extend(ending("tool_calls", 1, 1));
        let events: Vec<_> = read(&chunks)
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        let deltas: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ToolCallDelta(delta) => Some(delta),
                StreamEvent::TextDelta(_)
                | StreamEvent::ToolCall(_)
                | StreamEvent::Done(_)
                | StreamEvent::ThinkingDelta(_) => None,
            })
            .collect();
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ToolCall(call) => Some(call),
                StreamEvent::TextDelta(_)
                | StreamEvent::Done(_)
                | StreamEvent::ThinkingDelta(_)
                | StreamEvent::ToolCallDelta(_) => None,
            })
            .collect();
        assert_eq!(
            json!(
                deltas
                    .iter()
                    .map(|d| (&d.index, &d.name, &d.arguments_delta))
                    .collect::<Vec<_>>()
            ),
            case[2],
            "{}",
            case[0]
        );
        assert_eq!(
            json!(calls.iter().map(|c| &c.arguments).collect::<Vec<_>>()),
            case[3],
            "{}",
            case[0]
        );
        let ids: std::collections::HashSet<_> = calls.iter().map(|c| &c.id).collect();
        assert_eq!(ids.len(), calls.len());
        let mut by_slot = std::collections::HashMap::new();
        for delta in deltas {
            assert_eq!(by_slot.entry(delta.index).or_insert(&delta.id), &&delta.id);
            assert!(
                calls
                    .iter()
                    .any(|call| call.id == delta.id && call.name == delta.name)
            );
        }
        assert_eq!(
            by_slot
                .values()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            by_slot.len()
        );
        let first_call = events
            .iter()
            .position(|e| matches!(e, StreamEvent::ToolCall(_)));
        assert!(
            events
                .iter()
                .enumerate()
                .all(|(i, e)| !matches!(e, StreamEvent::ToolCallDelta(_)) || Some(i) < first_call)
        );
        assert!(matches!(events.last(), Some(StreamEvent::Done(_))));
        match case[0].as_str().unwrap() {
            "late-id-after-name" => assert!(calls[0].id.starts_with("call_search_")),
            "late-id-before-name" => assert_eq!(calls[0].id, "late"),
            _ => {}
        }
    }
}

#[tokio::test]
async fn composition_precedes_reasoning_and_text_in_the_same_chunk() {
    let chunks = [chunk(&json!({
        "tool_calls": [{"index":0,"id":"a","function":{"name":"search","arguments":"{}"}}],
        "reasoning_content":"checking", "content":"answer"
    }))];
    let events: Vec<_> = read(&chunks)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert!(matches!(
        events.as_slice(),
        [
            StreamEvent::ToolCallDelta(_),
            StreamEvent::ThinkingDelta(_),
            StreamEvent::TextDelta(_),
            StreamEvent::ToolCall(_),
            StreamEvent::Done(_)
        ]
    ));
}

#[tokio::test]
async fn a_failed_wire_keeps_its_fragment_without_a_complete_call() {
    let chunks = [
        chunk(&json!({"tool_calls":[{"index":0,"id":"a",
        "function":{"name":"search","arguments":"{secret"}}]})),
        json!({"error":{"message":"failed","code":"invalid_request_error"}}),
    ];
    let events = read(&chunks).await;
    assert_eq!(events.len(), 2);
    assert!(
        matches!(&events[0], Ok(StreamEvent::ToolCallDelta(d)) if d.arguments_delta == "{secret")
    );
    assert!(events[1].is_err());
}

#[tokio::test]
async fn a_later_malformed_call_keeps_the_first_fragment_of_its_chunk() {
    for fault in ["id", "name", "content"] {
        let mut delta = json!({"tool_calls":[
            {"index":0,"id":"a","function":{"name":"search","arguments":"{}"}}
        ]});
        if fault == "content" {
            delta["content"] = 7.into();
        } else {
            let mut malformed =
                json!({"index":1,"id":"b","function":{"name":"search","arguments":"{}"}});
            if fault == "id" {
                malformed["id"] = 7.into();
            } else {
                malformed["function"]["name"] = 7.into();
            }
            delta["tool_calls"].as_array_mut().unwrap().push(malformed);
        }
        let events = read(&[chunk(&delta)]).await;
        assert_eq!(events.len(), 2, "{fault}");
        assert!(
            matches!(&events[0], Ok(StreamEvent::ToolCallDelta(d)) if d.id == "a" && d.arguments_delta == "{}")
        );
        assert!(events[1].is_err());
    }
}

#[tokio::test]
async fn a_cut_call_keeps_its_fragments_and_partial_arguments() {
    let mut chunks = vec![chunk(&json!({"tool_calls":[
        {"index":0,"function":{"name":"search","arguments":"{\"q\":"}}
    ]}))];
    chunks.extend(ending("length", 1, 1));
    let events: Vec<_> = read(&chunks)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    let [
        StreamEvent::ToolCallDelta(delta),
        StreamEvent::ToolCall(call),
        StreamEvent::Done(done),
    ] = events.as_slice()
    else {
        panic!("unexpected events: {events:?}");
    };
    assert_eq!(delta.id, call.id);
    assert!(call.partial);
    assert_eq!(json!(call.arguments), json!({"raw":"{\"q\":"}));
    assert_eq!(done.finish_reason.as_deref(), Some("length"));
}

#[tokio::test]
async fn wire_composition_reaches_the_bus_before_execution() {
    use roomkit::SubscribeRoomOptions;
    use roomkit::realtime::EphemeralEventType;
    use std::time::Duration;

    let mut first = vec![
        chunk(
            &json!({"tool_calls":[{"index":0,"id":"a","function":{"name":"search","arguments":"{\"q\":"}}]}),
        ),
        chunk(&json!({"tool_calls":[{"index":0,"function":{"arguments":"\"secreté\"}"}}]})),
    ];
    first.extend(ending("tool_calls", 1, 1));
    let room = super::Room::built(
        vec![
            Reply::events(&first),
            Reply::events(&[chunk(&json!({"content":"Done."}))]),
        ],
        "test",
        |config| config,
        |options| roomkit_ai::AiChannelOptions {
            thinking_coalesce_ms: 0.0,
            ..options
        },
    )
    .await;
    let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
    room.kit
        .subscribe_room(
            &super::room_id(),
            move |event| {
                let sender = sender.clone();
                async move {
                    if matches!(
                        event.r#type,
                        EphemeralEventType::ToolCallDelta
                            | EphemeralEventType::ToolCallStart
                            | EphemeralEventType::ToolCallEnd
                    ) {
                        sender.send(event).unwrap();
                    }
                }
            },
            SubscribeRoomOptions::default(),
        )
        .await
        .unwrap();
    let result = room.says(&room.sms, "alice", "Alice", "go").await;
    assert!(result.error.is_none());
    let mut events = Vec::new();
    for _ in 0..5 {
        events.push(
            tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    room.kit.close().await.unwrap();
    assert!(received.try_recv().is_err());
    assert_eq!(
        events.iter().map(|e| &e.r#type).collect::<Vec<_>>(),
        [
            &EphemeralEventType::ToolCallDelta,
            &EphemeralEventType::ToolCallDelta,
            &EphemeralEventType::ToolCallDelta,
            &EphemeralEventType::ToolCallStart,
            &EphemeralEventType::ToolCallEnd,
        ]
    );
    assert_eq!(
        events
            .iter()
            .take(3)
            .map(|e| &e.data["tool_calls"])
            .collect::<Vec<_>>(),
        [
            &json!([{"id":"a","name":"search","arguments_chars":5}]),
            &json!([{"id":"a","name":"search","arguments_chars":15}]),
            &json!([]),
        ]
    );
    assert_eq!(
        events[3].data["tool_calls"][0]["arguments"],
        json!({"q":"secreté"})
    );
    assert_eq!(room.fake.requests().len(), 2);
    let history = room.timeline().await;
    assert_eq!(
        history
            .iter()
            .filter(|e| e.r#type == roomkit::model::EventType::ToolCallStart)
            .count(),
        1
    );
    assert!(history.iter().all(|e| json!(e.r#type) != "tool_call_delta"));
}
