//! A model's reasoning as the provider reads it (RFC §6.7): the field
//! beside `content` and the `<think>` tags on Chat Completions, translated
//! from Python's provider tests, and the reasoning items of Responses.

use std::pin::Pin;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::ProcessInboundOptions;
use roomkit::model::{EventType, InboundMessage, MediaContent};
use roomkit_ai::{
    AiContext, AiMessage, AiProvider, AiRole, AiTool, StreamEvent, ToolResult, Tools,
};
use roomkit_core::rules::transcoding::text_of;
use roomkit_openai::{OpenAiAiProvider, OpenAiApi, OpenAiConfig};
use serde_json::{Value, json};

use super::fake::{FakeOpenAi, Reply};
use super::{API_KEY, Room, channel, chunk, ending, messages, room_id};

async fn provider(api: OpenAiApi, reply: Reply) -> (OpenAiAiProvider, FakeOpenAi) {
    let fake = FakeOpenAi::start(vec![reply]).await;
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

/// A whole completion whose message carries `message`'s members.
fn completion(message: &Value) -> Reply {
    let body = json!({ "choices": [{ "message": message, "finish_reason": "stop" }] });
    Reply::status(200, &body.to_string())
}

/// What `generate` reads of `message`: its reasoning, its text.
async fn generated(message: &Value) -> (Option<String>, String) {
    let (provider, _fake) = provider(OpenAiApi::ChatCompletions, completion(message)).await;
    let response = provider.generate(&hello()).await.unwrap();
    (response.thinking, response.content)
}

/// A piece of a stream, as the provider hands it.
#[derive(Debug, PartialEq, Eq)]
enum Piece {
    Thinking(String),
    Text(String),
}

fn thinking(text: &str) -> Piece {
    Piece::Thinking(text.to_owned())
}

fn text(text: &str) -> Piece {
    Piece::Text(text.to_owned())
}

/// The reasoning and text `reply` streams, in order.
async fn streamed(api: OpenAiApi, reply: Reply) -> Vec<Piece> {
    let (provider, _fake) = provider(api, reply).await;
    let context = hello();
    let mut events = provider.generate_structured_stream(&context);
    let mut pieces = Vec::new();
    while let Some(event) = std::future::poll_fn(|cx| Pin::new(&mut events).poll_next(cx)).await {
        match event.unwrap() {
            StreamEvent::ThinkingDelta(delta) => pieces.push(Piece::Thinking(delta.thinking)),
            StreamEvent::TextDelta(delta) => pieces.push(Piece::Text(delta.text)),
            StreamEvent::ToolCall(_) | StreamEvent::Done(_) => {}
        }
    }
    pieces
}

/// The chunks of a stream, each delta one of `deltas`.
fn chunks(deltas: &[Value]) -> Reply {
    let chunks: Vec<Value> = deltas
        .iter()
        .map(|delta| json!({ "choices": [{ "index": 0, "delta": delta }] }))
        .collect();
    Reply::events(&chunks)
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_generate_extracts_think_tags
#[tokio::test]
async fn generate_extracts_think_tags() {
    let read = generated(&json!({ "content": "<think>Let me reason</think>The answer." })).await;
    assert_eq!(
        read,
        (Some("Let me reason".to_owned()), "The answer.".to_owned())
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_generate_no_think_tags
#[tokio::test]
async fn generate_no_think_tags() {
    let read = generated(&json!({ "content": "Plain." })).await;
    assert_eq!(read, (None, "Plain.".to_owned()));
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_generate_reads_reasoning_content_field
#[tokio::test]
async fn generate_reads_reasoning_content_field() {
    let message = json!({ "content": "42.", "reasoning_content": "Weighing." });
    assert_eq!(
        generated(&message).await,
        (Some("Weighing.".to_owned()), "42.".to_owned())
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_generate_reads_openrouter_reasoning_field
#[tokio::test]
async fn generate_reads_openrouter_reasoning_field() {
    let message = json!({ "content": "42.", "reasoning": "Weighing." });
    assert_eq!(
        generated(&message).await,
        (Some("Weighing.".to_owned()), "42.".to_owned())
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_generate_merges_both_reasoning_conventions
#[tokio::test]
async fn generate_merges_both_reasoning_conventions() {
    let message = json!({ "content": "<think>inline.</think>42.", "reasoning_content": "field." });
    assert_eq!(
        generated(&message).await,
        (Some("inline.field.".to_owned()), "42.".to_owned())
    );
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_structured_stream_with_think_tags
#[tokio::test]
async fn structured_stream_with_think_tags() {
    let deltas = ["<think>", "reason", "</think>", "answer"].map(|c| json!({ "content": c }));
    let read = streamed(OpenAiApi::ChatCompletions, chunks(&deltas)).await;
    assert_eq!(read, [thinking("reason"), text("answer")]);
}

// Python: tests/text_conformance/test_responses.py::test_a_think_block_the_cap_cut_is_reasoning
// (a stream that ends inside its block flushes it as reasoning).
#[tokio::test]
async fn a_stream_cut_inside_its_think_block_flushes_it_as_reasoning() {
    let deltas = ["<think>still weighing</thi"].map(|c| json!({ "content": c }));
    let read = streamed(OpenAiApi::ChatCompletions, chunks(&deltas)).await;
    assert_eq!(read, [thinking("still weighing"), thinking("</thi")]);
}

// Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_structured_stream_with_reasoning_content_field
#[tokio::test]
async fn structured_stream_with_reasoning_content_field() {
    let deltas = [
        json!({ "content": null, "reasoning_content": "thinking" }),
        json!({ "content": "answer" }),
    ];
    let read = streamed(OpenAiApi::ChatCompletions, chunks(&deltas)).await;
    assert_eq!(read, [thinking("thinking"), text("answer")]);
}

/// RMK-1234 item 1, RFC §6.4: a chunk of reasoning alone is an event, so a
/// stream under way: a server that reasons and then fails does not refuse
/// the turn's author image, which the next turn sends again. Python's
/// `RefusedImages` asks that nothing was handed.
#[tokio::test]
async fn a_stream_that_only_reasoned_before_it_failed_refuses_no_image() {
    let reasoned = json!({ "choices": [{ "index": 0, "delta": { "reasoning_content": "Hm." } }] });
    let failing = json!({ "error": { "message": "invalid image", "code": 400 } });
    let reply = Reply::sse(&format!("data: {reasoned}\n\ndata: {failing}\n\n"));
    let room = Room::configured(
        vec![reply, Reply::completion("I see it.")],
        API_KEY,
        |config| OpenAiConfig {
            vision: true,
            ..config
        },
    )
    .await;
    let photo = MediaContent::new("data:image/png;base64,AQIDBA==", "image/png").unwrap();
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let message = InboundMessage::new(channel("web"), "alice", photo);
    let first = room.kit.process_inbound(message, options).await.unwrap();
    assert!(first.error.is_some(), "the turn fails");
    room.says(&room.web, "alice", "Alice", "and now?").await;
    let sent = Value::Array(messages(&room.fake, 1)).to_string();
    assert!(!sent.contains("Image refused"), "{sent}");
    assert!(sent.contains("image_url"), "{sent}");
}

/// A Responses reasoning item.
fn reasoning(summary: &str) -> Value {
    json!({ "type": "reasoning", "id": "rs_1", "encrypted_content": "opaque",
        "summary": [{ "type": "summary_text", "text": summary }] })
}

/// A Responses message saying `text`.
fn said(text: &str) -> Value {
    json!({ "type": "message", "role": "assistant", "status": "completed",
        "content": [{ "type": "output_text", "text": text, "annotations": [] }] })
}

/// A terminal Responses event holding `output`.
fn terminal(output: &[Value]) -> Value {
    json!({ "type": "response.completed", "response": {
        "id": "resp_1", "status": "completed", "model": "fake", "output": output,
        "usage": { "input_tokens": 1, "output_tokens": 1 } } })
}

/// A Responses reasoning item's summary is the model's reasoning, read
/// whole or streamed; a Rust-only API, Python's provider has no Responses.
#[tokio::test]
async fn a_responses_reasoning_item_reads_as_thinking() {
    let done = terminal(&[reasoning("Weighing."), said("42.")]);
    let read = streamed(
        OpenAiApi::Responses,
        Reply::events(std::slice::from_ref(&done)),
    )
    .await;
    assert_eq!(read, [thinking("Weighing."), text("42.")]);
    let whole = Reply::status(200, &done["response"].to_string());
    let (provider, _fake) = provider(OpenAiApi::Responses, whole).await;
    let response = provider.generate(&hello()).await.unwrap();
    assert_eq!(response.thinking.as_deref(), Some("Weighing."));
    assert_eq!(response.content, "42.");
}

/// Reasoning deltas come as written, and the terminal response repeats none.
#[tokio::test]
async fn a_responses_stream_s_reasoning_deltas_come_once() {
    let events = [
        json!({ "type": "response.reasoning_summary_text.delta", "delta": "Weigh" }),
        json!({ "type": "response.reasoning_summary_text.delta", "delta": "ing." }),
        json!({ "type": "response.output_text.delta", "delta": "42." }),
        terminal(&[reasoning("Weighing."), said("42.")]),
    ];
    let read = streamed(OpenAiApi::Responses, Reply::events(&events)).await;
    assert_eq!(read, [thinking("Weigh"), thinking("ing."), text("42.")]);
}

// Python: tests/text_conformance/test_responses.py::test_a_think_block_reads_the_same_streamed_or_not
// (RMK-531): the same answer, spaces included, whole or streamed, a blank
// answer included.
#[tokio::test]
async fn a_think_block_reads_the_same_streamed_or_not() {
    for (content, reasoning, answer) in [
        (
            "<think> weighing it \n</think>\n\nThe answer.  ",
            " weighing it \n",
            "\n\nThe answer.  ",
        ),
        ("<think>x</think>\n\n", "x", "\n\n"),
    ] {
        let whole = generated(&json!({ "content": content })).await;
        assert_eq!(whole, (Some(reasoning.to_owned()), answer.to_owned()));
        let read = streamed(
            OpenAiApi::ChatCompletions,
            chunks(&[json!({ "content": content })]),
        )
        .await;
        assert_eq!(read, [thinking(reasoning), text(answer)]);
    }
}

/// A reasoning delta that is no text or empty reads as nothing, the
/// terminal response's reasoning then standing.
#[tokio::test]
async fn a_responses_reasoning_delta_with_nothing_in_it_reads_as_none() {
    for delta in [json!(""), json!(null)] {
        let events = [
            json!({ "type": "response.reasoning_summary_text.delta", "delta": delta }),
            terminal(&[reasoning("Weighing."), said("42.")]),
        ];
        let read = streamed(OpenAiApi::Responses, Reply::events(&events)).await;
        assert_eq!(read, [thinking("Weighing."), text("42.")]);
    }
}

// Python: tests/test_providers/test_chat_request.py::test_a_provider_without_a_reasoning_field_sends_none
/// End to end on Chat Completions (RFC §6.4): a round goes back with its text
/// and its call, and its reasoning reaches neither the request nor the room.
#[tokio::test]
async fn a_round_s_reasoning_reaches_no_request_and_is_not_stored() {
    let calls = json!([{ "id": "c1", "type": "function",
        "function": { "name": "lookup", "arguments": "{}" } }]);
    // In a field beside the text, and in the text as `<think>` tags.
    for round in [
        json!({ "reasoning_content": "Why.", "content": "Look.", "tool_calls": calls }),
        json!({ "content": "<think>Why.</think>Look.", "tool_calls": calls }),
    ] {
        let room = Room::new(
            vec![completion(&round), Reply::completion("Done.")],
            API_KEY,
        )
        .await;
        room.says(&room.web, "alice", "Alice", "go").await;

        let sent = messages(&room.fake, 1);
        let replayed = sent.iter().find(|message| message["role"] == "assistant");
        let replayed = replayed.expect("the round goes back");
        assert_eq!(replayed["content"], "Look.");
        assert_eq!(replayed["tool_calls"][0]["id"], "c1");
        let sent = Value::Array(sent.clone()).to_string();
        assert!(!sent.contains("Why."), "{sent}");
        let stored = format!("{:?}", room.timeline().await);
        assert!(!stored.contains("Why."), "{stored}");
    }
}

/// What the room stored of the agent's messages.
async fn answers(room: &Room) -> Vec<String> {
    let timeline = room.timeline().await;
    let answers = timeline.iter().filter(|event| {
        event.source.channel_id == channel("assistant") && event.r#type == EventType::Message
    });
    answers.map(|event| text_of(&event.content)).collect()
}

// Python: tests/test_providers/test_openai.py::TestThinkTagsInARoom::test_each_round_of_a_tool_loop_reads_its_own_start
/// RMK-1327: each round of a tool loop is its own generation, its opening
/// block reasoning, stored nowhere and sent back in no request.
#[tokio::test]
async fn each_round_of_a_tool_loop_reads_its_own_start() {
    let mut first = vec![
        chunk(&json!({ "content": "<think>WHY-1</think>" })),
        chunk(
            &json!({ "tool_calls": [{ "index": 0, "id": "c1", "type": "function",
            "function": { "name": "lookup", "arguments": "{}" } }] }),
        ),
    ];
    first.extend(ending("tool_calls", 1, 1));
    let mut second = vec![chunk(&json!({ "content": "<think>WHY-2</think>Sunny." }))];
    second.extend(ending("stop", 1, 1));
    let lookup = AiTool::new("lookup", "Look it up.").unwrap();
    let tools = Tools::new().tool(lookup, |_call, _context| async move {
        Ok(ToolResult::Text("sunny".to_owned()))
    });
    let room = Room::serving(vec![Reply::events(&first), Reply::events(&second)], tools).await;
    room.says(&room.web, "alice", "Alice", "weather?").await;

    assert_eq!(answers(&room).await, ["Sunny."]);
    let replayed = Value::Array(messages(&room.fake, 1)).to_string();
    assert!(!replayed.contains("WHY-1"), "{replayed}");
    assert!(!replayed.contains("<think>"), "{replayed}");
}
