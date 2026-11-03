//! The OpenAI agent in a kit, against a fake of the OpenAI wire (RFC §6.4,
//! §9.2): what a turn sends, what the room keeps of its answer, and how a
//! provider's failure surfaces.

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "a test fails on a broken fixture"
)]

mod credentials;
mod fake;
mod generate;
mod reasoning;
mod responses;

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, ChannelType, ErrorRecord, EventType, InboundMessage, MediaContent, RoomEvent,
    RoomId, TextContent, Timestamp,
};
use roomkit::testing::{
    RecordingTransport, RecordingTransportOptions, SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, InboundResult, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{AiChannel, AiChannelOptions, AiContext, AiProvider, AiTool, ToolResult, Tools};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use self::fake::{FakeOpenAi, Reply};
use roomkit_core::rules::transcoding::text_of;

const SYSTEM_PROMPT: &str = "You answer a bakery's customers, in a few words.";

const API_KEY: &str = "sk-test";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A room with an SMS transport, a web chat and the agent, which calls the
/// fake with `api_key`.
struct Room {
    kit: RoomKit,
    sms: RecordingTransport,
    web: RecordingTransport,
    fake: FakeOpenAi,
}

impl Room {
    async fn new(replies: Vec<Reply>, api_key: &str) -> Self {
        Self::configured(replies, api_key, |config| config).await
    }

    /// The room, its agent's configuration adjusted by `adjust`.
    async fn configured(
        replies: Vec<Reply>,
        api_key: &str,
        adjust: impl FnOnce(OpenAiConfig) -> OpenAiConfig,
    ) -> Self {
        Self::built(replies, api_key, adjust, |options| options).await
    }

    /// The room, its agent serving `tools`.
    async fn serving(replies: Vec<Reply>, tools: Tools) -> Self {
        Self::built(
            replies,
            API_KEY,
            |config| config,
            |options| AiChannelOptions {
                tools: tools.into(),
                ..options
            },
        )
        .await
    }

    async fn built(
        replies: Vec<Reply>,
        api_key: &str,
        adjust: impl FnOnce(OpenAiConfig) -> OpenAiConfig,
        equip: impl FnOnce(AiChannelOptions) -> AiChannelOptions,
    ) -> Self {
        let fake = FakeOpenAi::start(replies).await;
        let kit = kit_with_environment(
            RoomKitOptions::default(),
            SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
        );
        let sms = RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        let web = RecordingTransport::new(
            channel("web"),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        );
        let config = adjust(OpenAiConfig {
            base_url: Some(fake.base_url().to_owned()),
            ..OpenAiConfig::new(api_key, "fake-model")
        });
        let options = equip(AiChannelOptions {
            system_prompt: Some(SYSTEM_PROMPT.into()),
            ..AiChannelOptions::default()
        });
        let provider = OpenAiAiProvider::new(config).unwrap();
        let agent = AiChannel::new(channel("assistant"), provider, options).unwrap();
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(web.clone()).unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["sms", "web", "assistant"] {
            kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        Self {
            kit,
            sms,
            web,
            fake,
        }
    }

    /// `text` from `sender` on `transport`, signed `name`.
    async fn says(
        &self,
        transport: &RecordingTransport,
        sender: &str,
        name: &str,
        text: &str,
    ) -> InboundResult {
        let mut message = InboundMessage::new(
            transport.channel_id().clone(),
            sender,
            TextContent::new(text),
        );
        message
            .metadata
            .insert("sender_name".to_owned(), name.into());
        self.kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }

    async fn timeline(&self) -> Vec<RoomEvent> {
        self.kit
            .store()
            .list_events(&room_id(), every_row())
            .await
            .unwrap()
    }
}

/// The events `ON_ERROR` fires with in `room`, as an observer receives them.
fn on_error(room: &Room) -> mpsc::UnboundedReceiver<RoomEvent> {
    let (errors, reported) = mpsc::unbounded_channel();
    let spec = HookOptions {
        execution: HookExecution::Async,
        name: "errors".to_owned(),
        ..Default::default()
    };
    room.kit
        .hook(
            HookTrigger::OnError,
            move |event: RoomEvent, _| {
                errors.send(event).unwrap();
                async { Ok(roomkit::model::HookResult::allow(AllowOptions::default())) }
            },
            spec,
        )
        .unwrap();
    reported
}

/// The next event `reported`, or a failure after a second.
async fn next(reported: &mut mpsc::UnboundedReceiver<RoomEvent>) -> RoomEvent {
    tokio::time::timeout(Duration::from_secs(1), reported.recv())
        .await
        .unwrap()
        .unwrap()
}

/// The messages of the `index`th request the fake received.
fn messages(fake: &FakeOpenAi, index: usize) -> Vec<Value> {
    fake.requests()[index].body["messages"]
        .as_array()
        .unwrap()
        .clone()
}

/// The body Python's provider sends for the same turn, its temperature
/// and its token bound included
/// (`tasks/probes/step5-lot5/py_openai_body.out`).
#[tokio::test]
async fn a_turn_posts_the_system_prompt_and_the_conversation_to_chat_completions() {
    let room = Room::new(vec![Reply::completion("Yes.")], API_KEY).await;
    room.says(&room.sms, "+15550001", "Alice", "Are you open on Sunday?")
        .await;
    let requests = room.fake.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/chat/completions");
    assert_eq!(requests[0].authorization.as_deref(), Some("Bearer sk-test"));
    assert_eq!(
        requests[0].body,
        json!({
            "model": "fake-model",
            "messages": [
                { "role": "system", "content": SYSTEM_PROMPT },
                { "role": "user", "content": "Are you open on Sunday?" },
            ],
            "stream": true,
            "stream_options": { "include_usage": true },
            "temperature": 0.7,
            "max_tokens": 1024,
        })
    );
}

/// Python's configuration keywords on the wire: no temperature for a model
/// that takes none, the bound as `max_completion_tokens`, no usage asked
/// of the stream (`tasks/probes/step5-lot5/py_openai_body.out`).
#[tokio::test]
async fn the_configuration_shapes_the_completion_request_as_python_s() {
    let room = Room::configured(vec![Reply::completion("Yes.")], API_KEY, |config| {
        OpenAiConfig {
            supports_custom_temperature: Some(false),
            use_max_completion_tokens: Some(true),
            include_stream_usage: false,
            ..config
        }
    })
    .await;
    room.says(&room.sms, "+15550001", "Alice", "Are you open on Sunday?")
        .await;
    assert_eq!(
        room.fake.requests()[0].body,
        json!({
            "model": "fake-model",
            "messages": [
                { "role": "system", "content": SYSTEM_PROMPT },
                { "role": "user", "content": "Are you open on Sunday?" },
            ],
            "stream": true,
            "max_completion_tokens": 1024,
        })
    );
}

/// RFC §6.4: the round declares the tools, and the call's result goes back
/// in the next completion, after the assistant's message that made it.
#[tokio::test]
async fn a_call_s_result_goes_back_in_the_next_completion() {
    let lookup = AiTool {
        parameters: json!({ "type": "object", "properties": { "order": { "type": "string" } } }),
        ..AiTool::new("lookup_order", "Find an order").unwrap()
    };
    let tools = Tools::new().tool(lookup, |_call, _context| async {
        Ok(ToolResult::Json(json!({ "status": "shipped" })))
    });
    let replies = vec![
        Reply::calling(
            "Let me check.",
            "call_1",
            "lookup_order",
            r#"{"order": "42"}"#,
        ),
        Reply::completion("It shipped."),
    ];
    let room = Room::serving(replies, tools).await;
    room.says(&room.sms, "+15550001", "Alice", "Where is order 42?")
        .await;
    let requests = room.fake.requests();
    assert_eq!(
        (
            requests[0].body["tools"].clone(),
            messages(&room.fake, 1).get(2..).unwrap().to_vec()
        ),
        (
            json!([{
                "type": "function",
                "function": {
                    "name": "lookup_order",
                    "description": "Find an order",
                    "parameters": { "type": "object", "properties": { "order": { "type": "string" } } },
                },
            }]),
            vec![
                json!({
                    "role": "assistant",
                    "content": "Let me check.",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "lookup_order", "arguments": r#"{"order":"42"}"# },
                    }],
                }),
                json!({ "role": "tool", "tool_call_id": "call_1", "content": r#"{"status": "shipped"}"# }),
            ]
        )
    );
}

/// Python's `extra_body` wins over RoomKit's own members, and an
/// `Authorization` among `default_headers` replaces the key's bearer
/// (`tasks/probes/step5-review/mcp-openai/py_openai_wire.out`).
#[tokio::test]
async fn extra_body_and_default_headers_win_as_python_s() {
    let room = Room::configured(vec![Reply::completion("Yes.")], API_KEY, |config| {
        OpenAiConfig {
            use_max_completion_tokens: Some(true),
            extra_body: Some(
                serde_json::from_value(
                    json!({ "temperature": 0.1, "max_completion_tokens": 7, "top_k": 3 }),
                )
                .unwrap(),
            ),
            default_headers: Some(
                [("Authorization", "Basic dXNlcjpwYXNz"), ("X-Proxy", "1")]
                    .map(|(name, value)| (name.to_owned(), value.to_owned()))
                    .into(),
            ),
            ..config
        }
    })
    .await;
    room.says(&room.sms, "+15550001", "Alice", "Are you open on Sunday?")
        .await;
    let request = &room.fake.requests()[0];
    assert_eq!(
        (
            ["temperature", "max_completion_tokens", "top_k"]
                .map(|name| request.body[name].clone()),
            request.authorizations.clone()
        ),
        (
            [json!(0.1), json!(7), json!(3)],
            vec!["Basic dXNlcjpwYXNz".to_owned()]
        )
    );
}

#[tokio::test]
async fn a_request_asks_for_an_uncompressed_answer_even_with_gzip_enabled() {
    let room = Room::new(vec![Reply::completion("Yes.")], "sk-test").await;
    room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(room.fake.requests()[0].accept_encoding, None);
}

#[tokio::test]
async fn an_empty_api_key_sends_no_authorization() {
    let room = Room::new(vec![Reply::completion("Yes.")], "").await;
    room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(room.fake.requests()[0].authorization, None);
}

#[tokio::test]
async fn the_answer_becomes_the_agent_s_response() {
    let room = Room::new(vec![Reply::completion("Yes, from 8 to noon.")], API_KEY).await;
    let result = room
        .says(&room.sms, "+15550001", "Alice", "Are you open on Sunday?")
        .await;
    assert_eq!(result.error, None);
    assert_eq!(room.sms.texts(), ["Yes, from 8 to noon."]);
    let timeline = room.timeline().await;
    let answer = timeline.last().unwrap();
    assert_eq!(answer.source.channel_id, channel("assistant"));
    assert_eq!(
        answer.responds_to,
        result.event.map(|asked| asked.id),
        "the answer answers the message that asked"
    );
}

#[tokio::test]
async fn two_senders_with_no_name_on_one_number_are_labelled_apart() {
    let room = Room::new(vec![Reply::completion("Noted.")], API_KEY).await;
    for (sender, text) in [("+15550001", "Refund me"), ("+15550002", "Do not")] {
        let message = InboundMessage::new(
            room.sms.channel_id().clone(),
            sender,
            TextContent::new(text),
        );
        room.kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let sent = messages(&room.fake, 1);
    assert_eq!(sent[1]["content"], "@sms: \"Refund me\"");
    let input = sent[3]["content"].as_str().unwrap();
    assert!(input.starts_with("@sms (2): \"Do not\"\n\n"), "{input}");
}

#[tokio::test]
async fn an_instruction_is_sent_as_the_application_s_marked_input() {
    let room = Room::new(vec![Reply::completion("Welcome!")], API_KEY).await;
    let instruction = InboundMessage {
        event_type: EventType::Instruction,
        addressed_to: Some(vec![channel("assistant")]),
        ..InboundMessage::new(
            room.web.channel_id().clone(),
            "app",
            TextContent::new("Greet the customer."),
        )
    };
    room.kit
        .process_inbound(
            instruction,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        messages(&room.fake, 0)[1],
        json!({
            "role": "user",
            "content": "[Instruction from the application: nobody in this conversation said \
                        it. Act on it now, in your own words, without quoting it.]\nGreet the \
                        customer.",
        })
    );
    assert_eq!(room.sms.texts(), ["Welcome!"]);
}

#[tokio::test]
async fn a_provider_error_fails_the_turn_and_fires_on_error() {
    let room = Room::new(
        vec![Reply::status(
            401,
            r#"{"error": {"message": "Incorrect API key provided.", "type": "invalid_request_error"}}"#,
        )],
        API_KEY,
    )
    .await;
    let mut reported = on_error(&room);
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    let message = "openai (401): the provider answered HTTP 401 (invalid_request_error): \
                   Incorrect API key provided.";
    // Raised as Python's `ProviderError`, its class kept (RMK-1208), its
    // attributes as Python's `error_record` keeps them (RMK-1239).
    let attributes = json!({
        "retryable": false,
        "provider": "openai",
        "status_code": 401,
        "context_overflow": null,
    });
    assert_eq!(
        result.error,
        Some(ErrorRecord {
            attributes: Box::new(attributes.as_object().unwrap().clone()),
            ..ErrorRecord::new("ProviderError", message)
        })
    );
    assert!(!result.blocked, "the message stays delivered");
    assert_eq!(
        room.sms.texts(),
        Vec::<String>::new(),
        "nothing reaches the room"
    );
    let on_error = next(&mut reported).await;
    assert_eq!(text_of(&on_error.content), message);
    // The turn's response failed as it was read (RFC §12.2 step 13s).
    assert_eq!(on_error.metadata["error_category"], "streaming");
}

/// RMK-1197: an author's image whose `data:` URI does not read, here
/// URL-safe base64, fails its turn before anything is sent, as Python's
/// `image_part_uri` refuses it; the room then reads it as refused
/// (Python's `RefusedImages`, RFC §6.4), so the next turn goes.
// Python: tests/test_providers/test_image_parts.py::test_a_malformed_uri_is_a_non_retryable_provider_error_naming_the_cause
#[tokio::test]
async fn an_image_its_provider_cannot_read_fails_one_turn_then_reads_as_refused() {
    let room = Room::configured(
        vec![Reply::completion("I cannot see it.")],
        API_KEY,
        |config| OpenAiConfig {
            vision: true,
            ..config
        },
    )
    .await;
    let mut photo = MediaContent::new("data:image/png;base64,AQ-_BA==", "image/png").unwrap();
    photo.filename = Some("lamp.png".to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let message = InboundMessage::new(channel("web"), "alice", photo);
    let first = room.kit.process_inbound(message, options).await.unwrap();
    assert_eq!(
        first.error.map(|error| error.message),
        Some("openai: invalid image part: data URI payload is not valid base64".to_owned())
    );
    assert!(room.fake.requests().is_empty(), "nothing is sent");
    room.says(&room.web, "alice", "Alice", "and now?").await;
    let sent = Value::Array(messages(&room.fake, 0)).to_string();
    assert!(
        sent.contains("[Image refused by the model's provider: “lamp.png”]"),
        "{sent}"
    );
}

#[tokio::test]
async fn an_answer_that_is_no_chat_completion_fails_the_turn() {
    let room = Room::new(
        vec![Reply::status(200, "<html>Bad gateway</html>")],
        API_KEY,
    )
    .await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    let error = result.error.unwrap();
    assert!(
        error
            .message
            .starts_with("openai: the provider's answer is malformed: "),
        "{error:?}"
    );
    assert_eq!(room.sms.texts(), Vec::<String>::new());
}

#[tokio::test]
async fn an_unreachable_endpoint_fails_the_turn_without_naming_its_url() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let config = OpenAiConfig {
        base_url: Some(format!("http://{address}/v1")),
        ..OpenAiConfig::new(API_KEY, "m")
    };
    let provider = OpenAiAiProvider::new(config).unwrap();
    let agent =
        AiChannel::new(channel("assistant"), provider, AiChannelOptions::default()).unwrap();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "assistant"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(
        sms.channel_id().clone(),
        "+15550001",
        TextContent::new("Hi"),
    );
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let error = result.error.unwrap();
    assert!(
        error
            .message
            .starts_with("openai: the completion request failed: "),
        "{error:?}"
    );
    assert!(
        !error.message.contains(&address.to_string()),
        "a base URL may carry a secret: {error:?}"
    );
}

#[tokio::test]
async fn the_api_key_an_error_answer_echoes_reaches_neither_the_error_nor_on_error() {
    for body in [
        r#"{"error": {"message": "Bearer sk-test is refused", "type": "invalid_request_error", "code": "invalid_api_key"}}"#,
        "Authorization: Bearer sk-test\r\nrefused",
    ] {
        let room = Room::new(vec![Reply::status(401, body)], API_KEY).await;
        let mut reported = on_error(&room);
        let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
        let error = result.error.unwrap();
        assert!(!error.message.contains(API_KEY), "{error:?}");
        let on_error = serde_json::to_string(&next(&mut reported).await).unwrap();
        assert!(!on_error.contains(API_KEY), "{on_error}");
    }
}

/// Past the 4 MiB a turn reads of an answer.
const TOO_LONG: usize = 5 * 1024 * 1024;

#[tokio::test]
async fn an_answer_streaming_past_the_limit_fails_the_turn() {
    let room = Room::new(vec![Reply::streaming(TOO_LONG)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "openai: the provider's answer is longer than 4194304 bytes"
    );
}

#[tokio::test]
async fn an_answer_declaring_more_than_the_limit_fails_the_turn() {
    let room = Room::new(vec![Reply::declaring(TOO_LONG)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "openai: the provider's answer is longer than 4194304 bytes"
    );
}

#[tokio::test]
async fn a_redirect_is_refused_and_the_request_never_follows_it() {
    let target = FakeOpenAi::start(vec![Reply::completion("Followed.")]).await;
    let location = format!("{}/chat/completions", target.base_url());
    let room = Room::new(vec![Reply::redirect(&location)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "openai (307): the provider answered HTTP 307: Temporary Redirect"
    );
    assert_eq!(target.requests().len(), 0, "nothing followed the redirect");
}

#[tokio::test]
async fn a_completion_past_the_timeout_fails_the_turn() {
    let room = Room::configured(vec![Reply::silent()], API_KEY, |config| OpenAiConfig {
        timeout: Duration::from_millis(50),
        ..config
    })
    .await;
    let turn = room.says(&room.sms, "+15550001", "Alice", "Hi");
    let result = tokio::time::timeout(Duration::from_secs(2), turn)
        .await
        .expect("the turn ends at its timeout");
    let error = result.error.unwrap();
    assert!(
        error
            .message
            .starts_with("openai: the completion request failed: ")
            && error.message.contains("timed out"),
        "{error:?}"
    );
}

/// The timeout bounds the wait between two reads, not the whole answer,
/// as Python's `httpx.Timeout(timeout)` bounds each read: a stream whose
/// events come 50 ms apart is read whole under a 150 ms timeout, though it
/// lasts 250 ms (RMK-1103).
#[tokio::test]
async fn the_timeout_bounds_reads_not_the_stream() {
    let mut chunks = vec![
        chunk(&json!({ "role": "assistant", "content": "We " })),
        chunk(&json!({ "content": "open " })),
        chunk(&json!({ "content": "at nine." })),
    ];
    chunks.extend(ending("stop", 30, 6));
    let paced = Reply::paced(&chunks, Duration::from_millis(50));
    let room = Room::configured(vec![paced], API_KEY, |config| OpenAiConfig {
        timeout: Duration::from_millis(150),
        ..config
    })
    .await;
    let result = room.says(&room.sms, "+15550001", "Alice", "When?").await;
    assert_eq!(result.error, None);
    assert_eq!(room.sms.texts(), ["We open at nine."]);
}

/// One chunk of a streamed completion, its first choice's `delta`.
fn chunk(delta: &Value) -> Value {
    json!({ "object": "chat.completion.chunk", "choices": [{ "index": 0, "delta": delta }] })
}

/// The last chunks of a streamed completion: its end, then its usage
/// (`stream_options.include_usage`).
fn ending(finish_reason: &str, prompt: u64, completion: u64) -> [Value; 2] {
    [
        json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": finish_reason }] }),
        json!({ "choices": [], "usage": { "prompt_tokens": prompt, "completion_tokens": completion } }),
    ]
}

/// RFC §6.4: a streamed completion's text deltas join to the agent's
/// answer, and the usage its last chunk carries is the turn's record.
#[tokio::test]
async fn a_streamed_completion_s_deltas_join_to_the_answer_and_its_usage_is_recorded() {
    let mut chunks = vec![
        chunk(&json!({ "role": "assistant", "content": "We open " })),
        chunk(&json!({ "content": "at nine." })),
    ];
    chunks.extend(ending("stop", 30, 6));
    let room = Room::new(vec![Reply::events(&chunks)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "When?").await;
    assert_eq!(result.error, None);
    let answer = room.timeline().await.pop().unwrap();
    assert_eq!(
        (
            text_of(&answer.content),
            answer.metadata.get("ai_usage").cloned()
        ),
        (
            "We open at nine.".to_owned(),
            Some(json!({ "input_tokens": 30, "output_tokens": 6 }))
        )
    );
    assert_eq!(room.sms.texts(), ["We open at nine."]);
}

/// RFC §6.4 "What a provider hands the loop for a call": a streamed call's
/// fragments are folded by their index into one call, which runs, its
/// result going back in the next completion.
#[tokio::test]
async fn a_streamed_call_s_fragments_fold_into_the_call_that_runs() {
    let mut first = vec![
        chunk(
            &json!({ "tool_calls": [{ "index": 0, "id": "call_7", "type": "function",
            "function": { "name": "lookup_order", "arguments": "" } }] }),
        ),
        chunk(
            &json!({ "tool_calls": [{ "index": 0, "function": { "arguments": "{\"order\"" } }] }),
        ),
        chunk(&json!({ "tool_calls": [{ "index": 0, "function": { "arguments": ": \"42\"}" } }] })),
    ];
    first.extend(ending("tool_calls", 20, 5));
    let mut second = vec![chunk(&json!({ "content": "Shipped." }))];
    second.extend(ending("stop", 40, 2));
    let lookup = AiTool {
        parameters: json!({ "type": "object", "properties": { "order": { "type": "string" } } }),
        ..AiTool::new("lookup_order", "Find an order").unwrap()
    };
    let tools = Tools::new().tool(lookup, |call, _context| async move {
        Ok(ToolResult::Json(
            json!({ "order": call.arguments["order"] }),
        ))
    });
    let room = Room::serving(vec![Reply::events(&first), Reply::events(&second)], tools).await;
    room.says(&room.sms, "+15550001", "Alice", "Where is 42?")
        .await;
    let results: Vec<Value> = messages(&room.fake, 1)
        .into_iter()
        .filter(|message| message["role"] == "tool")
        .collect();
    assert_eq!(
        results,
        [json!({ "role": "tool", "content": "{\"order\": \"42\"}", "tool_call_id": "call_7" })]
    );
}

/// Security: a stream whose line never ends is cut at the line's bound, 1
/// MiB, the turn failing, rather than growing the memory without end.
#[tokio::test]
async fn a_stream_that_never_ends_a_line_is_cut_at_the_line_s_bound() {
    let room = Room::new(vec![Reply::endless("x", TOO_LONG)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "openai: the provider's stream sent a line longer than 1048576 bytes"
    );
}

/// Security: a stream whose events never stop is cut at the answer's
/// bound, 4 MiB, the turn failing.
#[tokio::test]
async fn a_stream_of_endless_events_is_cut_at_the_answer_s_bound() {
    let event = format!("data: {}\n\n", chunk(&json!({ "content": "" })));
    let room = Room::new(vec![Reply::endless(&event, TOO_LONG)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "openai: the provider's answer is longer than 4194304 bytes"
    );
}

/// Security: a stream whose one event never ends, data line after data
/// line, is cut at the event's bound, 1 MiB.
#[tokio::test]
async fn a_stream_whose_event_never_ends_is_cut_at_the_event_s_bound() {
    let line = format!("data: {}\n", "x".repeat(1000));
    let room = Room::new(vec![Reply::endless(&line, TOO_LONG)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "openai: the provider's stream sent an event longer than 1048576 bytes"
    );
}

/// RFC §6.7: on OpenAI's own endpoint, a tool whose name OpenAI refuses
/// (`[A-Za-z0-9_-]{1,128}`, the rule the reference implementation measured)
/// fails the request before it is sent, naming the tool and the rule, as
/// Python's provider raises a `ProviderError` not retried
/// (`tasks/probes/step4-m9/fix-pass/py_openai_tool_names.out`); the rule
/// for each base URL is `names`' unit test.
#[tokio::test]
async fn a_tool_name_openai_refuses_fails_the_request_before_it_is_sent() {
    let provider = OpenAiAiProvider::new(OpenAiConfig::new(API_KEY, "gpt-6-luna")).unwrap();
    let tool = AiTool {
        parameters: json!({ "type": "object" }),
        ..AiTool::new("files.read", "Reads a file").unwrap()
    };
    let context = AiContext {
        tools: vec![tool],
        ..AiContext::default()
    };
    let failed = provider.generate(&context).await.unwrap_err();
    assert_eq!(
        (
            failed.message.as_str(),
            failed.retryable,
            failed.provider.as_str(),
            failed.context_overflow
        ),
        (
            "tool 'files.read': openai accepts tool names matching [A-Za-z0-9_-]{1,128}",
            false,
            "openai",
            Some(false)
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}
