//! The OpenAI agent in a kit, against a fake of the OpenAI wire (RFC §6.4,
//! §9.2): what a turn sends, what the room keeps of its answer, and how a
//! provider's failure surfaces.

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "a test fails on a broken fixture"
)]

mod fake;

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, ErrorRecord, EventContent, EventType, InboundMessage, RoomEvent,
    RoomId, Timestamp,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    ConversationStore, HookExecution, HookSpec, HookTrigger, InboundResult, RoomKit,
    SequentialEnvironment, text_of,
};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use self::fake::{FakeOpenAi, Reply};

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
        let fake = FakeOpenAi::start(replies).await;
        let kit = RoomKit::new();
        kit.set_environment(SequentialEnvironment::new(
            Timestamp::from_unix_millis(1_792_677_780_000).unwrap(),
        ));
        let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
        let web = RecordingTransport::new(channel("web"), ChannelType::Websocket);
        let config = adjust(
            OpenAiConfig::new(fake.base_url(), api_key, "fake-model")
                .with_system_prompt(SYSTEM_PROMPT),
        );
        let agent = OpenAiAgent::new(channel("assistant"), config).unwrap();
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(web.clone()).unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        for id in ["sms", "web", "assistant"] {
            kit.attach(&room_id(), &channel(id)).await.unwrap();
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
        let mut message =
            InboundMessage::new(transport.id().clone(), sender, EventContent::text(text));
        message
            .metadata
            .insert("sender_name".to_owned(), name.into());
        self.kit
            .process_inbound_in(&room_id(), message)
            .await
            .unwrap()
    }

    async fn timeline(&self) -> Vec<RoomEvent> {
        self.kit
            .store()
            .list_events(&room_id(), true)
            .await
            .unwrap()
    }
}

/// The events `ON_ERROR` fires with in `room`, as an observer receives them.
fn on_error(room: &Room) -> mpsc::UnboundedReceiver<RoomEvent> {
    let (errors, reported) = mpsc::unbounded_channel();
    let spec = HookSpec::new("errors", HookTrigger::OnError, HookExecution::Async);
    room.kit
        .hook(spec, move |event, _| {
            errors.send(event).unwrap();
            async { Ok(roomkit::model::HookResult::allow()) }
        })
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
        })
    );
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
        let message = InboundMessage::new(room.sms.id().clone(), sender, EventContent::text(text));
        room.kit
            .process_inbound_in(&room_id(), message)
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
            room.web.id().clone(),
            "app",
            EventContent::text("Greet the customer."),
        )
    };
    room.kit
        .process_inbound_in(&room_id(), instruction)
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
    let message =
        "the provider answered HTTP 401 (invalid_request_error): Incorrect API key provided.";
    assert_eq!(
        result.error,
        Some(ErrorRecord::new("ChannelError", message))
    );
    assert!(!result.blocked, "the message stays delivered");
    assert_eq!(
        room.sms.texts(),
        Vec::<String>::new(),
        "nothing reaches the room"
    );
    let on_error = next(&mut reported).await;
    assert_eq!(text_of(&on_error.content), message);
    assert_eq!(on_error.metadata["error_category"], "generation");
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
            .starts_with("the provider's answer is not a chat completion: "),
        "{error:?}"
    );
    assert_eq!(room.sms.texts(), Vec::<String>::new());
}

#[tokio::test]
async fn an_unreachable_endpoint_fails_the_turn_without_naming_its_url() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
    let config = OpenAiConfig::new(format!("http://{address}/v1"), API_KEY, "m");
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(OpenAiAgent::new(channel("assistant"), config).unwrap())
        .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    for id in ["sms", "assistant"] {
        kit.attach(&room_id(), &channel(id)).await.unwrap();
    }
    let message = InboundMessage::new(sms.id().clone(), "+15550001", EventContent::text("Hi"));
    let result = kit.process_inbound_in(&room_id(), message).await.unwrap();
    let error = result.error.unwrap();
    assert!(
        error.message.starts_with("the completion request failed: "),
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
        "the provider's answer is longer than 4194304 bytes"
    );
}

#[tokio::test]
async fn an_answer_declaring_more_than_the_limit_fails_the_turn() {
    let room = Room::new(vec![Reply::declaring(TOO_LONG)], API_KEY).await;
    let result = room.says(&room.sms, "+15550001", "Alice", "Hi").await;
    assert_eq!(
        result.error.unwrap().message,
        "the provider's answer is longer than 4194304 bytes"
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
        "the provider answered HTTP 307: Temporary Redirect"
    );
    assert_eq!(target.requests().len(), 0, "nothing followed the redirect");
}

#[tokio::test]
async fn a_completion_past_the_timeout_fails_the_turn() {
    let room = Room::configured(vec![Reply::silent()], API_KEY, |config| {
        config.with_timeout(Duration::from_millis(50))
    })
    .await;
    let turn = room.says(&room.sms, "+15550001", "Alice", "Hi");
    let result = tokio::time::timeout(Duration::from_secs(2), turn)
        .await
        .expect("the turn ends at its timeout");
    let error = result.error.unwrap();
    assert!(
        error.message.starts_with("the completion request failed: ")
            && error.message.contains("timed out"),
        "{error:?}"
    );
}
