//! An agent's greeting, as a caller's call: what the room's other agents make
//! of the first message's bound, and a caller gone before its commit, which no
//! shared scenario can say.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, ChannelType, EventContent, HookResult, InboundMessage, RoomEvent, TextContent,
};
use roomkit::store::{ConversationStore, ListEventsOptions};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, SendGreetingOptions,
};
use roomkit_ai::{Agent, AgentOptions, MockAiProvider, MockAiProviderOptions};
use tokio::sync::Notify;

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

/// What `event` says; a lifecycle record says nothing.
fn text(event: &RoomEvent) -> Option<String> {
    if let EventContent::Text(text) = &event.content {
        return Some(text.body.clone());
    }
    None
}

// Python: tests/test_session_started.py::TestTextSessionStarted::test_another_agent_reading_the_greeting_holds_no_message
/// RMK-503: there is no greeting gate on a text session, so another agent's
/// reading of the greeting holds the message only as any work inside the call
/// does: the greeting's cascade is awaited within `process_timeout`, past
/// which the message is refused
/// (`greeting/a-greeting-read-past-process-timeout-expires-its-message`).
#[tokio::test]
async fn another_agent_that_reads_the_greeting_holds_the_message_only_within_the_bound() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms.clone()).unwrap();
    let model = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["Noted".to_owned()],
        ..MockAiProviderOptions::default()
    });
    let options = AgentOptions {
        greeting: Some("Welcome!".to_owned()),
        ..AgentOptions::default()
    };
    kit.register_channel(Agent::new(id("agent"), model, options).unwrap())
        .unwrap();
    kit.register_channel(EchoAgent::new(
        id("helper"),
        "Hello",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let attaching = kit.downgrade();
    kit.hook(
        HookTrigger::OnRoomCreated,
        move |event: RoomEvent, _| {
            let kit = attaching.upgrade();
            async move {
                let kit = kit.expect("the kit runs its hook");
                for agent in ["agent", "helper"] {
                    kit.attach_channel(&event.room_id, &id(agent), AttachChannelOptions::default())
                        .await
                        .unwrap();
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "attach_agents".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();

    let message = InboundMessage::new(id("sms"), "+15551234567", TextContent::new("Hi"));
    let inbound = kit.process_inbound(message, ProcessInboundOptions::default());
    let result = tokio::time::timeout(Duration::from_secs(5), inbound)
        .await
        .expect("the greeting holds no message")
        .unwrap();

    assert!(!result.blocked);
    let room_id = result
        .event
        .as_ref()
        .map(|event| event.room_id.clone())
        .unwrap();
    let timeline = kit
        .store()
        .list_events(&room_id, ListEventsOptions::default())
        .await
        .unwrap();
    let said: Vec<String> = timeline.iter().filter_map(text).collect();
    assert_eq!(said.first().map(String::as_str), Some("Welcome!"));
    assert!(said.iter().any(|body| body == "Hi"));
    assert_eq!(sms.texts().first().map(String::as_str), Some("Welcome!"));
}

// Python: a `send_greeting` cancelled while another message holds the room's
// lock stores nothing: its timeline is `[Hi]`.
/// RFC §13.6, gap G-076: a greeting its caller drops before its commit goes
/// with its caller. A caller cannot be dropped in a shared scenario.
#[tokio::test]
async fn a_greeting_dropped_before_its_commit_stores_nothing() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let model = MockAiProvider::new(MockAiProviderOptions::default());
    let options = AgentOptions {
        greeting: Some("Welcome!".to_owned()),
        auto_greet: false,
        ..AgentOptions::default()
    };
    kit.register_channel(Agent::new(id("agent"), model, options).unwrap())
        .unwrap();
    let room_id = kit
        .create_room(CreateRoomOptions::default())
        .await
        .unwrap()
        .id;
    for channel_id in ["sms", "agent"] {
        kit.attach_channel(&room_id, &id(channel_id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (holding, released) = (Arc::clone(&entered), Arc::clone(&release));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let (holding, released) = (Arc::clone(&holding), Arc::clone(&released));
            let the_message = text(&event).as_deref() == Some("Hi");
            async move {
                if the_message {
                    holding.notify_one();
                    released.notified().await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "holds_the_lock".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let message = tokio::spawn({
        let kit = kit.clone();
        let options = ProcessInboundOptions {
            room_id: Some(room_id.clone()),
            ..ProcessInboundOptions::default()
        };
        async move {
            let said = InboundMessage::new(id("sms"), "+15551234567", TextContent::new("Hi"));
            kit.process_inbound(said, options).await
        }
    });
    entered.notified().await;

    let greeting = kit.send_greeting(&room_id, SendGreetingOptions::default());
    tokio::time::timeout(Duration::from_millis(50), greeting)
        .await
        .expect_err("the room's lock is held");
    release.notify_one();
    message.await.unwrap().unwrap();
    kit.close().await.unwrap();

    let timeline = kit
        .store()
        .list_events(&room_id, ListEventsOptions::default())
        .await
        .unwrap();
    let said: Vec<String> = timeline.iter().filter_map(text).collect();
    assert!(said.contains(&"Hi".to_owned()), "{said:?}");
    assert!(!said.contains(&"Welcome!".to_owned()), "{said:?}");
}
