//! What a shared scenario cannot hold of an agent's greeting: Python refuses
//! this message, which it should not.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::time::Duration;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, InboundMessage, RoomEvent, TextContent,
};
use roomkit::store::{ConversationStore, ListEventsOptions};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};
use roomkit_ai::{Agent, AgentOptions, MockAiProvider, MockAiProviderOptions};

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

// Python: tests/test_session_started.py::TestGreetingGate::test_multi_agent_gate_waits_for_all
/// That Python test attaches its agents as transports: as intelligence
/// channels, the greeting gate its delivery to the other agent waits on is
/// held by the greeting itself, and the message is refused `process_timeout`
/// (RMK-503). roomkit-rs has no gate on a text session: the greeting and the
/// message both go on.
#[tokio::test]
async fn another_agent_that_reads_the_greeting_holds_no_message() {
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
