//! The handoff (RFC §19.6) driven through the public API: what the shared
//! scenarios (roomkit-specs `conformance/scenarios/orchestration/`, the
//! handoff's) cannot say, a host's callback, a live agent, and what a
//! second setup is refused.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use roomkit::model::{ChannelId, ChannelType, Metadata, RoomId};
use roomkit::orchestration::{
    ConversationRouterOptions, ConversationState, HandoffHandlerOptions, InstallOptions,
    OnHandoffComplete, StrategyAgent, get_conversation_state, set_conversation_state,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationRouter, CreateRoomOptions, GetRoomOptions, HandoffHandler,
    HandoffRequest, HandoffResult, RoomKit, RoomKitError, RoomKitOptions,
};
use roomkit_ai::{
    Agent, AgentOptions, MockAiProvider, MockAiProviderOptions, SetupHandoffOptions, setup_handoff,
};
use serde_json::json;

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn agent(name: &str, language: Option<&str>) -> Agent<MockAiProvider> {
    let options = AgentOptions {
        role: Some(name.to_owned()),
        language: language.map(ToOwned::to_owned),
        ..AgentOptions::default()
    };
    let model = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["ok".to_owned()],
        ..MockAiProviderOptions::default()
    });
    Agent::new(id(name), model, options).unwrap()
}

/// A kit holding room `r1`, an SMS transport and the agents `a` and `b`,
/// all three attached.
async fn kit_with_room() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(agent("a", None)).unwrap();
    kit.register_channel(agent("b", Some("French"))).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    for channel in ["sms", "a", "b"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

fn router() -> ConversationRouter {
    ConversationRouter::new(ConversationRouterOptions {
        default_agent_id: Some(id("a")),
        ..ConversationRouterOptions::default()
    })
}

fn arguments(target: &str) -> Metadata {
    Metadata::from_iter([
        ("target".to_owned(), json!(target)),
        ("reason".to_owned(), json!("Why.")),
        ("summary".to_owned(), json!("What.")),
    ])
}

/// Each room and result a callback was handed.
type Kept = Arc<Mutex<Vec<(RoomId, HandoffResult)>>>;

/// A callback keeping each room and result it is handed.
fn kept() -> (OnHandoffComplete, Kept) {
    let seen: Kept = Arc::default();
    let noted = Arc::clone(&seen);
    let callback: OnHandoffComplete = Arc::new(move |room_id, result| {
        noted.lock().unwrap().push((room_id, result));
        Box::pin(async {})
    });
    (callback, seen)
}

// Python: tests/orchestration/test_handoff.py::TestOnHandoffComplete::test_callback_called_on_success
// Python: tests/orchestration/test_handoff.py::TestOnHandoffComplete::test_callback_not_called_on_rejection
#[tokio::test]
async fn a_host_is_called_back_on_a_handoff_that_went_through_alone() {
    let kit = kit_with_room().await;
    let (callback, seen) = kept();
    let options = HandoffHandlerOptions {
        on_handoff_complete: Some(callback),
        ..HandoffHandlerOptions::default()
    };
    let handler = HandoffHandler::new(&kit, router(), options);

    let refused = handler
        .handle(&room(), &id("a"), arguments("nobody"))
        .await
        .unwrap();
    assert!(!refused.accepted);
    assert!(seen.lock().unwrap().is_empty());

    let result = handler
        .handle(&room(), &id("a"), arguments("b"))
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [(room(), result)]);
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_handoff.py::TestSetupHandoff::test_double_setup_raises
#[tokio::test]
async fn a_second_setup_where_it_is_declared_is_refused() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let in_room = |room_id: Option<RoomId>| SetupHandoffOptions {
        room_id,
        ..SetupHandoffOptions::default()
    };
    setup_handoff(id("a"), &handler, in_room(None)).unwrap();

    for refused in [in_room(None), in_room(Some(room()))] {
        assert!(matches!(
            setup_handoff(id("a"), &handler, refused),
            Err(RoomKitError::Runtime(message))
                if message == "setup_handoff() already called for channel 'a'"
        ));
    }
    // Another agent, and a room only one of them serves it in.
    setup_handoff(id("b"), &handler, in_room(Some(room()))).unwrap();
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_router.py::TestRouterInstall::test_install_registers_hook_and_returns_handler
// (nothing installed for an id naming no channel of the kit: router.py install)
#[tokio::test]
async fn a_router_s_install_refuses_an_id_naming_no_channel_of_the_kit() {
    let kit = kit_with_room().await;
    let agents = vec![
        StrategyAgent::from(id("a")),
        StrategyAgent::from(id("nobody")),
    ];

    let refused = router().install(&kit, &agents, InstallOptions::default());

    assert!(matches!(
        refused,
        Err(RoomKitError::ChannelNotRegistered { .. })
    ));
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    setup_handoff(id("a"), &handler, SetupHandoffOptions::default()).unwrap();
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_handoff.py::test_a_handler_without_a_realtime_channel_takes_a_declared_agent
// (HandoffHandler._get_room_language: room override > agent default)
#[tokio::test]
async fn the_room_s_language_goes_before_its_agent_s_own() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    handler.set_agents(HashMap::from_iter([
        (id("a"), StrategyAgent::from(id("a"))),
        (id("b"), StrategyAgent::from(id("b"))),
    ]));
    assert_eq!(handler.agents().len(), 2);
    let plain = kit
        .get_room(&room(), GetRoomOptions::default())
        .await
        .unwrap();

    let read =
        |agent: Option<ChannelId>| handler.get_room_language(&plain, agent.as_ref()).unwrap();
    assert_eq!(read(Some(id("b"))).as_deref(), Some("French"));
    assert_eq!(read(Some(id("a"))), None);
    assert_eq!(read(None), None);

    let mut state = ConversationState::new();
    state.context.insert("language".to_owned(), json!("German"));
    let set = set_conversation_state(&plain, &state);
    assert_eq!(
        handler
            .get_room_language(&set, Some(&id("b")))
            .unwrap()
            .as_deref(),
        Some("German")
    );
    kit.close().await.unwrap();
}

// Python: orchestration/handoff.py HandoffRequest and HandoffResult, their
// fields' defaults (tests/orchestration/test_handoff.py builds them so).
#[test]
fn a_request_and_a_result_have_python_s_defaults() {
    let request = HandoffRequest::new("b");
    assert_eq!(request.target_agent_id, "b");
    assert!(request.reason.is_empty() && request.summary.is_empty());
    assert!(request.context.is_empty() && !request.urgent);
    assert!(request.channel_escalation.is_none());
    let result = HandoffResult::default();
    assert!(result.accepted);
    assert!(result.new_agent_id.is_none() && result.new_phase.is_none());
    assert!(result.message.is_empty() && result.reason.is_empty());
}

// Python: tests/orchestration/test_handoff.py::TestSetupHandoff::test_handoff_tool_call_dispatched
// (the state's stored form, read back through the kit's room)
#[tokio::test]
async fn an_argument_that_is_no_text_refuses_the_handoff_as_validation_does() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let mut asked = arguments("b");
    asked.insert("reason".to_owned(), json!(42));

    let refused = handler.handle(&room(), &id("a"), asked).await;

    assert!(matches!(refused, Err(RoomKitError::Value(_))));
    let room_now = kit
        .get_room(&room(), GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(get_conversation_state(&room_now).unwrap().handoff_count, 0);
    kit.close().await.unwrap();
}
