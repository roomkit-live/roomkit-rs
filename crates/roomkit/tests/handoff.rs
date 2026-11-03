//! The handoff (RFC §19.6) driven through the public API: what the shared
//! scenarios (roomkit-specs `conformance/scenarios/orchestration/`, the
//! handoff's) cannot say: a host's callback, live agents, a call under the
//! room's lock, and what a setup refuses.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    Metadata, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::orchestration::{
    ConversationRouterOptions, ConversationState, HandoffHandlerOptions, InstallOptions,
    OnHandoffComplete, StrategyAgent, StrategyFuture, get_conversation_state,
    set_conversation_state,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AgentIdentity, AttachChannelOptions, Channel, ChannelDeclaration, ChannelError,
    ConversationRouter, CreateRoomOptions, GetRoomOptions, GetTimelineOptions, HandoffHandler,
    HandoffRequest, HandoffResult, HookExecution, HookOptions, HookTrigger, InstallStrategyOptions,
    Orchestration, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, Swarm,
};
use roomkit_ai::{
    Agent, AgentOptions, AiChannel, AiChannelOptions, MockAiProvider, MockAiProviderOptions,
    SetupHandoffOptions, setup_handoff,
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

// Python: orchestration/handoff.py HandoffRequest, a pydantic model: an
// argument that is no text is refused, nothing written.
#[tokio::test]
async fn an_argument_that_is_no_text_refuses_the_handoff_as_validation_does() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    for (key, value) in [("reason", json!(42)), ("next_phase", json!(5))] {
        let mut asked = arguments("b");
        asked.insert(key.to_owned(), value);

        let refused = handler.handle(&room(), &id("a"), asked).await;

        assert!(matches!(refused, Err(RoomKitError::Value(_))), "{key}");
    }
    // Refused first as Python's: it validates the model last.
    let mut asked = arguments("nobody");
    asked.insert("next_phase".to_owned(), json!(5));
    let result = handler.handle(&room(), &id("a"), asked).await.unwrap();
    assert_eq!(result.reason, "Agent nobody not found in room");
    let options = HandoffHandlerOptions {
        allowed_transitions: Some(HashMap::new()),
        ..HandoffHandlerOptions::default()
    };
    let bound = HandoffHandler::new(&kit, router(), options);
    let mut asked = arguments("b");
    asked.insert("next_phase".to_owned(), json!(5));
    let result = bound.handle(&room(), &id("a"), asked).await.unwrap();
    assert_eq!(
        result.reason,
        "Transition from 'intake' to '5' is not allowed"
    );
    let room_now = kit
        .get_room(&room(), GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(get_conversation_state(&room_now).unwrap().handoff_count, 0);
    kit.close().await.unwrap();
}

/// An agent of the kit whose turns take no tool of the owner, as an ACP
/// agent's: Python's `ChannelDeclaration(takes_owner_tools=False)`.
struct NoOwnerTools;

impl Channel for NoOwnerTools {
    fn channel_id(&self) -> ChannelId {
        id("acp")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn declaration(&self) -> ChannelDeclaration {
        let mut declaration = ChannelDeclaration::new(self.channel_id());
        declaration.takes_owner_tools = false;
        declaration.identity = Some(AgentIdentity::default());
        declaration
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("no inbound".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// A host's strategy whose install keeps a language for the room.
struct SetsLanguage;

impl Orchestration for SetsLanguage {
    fn agents(&self) -> Vec<StrategyAgent> {
        Vec::new()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async move {
            let handler = HandoffHandler::new(kit, router(), HandoffHandlerOptions::default());
            handler.set_language(room_id, "French").await
        })
    }
}

async fn state_of(kit: &RoomKit) -> ConversationState {
    let room_now = kit
        .get_room(&room(), GetRoomOptions::default())
        .await
        .unwrap();
    get_conversation_state(&room_now).unwrap()
}

// Python: core/locks.py's lock is reentrant (RFC §13.5), so a hook, which runs
// under the room's lock, hands off and sets the language at once; tests/orchestration
// has no case, the review's probe ran both.
#[tokio::test]
async fn a_hook_under_the_room_s_lock_hands_off_and_sets_the_language() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let hook = {
        let handler = handler.clone();
        move |event: RoomEvent, _context: RoomContext| {
            let handler = handler.clone();
            async move {
                if event.r#type != roomkit::model::EventType::Message {
                    return;
                }
                handler.set_language(&room(), "French").await.unwrap();
                handler
                    .handle(&room(), &id("a"), arguments("b"))
                    .await
                    .unwrap();
            }
        }
    };
    let options = HookOptions {
        execution: HookExecution::Sync,
        ..HookOptions::default()
    };
    kit.hook(HookTrigger::BeforeBroadcast, hook, options)
        .unwrap();
    let message = InboundMessage::new(id("sms"), "u1", TextContent::new("Hi."));

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        kit.process_inbound(message, ProcessInboundOptions::default()),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(!result.blocked, "{result:?}");
    let state = state_of(&kit).await;
    assert_eq!(state.active_agent_id, Some(id("b")));
    assert_eq!(state.context["language"], json!("French"));
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_handoff.py::TestSetLanguage::test_set_language_stores_in_state,
// from a strategy's install, which holds the room's lock.
#[tokio::test]
async fn an_install_sets_the_language_under_its_own_lock() {
    let kit = kit_with_room().await;

    tokio::time::timeout(
        Duration::from_secs(2),
        kit.install_strategy(
            &room(),
            Arc::new(SetsLanguage),
            InstallStrategyOptions::default(),
        ),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(state_of(&kit).await.context["language"], json!("French"));
    kit.close().await.unwrap();
}

// Python: orchestration/_registry.py register, its refusal of a tool a room
// serves ("Tool 'handoff_conversation' is already served in room 'r1' ..."): the
// agent would declare it twice there, and its turn fail.
#[tokio::test]
async fn a_kit_wide_handoff_over_a_room_s_is_refused() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let in_room = SetupHandoffOptions {
        room_id: Some(room()),
        ..SetupHandoffOptions::default()
    };
    setup_handoff(id("a"), &handler, in_room).unwrap();

    let refused = setup_handoff(id("a"), &handler, SetupHandoffOptions::default());

    let message = refused.unwrap_err().to_string();
    assert!(message.contains("served in room 'r1'"), "{message}");
    kit.close().await.unwrap();
}

// Python: tests/test_declared_strategies.py::test_no_owner_tool_is_set_up_for_a_channel_that_takes_none
// (setup_handoff, and the router's install, nothing installed).
#[tokio::test]
async fn a_channel_that_takes_no_owner_tool_serves_no_handoff() {
    let kit = kit_with_room().await;
    kit.register_channel(NoOwnerTools).unwrap();
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());

    let refused = setup_handoff(id("acp"), &handler, SetupHandoffOptions::default());
    assert!(matches!(refused, Err(RoomKitError::Value(_))));

    let agents = [StrategyAgent::from(id("a")), StrategyAgent::from(id("acp"))];
    let refused = router().install(&kit, &agents, InstallOptions::default());
    assert!(matches!(refused, Err(RoomKitError::Value(_))));
    // Nothing was installed for "a": its handoff is still free to set up.
    setup_handoff(id("a"), &handler, SetupHandoffOptions::default()).unwrap();
    kit.close().await.unwrap();
}

// Python: orchestration/handoff.py setup_handoff reads the channel through
// owner_tools_of, no Agent asked of it; router.py install reads each through
// held, which is.
#[tokio::test]
async fn setup_handoff_takes_a_plain_ai_channel_where_the_install_does_not() {
    let kit = kit_with_room().await;
    let model = MockAiProvider::new(MockAiProviderOptions::default());
    let plain = AiChannel::new(id("plain"), model, AiChannelOptions::default()).unwrap();
    kit.register_channel(plain).unwrap();
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());

    let agents = [StrategyAgent::from(id("plain"))];
    let refused = router().install(&kit, &agents, InstallOptions::default());
    assert!(matches!(refused, Err(RoomKitError::Type(_))));

    setup_handoff(id("plain"), &handler, SetupHandoffOptions::default()).unwrap();
    kit.close().await.unwrap();
}

// Python: router.py install reads every agent before anything is installed. A
// live agent is given to the kit, which registers it, where Python's stays the
// host's to register (an `Agent` is not `Clone`: HITL e865d4e0).
#[tokio::test]
async fn a_live_agent_is_registered_once_every_agent_was_read() {
    let kit = kit_with_room().await;
    let agents = [
        StrategyAgent::channel(agent("live", None)),
        StrategyAgent::from(id("nobody")),
    ];
    let refused = router().install(&kit, &agents, InstallOptions::default());
    assert!(matches!(
        refused,
        Err(RoomKitError::ChannelNotRegistered { .. })
    ));
    assert!(kit.get_channel(&id("live")).is_none());

    let agents = [StrategyAgent::channel(agent("live", None))];
    let handler = router()
        .install(&kit, &agents, InstallOptions::default())
        .unwrap();
    assert!(kit.get_channel(&id("live")).is_some());
    let in_room = SetupHandoffOptions {
        room_id: Some(room()),
        ..SetupHandoffOptions::default()
    };
    setup_handoff(
        StrategyAgent::channel(agent("another", None)),
        &handler,
        in_room,
    )
    .unwrap();
    assert!(kit.get_channel(&id("another")).is_some());
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_handoff.py::test_a_handler_without_a_realtime_channel_takes_a_declared_agent
// (its event_channel_id: the record's source).
#[tokio::test]
async fn the_handler_s_event_channel_sends_the_record() {
    let kit = kit_with_room().await;
    let options = HandoffHandlerOptions {
        event_channel_id: Some(id("sms")),
        ..HandoffHandlerOptions::default()
    };
    let handler = HandoffHandler::new(&kit, router(), options);

    handler
        .handle(&room(), &id("a"), arguments("b"))
        .await
        .unwrap();

    let timeline = kit
        .get_timeline(&room(), GetTimelineOptions::default())
        .await
        .unwrap();
    let record = timeline
        .iter()
        .find(|event| event.metadata.contains_key("handoff"))
        .unwrap();
    assert_eq!(record.source.channel_id, id("sms"));
    kit.close().await.unwrap();
}

// Python: orchestration/handoff.py set_up_handoffs checks every agent before it
// registers any: a refused install leaves what an earlier one served.
#[tokio::test]
async fn a_swarm_refused_on_one_agent_leaves_the_others_their_tools() {
    let kit = kit_with_room().await;
    kit.register_channel(agent("c", None)).unwrap();
    let swarm = Swarm::new(vec![id("a").into(), id("b").into(), id("c").into()], None);
    Orchestration::<roomkit::store::InMemoryStore>::install(&swarm, &kit, &room())
        .await
        .unwrap();
    // "c" leaves the kit and comes back, handed a room's handoff of its own.
    kit.unregister_channel(&id("c")).unwrap();
    kit.register_channel(agent("c", None)).unwrap();
    let other = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let in_room = || SetupHandoffOptions {
        room_id: Some(room()),
        ..SetupHandoffOptions::default()
    };
    setup_handoff(id("c"), &other, in_room()).unwrap();

    let again =
        Orchestration::<roomkit::store::InMemoryStore>::install(&swarm, &kit, &room()).await;

    assert!(
        matches!(again, Err(RoomKitError::ToolNameCollision { .. })),
        "{again:?}"
    );
    // "a" keeps the handoff its first install served.
    assert!(matches!(
        setup_handoff(id("a"), &other, in_room()),
        Err(RoomKitError::Runtime(_))
    ));
    kit.close().await.unwrap();
}

// Python: orchestration/handoff.py set_up_handoffs: all of them, or none.
#[tokio::test]
async fn a_swarm_refused_on_one_agent_serves_none() {
    let kit = kit_with_room().await;
    kit.register_channel(agent("c", None)).unwrap();
    let other = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let in_room = || SetupHandoffOptions {
        room_id: Some(room()),
        ..SetupHandoffOptions::default()
    };
    setup_handoff(id("c"), &other, in_room()).unwrap();
    let swarm = Swarm::new(vec![id("a").into(), id("c").into()], None);

    let room_id = room();
    let refused = Orchestration::<roomkit::store::InMemoryStore>::install(&swarm, &kit, &room_id);

    assert!(matches!(
        refused.await,
        Err(RoomKitError::ToolNameCollision { .. })
    ));
    // "a" came first and was left with nothing: its handoff is free to set up.
    setup_handoff(id("a"), &other, in_room()).unwrap();
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_strategy_install.py::test_a_swarm_installed_on_a_live_room_reaches_the_agent_it_hands_off_to,
// with live agents.
#[tokio::test]
async fn a_swarm_of_live_agents_is_installed_and_holds_them() {
    let kit = kit_with_room().await;
    let room_id = RoomId::new("r2").unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room_id.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    let swarm = Swarm::new(
        vec![
            StrategyAgent::channel(agent("left", None)),
            StrategyAgent::channel(agent("right", None)),
        ],
        None,
    );

    kit.install_strategy(&room_id, Arc::new(swarm), InstallStrategyOptions::default())
        .await
        .unwrap();

    assert!(kit.get_channel(&id("left")).is_some());
    let room_now = kit
        .get_room(&room_id, GetRoomOptions::default())
        .await
        .unwrap();
    let state = get_conversation_state(&room_now).unwrap();
    assert_eq!(
        (state.phase.as_str(), state.active_agent_id),
        ("swarm", Some(id("left")))
    );
    kit.close().await.unwrap();
}

// Python: orchestration/handoff.py handle, its state updated under the room's
// lock "to prevent lost updates".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_handoffs_lose_nothing() {
    let kit = kit_with_room().await;
    let handler = HandoffHandler::new(&kit, router(), HandoffHandlerOptions::default());
    let calls: Vec<_> = (0..20)
        .map(|_| {
            let handler = handler.clone();
            tokio::spawn(async move { handler.handle(&room(), &id("a"), arguments("b")).await })
        })
        .collect();
    for call in calls {
        assert!(call.await.unwrap().unwrap().accepted);
    }
    assert_eq!(state_of(&kit).await.phase_history.len(), 20);
    kit.close().await.unwrap();
}
