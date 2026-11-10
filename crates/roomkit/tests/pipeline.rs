//! The pipeline (RFC §19.5, §19.7.1), what a scenario cannot say: agents given
//! live, and a strategy's own `install` called again. Scenarios
//! `orchestration/*pipeline*` hold the rest.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;

use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::orchestration::pipeline::InstallOptions;
use roomkit::orchestration::{
    ConversationPipelineOptions, ConversationRouterOptions, ConversationState,
    InstallOptions as RouterInstallOptions, PipelineOptions, StrategyAgent,
    save_conversation_state,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationPipeline, ConversationRouter, CreateRoomOptions,
    InstallStrategyOptions, Orchestration, Pipeline, PipelineStage, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions, Swarm,
};
use roomkit_ai::{
    Agent, AgentOptions, AiChannelOptions, AiTool, MockAiProvider, MockAiProviderOptions,
};

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn model() -> MockAiProvider {
    MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["ok".to_owned(), "again".to_owned()],
        ..MockAiProviderOptions::default()
    })
}

fn agent(name: &str, greeting: Option<&str>) -> Agent<MockAiProvider> {
    let options = AgentOptions {
        greeting: greeting.map(ToOwned::to_owned),
        ..AgentOptions::default()
    };
    Agent::new(id(name), model(), options).unwrap()
}

fn chain() -> ConversationPipeline {
    let first = PipelineStage {
        next: Some("b".to_owned()),
        ..PipelineStage::new("a", id("a"))
    };
    let stages = vec![first, PipelineStage::new("b", id("b"))];
    ConversationPipeline::new(stages, ConversationPipelineOptions::default()).unwrap()
}

// Python: tests/orchestration/test_pipeline.py::TestPipelineInstall::test_install_returns_router_and_handler
#[tokio::test]
async fn an_install_returns_its_router_and_a_handler_holding_its_agents() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let agents = [
        StrategyAgent::channel(agent("a", Some("Hello from a."))),
        StrategyAgent::channel(agent("b", None)),
    ];

    let (_router, handler) = chain()
        .install(&kit, &agents, InstallOptions::default())
        .unwrap();

    assert_eq!(handler.agents().len(), 2);
    assert_eq!(
        handler.greeting_map().get(&id("a")).map(String::as_str),
        Some("Hello from a.")
    );
    assert!(!handler.greeting_map().contains_key(&id("b")));
    kit.close().await.unwrap();
}

// Python: orchestration/pipeline.py install (held(kit, a) for each agent, before anything)
#[tokio::test]
async fn a_live_agent_is_registered_once_every_agent_was_read() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let agents = [
        StrategyAgent::channel(agent("a", None)),
        StrategyAgent::from(id("nobody")),
    ];

    let refused = chain().install(&kit, &agents, InstallOptions::default());

    assert!(matches!(
        refused,
        Err(RoomKitError::ChannelNotRegistered { .. })
    ));
    assert!(kit.get_channel(&id("a")).is_none());

    let agents = [
        StrategyAgent::channel(agent("a", None)),
        StrategyAgent::channel(agent("b", None)),
    ];
    chain()
        .install(&kit, &agents, InstallOptions::default())
        .unwrap();
    assert!(kit.get_channel(&id("a")).is_some());
    assert!(kit.get_channel(&id("b")).is_some());
    kit.close().await.unwrap();
}

// Python: orchestration/pipeline.py _wire_handoff (setup_handoff refuses an agent set up already)
#[tokio::test]
async fn a_second_install_on_the_same_agents_is_refused() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent("a", None)).unwrap();
    kit.register_channel(agent("b", None)).unwrap();
    let agents = [StrategyAgent::from(id("a")), StrategyAgent::from(id("b"))];
    chain()
        .install(&kit, &agents, InstallOptions::default())
        .unwrap();

    let again = chain().install(&kit, &agents, InstallOptions::default());

    assert!(matches!(again, Err(RoomKitError::Runtime(_))));
    kit.close().await.unwrap();
}

/// An agent whose own tools hold the handoff's name.
fn clashing(name: &str) -> Agent<MockAiProvider> {
    let tool = AiTool::new("handoff_conversation", "host").unwrap();
    let options = AgentOptions {
        ai: AiChannelOptions {
            tools: vec![tool.into()],
            ..AiChannelOptions::default()
        },
        ..AgentOptions::default()
    };
    Agent::new(id(name), model(), options).unwrap()
}

// Python: tests/orchestration/test_pipeline.py::test_an_install_refused_by_an_agent_s_tool_sets_nothing_up[pipeline]
#[tokio::test]
async fn a_pipeline_s_install_refused_by_a_tool_or_a_repeat_sets_nothing_up() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent("a", None)).unwrap();
    kit.register_channel(clashing("clash")).unwrap();
    let chain = ConversationPipeline::new(
        vec![PipelineStage::new("a", id("a"))],
        ConversationPipelineOptions::default(),
    )
    .unwrap();
    let options = InstallOptions::default;

    let clash = [
        StrategyAgent::from(id("a")),
        StrategyAgent::from(id("clash")),
    ];
    assert!(matches!(
        chain.install(&kit, &clash, options()),
        Err(RoomKitError::ToolNameCollision(_))
    ));
    let repeat = [StrategyAgent::from(id("a")), StrategyAgent::from(id("a"))];
    assert!(matches!(
        chain.install(&kit, &repeat, options()),
        Err(RoomKitError::Runtime(_))
    ));
    assert!(chain.install(&kit, &repeat[..1], options()).is_ok());
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_pipeline.py::test_an_install_refused_by_an_agent_s_tool_sets_nothing_up[router]
#[tokio::test]
async fn a_router_s_install_refused_by_a_tool_or_a_repeat_sets_nothing_up() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent("a", None)).unwrap();
    kit.register_channel(clashing("clash")).unwrap();
    let router = ConversationRouter::new(ConversationRouterOptions {
        default_agent_id: Some(id("a")),
        ..ConversationRouterOptions::default()
    });
    let options = RouterInstallOptions::default;

    let clash = [
        StrategyAgent::from(id("a")),
        StrategyAgent::from(id("clash")),
    ];
    assert!(matches!(
        router.install(&kit, &clash, options()),
        Err(RoomKitError::ToolNameCollision(_))
    ));
    let repeat = [StrategyAgent::from(id("a")), StrategyAgent::from(id("a"))];
    assert!(matches!(
        router.install(&kit, &repeat, options()),
        Err(RoomKitError::Runtime(_))
    ));
    assert!(router.install(&kit, &repeat[..1], options()).is_ok());
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_framework_orchestration.py::TestCreateRoomOrchestration::test_agents_registered_once
#[tokio::test]
async fn a_strategy_s_live_agent_the_kit_holds_is_not_registered_again() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent("shared", None)).unwrap();
    let pipeline = Pipeline::new(
        vec![
            StrategyAgent::channel(agent("shared", None)),
            StrategyAgent::channel(agent("other", None)),
        ],
        PipelineOptions::default(),
    );

    let options = CreateRoomOptions {
        room_id: Some(room()),
        orchestration: Some(Some(Arc::new(pipeline))),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();

    assert!(kit.get_channel(&id("shared")).is_some());
    assert!(kit.get_channel(&id("other")).is_some());
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_strategy_pipeline.py::test_a_pipeline_with_no_agent_is_refused_at_its_install
#[tokio::test]
async fn a_pipeline_with_no_agent_is_refused_at_its_install() {
    let (kit, _) = kit_with_room().await;
    let pipeline = Pipeline::new(Vec::new(), PipelineOptions::default());

    let refused = kit
        .install_strategy(
            &room(),
            Arc::new(pipeline),
            InstallStrategyOptions::default(),
        )
        .await;

    let Err(RoomKitError::Value(message)) = refused else {
        panic!("expected a refusal, got {refused:?}");
    };
    assert_eq!(message, "Pipeline needs at least one agent");
    kit.close().await.unwrap();
}

/// A kit holding room `r1` and an SMS transport bound to it, with the agents
/// `a` and `b`, whose models it returns.
async fn kit_with_room() -> (RoomKit, [MockAiProvider; 2]) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let first = model();
    let options = AgentOptions::default();
    kit.register_channel(Agent::new(id("a"), first.clone(), options).unwrap())
        .unwrap();
    let second = model();
    kit.register_channel(Agent::new(id("b"), second.clone(), AgentOptions::default()).unwrap())
        .unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    (kit, [first, second])
}

// Python: tests/orchestration/test_room_installs.py::test_installing_a_strategy_again_in_its_room_replaces_its_own[pipeline]
// Python: tests/orchestration/test_room_installs.py::test_installing_a_strategy_again_in_its_room_replaces_its_own[swarm]
#[tokio::test]
async fn installing_it_again_in_its_room_replaces_its_own() {
    let agents = vec![id("a").into(), id("b").into()];
    let pipeline = Pipeline::new(agents.clone(), PipelineOptions::default());
    let swarm = Swarm::new(agents, None);
    let strategies: [Arc<dyn Orchestration>; 2] = [Arc::new(pipeline), Arc::new(swarm)];
    for strategy in strategies {
        reinstall_without_another_handoff(strategy).await;
    }
}

async fn reinstall_without_another_handoff(strategy: Arc<dyn Orchestration>) {
    let (kit, providers) = kit_with_room().await;
    let same = Arc::clone(&strategy);
    kit.install_strategy(&room(), same, InstallStrategyOptions::default())
        .await
        .unwrap();

    Orchestration::install(&*strategy, &kit, &room())
        .await
        .unwrap();

    for (name, provider) in ["a", "b"].into_iter().zip(providers) {
        let state = ConversationState {
            active_agent_id: Some(id(name)),
            ..ConversationState::default()
        };
        save_conversation_state(kit.store(), &room(), &state)
            .await
            .unwrap();
        let message = InboundMessage::new(id("sms"), "u1", TextContent::new("Hi."));
        kit.process_inbound(message, ProcessInboundOptions::default())
            .await
            .unwrap();
        let calls = provider.calls();
        assert_eq!(calls.len(), 1, "one turn of {name}");
        let turn = calls.first().unwrap();
        let handoffs = turn
            .tools
            .iter()
            .filter(|tool| tool.name == "handoff_conversation");
        assert_eq!(handoffs.count(), 1, "one handoff for {name}");
    }
    kit.close().await.unwrap();
}
