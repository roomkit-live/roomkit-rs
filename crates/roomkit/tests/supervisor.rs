//! The supervisor strategy (RFC §19.7.3) driven through the public API the
//! way an integrator does: live agents given to a `Supervisor`, installed
//! with `create_room(orchestration=...)`, a message in, the supervisor's
//! answer out. The shared scenarios (roomkit-specs
//! `conformance/scenarios/orchestration/`, the supervisor's) name every
//! agent by its id; these give them live, install twice, and fail a
//! delegation.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, InboundMessage, Metadata, RoomId, TextContent,
};
use roomkit::orchestration::strategies::supervisor::WorkerStrategy;
use roomkit::orchestration::{StrategyAgent, SupervisorOptions};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, GetTimelineOptions, Orchestration,
    ProcessInboundOptions, RoomKit, RoomKitOptions, Supervisor,
};
use roomkit_ai::{
    Agent, AgentOptions, AiContentPart, AiContext, AiMessageContent, AiResponse, AiToolCall,
    MockAiProvider, MockAiProviderOptions, ToolResult,
};
use serde_json::json;

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

/// A model answering `responses` in turn.
fn model(responses: Vec<AiResponse>) -> MockAiProvider {
    MockAiProvider::new(MockAiProviderOptions {
        ai_responses: Some(responses),
        ..MockAiProviderOptions::default()
    })
}

/// A response saying `text`.
fn says(text: &str) -> AiResponse {
    AiResponse::new(text)
}

/// A response calling `tool` with the task `task`.
fn calls(tool: &str, task: &str) -> AiResponse {
    let mut call = AiToolCall::new("c1", tool);
    call.arguments = Metadata::from_iter([("task".to_owned(), json!(task))]);
    AiResponse {
        finish_reason: Some("tool_calls".to_owned()),
        tool_calls: vec![call],
        ..AiResponse::new("")
    }
}

/// An agent `name` of `role` over `provider`.
fn agent(name: &str, role: &str, provider: MockAiProvider) -> Agent<MockAiProvider> {
    let options = AgentOptions {
        role: Some(role.to_owned()),
        ..AgentOptions::default()
    };
    Agent::new(id(name), provider, options).unwrap()
}

/// A kit holding an SMS transport and room `r1` binding it.
async fn kit_with_room(strategy: Option<Arc<dyn Orchestration>>) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(RoomId::new("r1").unwrap()),
        orchestration: Some(strategy),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.attach_channel(&room, &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

async fn ask(kit: &RoomKit, body: &str) {
    let message = InboundMessage::new(id("sms"), "u1", TextContent::new(body));
    let options = ProcessInboundOptions::default();
    let result = kit.process_inbound(message, options).await.unwrap();
    assert!(!result.blocked, "{result:?}");
}

/// The texts of `r1`'s messages, by sender, in order.
async fn messages(kit: &RoomKit) -> Vec<(String, String)> {
    let room = RoomId::new("r1").unwrap();
    let timeline = kit
        .get_timeline(&room, GetTimelineOptions::default())
        .await
        .unwrap();
    timeline
        .into_iter()
        .filter_map(|event| {
            let EventContent::Text(text) = event.content else {
                return None;
            };
            Some((event.source.channel_id.to_string(), text.body))
        })
        .collect()
}

/// The tool results `context` hands its model: each one's text and
/// whether it reads as failed.
fn tool_results(context: &AiContext) -> Vec<(String, bool)> {
    let parts = context
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            AiMessageContent::Parts(parts) => Some(parts),
            AiMessageContent::Text(_) => None,
        });
    parts
        .flatten()
        .filter_map(|part| match part {
            AiContentPart::ToolResult(result) => {
                let text = match &result.result {
                    ToolResult::Text(text) => text.clone(),
                    other @ (ToolResult::Json(_) | ToolResult::Parts(_)) => format!("{other:?}"),
                };
                Some((text, result.is_error))
            }
            AiContentPart::Text(_) | AiContentPart::Image(_) | AiContentPart::ToolCall(_) => None,
        })
        .collect()
}

/// The names of the tools `context` declares.
fn declared(context: &AiContext) -> Vec<String> {
    context.tools.iter().map(|tool| tool.name.clone()).collect()
}

// Python: tests/orchestration/test_supervisor_inline.py::TestWaitForResultBasic::test_inline_delegation_returns_worker_output
// Python: tests/orchestration/test_strategy_supervisor.py::TestSupervisorInstall::test_delegation_tool_handler
#[tokio::test]
async fn a_supervisor_given_live_agents_delegates_and_answers() {
    let lead_model = model(vec![
        calls("delegate_to_researcher", "Find it."),
        says("Found."),
    ]);
    let lead = agent("sup", "Coordinator", lead_model.clone());
    let researcher = agent("researcher", "Researcher", model(vec![says("The facts.")]));
    let team = Supervisor::new(
        StrategyAgent::channel(lead),
        vec![StrategyAgent::channel(researcher)],
        SupervisorOptions::default(),
    )
    .unwrap();
    let kit = kit_with_room(Some(Arc::new(team))).await;

    ask(&kit, "Look it up.").await;

    assert_eq!(
        messages(&kit).await,
        [
            ("sms".to_owned(), "Look it up.".to_owned()),
            ("sup".to_owned(), "Found.".to_owned())
        ]
    );
    let requests = lead_model.calls();
    assert_eq!(declared(&requests[0]), ["delegate_to_researcher"]);
    let answer = r#"{"status": "completed", "worker": "researcher", "result": "The facts."}"#;
    assert_eq!(tool_results(&requests[1]), [(answer.to_owned(), false)]);
    // The worker is the kit's, never attached to the room.
    assert!(kit.get_channel(&id("researcher")).is_some());
    kit.close().await.unwrap();
}

// Python: tests/test_supervisor.py::TestSupervisorInstall::test_double_install_strategy_tool_no_duplicate
// Python: tests/orchestration/test_strategy_supervisor.py::TestSupervisorInstall::test_double_install_skips_tools
#[tokio::test]
async fn a_supervisor_installed_again_in_its_room_serves_its_tool_once() {
    let lead_model = model(vec![says("Hello.")]);
    let lead = agent("sup", "Coordinator", lead_model.clone());
    let analyst = agent("analyst", "Analyst", model(vec![]));
    let options = SupervisorOptions {
        strategy: Some(WorkerStrategy::Parallel),
        ..SupervisorOptions::default()
    };
    let team = Arc::new(
        Supervisor::new(
            StrategyAgent::channel(lead),
            vec![StrategyAgent::channel(analyst)],
            options,
        )
        .unwrap(),
    );
    let installed: Arc<dyn Orchestration> = Arc::<Supervisor>::clone(&team);
    let kit = kit_with_room(Some(installed)).await;
    let room = RoomId::new("r1").unwrap();

    // Its own install, called again in the room it holds.
    Orchestration::install(team.as_ref(), &kit, &room)
        .await
        .unwrap();
    ask(&kit, "Hi.").await;

    assert_eq!(declared(&lead_model.calls()[0]), ["delegate_workers"]);
    kit.close().await.unwrap();
}

// Python: tests/test_supervisor.py::TestPerWorkerDelegation::test_delegation_exception_returns_error
// Python: tests/test_supervisor.py::TestStrategyToolHandler::test_strategy_tool_exception_returns_error
// Python: tests/orchestration/test_strategy_behavioral.py::TestSupervisorHandlerIdempotency::test_delegation_error_is_raised_to_the_channel
#[tokio::test]
async fn a_delegation_that_fails_answers_its_call_failed() {
    for (strategy, tool) in [
        (None, "delegate_to_worker"),
        (Some(WorkerStrategy::Parallel), "delegate_workers"),
        (Some(WorkerStrategy::Sequential), "delegate_workers"),
    ] {
        // A sequential team's supervisor frames the first task first, in a
        // child room of its own.
        let framing = (strategy == Some(WorkerStrategy::Sequential)).then(|| says("Do it well."));
        let mut answers = vec![calls(tool, "Do it.")];
        answers.extend(framing);
        answers.push(says("It failed."));
        let lead_model = model(answers);
        let lead = agent("sup", "Coordinator", lead_model.clone());
        let options = SupervisorOptions {
            strategy,
            ..SupervisorOptions::default()
        };
        let worker: StrategyAgent = id("worker").into();
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(agent("worker", "Worker", model(vec![])))
            .unwrap();
        let team = Supervisor::new(StrategyAgent::channel(lead), vec![worker], options).unwrap();
        let sms = RecordingTransport::new(
            id("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(sms).unwrap();
        let room = RoomId::new("r1").unwrap();
        let created = CreateRoomOptions {
            room_id: Some(room.clone()),
            orchestration: Some(Some(Arc::new(team))),
            ..CreateRoomOptions::default()
        };
        kit.create_room(created).await.unwrap();
        kit.attach_channel(&room, &id("sms"), AttachChannelOptions::default())
            .await
            .unwrap();
        // The worker leaves the kit: its delegation fails.
        kit.unregister_channel(&id("worker")).unwrap();

        ask(&kit, "Go.").await;

        let requests = lead_model.calls();
        let results = tool_results(requests.last().unwrap());
        assert_eq!(results.len(), 1, "{tool}: {results:?}");
        assert!(results[0].1, "{tool}: {results:?}");
        assert!(
            results[0].0.contains("ChannelNotRegisteredError"),
            "{tool}: {results:?}"
        );
        kit.close().await.unwrap();
    }
}
