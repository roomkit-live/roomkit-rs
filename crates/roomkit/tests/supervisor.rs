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
    Agent, AgentOptions, AiContentPart, AiContext, AiMessageContent, AiProvider, AiResponse,
    AiToolCall, MockAiProvider, MockAiProviderOptions, ProviderError, ToolResult,
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
            AiContentPart::Text(_)
            | AiContentPart::Image(_)
            | AiContentPart::Thinking(_)
            | AiContentPart::ToolCall(_) => None,
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
    // The re-read of stored results after it (RFC §21.5).
    assert_eq!(
        declared(&requests[0]),
        ["delegate_to_researcher", "read_stored_result"]
    );
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

    assert_eq!(
        declared(&lead_model.calls()[0]),
        ["delegate_workers", "read_stored_result"]
    );
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

/// The last text the model is handed.
fn last_text(context: &AiContext) -> String {
    context
        .messages
        .iter()
        .rev()
        .find_map(|message| match &message.content {
            AiMessageContent::Text(text) => Some(text.clone()),
            AiMessageContent::Parts(_) => None,
        })
        .unwrap_or_default()
}

/// A call of `tool` with `arguments`.
fn calling(tool: &str, arguments: serde_json::Value) -> AiResponse {
    let mut call = AiToolCall::new("c1", tool);
    if let serde_json::Value::Object(members) = arguments {
        call.arguments = members;
    }
    AiResponse {
        finish_reason: Some("tool_calls".to_owned()),
        tool_calls: vec![call],
        ..AiResponse::new("")
    }
}

/// A supervisor and its worker over one model: the supervisor delegates its
/// message to the team, frames the goal as the first task, and judges an
/// output marked GOOD approved and any other rejected, through
/// `submit_verdict`, its review pausing so that two reviews started together
/// are both under way at once; the worker hands back GOOD work for a GOOD
/// task. After a call's result, a closing line. Python's `_JudgeByOutput`.
#[derive(Clone, Default)]
struct JudgeByOutput {
    reviews_tools: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
}

impl JudgeByOutput {
    async fn answer(&self, context: &AiContext) -> AiResponse {
        if !tool_results(context).is_empty() {
            return says("Done.");
        }
        let text = last_text(context);
        if text.starts_with("You are the supervisor reviewing") {
            self.reviews_tools.lock().unwrap().push(declared(context));
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let approved = text.contains("GOOD");
            let feedback = if approved { "" } else { "BAD: add a source" };
            let verdict = json!({"approved": approved, "feedback": feedback, "next_task": ""});
            return calling("submit_verdict", verdict);
        }
        if text.starts_with("You are the supervisor of a worker team") {
            let goal = text
                .split("<task>\n")
                .nth(1)
                .and_then(|rest| rest.split("\n</task>").next());
            return says(goal.unwrap_or_default());
        }
        if declared(context).iter().any(|name| name == "submit_result") {
            let summary = if text.contains("GOOD") {
                "GOOD: Montreal"
            } else {
                "BAD: no idea"
            };
            return calling(
                "submit_result",
                json!({"status": "completed", "summary": summary}),
            );
        }
        calling("delegate_workers", json!({"task": text}))
    }
}

impl AiProvider for JudgeByOutput {
    fn model_name(&self) -> &'static str {
        "judge"
    }

    async fn generate<'a>(&'a self, context: &'a AiContext) -> Result<AiResponse, ProviderError> {
        Ok(self.answer(context).await)
    }
}

// Python: tests/orchestration/test_supervisor_review_isolation.py::TestReviewThroughTheToolLoop::test_a_review_reads_the_verdict_its_room_submitted
// Python: tests/orchestration/test_supervisor_review_isolation.py::TestReviewThroughTheToolLoop::test_two_reviews_at_once_each_read_their_own_verdict
#[tokio::test]
async fn two_reviews_at_once_each_read_their_own_verdict() {
    let judge = JudgeByOutput::default();
    let kit = RoomKit::new(RoomKitOptions::default());
    let identity = |role: &str| AgentOptions {
        role: Some(role.to_owned()),
        ..AgentOptions::default()
    };
    let boss = Agent::new(id("boss"), judge.clone(), identity("Supervisor")).unwrap();
    let worker = Agent::new(id("w1"), judge.clone(), identity("Researcher")).unwrap();
    kit.register_channel(boss).unwrap();
    kit.register_channel(worker).unwrap();
    let options = SupervisorOptions {
        strategy: Some(WorkerStrategy::Sequential),
        max_revisions: 1,
        ..SupervisorOptions::default()
    };
    let team: Arc<dyn Orchestration> =
        Arc::new(Supervisor::new(id("boss"), vec![id("w1").into()], options).unwrap());
    for (room, sms) in [("room-a", "sms-a"), ("room-b", "sms-b")] {
        let transport = RecordingTransportOptions::default();
        kit.register_channel(RecordingTransport::new(
            id(sms),
            ChannelType::SMS,
            transport,
        ))
        .unwrap();
        let created = CreateRoomOptions {
            room_id: Some(RoomId::new(room).unwrap()),
            orchestration: Some(Some(Arc::clone(&team))),
            ..CreateRoomOptions::default()
        };
        kit.create_room(created).await.unwrap();
        let room = RoomId::new(room).unwrap();
        kit.attach_channel(&room, &id(sms), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let ask_in = |sms: &str, body: &str| {
        let message = InboundMessage::new(id(sms), "u1", TextContent::new(body));
        kit.process_inbound(message, ProcessInboundOptions::default())
    };

    let (good, bad) = tokio::join!(
        ask_in("sms-a", "Find GOOD facts."),
        ask_in("sms-b", "Find facts.")
    );
    assert!(!good.unwrap().blocked && !bad.unwrap().blocked);

    let digest = |room: &str| {
        let room = RoomId::new(room).unwrap();
        let kit = kit.clone();
        async move {
            let timeline = kit
                .get_timeline(&room, GetTimelineOptions::default())
                .await
                .unwrap();
            timeline
                .into_iter()
                .find_map(|event| {
                    let EventContent::ToolCall(call) = event.content else {
                        return None;
                    };
                    call.result.as_str().map(str::to_owned)
                })
                .unwrap_or_default()
        }
    };
    let (good, bad) = (digest("room-a").await, digest("room-b").await);
    assert!(good.contains("[Researcher (validated)]"), "{good}");
    assert!(
        bad.contains("[Researcher (FAILED after 1 attempts)]"),
        "{bad}"
    );
    // The verdict tool was offered once per review, never twice.
    let reviews = judge.reviews_tools.lock().unwrap().clone();
    assert_eq!(reviews.len(), 2);
    assert!(reviews.iter().all(|names| {
        names
            .iter()
            .filter(|name| *name == "submit_verdict")
            .count()
            == 1
    }));
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_strategy_supervisor.py::TestSupervisorInstall::test_skips_already_registered_workers
#[tokio::test]
async fn a_live_worker_the_kit_holds_is_not_registered_again() {
    // The kit holds an agent under the worker's id: the live one given the
    // supervisor is not registered over it.
    let lead = agent("sup", "Coordinator", model(vec![says("Hello.")]));
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent("researcher", "Researcher", model(vec![])))
        .unwrap();
    let worker = agent("researcher", "Researcher", model(vec![]));
    let team = Supervisor::new(
        StrategyAgent::channel(lead),
        vec![StrategyAgent::channel(worker)],
        SupervisorOptions::default(),
    )
    .unwrap();
    let created = CreateRoomOptions {
        room_id: Some(RoomId::new("r1").unwrap()),
        orchestration: Some(Some(Arc::new(team))),
        ..CreateRoomOptions::default()
    };

    kit.create_room(created).await.unwrap();

    let held: Vec<String> = kit
        .list_channels()
        .iter()
        .map(|channel| channel.channel_id().to_string())
        .collect();
    assert_eq!(held, ["researcher", "sup"]);
    kit.close().await.unwrap();
}
