//! `delegate_task` (RFC §23.4) driven through the public API: what the shared
//! scenarios (roomkit-specs `conformance/scenarios/tasks/delegate-task-*`)
//! cannot say: a host's own entry in the cache, a delegation serialized per
//! room, a task the host cancels, and the tool's definition.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::InMemoryStore;
use roomkit::model::{ChannelId, ChannelType, Metadata, RoomId};
use roomkit::tasks::{
    CompletedTaskCache, DelegateHandler, DelegateHandlerOptions, DelegatedTask, InMemoryTaskRunner,
    PutOptions, SubmitOptions, TaskRunner,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{AttachChannelOptions, CreateRoomOptions, RoomKit, RoomKitError, RoomKitOptions};
use roomkit_ai::tasks::{
    DELEGATE_TOOL, SetupDelegationOptions, build_delegate_tool, setup_delegation,
};
use roomkit_ai::{Agent, AgentOptions, MockAiProvider, MockAiProviderOptions};
use serde_json::json;
use tokio::sync::Notify;

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn agent(name: &str) -> Agent<MockAiProvider> {
    let model = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["Done.".to_owned()],
        ..MockAiProviderOptions::default()
    });
    Agent::new(id(name), model, AgentOptions::default()).unwrap()
}

async fn in_room(kit: &RoomKit) {
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(agent("worker")).unwrap();
    kit.register_channel(agent("other")).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
}

fn call(task: &str) -> Metadata {
    Metadata::from_iter([
        ("agent".to_owned(), json!("worker")),
        ("task".to_owned(), json!(task)),
    ])
}

// Python: tests/tasks/test_cache.py::TestDelegateHandlerWithCache::test_returns_cached_result
#[tokio::test]
async fn a_task_the_host_wrote_answers_its_call() {
    let kit = RoomKit::new(RoomKitOptions::default());
    in_room(&kit).await;
    let cache = CompletedTaskCache::default();
    let result = Metadata::from_iter([
        ("status".to_owned(), json!("delegated")),
        ("task_id".to_owned(), json!("t-old")),
    ]);
    cache.put(
        &room(),
        &id("worker"),
        "Find it.",
        result,
        PutOptions::default(),
    );
    let handler = DelegateHandler::new(
        &kit,
        DelegateHandlerOptions {
            cache: Some(cache),
            ..DelegateHandlerOptions::default()
        },
    );

    let answer = handler
        .handle(&room(), &id("boss"), &call("Find it."))
        .await
        .unwrap();

    assert_eq!(answer["task_id"], json!("t-old"));
    assert_eq!(answer["from_cache"], json!(true));
    kit.close().await.unwrap();
}

/// A runner whose first submit holds until it is let go, counting the
/// submits it was asked.
#[derive(Clone)]
struct Holding {
    asked: Arc<AtomicUsize>,
    reached: Arc<Notify>,
    go: Arc<Notify>,
    inner: InMemoryTaskRunner,
}

impl TaskRunner for Holding {
    fn submit(
        &self,
        kit: RoomKit,
        task: DelegatedTask,
        options: SubmitOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send>> {
        let held = self.clone();
        Box::pin(async move {
            if held.asked.fetch_add(1, Ordering::SeqCst) == 0 {
                held.reached.notify_one();
                held.go.notified().await;
            }
            held.inner.submit(kit, task, options).await
        })
    }

    fn cancel(&self, task_id: &str) -> Pin<Box<dyn Future<Output = bool> + Send>> {
        TaskRunner::<InMemoryStore>::cancel(&self.inner, task_id)
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        TaskRunner::<InMemoryStore>::close(&self.inner)
    }
}

// Python: tests/tasks/test_cache.py::TestDelegateHandlerSerialization::test_serializes_per_room
#[tokio::test]
async fn delegations_of_a_room_start_one_at_a_time() {
    for (serialize_per_room, started_meanwhile) in [(true, 1), (false, 2)] {
        let runner = Holding {
            asked: Arc::default(),
            reached: Arc::default(),
            go: Arc::default(),
            inner: InMemoryTaskRunner::default(),
        };
        let kit = RoomKit::new(RoomKitOptions {
            task_runner: Some(Arc::new(runner.clone())),
            ..RoomKitOptions::default()
        });
        in_room(&kit).await;
        let handler = DelegateHandler::new(
            &kit,
            DelegateHandlerOptions {
                serialize_per_room,
                ..DelegateHandlerOptions::default()
            },
        );

        let first = {
            let handler = handler.clone();
            tokio::spawn(async move { handler.handle(&room(), &id("boss"), &call("a")).await })
        };
        runner.reached.notified().await;
        let second = {
            let handler = handler.clone();
            tokio::spawn(async move { handler.handle(&room(), &id("boss"), &call("b")).await })
        };
        // The second call runs as far as it can while the first is held: on
        // this test's one thread, until it waits for something.
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            runner.asked.load(Ordering::SeqCst),
            started_meanwhile,
            "serialize_per_room={serialize_per_room}"
        );

        runner.go.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        kit.close().await.unwrap();
    }
}

// Python: tests/tasks/test_delegate.py::TestDelegateTool::test_tool_definition
#[test]
fn the_generic_tool_names_what_it_takes() {
    assert_eq!(DELEGATE_TOOL.name, "delegate_task");
    let properties = DELEGATE_TOOL.parameters["properties"].as_object().unwrap();
    assert_eq!(
        properties.keys().collect::<Vec<_>>(),
        ["agent", "task", "context", "share_channels"]
    );
    assert_eq!(
        DELEGATE_TOOL.parameters["required"],
        json!(["agent", "task"])
    );
}

// Python: tests/tasks/test_delegate.py::TestDelegateTool::test_build_delegate_tool_empty_targets
#[test]
fn no_target_builds_the_generic_tool() {
    let built = build_delegate_tool(&[]);
    assert_eq!(built.name, DELEGATE_TOOL.name);
    assert_eq!(built.description, DELEGATE_TOOL.description);
    assert_eq!(built.parameters, DELEGATE_TOOL.parameters);
}

// Python: tests/tasks/test_delegate.py::TestDelegateTool::test_build_delegate_tool_with_targets
#[test]
fn targets_hold_the_tool_s_worker() {
    let tool = build_delegate_tool(&[
        ("pr-reviewer".to_owned(), Some("Reviews PRs".to_owned())),
        ("code-writer".to_owned(), None),
    ]);
    assert_eq!(tool.name, "delegate_task");
    assert_eq!(
        tool.parameters["properties"]["agent"]["enum"],
        json!(["pr-reviewer", "code-writer"])
    );
    assert_eq!(
        tool.description,
        "Delegate a task to a background agent.\nAvailable agents:\n  - pr-reviewer: Reviews \
         PRs\n  - code-writer"
    );
}

// Python: tests/tasks/test_delegate.py::TestSetupDelegation::test_double_setup_raises
#[tokio::test]
async fn a_setup_is_refused_twice() {
    let kit = RoomKit::new(RoomKitOptions::default());
    in_room(&kit).await;
    let handler = DelegateHandler::new(&kit, DelegateHandlerOptions::default());

    setup_delegation(id("worker"), &handler, SetupDelegationOptions::default()).unwrap();
    let twice = setup_delegation(id("worker"), &handler, SetupDelegationOptions::default());
    assert!(
        matches!(&twice, Err(RoomKitError::Runtime(message)) if message.contains("already called")),
        "{twice:?}"
    );

    // A tool whose list of agents is no list is refused, nothing set up.
    let mut malformed = build_delegate_tool(&[]);
    malformed.parameters["properties"]["agent"]["enum"] = json!("worker");
    let refused = setup_delegation(
        id("other"),
        &handler,
        SetupDelegationOptions {
            tool: Some(malformed),
        },
    );
    assert!(
        matches!(&refused, Err(RoomKitError::Value(_))),
        "{refused:?}"
    );
    // A null list is none, as Python reads it: any agent.
    let mut unlisted = build_delegate_tool(&[]);
    unlisted.parameters["properties"]["agent"]["enum"] = serde_json::Value::Null;
    setup_delegation(
        id("other"),
        &handler,
        SetupDelegationOptions {
            tool: Some(unlisted),
        },
    )
    .unwrap();

    kit.close().await.unwrap();
}

/// An agent whose own tools take `name`.
fn holding(name: &str) -> roomkit_ai::AiChannel<MockAiProvider> {
    let own = roomkit_ai::AiTool {
        name: name.to_owned(),
        description: "Mine.".to_owned(),
        parameters: json!({"type": "object"}),
        tags: Vec::new(),
        changes_state: false,
        defer_loading: false,
    };
    let model = MockAiProvider::new(MockAiProviderOptions::default());
    roomkit_ai::AiChannel::new(
        id("holding"),
        model,
        roomkit_ai::AiChannelOptions {
            tools: vec![own.into()],
            ..roomkit_ai::AiChannelOptions::default()
        },
    )
    .unwrap()
}

// HITL 76eb88d0: a setup refused on a live agent's own tool leaves nothing
// behind, the handoff's as the delegation's.
#[tokio::test]
async fn a_setup_refused_on_a_live_agent_registers_nothing() {
    let kit = RoomKit::new(RoomKitOptions::default());
    in_room(&kit).await;
    let handler = DelegateHandler::new(&kit, DelegateHandlerOptions::default());
    let refused = setup_delegation(
        roomkit::orchestration::StrategyAgent::channel(holding("delegate_task")),
        &handler,
        SetupDelegationOptions::default(),
    );
    assert!(
        matches!(&refused, Err(RoomKitError::ToolNameCollision(_))),
        "{refused:?}"
    );
    assert!(kit.get_channel(&id("holding")).is_none());

    let handoff = roomkit::HandoffHandler::new(
        &kit,
        roomkit::ConversationRouter::new(
            roomkit::orchestration::ConversationRouterOptions::default(),
        ),
        roomkit::orchestration::HandoffHandlerOptions::default(),
    );
    let refused = roomkit_ai::setup_handoff(
        roomkit::orchestration::StrategyAgent::channel(holding("handoff_conversation")),
        &handoff,
        roomkit_ai::SetupHandoffOptions::default(),
    );
    assert!(
        matches!(&refused, Err(RoomKitError::ToolNameCollision(_))),
        "{refused:?}"
    );
    assert!(kit.get_channel(&id("holding")).is_none());
    kit.close().await.unwrap();
}

// Python: a closed kit answers what it would answer open until `delegate`
// refuses it: a call naming no agent is refused as the kit is closed.
#[tokio::test]
async fn a_closed_kit_refuses_a_call_naming_no_agent_as_closed() {
    let kit = RoomKit::new(RoomKitOptions::default());
    in_room(&kit).await;
    let handler = DelegateHandler::new(&kit, DelegateHandlerOptions::default());
    kit.close().await.unwrap();

    let named_none = Metadata::from_iter([("task".to_owned(), json!("T"))]);
    let refused = handler.handle(&room(), &id("boss"), &named_none).await;

    assert!(
        matches!(&refused, Err(RoomKitError::KitClosed)),
        "{refused:?}"
    );
}
