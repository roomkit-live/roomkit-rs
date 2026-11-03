//! The supervisor's per-worker tools in the background (RFC §19.7.3,
//! `wait_for_result: false`) driven through the public API: what the shared
//! scenarios (roomkit-specs `conformance/scenarios/orchestration/`, the
//! `a-background-worker-*` ones) cannot say, the kit's close and the task
//! runner's cancel reaching a background run.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelId, ChannelType, InboundMessage, Metadata, RoomEvent, RoomId, TextContent,
};
use roomkit::orchestration::{RecentOptions, StatusLevel, StrategyAgent, SupervisorOptions};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions, idle};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, Supervisor,
};
use roomkit_ai::{
    Agent, AgentOptions, AiContext, AiProvider, AiResponse, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ProviderError,
};
use serde_json::json;
use tokio::sync::{Notify, Semaphore};

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A model answering `responses` in turn.
fn model(responses: Vec<AiResponse>) -> MockAiProvider {
    MockAiProvider::new(MockAiProviderOptions {
        ai_responses: Some(responses),
        ..MockAiProviderOptions::default()
    })
}

fn says(text: &str) -> AiResponse {
    AiResponse::new(text)
}

/// A response calling `tool` with the task `task`, the call `call_id`.
fn calls(call_id: &str, tool: &str, task: &str) -> AiResponse {
    let mut call = AiToolCall::new(call_id, tool);
    call.arguments = Metadata::from_iter([("task".to_owned(), json!(task))]);
    AiResponse {
        finish_reason: Some("tool_calls".to_owned()),
        tool_calls: vec![call],
        ..AiResponse::new("")
    }
}

/// A worker's model that waits, once called, until a permit is released,
/// then answers: Python's `_Held`.
#[derive(Clone)]
struct Held {
    started: Arc<Notify>,
    release: Arc<Semaphore>,
    answered: Arc<AtomicUsize>,
}

impl Held {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            release: Arc::new(Semaphore::new(0)),
            answered: Arc::default(),
        }
    }
}

impl AiProvider for Held {
    fn model_name(&self) -> &'static str {
        "held"
    }

    async fn generate<'a>(&'a self, _context: &'a AiContext) -> Result<AiResponse, ProviderError> {
        self.started.notify_one();
        self.release.acquire().await.unwrap().forget();
        self.answered.fetch_add(1, Ordering::SeqCst);
        Ok(says("The draft."))
    }
}

/// A kit holding an SMS transport and room `r1` binding it, the room
/// installed with a supervisor `boss` over `boss_model` that does not wait
/// for its one worker, `worker`, over `worker_model`.
async fn kit_with<P: AiProvider + Clone + 'static>(
    boss_model: MockAiProvider,
    worker_model: P,
) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let role = |role: &str| AgentOptions {
        role: Some(role.to_owned()),
        ..AgentOptions::default()
    };
    let boss = Agent::new(id("boss"), boss_model, role("Lead")).unwrap();
    let worker = Agent::new(id("worker"), worker_model, role("Writer")).unwrap();
    let options = SupervisorOptions {
        wait_for_result: false,
        ..SupervisorOptions::default()
    };
    let team = Supervisor::new(
        StrategyAgent::channel(boss),
        vec![StrategyAgent::channel(worker)],
        options,
    )
    .unwrap();
    let created = CreateRoomOptions {
        room_id: Some(room()),
        orchestration: Some(Some(Arc::new(team))),
        ..CreateRoomOptions::default()
    };
    kit.create_room(created).await.unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

async fn ask(kit: &RoomKit, body: &str) {
    let message = InboundMessage::new(id("sms"), "u1", TextContent::new(body));
    let result = kit
        .process_inbound(message, ProcessInboundOptions::default())
        .await
        .unwrap();
    assert!(!result.blocked, "{result:?}");
}

/// The kit's status entries of the worker and the orchestration, each as
/// (agent, level, detail), oldest first.
async fn terminal_entries(kit: &RoomKit) -> Vec<(String, StatusLevel, String)> {
    let options = RecentOptions {
        n: 50,
        ..RecentOptions::default()
    };
    kit.status_bus()
        .recent(options)
        .await
        .into_iter()
        .filter(|entry| ["worker", "orchestration"].contains(&entry.agent_id.as_str()))
        .map(|entry| (entry.agent_id, entry.status, entry.detail))
        .collect()
}

/// Records each `trigger` event's metadata `key`.
fn record(kit: &RoomKit, trigger: HookTrigger, key: &'static str) -> Arc<Mutex<Vec<String>>> {
    let heard: Arc<Mutex<Vec<String>>> = Arc::default();
    let noted = Arc::clone(&heard);
    let observer = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    let hook = move |event: RoomEvent, _| {
        let value = event.metadata.get(key).and_then(|value| value.as_str());
        noted
            .lock()
            .unwrap()
            .push(value.unwrap_or_default().to_owned());
        ready(())
    };
    kit.hook(trigger, hook, observer).unwrap();
    heard
}

// Python: tests/orchestration/test_background_close.py::test_close_ends_a_background_run[per-worker]
// Python: tests/orchestration/test_background_close.py::test_a_run_held_as_the_kit_closes_ends_cancelled_without_its_work[per-worker]
#[tokio::test]
async fn the_kit_s_close_ends_a_background_worker_s_run_cancelled() {
    let held = Held::new();
    let boss = model(vec![
        calls("c1", "delegate_to_worker", "Write it."),
        says("ok"),
    ]);
    let kit = kit_with(boss, held.clone()).await;
    let ends = record(&kit, HookTrigger::OnTaskCompleted, "task_status");

    ask(&kit, "Write me something.").await;
    tokio::time::timeout(Duration::from_secs(5), held.started.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), kit.close())
        .await
        .unwrap()
        .unwrap();
    held.release.add_permits(1);
    tokio::task::yield_now().await;

    assert_eq!(*ends.lock().unwrap(), ["cancelled"]);
    let entries = terminal_entries(&kit).await;
    let last: Vec<_> = entries.iter().rev().take(2).rev().cloned().collect();
    assert_eq!(
        last,
        [
            (
                "worker".to_owned(),
                StatusLevel::Failed,
                "cancelled".to_owned()
            ),
            (
                "orchestration".to_owned(),
                StatusLevel::Failed,
                "cancelled".to_owned()
            ),
        ]
    );
    assert_eq!(held.answered.load(Ordering::SeqCst), 0);
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_supervisors_worker_is_free_once_its_task_was_cancelled
// Python: tests/orchestration/test_supervisor_text_background.py::test_a_per_worker_background_task_is_the_task_runner_s
#[tokio::test]
async fn a_worker_whose_task_the_runner_cancelled_is_free_and_told_not_completed() {
    let held = Held::new();
    let boss_model = model(vec![
        calls("c1", "delegate_to_worker", "Find it."),
        says("Started."),
        says("It did not finish."),
        calls("c2", "delegate_to_worker", "Try again."),
        says("Started again."),
    ]);
    let kit = kit_with(boss_model.clone(), held.clone()).await;
    let delegated = record(&kit, HookTrigger::OnTaskDelegated, "task_id");

    ask(&kit, "Find it.").await;
    tokio::time::timeout(Duration::from_secs(5), held.started.notified())
        .await
        .unwrap();
    let task_id = delegated.lock().unwrap()[0].clone();
    assert!(kit.task_runner().cancel(&task_id).await);
    idle(&kit).await;
    let entries = terminal_entries(&kit).await;
    assert!(entries.contains(&(
        "orchestration".to_owned(),
        StatusLevel::Failed,
        "not completed".to_owned()
    )));
    ask(&kit, "Try again.").await;

    // The second call delegated anew, the worker no longer already running.
    assert_eq!(delegated.lock().unwrap().len(), 2);
    let requests = boss_model.calls();
    let told = requests[2]
        .messages
        .last()
        .map(|message| format!("{message:?}"));
    let told = told.unwrap_or_default();
    assert!(
        told.contains("[Your background task for worker did not complete."),
        "{told}"
    );
    assert!(told.contains("The task was cancelled."), "{told}");
    held.release.add_permits(1);
    kit.close().await.unwrap();
}
