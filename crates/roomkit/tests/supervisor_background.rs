//! The supervisor's per-worker tools in the background (RFC §19.7.3,
//! `wait_for_result: false`) driven through the public API: what the shared
//! scenarios (roomkit-specs `conformance/scenarios/orchestration/`, the
//! `a-background-worker-*` ones) cannot say, the kit's close and the task
//! runner's cancel reaching a background run.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;
use std::time::Duration;

use roomkit::model::{
    ChannelId, ChannelType, InboundMessage, Metadata, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::orchestration::strategies::supervisor::WorkerStrategy;
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
async fn kit_with<B, P>(boss_model: B, worker_model: P) -> RoomKit
where
    B: AiProvider + Clone + 'static,
    P: AiProvider + Clone + 'static,
{
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
// Python: tests/tasks/test_cancelled_delegation.py::test_a_supervisor_reads_a_cancelled_task_as_cancelled
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

// Python: core/framework.py _cut_at_once, which cuts a background run at the
// close's start wherever it is (tests/test_close_during_handback.py's
// per-worker door holds the hand-back's turn; this holds its delivery).
#[tokio::test]
async fn the_kit_s_close_cuts_a_hand_back_under_way_at_once() {
    let boss = model(vec![
        calls("c1", "delegate_to_worker", "Write it."),
        says("ok"),
    ]);
    let kit = kit_with(boss, model(vec![says("The draft.")])).await;
    let reached = Arc::new(Notify::new());
    let holding = Arc::clone(&reached);
    // Holds the hand-back's delivery, never anything else.
    let hold = move |event: RoomEvent, _context: RoomContext| {
        let holding = Arc::clone(&holding);
        let handed_back = format!("{:?}", event.content).contains("Your background task");
        async move {
            if handed_back {
                holding.notify_one();
                std::future::pending::<()>().await;
            }
        }
    };
    kit.hook(HookTrigger::BeforeDeliver, hold, HookOptions::default())
        .unwrap();

    ask(&kit, "Write me something.").await;
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    // Well within the close's grace of 5 seconds: the run does not wait
    // its hand-back out.
    tokio::time::timeout(Duration::from_secs(2), kit.close())
        .await
        .unwrap()
        .unwrap();

    let entries = terminal_entries(&kit).await;
    assert_eq!(
        entries.last(),
        Some(&(
            "orchestration".to_owned(),
            StatusLevel::Failed,
            "cancelled".to_owned()
        ))
    );
}

/// A supervisor's model that delegates at its first generation, lets its
/// worker answer at its second, once it answered the call, closes the kit
/// at its generation `close_at`, and says `ok` otherwise.
#[derive(Clone)]
struct ClosingBoss {
    kit: Arc<OnceLock<RoomKit>>,
    generations: Arc<AtomicUsize>,
    close_at: usize,
    closing: Arc<Notify>,
    worker: Held,
}

impl ClosingBoss {
    fn new(close_at: usize) -> Self {
        Self {
            kit: Arc::default(),
            generations: Arc::default(),
            close_at,
            closing: Arc::default(),
            worker: Held::new(),
        }
    }
}

impl AiProvider for ClosingBoss {
    fn model_name(&self) -> &'static str {
        "closing-boss"
    }

    async fn generate<'a>(&'a self, _context: &'a AiContext) -> Result<AiResponse, ProviderError> {
        let generation = self.generations.fetch_add(1, Ordering::SeqCst);
        if generation == 0 {
            return Ok(calls("c1", "delegate_to_worker", "Write it."));
        }
        if generation == 1 && self.close_at != 1 {
            self.worker.release.add_permits(1);
        }
        if generation == self.close_at {
            let kit = self.kit.get().unwrap().clone();
            let mut close = Box::pin(kit.close());
            // Its first poll starts the close, from within this turn; only
            // then may another close join it.
            let first = std::future::poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
            self.closing.notify_one();
            if first.is_pending() {
                drop(close.await);
            }
        }
        Ok(says("ok"))
    }
}

/// What the orchestration's entries say once `boss` closed the kit.
async fn closed_by(boss: ClosingBoss) -> Vec<(StatusLevel, String)> {
    let kit = kit_with(boss.clone(), boss.worker.clone()).await;
    boss.kit.set(kit.clone()).ok().unwrap();
    let asking = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message = InboundMessage::new(id("sms"), "u1", TextContent::new("Go."));
            drop(
                kit.process_inbound(message, ProcessInboundOptions::default())
                    .await,
            );
        })
    };
    tokio::time::timeout(Duration::from_secs(5), boss.closing.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), kit.close())
        .await
        .unwrap()
        .unwrap();
    drop(asking.await);
    terminal_entries(&kit)
        .await
        .into_iter()
        .filter(|(agent, _, _)| agent == "orchestration")
        .map(|(_, level, detail)| (level, detail))
        .collect()
}

// Python: core/framework.py _cut_at_once, its run held apart from the turn
// that started it (no Python test closes from that turn; checked by a probe
// on both).
#[tokio::test]
async fn a_close_from_the_turn_that_started_a_run_cuts_it() {
    let entries = closed_by(ClosingBoss::new(1)).await;
    assert_eq!(entries, [(StatusLevel::Failed, "cancelled".to_owned())]);
}

// Python: core/mixins/lane_execution.py _consume, which runs a delivery's
// turn apart from the run that handed it back, so the close cuts that run
// (no Python test closes from that turn; checked by a probe on both).
#[tokio::test]
async fn a_close_from_the_hand_back_s_own_turn_reads_cancelled() {
    let entries = closed_by(ClosingBoss::new(2)).await;
    assert_eq!(entries, [(StatusLevel::Failed, "cancelled".to_owned())]);
}

/// A supervisor's model whose first generation calls `delegate_workers`
/// with "Do it.", every later one answering "ok", never a verdict: Python's
/// `MockAIProvider(responses=["ok"])` behind a call the test makes.
#[derive(Clone, Default)]
struct DispatchesOnce {
    called: Arc<AtomicUsize>,
}

impl AiProvider for DispatchesOnce {
    fn model_name(&self) -> &'static str {
        "dispatches-once"
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's method, answered at once"
    )]
    async fn generate<'a>(&'a self, _context: &'a AiContext) -> Result<AiResponse, ProviderError> {
        if self.called.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(calls("c1", "delegate_workers", "Do it."));
        }
        Ok(says("ok"))
    }
}

/// The agents a sequential team's run delegates to, in order, its
/// supervisor's team tool answering at once and running the team in the
/// background with `async_delivery`, else within its turn: Python's
/// `_team_delegations`.
async fn team_delegations(async_delivery: bool) -> Vec<String> {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let boss = Agent::new(
        id("boss"),
        DispatchesOnce::default(),
        AgentOptions::default(),
    )
    .unwrap();
    let workers = ["w1", "w2"].map(|name| {
        let answers = model(vec![says(name), says(name), says(name), says(name)]);
        StrategyAgent::channel(Agent::new(id(name), answers, AgentOptions::default()).unwrap())
    });
    let options = SupervisorOptions {
        strategy: Some(WorkerStrategy::Sequential),
        async_delivery,
        ..SupervisorOptions::default()
    };
    let team = Supervisor::new(StrategyAgent::channel(boss), workers.to_vec(), options).unwrap();
    let created = CreateRoomOptions {
        room_id: Some(room()),
        orchestration: Some(Some(Arc::new(team))),
        ..CreateRoomOptions::default()
    };
    kit.create_room(created).await.unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    let delegated = record(&kit, HookTrigger::OnTaskDelegated, "agent_id");

    ask(&kit, "Do it.").await;
    // A background run ends with its terminal entry, posted once handed
    // back: it delegates nothing after it.
    idle(&kit).await;
    kit.close().await.unwrap();
    delegated.lock().unwrap().clone()
}

// Python: tests/orchestration/test_supervisor_text_background.py::test_a_sequential_team_is_supervised_in_the_background_as_in_its_turn[team-tool-background]
#[tokio::test]
async fn a_sequential_team_is_supervised_in_the_background_as_in_its_turn() {
    let in_turn = team_delegations(false).await;
    assert_eq!(in_turn.first().map(String::as_str), Some("boss"));
    let in_background = team_delegations(true).await;
    assert_eq!(in_background, in_turn);
}

/// A kit holding room `r1`, installed with a supervisor `boss` over
/// `boss_model` whose team, its one worker `worker` over `worker_model`,
/// runs in the background as `strategy` says.
async fn team_kit_with<B, P>(boss_model: B, worker_model: P, strategy: WorkerStrategy) -> RoomKit
where
    B: AiProvider + Clone + 'static,
    P: AiProvider + Clone + 'static,
{
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let boss = Agent::new(id("boss"), boss_model, AgentOptions::default()).unwrap();
    let worker = Agent::new(id("worker"), worker_model, AgentOptions::default()).unwrap();
    let options = SupervisorOptions {
        strategy: Some(strategy),
        async_delivery: true,
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

/// The kit's close cuts a background team's run while its worker runs:
/// the worker's task ends cancelled, the worker's terminal entry then the
/// run's are posted failed `cancelled`, in that order, and the worker never
/// answers.
async fn a_closed_background_team_ends_cancelled(strategy: WorkerStrategy) {
    let held = Held::new();
    let kit = team_kit_with(DispatchesOnce::default(), held.clone(), strategy).await;
    let ends = Arc::new(Mutex::new(Vec::new()));
    let noted = Arc::clone(&ends);
    let observer = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    let hook = move |event: RoomEvent, _| {
        let agent = event
            .metadata
            .get("agent_id")
            .and_then(|value| value.as_str());
        if agent == Some("worker") {
            let status = event
                .metadata
                .get("task_status")
                .and_then(|value| value.as_str());
            noted
                .lock()
                .unwrap()
                .push(status.unwrap_or_default().to_owned());
        }
        ready(())
    };
    kit.hook(HookTrigger::OnTaskCompleted, hook, observer)
        .unwrap();

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
    let cancelled = |agent: &str| {
        (
            agent.to_owned(),
            StatusLevel::Failed,
            "cancelled".to_owned(),
        )
    };
    assert_eq!(last, [cancelled("worker"), cancelled("orchestration")]);
    assert_eq!(held.answered.load(Ordering::SeqCst), 0);
}

// Python: tests/orchestration/test_background_close.py::test_close_ends_a_background_run[supervisor-tool]
#[tokio::test]
async fn the_kit_s_close_ends_a_background_team_s_run_cancelled() {
    a_closed_background_team_ends_cancelled(WorkerStrategy::Parallel).await;
}

// Python: tests/orchestration/test_background_close.py::test_close_ends_a_background_run[supervised-tool]
#[tokio::test]
async fn the_kit_s_close_ends_a_background_supervised_team_s_run_cancelled() {
    a_closed_background_team_ends_cancelled(WorkerStrategy::Sequential).await;
}
