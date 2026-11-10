//! Delegated tasks cut from outside (RFC §23.3, §12.10.4): by the kit's
//! close, by a caller that stops waiting, by a cancel cut midway; what no
//! shared scenario can say, the suite having no call that closes the kit
//! nor one that stops waiting (Python's
//! `tests/tasks/test_cancelled_delegation.py`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "support/warnings.rs"]
mod warnings;

use std::future::{Future, pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventDirection,
    EventSource, EventType, InboundMessage, RoomContext, RoomEvent, RoomId, TaskStatus,
    TextContent,
};
use roomkit::orchestration::{RecentOptions, StatusEntry};
use roomkit::store::FindRoomsOptions;
use roomkit::tasks::{
    DelegatedTask, DelegatedTaskResult, OnCompleteCallback, SubmitOptions, TaskRunner,
    TaskToolWindow, WaitOptions,
};
use roomkit::telemetry::{Attr, MockTelemetryProvider, SpanKind};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
};
use roomkit::{
    AttachChannelOptions, CancelTaskOptions, Channel, ChannelError, ConversationStore,
    CreateRoomOptions, DelegateOptions, HookExecution, HookOptions, HookTrigger, RoomKit,
    RoomKitError, RoomKitOptions,
};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;
use warnings::Warnings;

/// How long a wait that should end may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

/// An agent whose every turn tells `started`, then never ends, unless it
/// only records the events it is asked (`hangs` false), answering what it
/// `says`, its turn's record naming how it `ends`.
#[derive(Clone)]
struct Agent {
    id: ChannelId,
    hangs: bool,
    started: Arc<Notify>,
    asked: Arc<Mutex<Vec<RoomEvent>>>,
    says: Option<&'static str>,
    ends: Option<&'static str>,
}

impl Agent {
    fn new(id: &str, hangs: bool) -> Self {
        Self {
            id: channel(id),
            hangs,
            started: Arc::default(),
            asked: Arc::default(),
            says: None,
            ends: None,
        }
    }

    /// Each turn answers `text`.
    fn saying(self, text: &'static str) -> Self {
        Self {
            says: Some(text),
            ..self
        }
    }

    /// Each turn's record says it ended `reason`, with no answer.
    fn ending(self, reason: &'static str) -> Self {
        Self {
            ends: Some(reason),
            ..self
        }
    }

    /// What a turn on `event` gives.
    fn output(&self, event: &RoomEvent) -> ChannelOutput {
        let mut output = ChannelOutput::empty();
        if let Some(text) = self.says {
            let source = EventSource {
                direction: EventDirection::Outbound,
                ..EventSource::new(self.id.clone(), ChannelType::AI)
            };
            // The pipeline gives the response its own id, room and depth.
            output.response_events.push(RoomEvent::at(
                event.id.clone(),
                event.room_id.clone(),
                EventType::Message,
                source,
                TextContent::new(text),
                event.created_at,
            ));
            output.responded = true;
        }
        if let Some(reason) = self.ends {
            let end = serde_json::json!(reason);
            output
                .response_metadata
                .insert("loop_end_reason".to_owned(), end);
            output.responded = true;
        }
        output
    }
}

impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let agent = self.clone();
        async move {
            let output = agent.output(&event);
            agent.asked.lock().unwrap().push(event);
            agent.started.notify_one();
            if agent.hangs {
                pending::<()>().await;
            }
            Ok(output)
        }
    }
}

// Python: tests/tasks/test_cancelled_delegation.py::test_closing_the_kit_hands_nothing_back
// Python: tests/tasks/test_delegation_result_delivery.py::test_a_closing_framework_hands_nothing_back
#[tokio::test]
async fn closing_the_kit_ends_a_background_task_cancelled_and_hands_nothing_back() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let (worker, assistant) = (Agent::new("worker", true), Agent::new("assistant", false));
    kit.register_channel(worker.clone()).unwrap();
    kit.register_channel(assistant.clone()).unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("phone"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("p").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room, &channel("phone"), AttachChannelOptions::default())
        .await
        .unwrap();
    let as_agent = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..AttachChannelOptions::default()
    };
    kit.attach_channel(&room, &channel("assistant"), as_agent)
        .await
        .unwrap();
    let completed: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let heard = Arc::clone(&completed);
    let options = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    kit.hook(
        HookTrigger::OnTaskCompleted,
        move |event: RoomEvent, _| {
            let read = |key: &str| {
                let value = event.metadata.get(key).and_then(|value| value.as_str());
                value.unwrap_or_default().to_owned()
            };
            heard
                .lock()
                .unwrap()
                .push((read("task_status"), read("error")));
            ready(())
        },
        options,
    )
    .unwrap();
    let delegation = DelegateOptions {
        notify: Some(channel("assistant")),
        ..DelegateOptions::default()
    };
    let task = kit
        .delegate(&room, &channel("worker"), "go", delegation)
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();

    let ended = task.result().map(|result| result.status);
    let heard = completed.lock().unwrap().clone();
    assert_eq!(
        (ended, heard, assistant.asked.lock().unwrap().len()),
        (
            Some(TaskStatus::Cancelled),
            vec![("cancelled".to_owned(), "cancelled".to_owned())],
            0
        )
    );
}

// Python: tests/tasks/test_runner.py::TestInMemoryTaskRunner::test_close_cancels_all_inflight
#[tokio::test]
async fn closing_the_kit_ends_every_background_task_cancelled() {
    let worker = Agent::new("worker", true);
    let (kit, _, _) = kit_with(worker.clone(), RoomKitOptions::default()).await;
    let room = RoomId::new("p").unwrap();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let task = kit
            .delegate(&room, &channel("worker"), "go", DelegateOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(BOUND, worker.started.notified())
            .await
            .unwrap();
        tasks.push(task);
    }

    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();

    let ended: Vec<_> = tasks
        .iter()
        .map(|task| task.result().map(|result| result.status))
        .collect();
    assert_eq!(ended, vec![Some(TaskStatus::Cancelled); 2]);
}

// Python: tests/tasks/test_cancelled_delegation.py::test_closing_the_runner_ends_its_tasks_cancelled
#[tokio::test]
async fn closing_the_runner_ends_its_tasks_cancelled() {
    let worker = Agent::new("worker", true);
    let (kit, completed, heard) = kit_with(worker.clone(), RoomKitOptions::default()).await;
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "go", DelegateOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    tokio::time::timeout(BOUND, kit.task_runner().close())
        .await
        .unwrap();
    settled(&heard).await;

    assert_eq!(
        (task.status(), completed.lock().unwrap().clone()),
        (TaskStatus::Cancelled, cancelled())
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_task_delegated_while_the_runner_closes_ends_at_once
#[tokio::test]
async fn a_task_delegated_while_the_runner_closes_ends_at_once() {
    let worker = Agent::new("worker", true);
    let (kit, completed, _) = kit_with(worker.clone(), RoomKitOptions::default()).await;
    let again: Arc<Mutex<Vec<DelegatedTask>>> = Arc::default();
    let (delegating, retries) = (kit.clone(), Arc::clone(&again));
    let delegate_again: OnCompleteCallback = Arc::new(move |_: DelegatedTaskResult| {
        let (kit, retries) = (delegating.clone(), Arc::clone(&retries));
        Box::pin(async move {
            let (room, worker) = (RoomId::new("p").unwrap(), channel("worker"));
            let options = DelegateOptions::default();
            let retry = kit.delegate(&room, &worker, "again", options).await;
            retries.lock().unwrap().push(retry.unwrap());
        })
    });
    let options = DelegateOptions {
        on_complete: Some(delegate_again),
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();
    kit.delegate(&room, &channel("worker"), "go", options)
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    tokio::time::timeout(BOUND, kit.task_runner().close())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();

    let statuses: Vec<TaskStatus> = again
        .lock()
        .unwrap()
        .iter()
        .map(DelegatedTask::status)
        .collect();
    let ends: Vec<String> = completed
        .lock()
        .unwrap()
        .iter()
        .map(|(status, _)| status.clone())
        .collect();
    assert_eq!(
        (statuses, ends),
        (
            vec![TaskStatus::Cancelled],
            vec!["cancelled".to_owned(), "cancelled".to_owned()]
        )
    );
    kit.close().await.unwrap();
}

/// The background task the kit's close cuts posts its end on the status
/// bus, `failed` saying it was cancelled, which the room's
/// `ON_STATUS_POSTED` hooks hear during the close:
/// they fire until the kit is sealed (RFC §19.8, §12.10.4). Python's
/// `_on_status_posted` fires them until `_build_context` refuses, the
/// store sealed (core/framework.py). On a6aa969 they stopped at the
/// close's start.
#[tokio::test]
async fn the_end_of_a_task_the_close_cuts_reaches_the_status_hooks() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let worker = Agent::new("worker", true);
    kit.register_channel(worker.clone()).unwrap();
    let room = RoomId::new("p").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    let heard: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let noted = Arc::clone(&heard);
    let options = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    kit.hook(
        HookTrigger::OnStatusPosted,
        move |entry: StatusEntry, _| {
            let level = entry.status.to_string();
            noted.lock().unwrap().push((level, entry.detail));
            ready(())
        },
        options,
    )
    .unwrap();
    kit.delegate(&room, &channel("worker"), "go", DelegateOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();

    let posted = |level: &str, detail: &str| (level.to_owned(), detail.to_owned());
    assert_eq!(
        heard.lock().unwrap().clone(),
        vec![posted("pending", "go"), posted("failed", "cancelled")]
    );
}

/// A kit with its room `p` and `worker`, and what its `ON_TASK_COMPLETED`
/// hooks heard: each task's status and error, told by `heard`.
async fn kit_with(
    worker: impl Channel,
    options: RoomKitOptions,
) -> (RoomKit, Arc<Mutex<Vec<(String, String)>>>, Arc<Notify>) {
    let kit = RoomKit::new(options);
    kit.register_channel(worker).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(RoomId::new("p").unwrap()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    let completed: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let heard = Arc::new(Notify::new());
    let (noted, told) = (Arc::clone(&completed), Arc::clone(&heard));
    let options = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    kit.hook(
        HookTrigger::OnTaskCompleted,
        move |event: RoomEvent, _| {
            let read = |key: &str| {
                let value = event.metadata.get(key).and_then(|value| value.as_str());
                value.unwrap_or_default().to_owned()
            };
            noted
                .lock()
                .unwrap()
                .push((read("task_status"), read("error")));
            told.notify_one();
            ready(())
        },
        options,
    )
    .unwrap();
    (kit, completed, heard)
}

/// A completion callback holding its task's end open until released:
/// `seen` records that it entered, then that it finished (Python's
/// `_Ending`).
#[derive(Clone, Default)]
struct Ending {
    seen: Arc<Mutex<Vec<String>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Ending {
    fn on_complete(&self) -> OnCompleteCallback {
        let ending = self.clone();
        Arc::new(move |result: DelegatedTaskResult| {
            let ending = ending.clone();
            Box::pin(async move {
                ending
                    .seen
                    .lock()
                    .unwrap()
                    .push(format!("{:?}", result.status));
                ending.entered.notify_one();
                ending.release.notified().await;
                ending.seen.lock().unwrap().push("done".to_owned());
            })
        })
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

/// A completion callback recording each result's status.
fn recording(ended: &Arc<Mutex<Vec<TaskStatus>>>) -> OnCompleteCallback {
    let ended = Arc::clone(ended);
    Arc::new(move |result: DelegatedTaskResult| {
        ended.lock().unwrap().push(result.status);
        Box::pin(async {})
    })
}

/// Runs `call` until `cut` happens, then drops it, as a caller's timeout
/// does; the call must not end first.
async fn cut_once<T>(call: impl Future<Output = T>, cut: impl Future<Output = ()>) {
    tokio::select! {
        _ = call => panic!("the call ended before its cut"),
        () = cut => {}
    }
}

/// Waits until `heard` told of a completion, within the bound.
async fn settled(heard: &Notify) {
    tokio::time::timeout(BOUND, heard.notified()).await.unwrap();
}

fn cancelled() -> Vec<(String, String)> {
    vec![("cancelled".to_owned(), "cancelled".to_owned())]
}

/// The existing kit fixture with observable delegation spans.
fn traced(options: RoomKitOptions) -> (RoomKitOptions, Arc<MockTelemetryProvider>) {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let options = RoomKitOptions {
        telemetry: Some(Arc::clone(&telemetry).into()),
        ..options
    };
    (options, telemetry)
}

/// One delegation ended as expected, with none left open. Check before
/// closing the kit, so shutdown cannot hide an unfinished delegation.
fn assert_delegation_ended(telemetry: &MockTelemetryProvider, name: &str, status: &str) {
    let spans = telemetry.get_spans(SpanKind::Delegation);
    let ended: Vec<_> = spans
        .iter()
        .map(|span| (span.name.as_str(), span.status.as_str()))
        .collect();
    assert_eq!(ended, vec![(name, status)]);
    assert!(
        !telemetry
            .get_active_spans()
            .iter()
            .any(|span| span.kind == SpanKind::Delegation),
        "a delegation span is still active"
    );
}

// Python: tests/tasks/test_cancelled_delegation.py::test_an_inline_delegation_its_caller_times_out_ends_cancelled
// On 0931119 the task never ended: its turn and its end ran in its caller's future.
// Python: tests/tasks/test_integration.py::TestDelegateIntegration::test_cancelled_inline_delegation_fires_completion_hook
#[tokio::test]
async fn an_inline_delegation_its_caller_times_out_ends_cancelled() {
    let worker = Agent::new("worker", true);
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, completed, heard) = kit_with(worker.clone(), options).await;
    let ended: Arc<Mutex<Vec<TaskStatus>>> = Arc::default();
    let options = DelegateOptions {
        wait: true,
        on_complete: Some(recording(&ended)),
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();

    let worker_id = channel("worker");
    let call = kit.delegate(&room, &worker_id, "go", options);
    cut_once(call, worker.started.notified()).await;
    settled(&heard).await;

    assert_eq!(
        (
            completed.lock().unwrap().clone(),
            ended.lock().unwrap().clone()
        ),
        (cancelled(), vec![TaskStatus::Cancelled])
    );
    assert_delegation_ended(&telemetry, "delegation.inline", "cancelled");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.inline", "cancelled");
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_background_delegation_the_runner_cancels_ends_cancelled
#[tokio::test]
async fn a_background_delegation_the_runner_cancels_ends_cancelled() {
    let worker = Agent::new("worker", true);
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, completed, heard) = kit_with(worker.clone(), options).await;
    let ended: Arc<Mutex<Vec<TaskStatus>>> = Arc::default();
    let options = DelegateOptions {
        on_complete: Some(recording(&ended)),
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "go", options)
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    assert!(kit.task_runner().cancel(&task.id).await);
    settled(&heard).await;
    let result = task.result().unwrap();
    assert_eq!(result.status, TaskStatus::Cancelled);
    assert_eq!(result.output, None);
    assert_eq!(result.error.as_deref(), Some("cancelled"));
    assert_eq!(completed.lock().unwrap().clone(), cancelled());
    assert_eq!(*ended.lock().unwrap(), vec![TaskStatus::Cancelled]);
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
}

/// What a task runner's calls return.
type Boxed<T> = std::pin::Pin<Box<dyn Future<Output = T> + Send>>;

/// A runner slow to take a task, as a remote queue can be: its submit
/// never returns.
struct NeverTakes;

impl TaskRunner for NeverTakes {
    fn submit(
        &self,
        _kit: RoomKit,
        _task: DelegatedTask,
        _options: SubmitOptions,
    ) -> Boxed<Result<(), RoomKitError>> {
        Box::pin(pending())
    }

    fn cancel(&self, _task_id: &str) -> Boxed<bool> {
        Box::pin(ready(false))
    }

    fn close(&self) -> Boxed<()> {
        Box::pin(ready(()))
    }
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_delegation_cut_before_its_task_ran_ends_it
// On 0931119 neither ended: nothing ends a task its caller cut after its
// announce.
#[tokio::test]
async fn a_delegation_cut_before_its_task_ran_ends_it() {
    for wait in [true, false] {
        let worker = Agent::new("worker", true);
        let options = RoomKitOptions {
            task_runner: (!wait).then(|| -> Arc<dyn TaskRunner> { Arc::new(NeverTakes) }),
            ..RoomKitOptions::default()
        };
        let (options, telemetry) = traced(options);
        let (kit, completed, heard) = kit_with(worker, options).await;
        let announced = Arc::new(Notify::new());
        let told = Arc::clone(&announced);
        let observer = HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        };
        kit.hook(
            HookTrigger::OnTaskDelegated,
            move |_event: RoomEvent, _| {
                told.notify_one();
                ready(())
            },
            observer,
        )
        .unwrap();
        let options = DelegateOptions {
            wait,
            ..DelegateOptions::default()
        };
        let room = RoomId::new("p").unwrap();

        let worker_id = channel("worker");
        let call = kit.delegate(&room, &worker_id, "go", options);
        cut_once(call, announced.notified()).await;
        settled(&heard).await;

        assert_eq!(
            (wait, completed.lock().unwrap().clone()),
            (wait, cancelled())
        );
        let name = if wait {
            "delegation.inline"
        } else {
            "delegation.background"
        };
        assert_delegation_ended(&telemetry, name, "cancelled");
        kit.close().await.unwrap();
        assert_delegation_ended(&telemetry, name, "cancelled");
    }
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_task_cancelled_before_it_ran_a_line_still_ends
#[tokio::test]
async fn a_task_cancelled_before_it_ran_a_line_still_ends() {
    let worker = Agent::new("worker", true);
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, completed, heard) = kit_with(worker.clone(), options).await;
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "go", DelegateOptions::default())
        .await
        .unwrap();

    kit.task_runner().cancel(&task.id).await;
    let waited = WaitOptions {
        timeout: Some(BOUND),
    };
    let ended = task.wait(waited).await.unwrap();
    settled(&heard).await;

    assert_eq!(
        (
            ended.status,
            worker.asked.lock().unwrap().len(),
            completed.lock().unwrap().clone()
        ),
        (TaskStatus::Cancelled, 0, cancelled())
    );
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
}

// Python: tests/tasks/test_task_turn_record.py::test_a_task_cut_before_its_turn_began_carries_none
// A cancel cuts the work before its first step, as Python's cancel of an
// asyncio task that never ran leaves it pending: its worker never asked,
// the task names no turn.
#[tokio::test]
async fn a_task_cut_before_its_turn_began_names_no_turn() {
    let worker = Agent::new("worker", true);
    let (kit, _, _) = kit_with(worker.clone(), RoomKitOptions::default()).await;
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "Go.", DelegateOptions::default())
        .await
        .unwrap();

    kit.cancel_task(&task.id, CancelTaskOptions::default())
        .await;
    kit.close().await.unwrap();

    let result = task.result().unwrap();
    assert_eq!(
        (result.status, result.metadata.get("turns").cloned()),
        (TaskStatus::Cancelled, None)
    );
}

/// A runner refusing every task, as a full queue does.
struct Refuses;

impl TaskRunner for Refuses {
    fn submit(
        &self,
        _kit: RoomKit,
        _task: DelegatedTask,
        _options: SubmitOptions,
    ) -> Boxed<Result<(), RoomKitError>> {
        let full = RoomKitError::Runtime("the task runner is full".to_owned());
        Box::pin(ready(Err(full)))
    }

    fn cancel(&self, _task_id: &str) -> Boxed<bool> {
        Box::pin(ready(false))
    }

    fn close(&self) -> Boxed<()> {
        Box::pin(ready(()))
    }
}

// Python: tests/tasks/test_delegation_ends_failed.py::test_a_task_its_runner_refuses_ends_failed
// RFC §23.3: the delegation fails with the runner's error and its task
// ends failed wherever its end is read: ON_TASK_COMPLETED, its callback,
// its child room's status and its line on the status bus, which
// CancelTaskTool reads, and its delegation span; nothing is handed back.
#[tokio::test]
async fn a_task_its_runner_refuses_ends_failed() {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let options = RoomKitOptions {
        telemetry: Some(Arc::clone(&telemetry).into()),
        task_runner: Some(Arc::new(Refuses)),
        ..RoomKitOptions::default()
    };
    let (kit, completed, heard) = kit_with(Agent::new("worker", false), options).await;
    let speaker = Agent::new("speaker", false);
    kit.register_channel(speaker.clone()).unwrap();
    let room = RoomId::new("p").unwrap();
    let as_agent = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..AttachChannelOptions::default()
    };
    kit.attach_channel(&room, &channel("speaker"), as_agent)
        .await
        .unwrap();
    let ended: Arc<Mutex<Vec<TaskStatus>>> = Arc::default();
    let options = DelegateOptions {
        notify: Some(channel("speaker")),
        on_complete: Some(recording(&ended)),
        ..DelegateOptions::default()
    };

    let refused = kit
        .delegate(&room, &channel("worker"), "Count.", options)
        .await;
    settled(&heard).await;
    idle(&kit).await;

    let rooms = kit
        .store()
        .find_rooms(FindRoomsOptions::default())
        .await
        .unwrap();
    let child = rooms.iter().find(|child| child.id != room).unwrap();
    let recent = RecentOptions {
        agent_id: Some("worker".to_owned()),
        ..RecentOptions::default()
    };
    let lines: Vec<String> = kit
        .status_bus()
        .recent(recent)
        .await
        .iter()
        .map(|entry| entry.status.to_string())
        .collect();
    let statuses: Vec<String> = completed
        .lock()
        .unwrap()
        .iter()
        .map(|(status, _)| status.clone())
        .collect();
    assert_eq!(
        (
            refused.err().map(|error| error.to_string()),
            statuses,
            ended.lock().unwrap().clone(),
            child.metadata.get("task_status").cloned(),
            lines,
            speaker.asked.lock().unwrap().len()
        ),
        (
            Some("the task runner is full".to_owned()),
            vec!["failed".to_owned()],
            vec![TaskStatus::Failed],
            Some(serde_json::json!("failed")),
            vec!["pending".to_owned(), "failed".to_owned()],
            0
        )
    );
    let task_id = child.metadata.get("task_id").unwrap().as_str().unwrap();
    assert_refused_task_stays_failed(&kit, &room, task_id, &telemetry).await;
    kit.close().await.unwrap();
    assert_eq!(telemetry.get_spans(SpanKind::Delegation).len(), 1);
    assert_eq!(completed.lock().unwrap().len(), 1);
    assert_eq!(ended.lock().unwrap().as_slice(), &[TaskStatus::Failed]);
}

/// The refused task's end stays visible to cancellation callers and telemetry.
async fn assert_refused_task_stays_failed(
    kit: &RoomKit,
    room: &RoomId,
    task_id: &str,
    telemetry: &MockTelemetryProvider,
) {
    let arguments = [("task_id".to_owned(), serde_json::json!(task_id))]
        .into_iter()
        .collect();
    // The same bridge serves CancelTaskTool: an ended task stays failed.
    let answer = kit
        .cancel_task_answer(
            room,
            Some(&channel("speaker")),
            &arguments,
            TaskToolWindow {
                window: 500,
                limit: 0,
            },
        )
        .await;
    let answer: serde_json::Value = serde_json::from_str(&answer).unwrap();
    assert_eq!(answer.get("status"), Some(&serde_json::json!("failed")));
    assert!(!kit.cancel_task(task_id, CancelTaskOptions::default()).await);
    let spans = telemetry.get_spans(SpanKind::Delegation);
    assert_eq!(spans.len(), 1);
    let span = spans.first().unwrap();
    assert_eq!(span.name, "delegation.background");
    assert_eq!(span.status, "error");
    assert_eq!(
        span.attributes.get(Attr::DELEGATION_STATUS),
        Some(&serde_json::json!("failed"))
    );
    assert_eq!(telemetry.get_active_spans(), vec![]);
}

// Python: tests/tasks/test_task_turn_record.py::test_a_task_cut_mid_turn_carries_its_turn_cancelled[close-inline]
#[tokio::test]
async fn an_inline_task_the_close_cuts_mid_turn_carries_its_turn_cancelled() {
    let worker = Agent::new("worker", true);
    let (kit, _, _) = kit_with(worker.clone(), RoomKitOptions::default()).await;
    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let delegating = kit.clone();
    let call = tokio::spawn(async move {
        let room = RoomId::new("p").unwrap();
        delegating
            .delegate(&room, &channel("worker"), "Go.", options)
            .await
    });
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();
    let task = tokio::time::timeout(BOUND, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let result = task.result().unwrap();
    assert_eq!(
        (result.status, result.metadata.get("turns").cloned()),
        (
            TaskStatus::Cancelled,
            Some(serde_json::json!({"worker": {"loop_end_reason": "cancelled"}}))
        )
    );
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_cancel_cut_while_the_task_ends_still_ends_it
// On 0931119 the task never ended: its end ran in the cancel's future.
#[tokio::test]
async fn a_cancel_cut_while_the_task_ends_still_ends_it() {
    let worker = Agent::new("worker", true);
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, completed, heard) = kit_with(worker.clone(), options).await;
    let ending = Ending::default();
    let options = DelegateOptions {
        on_complete: Some(ending.on_complete()),
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "go", options)
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    let runner = kit.task_runner();
    let task_id = task.id.clone();
    let cancelling = tokio::spawn(async move { runner.cancel(&task_id).await });
    tokio::time::timeout(BOUND, ending.entered.notified())
        .await
        .unwrap();
    cancelling.abort();
    drop(cancelling.await);
    ending.release.notify_one();

    let waited = WaitOptions {
        timeout: Some(BOUND),
    };
    let ended = task.wait(waited).await.unwrap();
    settled(&heard).await;
    assert_eq!(
        (ended.status, completed.lock().unwrap().clone()),
        (TaskStatus::Cancelled, cancelled())
    );
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
}

// Python: tests/tasks/test_cancelled_delegation.py::test_the_runner_ends_a_task_whose_handle_was_cancelled
// A handle's own cancel only lets its waiters go: the runner still ends
// the task when it cancels it.
#[tokio::test]
async fn the_runner_ends_a_task_whose_handle_was_cancelled() {
    let worker = Agent::new("worker", true);
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, completed, heard) = kit_with(worker.clone(), options).await;
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "go", DelegateOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.started.notified())
        .await
        .unwrap();

    task.cancel();
    let cancelled_it = kit.task_runner().cancel(&task.id).await;
    settled(&heard).await;

    assert_eq!(
        (cancelled_it, completed.lock().unwrap().clone()),
        (true, cancelled())
    );
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.background", "cancelled");
}

// Python: tests/tasks/test_cancelled_delegation.py::test_a_task_that_ran_ends_as_it_stands_whatever_cancels_it[cancel], [close]
// Python's test_a_task_that_ran_ends_whole_though_its_runner_task_is_cancelled
// cancels the runner's own asyncio task, which roomkit-rs keeps private: a
// cancel or a close is the one way to cut it from outside.
// On 0931119 the cancel answered at once, the task's end under way.
// Python: tests/tasks/test_integration.py::TestDelegateIntegration::test_on_complete_callback
// Python: tests/tasks/test_runner.py::TestInMemoryTaskRunner::test_on_complete_callback_called
#[tokio::test]
async fn a_task_that_ran_ends_as_it_stands_whatever_cancels_it() {
    for door in ["cancel", "close"] {
        let worker = EchoAgent::new(channel("worker"), "Done.", EchoAgentOptions::default());
        let (options, telemetry) = traced(RoomKitOptions::default());
        let (kit, completed, _) = kit_with(worker, options).await;
        let ending = Ending::default();
        let options = DelegateOptions {
            on_complete: Some(ending.on_complete()),
            ..DelegateOptions::default()
        };
        let room = RoomId::new("p").unwrap();
        let task = kit
            .delegate(&room, &channel("worker"), "go", options)
            .await
            .unwrap();
        tokio::time::timeout(BOUND, ending.entered.notified())
            .await
            .unwrap();

        let (runner, task_id) = (kit.task_runner(), task.id.clone());
        let mut asked = tokio::spawn(async move {
            if door == "cancel" {
                Some(runner.cancel(&task_id).await)
            } else {
                runner.close().await;
                Some(false)
            }
        });
        let waited = tokio::time::timeout(Duration::from_millis(50), &mut asked).await;
        let under_way = waited.is_err();
        ending.release.notify_one();
        let answer = tokio::time::timeout(BOUND, asked).await.unwrap().unwrap();

        let task_room = kit
            .store()
            .get_room(&task.child_room_id)
            .await
            .unwrap()
            .unwrap();
        let stamped =
            ["task_status", "task_result"].map(|key| task_room.metadata.get(key).cloned());
        assert_eq!(
            (door, under_way, answer, ending.seen(), task.status()),
            (
                door,
                true,
                Some(false),
                vec!["Completed".to_owned(), "done".to_owned()],
                TaskStatus::Completed
            )
        );
        assert_eq!(
            (completed.lock().unwrap().clone(), stamped),
            (
                vec![("completed".to_owned(), String::new())],
                [
                    Some(serde_json::json!("completed")),
                    Some(serde_json::json!("Done."))
                ]
            )
        );
        assert_delegation_ended(&telemetry, "delegation.background", "ok");
        kit.close().await.unwrap();
        assert_delegation_ended(&telemetry, "delegation.background", "ok");
    }
}

// Python: tests/tasks/test_cancelled_delegation.py::test_an_inline_caller_timing_out_while_the_task_ends_leaves_its_end_whole
// On 0931119 the end stopped with its caller, its callback halfway.
#[tokio::test]
async fn an_inline_caller_timing_out_while_the_task_ends_leaves_its_end_whole() {
    let worker = EchoAgent::new(channel("worker"), "Done.", EchoAgentOptions::default());
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, completed, heard) = kit_with(worker, options).await;
    let ending = Ending::default();
    let options = DelegateOptions {
        wait: true,
        on_complete: Some(ending.on_complete()),
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();

    let worker_id = channel("worker");
    let call = kit.delegate(&room, &worker_id, "go", options);
    cut_once(call, ending.entered.notified()).await;
    ending.release.notify_one();
    settled(&heard).await;
    let finished = tokio::time::timeout(BOUND, async {
        while ending.seen().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    assert_eq!(
        (
            finished.is_ok(),
            ending.seen(),
            completed.lock().unwrap().clone()
        ),
        (
            true,
            vec!["Completed".to_owned(), "done".to_owned()],
            vec![("completed".to_owned(), String::new())]
        )
    );
    assert_delegation_ended(&telemetry, "delegation.inline", "ok");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.inline", "ok");
}

// Python: tests/tasks/test_cancelled_delegation.py::test_an_inline_task_whose_turn_ended_before_its_caller_was_cut_ends_completed
// The caller is cut as its worker's turn returns: the work ran, so the task
// ends as it stands, completed with its answer.
#[tokio::test]
async fn an_inline_task_whose_turn_ended_before_its_caller_was_cut_ends_completed() {
    let worker = Agent::new("w1", false).saying("The findings.");
    let (options, telemetry) = traced(RoomKitOptions::default());
    let (kit, _, _) = kit_with(worker.clone(), options).await;
    let ends: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let heard = Arc::clone(&ends);
    let observer = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    kit.hook(
        HookTrigger::OnTaskCompleted,
        move |event: RoomEvent, _| {
            let status = event.metadata.get("task_status").and_then(|s| s.as_str());
            let status = status.unwrap_or_default().to_owned();
            heard
                .lock()
                .unwrap()
                .push((status, text_of(&event.content)));
            ready(())
        },
        observer,
    )
    .unwrap();
    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();

    let worker_id = channel("w1");
    let call = kit.delegate(&room, &worker_id, "Find it.", options);
    // Cut at the caller's first poll once the turn returned: on a
    // current-thread runtime the work may have ended the task by then,
    // which a cut leaves as it stands too.
    tokio::select! {
        biased;
        () = worker.started.notified() => {}
        _ = call => panic!("the call ended before its worker's turn"),
    }
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();

    assert_eq!(
        ends.lock().unwrap().clone(),
        vec![("completed".to_owned(), "The findings.".to_owned())]
    );
    assert_delegation_ended(&telemetry, "delegation.inline", "ok");
    kit.close().await.unwrap();
    assert_delegation_ended(&telemetry, "delegation.inline", "ok");
}

// RMK-1186, the light review: a cancel waiting for a task ending as it
// stands goes on once the task's runs are over, the close's cut among the
// ways, as Python's waits until the runs are over (wait_for_unspared).
// On c73c402 the cancel never returned: the cut run never told its end.
#[tokio::test(start_paused = true)]
async fn a_cancel_waiting_for_an_end_the_close_cuts_returns() {
    let worker = EchoAgent::new(channel("worker"), "Done.", EchoAgentOptions::default());
    let (kit, _, _) = kit_with(worker, RoomKitOptions::default()).await;
    let ending = Ending::default();
    let options = DelegateOptions {
        on_complete: Some(ending.on_complete()),
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();
    let task = kit
        .delegate(&room, &channel("worker"), "go", options)
        .await
        .unwrap();
    tokio::time::timeout(BOUND, ending.entered.notified())
        .await
        .unwrap();
    let (runner, task_id) = (kit.task_runner(), task.id.clone());
    let cancelling = tokio::spawn(async move { runner.cancel(&task_id).await });
    tokio::task::yield_now().await;

    kit.close().await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(120), cancelling).await;

    assert_eq!(answer.map(Result::unwrap).ok(), Some(false));
}

/// A runner answering every cancel `true`, counting them.
#[derive(Default)]
struct Cancels {
    asked: Mutex<usize>,
}

impl TaskRunner for Cancels {
    fn submit(
        &self,
        _kit: RoomKit,
        _task: DelegatedTask,
        _options: SubmitOptions,
    ) -> Boxed<Result<(), RoomKitError>> {
        Box::pin(ready(Ok(())))
    }

    fn cancel(&self, _task_id: &str) -> Boxed<bool> {
        *self.asked.lock().unwrap() += 1;
        Box::pin(ready(true))
    }

    fn close(&self) -> Boxed<()> {
        Box::pin(ready(()))
    }
}

// Python: tests/test_kit_close.py::test_a_teardown_call_returns_doing_nothing_once_the_close_started[cancel_task]
// On 1d8ce9e the kit asked its runner, which answered true.
#[tokio::test]
async fn cancel_task_does_nothing_once_the_close_started() {
    let runner = Arc::new(Cancels::default());
    let held: Arc<dyn TaskRunner> = Arc::<Cancels>::clone(&runner);
    let options = RoomKitOptions {
        task_runner: Some(held),
        ..RoomKitOptions::default()
    };
    let kit = RoomKit::new(options);

    kit.close().await.unwrap();
    let answer = kit.cancel_task("task", CancelTaskOptions::default()).await;

    assert_eq!((answer, *runner.asked.lock().unwrap()), (false, 0));
}

// RMK-1186, the second light review: the kit's close starts every held
// task's cancelled end at once, as Python's close cancels the runs it holds
// together; an end outlasting the grace keeps no other task from ending.
// On 41ccd46 the close ended the tasks one at a time under one grace: the
// first's held end used it up, and the second never ended, its caller
// waiting for ever.
#[tokio::test(start_paused = true)]
async fn the_close_starts_the_end_of_every_inline_task_at_once() {
    let worker = Agent::new("worker", true);
    let (kit, _, _) = kit_with(worker.clone(), RoomKitOptions::default()).await;
    let endings = [Ending::default(), Ending::default()];
    let callers: Vec<_> = endings
        .iter()
        .map(|ending| {
            let (kit, options) = (
                kit.clone(),
                DelegateOptions {
                    wait: true,
                    on_complete: Some(ending.on_complete()),
                    ..DelegateOptions::default()
                },
            );
            tokio::spawn(async move {
                let room = RoomId::new("p").unwrap();
                kit.delegate(&room, &channel("worker"), "go", options).await
            })
        })
        .collect();
    for _ in &callers {
        tokio::time::timeout(BOUND, worker.started.notified())
            .await
            .unwrap();
    }

    kit.close().await.unwrap();
    let mut returned = 0;
    for caller in callers {
        if tokio::time::timeout(Duration::from_secs(120), caller)
            .await
            .is_ok()
        {
            returned += 1;
        }
    }

    let entered = endings
        .iter()
        .map(|ending| ending.seen().len())
        .collect::<Vec<_>>();
    assert_eq!((returned, entered), (2, vec![1, 1]));
}

// Python: tests/tasks/test_worker_cut_short.py::test_the_cut_is_logged_once_as_a_warning
// The worker's turn record says its round cap cut it.
#[tokio::test]
async fn the_cut_is_logged_once_as_a_warning() {
    let (warnings, _capturing) = Warnings::capture();
    let worker = Agent::new("worker", false).ending("max_rounds");
    let (kit, _, _) = kit_with(worker, RoomKitOptions::default()).await;
    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let room = RoomId::new("p").unwrap();

    let task = kit
        .delegate(&room, &channel("worker"), "Find it.", options)
        .await
        .unwrap();
    kit.close().await.unwrap();

    let result = task.result().unwrap();
    let failed = warnings.containing("failed");
    assert_eq!(
        (
            result.status,
            result.error.as_deref(),
            failed.len(),
            failed.iter().all(|line| line.starts_with("WARN "))
        ),
        (
            TaskStatus::Failed,
            Some("The worker's turn ended max_rounds before its answer"),
            1,
            true
        )
    );
}
