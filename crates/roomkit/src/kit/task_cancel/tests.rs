//! A room's `cancel_task` answered from what the kit knows of the tasks it
//! runs for each room, never from what the status bus lists alone (RFC
//! §23.4): the tests of Python's `tests/tasks/test_cancel_task.py` that need
//! a bus slow at a chosen moment, a second kit, or the kit's record, and in
//! [`joins`] those of `tests/tasks/test_cancel_joins_an_ending.py`. Each call
//! is what roomkit-ai's `CancelTaskTool` answers, `cancel_task_answer`; the
//! conformance scenarios of roomkit-specs `tasks/` hold the others.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::hook::AllowOptions;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventType, HookResult,
    InboundMessage, Metadata, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger};
use serde_json::{Value, json};
use tokio::sync::{Notify, watch};

use crate::kit::background_tools::TaskToolWindow;
use crate::orchestration::backend::{RecentOptions, StatusBackend, StatusCallback, StatusFuture};
use crate::orchestration::{
    InMemoryStatusBackend, InMemoryStatusBackendOptions, PostAsyncOptions, StatusBus,
    StatusBusOptions, StatusEntry, StatusLevel,
};
use crate::store::{ConversationStore, InMemoryStore};
use crate::tasks::lines::{Line, ROOM_TASKS_WINDOW, room_lines};
use crate::tasks::{
    DelegatedTask, ENDED_ROOM_TASKS_KEPT, SubmitOptions, TaskReach, TaskRunner, WaitOptions,
};
use crate::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
};
use crate::{
    AttachChannelOptions, CancelTaskOptions, Channel, ChannelError, CreateRoomOptions,
    DelegateOptions, HookOptions, RoomKit, RoomKitError, RoomKitOptions,
};

mod joins;

/// How long a wait that should end may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

const CANCELLED: &str = "Cancelled: its result will not come back.";
const ENDED: &str = "The task had already ended.";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// A gate that stays open once opened.
#[derive(Clone)]
struct Latch(Arc<watch::Sender<bool>>);

impl Latch {
    fn new() -> Self {
        Self(Arc::new(watch::Sender::new(false)))
    }

    fn open(&self) {
        self.0.send_replace(true);
    }

    async fn passed(&self) {
        let mut open = self.0.subscribe();
        drop(open.wait_for(|open| *open).await);
    }
}

/// The worker: each turn tells `started`, then answers once `release` is
/// open; never released, it never ends (Python's `_Endless`, `_Gated`).
#[derive(Clone)]
struct Worker {
    answers: EchoAgent,
    started: Arc<Notify>,
    release: Latch,
}

impl Worker {
    fn endless() -> Self {
        Self {
            answers: EchoAgent::new(channel("worker"), "Done.", EchoAgentOptions::default()),
            started: Arc::default(),
            release: Latch::new(),
        }
    }

    fn answering() -> Self {
        let worker = Self::endless();
        worker.release.open();
        worker
    }

    async fn started(&self) {
        tokio::time::timeout(BOUND, self.started.notified())
            .await
            .unwrap();
    }
}

impl Channel for Worker {
    fn channel_id(&self) -> ChannelId {
        channel("worker")
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
        ready(Err("a worker is sent no message".into()))
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
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let worker = self.clone();
        async move {
            worker.started.notify_one();
            worker.release.passed().await;
            worker.answers.on_event(event, binding, context).await
        }
    }
}

/// The kit and its room `r`: a transport, the speaking agent the results
/// go to, the worker; what is handed back and how each task ended
/// (Python's `_Room`).
struct Fixture<S: ConversationStore = InMemoryStore> {
    kit: RoomKit<S>,
    worker: Worker,
    handed_back: Arc<Mutex<Vec<String>>>,
    ended: Arc<Mutex<Vec<String>>>,
}

impl<S: ConversationStore> Fixture<S> {
    async fn open(worker: Worker, options: RoomKitOptions<S>) -> Self {
        let kit = RoomKit::new(options);
        let sms = RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(sms).unwrap();
        let speaker = EchoAgent::new(
            channel("speaker"),
            "Here it is.",
            EchoAgentOptions::default(),
        );
        kit.register_channel(speaker).unwrap();
        kit.register_channel(worker.clone()).unwrap();
        create(&kit, "r").await;
        kit.attach_channel(
            &room_id("r"),
            &channel("sms"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        let as_agent = AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..AttachChannelOptions::default()
        };
        kit.attach_channel(&room_id("r"), &channel("speaker"), as_agent)
            .await
            .unwrap();
        let room = Self {
            kit,
            worker,
            handed_back: Arc::default(),
            ended: Arc::default(),
        };
        room.record_hand_backs();
        room.record_ends();
        room
    }

    /// Each instruction the room broadcasts, a hand-back, by the status it
    /// hands back.
    fn record_hand_backs(&self) {
        let handed_back = Arc::clone(&self.handed_back);
        let instructions = HookOptions {
            event_types: Some([EventType::Instruction].into()),
            ..HookOptions::default()
        };
        self.kit
            .hook(
                HookTrigger::BeforeBroadcast,
                move |event: RoomEvent, _| {
                    handed_back
                        .lock()
                        .unwrap()
                        .push(task_status(&event.metadata));
                    ready(HookResult::allow(AllowOptions::default()))
                },
                instructions,
            )
            .unwrap();
    }

    /// Each task's end, by the status `ON_TASK_COMPLETED` is told.
    fn record_ends(&self) {
        let ended = Arc::clone(&self.ended);
        let observer = HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        };
        self.kit
            .hook(
                HookTrigger::OnTaskCompleted,
                move |event: RoomEvent, _| {
                    ended.lock().unwrap().push(task_status(&event.metadata));
                    ready(())
                },
                observer,
            )
            .unwrap();
    }

    /// A background task of `room`, its result going to the speaker, its
    /// worker started.
    async fn running_task(&self, room: &str) -> DelegatedTask {
        let delegation = DelegateOptions {
            notify: Some(channel("speaker")),
            ..DelegateOptions::default()
        };
        let task = self
            .kit
            .delegate(
                &room_id(room),
                &channel("worker"),
                "Count to a thousand.",
                delegation,
            )
            .await
            .unwrap();
        self.worker.started().await;
        task
    }

    fn handed_back(&self) -> Vec<String> {
        self.handed_back.lock().unwrap().clone()
    }

    fn ended(&self) -> Vec<String> {
        self.ended.lock().unwrap().clone()
    }
}

async fn create<S: ConversationStore>(kit: &RoomKit<S>, room: &str) {
    let options = CreateRoomOptions {
        room_id: Some(room_id(room)),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
}

fn task_status(metadata: &Metadata) -> String {
    let status = metadata.get("task_status").and_then(Value::as_str);
    status.unwrap_or_default().to_owned()
}

/// The answer of `channel_id`'s `cancel_task` naming `task_id` in room `r`,
/// read from the bus's latest `window` entries: what roomkit-ai's
/// `CancelTaskTool` answers (Python's `_cancel`); no channel for a call
/// made with no tool call.
async fn cancel_in<S: ConversationStore>(
    kit: &RoomKit<S>,
    task_id: &str,
    channel_id: Option<&str>,
    window: usize,
) -> Value {
    let mut arguments = Metadata::new();
    arguments.insert("task_id".to_owned(), json!(task_id));
    let caller = channel_id.map(channel);
    let window = TaskToolWindow { window, limit: 0 };
    let answer = kit
        .cancel_task_answer(&room_id("r"), caller.as_ref(), &arguments, window)
        .await;
    serde_json::from_str(&answer).unwrap()
}

/// The speaker's `cancel_task` naming `task_id`, on the default window.
async fn cancel<S: ConversationStore>(kit: &RoomKit<S>, task_id: &str) -> Value {
    cancel_in(kit, task_id, Some("speaker"), ROOM_TASKS_WINDOW).await
}

fn answer(task_id: &str, status: &str, message: &str) -> Value {
    json!({ "task_id": task_id, "status": status, "message": message })
}

/// The lines the bus lists for room `r`, once it lists `count`: a post's
/// publication runs in the background.
async fn lines(bus: &StatusBus, count: usize) -> Vec<Metadata> {
    tokio::time::timeout(BOUND, async {
        loop {
            let lines = room_lines(bus, "r", 50).await;
            if lines.len() >= count {
                return lines.iter().map(Line::to_metadata).collect();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

/// Waits until `holds` says the kit holds what the test waits for.
async fn until(holds: impl Fn() -> bool) {
    tokio::time::timeout(BOUND, async {
        while !holds() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Posts a `task` line of `status` for room `r` naming `task_id`, as any
/// code holding the kit may.
async fn post_line(bus: &StatusBus, status: StatusLevel, task_id: &str, extra: Value) {
    let mut metadata = Metadata::new();
    metadata.insert("room_id".to_owned(), json!("r"));
    metadata.insert("task_id".to_owned(), json!(task_id));
    metadata.extend(extra.as_object().cloned().unwrap_or_default());
    let options = PostAsyncOptions {
        detail: "12 of 30".to_owned(),
        metadata: Some(metadata),
    };
    bus.post_async("worker", "task", status, options).await;
}

/// The in-memory backend, whose task ends (not their pending entries) wait
/// for `flush`, as on a remote bus lagging on them (Python's
/// `_LaggingBackend`).
struct Lagging {
    entries: InMemoryStatusBackend,
    flush: Latch,
}

impl Lagging {
    fn bus() -> (StatusBus, Latch) {
        let flush = Latch::new();
        let backend = Self {
            entries: InMemoryStatusBackend::new(InMemoryStatusBackendOptions::default()),
            flush: flush.clone(),
        };
        let options = StatusBusOptions {
            backend: Some(Arc::new(backend)),
            ..StatusBusOptions::default()
        };
        (StatusBus::new(options), flush)
    }
}

impl StatusBackend for Lagging {
    fn publish(&self, entry: StatusEntry) -> StatusFuture<'_, ()> {
        Box::pin(async move {
            if entry.action == "task" && entry.status != StatusLevel::Pending {
                self.flush.passed().await;
            }
            self.entries.publish(entry).await;
        })
    }

    fn recent(&self, n: usize, options: RecentOptions) -> StatusFuture<'_, Vec<StatusEntry>> {
        self.entries.recent(n, options)
    }

    fn subscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()> {
        self.entries.subscribe(callback)
    }

    fn unsubscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()> {
        self.entries.unsubscribe(callback)
    }
}

fn on_bus(bus: &StatusBus) -> RoomKitOptions {
    RoomKitOptions {
        status_bus: Some(bus.clone()),
        ..RoomKitOptions::default()
    }
}

// Python: tests/tasks/test_cancel_task.py::test_an_inline_delegation_is_said_running_not_unknown
#[tokio::test]
async fn an_inline_delegation_is_said_running_not_unknown() {
    let room = Fixture::open(Worker::endless(), RoomKitOptions::default()).await;
    let kit = room.kit.clone();
    let inline = tokio::spawn(async move {
        let delegation = DelegateOptions {
            wait: true,
            ..DelegateOptions::default()
        };
        let worker = channel("worker");
        kit.delegate(&room_id("r"), &worker, "Count to a thousand.", delegation)
            .await
    });
    room.worker.started().await;
    let line = lines(room.kit.status_bus(), 1).await.remove(0);
    let task_id = line["task_id"].as_str().unwrap().to_owned();

    let answered = cancel(&room.kit, &task_id).await;
    let still_running = !inline.is_finished();
    inline.abort();
    drop(inline.await);
    idle(&room.kit).await;

    assert_eq!(
        (
            &line["status"],
            answered,
            still_running,
            room.kit.inner.room_tasks.running()
        ),
        (
            &json!("running"),
            answer(
                &task_id,
                "running",
                "Still running, and not cancellable here: it runs inside the turn that asked for \
                 it, and ends with it."
            ),
            true,
            Vec::<String>::new()
        )
    );
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_task.py::test_a_task_whose_pending_entry_left_the_window_is_still_cancelled
#[tokio::test]
async fn a_task_whose_pending_entry_left_the_window_is_still_cancelled() {
    let room = Fixture::open(Worker::endless(), RoomKitOptions::default()).await;
    let task = room.running_task("r").await;
    let bus = room.kit.status_bus();
    lines(bus, 1).await;
    for n in 0..5 {
        let mut metadata = Metadata::new();
        metadata.insert("room_id".to_owned(), json!(format!("o{n}")));
        let options = PostAsyncOptions {
            metadata: Some(metadata),
            ..PostAsyncOptions::default()
        };
        bus.post_async("worker", "task", StatusLevel::Pending, options)
            .await;
    }
    post_line(bus, StatusLevel::Info, &task.id, json!({})).await;

    let answered = cancel_in(&room.kit, &task.id, Some("speaker"), 6).await;

    assert_eq!(answered["status"], "cancelled");
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_task.py::test_a_cancel_with_no_tool_call_is_held_to_the_room_too
#[tokio::test]
async fn a_cancel_with_no_tool_call_is_held_to_the_room_too() {
    let room = Fixture::open(Worker::endless(), RoomKitOptions::default()).await;
    create(&room.kit, "other").await;
    let theirs = room.running_task("other").await;
    let bus = room.kit.status_bus();
    post_line(bus, StatusLevel::Pending, &theirs.id, json!({})).await;

    let answered = cancel_in(&room.kit, &theirs.id, None, ROOM_TASKS_WINDOW).await;
    let still_running = theirs.result().is_none();

    assert_eq!(
        (answered["status"].clone(), still_running),
        (json!("unknown"), true)
    );
    room.kit.close().await.unwrap();
}

/// A runner refusing every task, as a full queue does.
struct Refuses;

impl TaskRunner for Refuses {
    fn submit(
        &self,
        _kit: RoomKit,
        _task: DelegatedTask,
        _options: SubmitOptions,
    ) -> crate::channel::BoxFuture<'static, Result<(), RoomKitError>> {
        Box::pin(ready(Err(RoomKitError::Runtime("down".to_owned()))))
    }

    fn cancel(&self, _task_id: &str) -> crate::channel::BoxFuture<'static, bool> {
        Box::pin(ready(false))
    }

    fn close(&self) -> crate::channel::BoxFuture<'static, ()> {
        Box::pin(ready(()))
    }
}

// Python: tests/tasks/test_cancel_task.py::test_a_submit_that_fails_leaves_no_task_of_the_room
#[tokio::test]
async fn a_submit_that_fails_leaves_no_task_of_the_room() {
    let options = RoomKitOptions {
        task_runner: Some(Arc::new(Refuses)),
        ..RoomKitOptions::default()
    };
    let room = Fixture::open(Worker::endless(), options).await;
    let delegation = DelegateOptions {
        notify: Some(channel("speaker")),
        ..DelegateOptions::default()
    };

    let refused = room
        .kit
        .delegate(&room_id("r"), &channel("worker"), "Count.", delegation)
        .await;

    assert_eq!(
        (refused.is_err(), room.kit.inner.room_tasks.running()),
        (true, Vec::<String>::new())
    );
    room.kit.close().await.unwrap();
}

/// The in-memory backend whose next read, once `armed`, lets the worker go
/// and waits for the task's end on the bus before it answers what it read:
/// the room's task ends while `cancel_task` reads its line (Python's
/// `ending_meanwhile`).
struct EndingMeanwhile {
    entries: InMemoryStatusBackend,
    armed: AtomicBool,
    release: Latch,
    ended: Notify,
}

impl StatusBackend for EndingMeanwhile {
    fn publish(&self, entry: StatusEntry) -> StatusFuture<'_, ()> {
        Box::pin(async move {
            let ends = entry.action == "task"
                && matches!(entry.status, StatusLevel::Completed | StatusLevel::Failed);
            self.entries.publish(entry).await;
            if ends {
                self.ended.notify_one();
            }
        })
    }

    fn recent(&self, n: usize, options: RecentOptions) -> StatusFuture<'_, Vec<StatusEntry>> {
        Box::pin(async move {
            let entries = self.entries.recent(n, options).await;
            if self.armed.swap(false, Ordering::SeqCst) {
                self.release.open();
                tokio::time::timeout(BOUND, self.ended.notified())
                    .await
                    .unwrap();
            }
            entries
        })
    }

    fn subscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()> {
        self.entries.subscribe(callback)
    }

    fn unsubscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()> {
        self.entries.unsubscribe(callback)
    }
}

/// The task of room `r` run inline (`wait`) or in the background, its
/// result handed back to nobody of the room: its run, which returns once
/// it ended.
fn delegated(
    kit: &RoomKit,
    wait: bool,
) -> tokio::task::JoinHandle<Result<DelegatedTask, RoomKitError>> {
    let kit = kit.clone();
    tokio::spawn(async move {
        let delegation = DelegateOptions {
            wait,
            ..DelegateOptions::default()
        };
        let task = kit
            .delegate(&room_id("r"), &channel("worker"), "Count.", delegation)
            .await?;
        task.wait(WaitOptions {
            timeout: Some(BOUND),
        })
        .await?;
        Ok(task)
    })
}

// Python: tests/tasks/test_cancel_task.py::test_the_room_s_task_ending_while_cancel_task_reads_the_bus_reads_ended
#[tokio::test]
async fn the_room_s_task_ending_while_cancel_task_reads_the_bus_reads_ended() {
    for wait in [true, false] {
        let worker = Worker::endless();
        let backend = Arc::new(EndingMeanwhile {
            entries: InMemoryStatusBackend::new(InMemoryStatusBackendOptions::default()),
            armed: AtomicBool::new(false),
            release: worker.release.clone(),
            ended: Notify::new(),
        });
        let shared: Arc<dyn StatusBackend> = Arc::<EndingMeanwhile>::clone(&backend);
        let bus = StatusBus::new(StatusBusOptions {
            backend: Some(shared),
            ..StatusBusOptions::default()
        });
        let room = Fixture::open(worker, on_bus(&bus)).await;
        let run = delegated(&room.kit, wait);
        room.worker.started().await;
        let running = room.kit.inner.room_tasks.running();
        backend.armed.store(true, Ordering::SeqCst);

        let answered = cancel(&room.kit, &running[0]).await;
        tokio::time::timeout(BOUND, run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(
            (wait, answered),
            (wait, answer(&running[0], "completed", ENDED))
        );
        room.kit.close().await.unwrap();
    }
}

// Python: tests/tasks/test_cancel_task.py::test_the_room_s_task_ended_before_the_bus_shows_it_reads_ended
#[tokio::test]
async fn the_room_s_task_ended_before_the_bus_shows_it_reads_ended() {
    for wait in [true, false] {
        let (bus, flush) = Lagging::bus();
        let room = Fixture::open(Worker::answering(), on_bus(&bus)).await;
        let task = delegated(&room.kit, wait).await.unwrap().unwrap();
        let line = lines(&bus, 1).await.remove(0);

        let answered = cancel(&room.kit, &task.id).await;
        flush.open();

        assert_eq!(
            (wait, &line["status"], answered),
            (
                wait,
                &json!("running"),
                answer(&task.id, "completed", ENDED)
            )
        );
        room.kit.close().await.unwrap();
    }
}

// Python: tests/tasks/test_cancel_task.py::test_past_the_bound_a_line_still_listed_running_is_no_task_here
#[tokio::test]
async fn past_the_bound_a_line_still_listed_running_is_no_task_here() {
    let (bus, flush) = Lagging::bus();
    let room = Fixture::open(Worker::answering(), on_bus(&bus)).await;
    let task = delegated(&room.kit, false).await.unwrap().unwrap();
    let line = lines(&bus, 1).await.remove(0);
    let record = &room.kit.inner.room_tasks;
    for n in 0..ENDED_ROOM_TASKS_KEPT {
        let elsewhere = format!("elsewhere-{n}");
        record.start(&elsewhere, &room_id("other"), TaskReach::Tool);
        record.end(&elsewhere, "completed");
    }

    let answered = cancel(&room.kit, &task.id).await;
    flush.open();

    assert_eq!(
        (&line["status"], answered),
        (
            &json!("running"),
            answer(&task.id, "unknown", &format!("No task {} here.", task.id))
        )
    );
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_task.py::test_a_strategy_s_inline_worker_run_is_the_strategy_s
// roomkit-rs has no run_worker nor post_task_progress: the strategy's inline
// run is a delegation it does not post, and the progress its worker posts
// is the info line written here.
#[tokio::test]
async fn a_strategy_s_inline_worker_run_is_the_strategy_s() {
    let room = Fixture::open(Worker::endless(), RoomKitOptions::default()).await;
    let kit = room.kit.clone();
    let run = tokio::spawn(async move {
        let delegation = DelegateOptions {
            wait: true,
            post_status: false,
            ..DelegateOptions::default()
        };
        kit.delegate(&room_id("r"), &channel("worker"), "Count.", delegation)
            .await
    });
    room.worker.started().await;
    let running = room.kit.inner.room_tasks.running();
    post_line(
        room.kit.status_bus(),
        StatusLevel::Info,
        &running[0],
        json!({}),
    )
    .await;

    let answered = cancel(&room.kit, &running[0]).await;
    run.abort();
    drop(run.await);
    idle(&room.kit).await;

    assert_eq!(
        answered,
        answer(
            &running[0],
            "running",
            "Still running, and not cancellable here: the strategy that runs it follows it."
        )
    );
    room.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_task.py::test_another_kit_s_task_on_a_shared_bus_is_no_task_here
#[tokio::test]
async fn another_kit_s_task_on_a_shared_bus_is_no_task_here() {
    let bus = StatusBus::default();
    let theirs = Fixture::open(Worker::endless(), on_bus(&bus)).await;
    let ours = Fixture::open(Worker::answering(), on_bus(&bus)).await;
    let task = theirs.running_task("r").await;
    lines(&bus, 1).await;

    let answered = cancel(&ours.kit, &task.id).await;
    theirs
        .kit
        .cancel_task(&task.id, CancelTaskOptions::default())
        .await;

    assert_eq!(
        answered,
        answer(&task.id, "unknown", &format!("No task {} here.", task.id))
    );
    ours.kit.close().await.unwrap();
    theirs.kit.close().await.unwrap();
}

// Python: tests/tasks/test_cancel_task.py::test_a_line_the_bus_lists_ended_is_answered_as_it_lists_it[another kit's ended task]
// Its forged line is the scenario
// tasks/a-line-the-bus-lists-ended-is-answered-as-it-lists-it.
#[tokio::test]
async fn another_kit_s_ended_task_is_answered_as_the_bus_lists_it() {
    let bus = StatusBus::default();
    let ours = Fixture::open(Worker::answering(), on_bus(&bus)).await;
    let theirs = Fixture::open(Worker::answering(), on_bus(&bus)).await;
    let task = delegated(&theirs.kit, true).await.unwrap().unwrap();
    tokio::time::timeout(BOUND, async {
        while lines(&bus, 1).await[0]["status"] != "completed" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let answered = cancel(&ours.kit, &task.id).await;

    assert_eq!(answered, answer(&task.id, "completed", ENDED));
    ours.kit.close().await.unwrap();
    theirs.kit.close().await.unwrap();
}
