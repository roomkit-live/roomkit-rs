//! Delegated tasks at the kit's close (RFC §23.3, §12.10.4): what no
//! shared scenario can say, the suite having no call that closes the kit.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId, TaskStatus,
};
use roomkit::orchestration::StatusEntry;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, DelegateOptions, HookExecution,
    HookOptions, HookTrigger, RoomKit, RoomKitOptions,
};
use tokio::sync::Notify;

/// How long a wait that should end may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

/// An agent whose every turn tells `started`, then never ends, unless it
/// only records the events it is asked (`hangs` false).
#[derive(Clone)]
struct Agent {
    id: ChannelId,
    hangs: bool,
    started: Arc<Notify>,
    asked: Arc<Mutex<Vec<RoomEvent>>>,
}

impl Agent {
    fn new(id: &str, hangs: bool) -> Self {
        Self {
            id: channel(id),
            hangs,
            started: Arc::default(),
            asked: Arc::default(),
        }
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
            agent.asked.lock().unwrap().push(event);
            agent.started.notify_one();
            if agent.hangs {
                pending::<()>().await;
            }
            Ok(ChannelOutput::empty())
        }
    }
}

// Python: tests/tasks/test_cancelled_delegation.py::test_closing_the_kit_hands_nothing_back
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
