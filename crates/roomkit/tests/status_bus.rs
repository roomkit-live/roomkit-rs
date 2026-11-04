//! The kit's status bus (RFC §19.8): Python's
//! `tests/test_framework_status_bus.py`. What a scenario says of it, an
//! entry's framework event and its room's hooks, is in the shared suite
//! (`status/`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::orchestration::backend::{RecentOptions as BackendRecentOptions, StatusBackend};
use roomkit::orchestration::{
    InMemoryStatusBackend, PostAsyncOptions, PostOptions, RecentOptions, StatusBus,
    StatusBusOptions, StatusEntry, StatusLevel,
};
use roomkit::{RoomKit, RoomKitOptions};

/// A backend that notes its close, the in-memory one doing the rest.
#[derive(Default)]
struct Closing {
    kept: InMemoryStatusBackend,
    closed: AtomicBool,
}

type Future<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

type Callback = Arc<dyn Fn(StatusEntry) -> Future<'static, ()> + Send + Sync>;

impl StatusBackend for Closing {
    fn publish(&self, entry: StatusEntry) -> Future<'_, ()> {
        self.kept.publish(entry)
    }

    fn recent(&self, n: usize, options: BackendRecentOptions) -> Future<'_, Vec<StatusEntry>> {
        self.kept.recent(n, options)
    }

    fn subscribe(&self, callback: Callback) -> Future<'_, ()> {
        self.kept.subscribe(callback)
    }

    fn unsubscribe(&self, callback: Callback) -> Future<'_, ()> {
        self.kept.unsubscribe(callback)
    }

    fn close(&self) -> Future<'_, ()> {
        self.closed.store(true, Ordering::SeqCst);
        Box::pin(async {})
    }
}

// Python: tests/test_framework_status_bus.py::test_status_bus_custom_instance
#[tokio::test]
async fn the_kit_s_bus_is_the_one_its_options_name() {
    let bus = StatusBus::default();
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(bus.clone()),
        ..RoomKitOptions::default()
    });
    bus.post_async(
        "exec",
        "search",
        StatusLevel::Ok,
        PostAsyncOptions::default(),
    )
    .await;
    let seen = kit.status_bus().recent(RecentOptions::default()).await;
    assert_eq!(
        seen.iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        ["search"]
    );
}

// Python: tests/test_framework_status_bus.py::test_context_manager_subscribes
// The kit subscribes to its bus as it is built, where Python's subscribes
// as its context manager opens: a status posted has reached its handlers by
// the time `post_async` returns, with no wait for the kit to go idle.
#[tokio::test]
async fn a_status_posted_has_reached_the_kit_s_handlers_once_posted() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let heard: Arc<Mutex<Vec<String>>> = Arc::default();
    let notes = Arc::clone(&heard);
    kit.on("status_posted", move |event| {
        let agent = event.data["agent_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        notes.lock().unwrap().push(agent);
        async {}
    })
    .unwrap();
    kit.status_bus()
        .post_async("a", "act", StatusLevel::Ok, PostAsyncOptions::default())
        .await;
    assert_eq!(heard.lock().unwrap().clone(), ["a"]);
}

// Python: tests/test_framework_status_bus.py::test_close_closes_status_bus
#[tokio::test]
async fn the_kit_s_close_closes_its_bus() {
    let backend = Arc::new(Closing::default());
    let held: Arc<Closing> = Arc::clone(&backend);
    let held: Arc<dyn StatusBackend> = held;
    let bus = StatusBus::new(StatusBusOptions {
        backend: Some(held),
        ..StatusBusOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(bus),
        ..RoomKitOptions::default()
    });
    kit.close().await.unwrap();
    assert!(backend.closed.load(Ordering::SeqCst));
}

// Python: tests/test_framework_status_bus.py::test_framework_event_on_post_sync
#[tokio::test]
async fn a_status_posted_without_waiting_is_a_framework_event_too() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let heard: Arc<Mutex<Vec<String>>> = Arc::default();
    let notes = Arc::clone(&heard);
    kit.on("status_posted", move |event| {
        let agent = event.data["agent_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        notes.lock().unwrap().push(agent);
        async {}
    })
    .unwrap();
    kit.status_bus()
        .post("voice", "greet", StatusLevel::Info, PostOptions::default());
    roomkit::testing::idle(&kit).await;
    for _ in 0..10 {
        if !heard.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(heard.lock().unwrap().clone(), ["voice"]);
}

// Python: tests/test_hook_observers.py (ON_FEEDBACK observes
// submit_feedback("r1", 0.9)), whose observation names no channel: its
// channel_id is empty (RFC §15.9).
#[tokio::test]
async fn a_feedback_naming_no_channel_is_an_observation_of_no_channel() {
    use roomkit::model::RoomId;
    use roomkit::{CreateRoomOptions, ListObservationsOptions, SubmitFeedbackOptions};

    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.submit_feedback(&room, 0.9, SubmitFeedbackOptions::default())
        .await
        .unwrap();
    let observations = kit
        .list_observations(&room, ListObservationsOptions::default())
        .await
        .unwrap();
    let written = serde_json::to_value(&observations).unwrap();
    assert_eq!(
        (
            observations
                .first()
                .map(|observation| observation.channel_id.clone()),
            written[0]["channel_id"].clone(),
            written[0]["content"].clone(),
        ),
        (
            Some(None),
            serde_json::json!(""),
            serde_json::json!("[overall] 0.90")
        )
    );
}

/// A backend whose subscription does not end at once, as one that reaches
/// a server does: it waits on the runtime's timer first, which a poll made
/// outside a runtime cannot, then subscribes, and says so.
#[derive(Default)]
struct Late {
    kept: InMemoryStatusBackend,
    subscribed: Arc<tokio::sync::Notify>,
}

impl StatusBackend for Late {
    fn publish(&self, entry: StatusEntry) -> Future<'_, ()> {
        self.kept.publish(entry)
    }

    fn recent(&self, n: usize, options: BackendRecentOptions) -> Future<'_, Vec<StatusEntry>> {
        self.kept.recent(n, options)
    }

    fn subscribe(&self, callback: Callback) -> Future<'_, ()> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            self.kept.subscribe(callback).await;
            self.subscribed.notify_one();
        })
    }

    fn unsubscribe(&self, callback: Callback) -> Future<'_, ()> {
        self.kept.unsubscribe(callback)
    }
}

// Python: tests/test_status_posted_without_context_manager.py::test_a_status_posted_after_room_activity_reaches_the_rooms_hooks
// A kit built outside a runtime, whose bus does not subscribe at once,
// subscribes at its first call made in one.
#[test]
fn a_status_posted_after_room_activity_reaches_the_rooms_hooks() {
    use roomkit::model::RoomId;
    use roomkit::{CreateRoomOptions, HookExecution, HookOptions, HookTrigger};

    let backend = Arc::new(Late::default());
    let subscribed = Arc::clone(&backend.subscribed);
    let held: Arc<dyn StatusBackend> = backend;
    let bus = StatusBus::new(StatusBusOptions {
        backend: Some(held),
        ..StatusBusOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(bus.clone()),
        ..RoomKitOptions::default()
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let heard = runtime.block_on(async {
        let room = RoomId::new("r").unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
        let bound = std::time::Duration::from_secs(5);
        let on_time = tokio::time::timeout(bound, subscribed.notified()).await;
        let (heard, told) = (
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(tokio::sync::Notify::new()),
        );
        let (notes, telling) = (Arc::clone(&heard), Arc::clone(&told));
        let options = HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        };
        kit.hook(
            HookTrigger::OnStatusPosted,
            move |entry: StatusEntry, _| {
                notes.lock().unwrap().push(entry.agent_id);
                telling.notify_one();
                std::future::ready(())
            },
            options,
        )
        .unwrap();
        let mut metadata = roomkit::model::Metadata::new();
        metadata.insert("room_id".to_owned(), serde_json::json!("r"));
        let options = PostAsyncOptions {
            metadata: Some(metadata),
            ..PostAsyncOptions::default()
        };
        bus.post_async("agent-1", "working", StatusLevel::Pending, options)
            .await;
        if on_time.is_ok() {
            drop(tokio::time::timeout(bound, told.notified()).await);
        }
        kit.close().await.unwrap();
        let heard = heard.lock().unwrap().clone();
        (on_time.is_ok(), heard)
    });
    assert_eq!(heard, (true, vec!["agent-1".to_owned()]));
}

/// A kit's first room activity.
#[derive(Clone, Copy, Debug)]
enum Activity {
    /// The room `r` created.
    CreateRoom,
    /// A message sent into the room `r` its store holds.
    ProcessInbound,
    /// An event sent into the room `r` its store holds.
    SendEvent,
    /// A message its router routes into the room `r` its store holds.
    RoutedInbound,
    /// A feedback on the room `r` its store holds.
    Feedback,
    /// The kit started, as Python's `async with`.
    Start,
}

/// A kit over `bus` whose store holds the room `r`, its channel `sms`
/// registered and bound there.
#[expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
async fn kit_with_a_room(bus: &StatusBus) -> RoomKit {
    use roomkit::model::{ChannelBinding, ChannelId, ChannelType, Room, RoomId};
    use roomkit::{ConversationStore, InMemoryStore};

    let store = Arc::new(InMemoryStore::default());
    let room = RoomId::new("r").unwrap();
    store.create_room(Room::new(room.clone())).await.unwrap();
    let sms = ChannelId::new("sms").unwrap();
    let binding = ChannelBinding::new(sms.clone(), room, ChannelType::SMS);
    store.add_binding(binding).await.unwrap();
    let kit = RoomKit::new(RoomKitOptions {
        store,
        status_bus: Some(bus.clone()),
        ..RoomKitOptions::default()
    });
    let transport = roomkit::testing::RecordingTransport::new(
        sms,
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    );
    kit.register_channel(transport).unwrap();
    kit
}

/// Makes `activity`, `kit`'s first.
#[expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
async fn first(kit: &RoomKit, activity: Activity) {
    use roomkit::model::{ChannelId, InboundMessage, RoomId, TextContent};
    use roomkit::{CreateRoomOptions, ProcessInboundOptions, SendEventOptions};

    let room = RoomId::new("r").unwrap();
    let sms = ChannelId::new("sms").unwrap();
    match activity {
        Activity::CreateRoom => {
            let options = CreateRoomOptions {
                room_id: Some(room),
                ..CreateRoomOptions::default()
            };
            kit.create_room(options).await.unwrap();
        }
        Activity::ProcessInbound => {
            let message = InboundMessage::new(sms, "+15551234567", TextContent::new("hi"));
            let options = ProcessInboundOptions {
                room_id: Some(room),
                ..ProcessInboundOptions::default()
            };
            kit.process_inbound(message, options).await.unwrap();
        }
        Activity::RoutedInbound => {
            let message = InboundMessage::new(sms, "+15551234567", TextContent::new("hi"));
            let options = ProcessInboundOptions::default();
            kit.process_inbound(message, options).await.unwrap();
        }
        Activity::Feedback => {
            let options = roomkit::SubmitFeedbackOptions::default();
            kit.submit_feedback(&room, 0.5, options).await.unwrap();
        }
        Activity::Start => kit.start().await.unwrap(),
        Activity::SendEvent => {
            let text = TextContent::new("hi");
            kit.send_event(&room, &sms, text, SendEventOptions::default())
                .await
                .unwrap();
        }
    }
}

/// What a status `bus` posted for room `r` right after `kit`'s first room
/// activity, `activity`, reaches of its `ON_STATUS_POSTED` hooks: the
/// agents heard.
#[expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
async fn heard_right_after(kit: &RoomKit, bus: &StatusBus, activity: Activity) -> Vec<String> {
    use roomkit::{HookExecution, HookOptions, HookTrigger};

    let (heard, told) = (
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(tokio::sync::Notify::new()),
    );
    let (notes, telling) = (Arc::clone(&heard), Arc::clone(&told));
    let options = HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    kit.hook(
        HookTrigger::OnStatusPosted,
        move |entry: StatusEntry, _| {
            notes.lock().unwrap().push(entry.agent_id);
            telling.notify_one();
            std::future::ready(())
        },
        options,
    )
    .unwrap();
    first(kit, activity).await;
    let mut metadata = roomkit::model::Metadata::new();
    metadata.insert("room_id".to_owned(), serde_json::json!("r"));
    let options = PostAsyncOptions {
        metadata: Some(metadata),
        ..PostAsyncOptions::default()
    };
    bus.post_async("agent-1", "working", StatusLevel::Pending, options)
        .await;
    drop(tokio::time::timeout(std::time::Duration::from_secs(1), told.notified()).await);
    kit.close().await.unwrap();
    heard.lock().unwrap().clone()
}

/// A status bus over a backend whose subscription does not end at once.
fn late_bus() -> StatusBus {
    let held: Arc<dyn StatusBackend> = Arc::new(Late::default());
    StatusBus::new(StatusBusOptions {
        backend: Some(held),
        ..StatusBusOptions::default()
    })
}

// Python: tests/test_status_posted_without_context_manager.py::test_a_status_posted_after_room_activity_reaches_the_rooms_hooks
// RMK-1201 (1): the first room activity awaits the kit's subscription
// (Python's `_ensure_status_bus_subscribed`), so a status posted right
// after it reaches the room's hooks over a bus that subscribes late,
// whether the kit was built in a runtime or outside one.
#[test]
fn a_status_posted_right_after_the_first_activity_reaches_a_late_bus_s_hooks() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let bus = late_bus();
    let outside = RoomKit::new(RoomKitOptions {
        status_bus: Some(bus.clone()),
        ..RoomKitOptions::default()
    });
    let heard_outside = runtime.block_on(heard_right_after(&outside, &bus, Activity::CreateRoom));
    let bus = late_bus();
    let heard_within = runtime.block_on(async {
        let within = RoomKit::new(RoomKitOptions {
            status_bus: Some(bus.clone()),
            ..RoomKitOptions::default()
        });
        heard_right_after(&within, &bus, Activity::CreateRoom).await
    });

    assert_eq!(
        (heard_outside, heard_within),
        (vec!["agent-1".to_owned()], vec!["agent-1".to_owned()])
    );
}

/// A backend whose first subscription never ends, as one whose server
/// does not answer; the next subscribes at once.
#[derive(Default)]
struct FirstTryHangs {
    kept: InMemoryStatusBackend,
    tries: std::sync::atomic::AtomicUsize,
}

impl StatusBackend for FirstTryHangs {
    fn publish(&self, entry: StatusEntry) -> Future<'_, ()> {
        self.kept.publish(entry)
    }

    fn recent(&self, n: usize, options: BackendRecentOptions) -> Future<'_, Vec<StatusEntry>> {
        self.kept.recent(n, options)
    }

    fn subscribe(&self, callback: Callback) -> Future<'_, ()> {
        Box::pin(async move {
            if self.tries.fetch_add(1, Ordering::SeqCst) == 0 {
                std::future::pending::<()>().await;
            }
            self.kept.subscribe(callback).await;
        })
    }

    fn unsubscribe(&self, callback: Callback) -> Future<'_, ()> {
        self.kept.unsubscribe(callback)
    }
}

// Python: core/framework.py _ensure_status_bus_subscribed, "a subscription
// that failed is tried again by the next one" (RMK-1201 (1)): an activity
// dropped while the kit subscribes leaves the subscription to the next.
#[tokio::test]
async fn a_subscription_cut_short_is_made_again_by_the_next_activity() {
    let held: Arc<dyn StatusBackend> = Arc::new(FirstTryHangs::default());
    let bus = StatusBus::new(StatusBusOptions {
        backend: Some(held),
        ..StatusBusOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(bus.clone()),
        ..RoomKitOptions::default()
    });
    let first = kit.create_room(roomkit::CreateRoomOptions::default());
    let cut = tokio::time::timeout(std::time::Duration::from_millis(10), first).await;

    let heard = heard_right_after(&kit, &bus, Activity::CreateRoom).await;

    assert_eq!((cut.is_err(), heard), (true, vec!["agent-1".to_owned()]));
}

// Python: core/mixins/helpers.py _build_context (process_inbound routed or
// not, submit_feedback), core/framework.py _send_event and __aenter__ await
// _ensure_status_bus_subscribed (RMK-1201 (1)): each is a first room
// activity too.
#[tokio::test]
async fn a_message_or_an_event_is_a_first_room_activity() {
    let mut heard = Vec::new();
    let activities = [
        Activity::ProcessInbound,
        Activity::RoutedInbound,
        Activity::SendEvent,
        Activity::Feedback,
        Activity::Start,
    ];
    for activity in activities {
        let bus = late_bus();
        let kit = kit_with_a_room(&bus).await;
        heard.push(heard_right_after(&kit, &bus, activity).await);
    }

    assert_eq!(heard, vec![vec!["agent-1".to_owned()]; 5]);
}

/// A backend whose subscription waits to be answered: Python's `_LateBackend`.
#[derive(Default)]
struct Answered {
    kept: InMemoryStatusBackend,
    asked: tokio::sync::watch::Sender<bool>,
    answer: tokio::sync::watch::Sender<bool>,
}

impl Answered {
    /// Until a call waits for the subscription: a kit built in a runtime asks
    /// it at once, and a paused clock moves once the call is at its wait.
    async fn until_waited_for(&self) {
        drop(self.asked.subscribe().wait_for(|asked| *asked).await);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    fn answer(&self) {
        self.answer.send_replace(true);
    }
}

impl StatusBackend for Answered {
    fn publish(&self, entry: StatusEntry) -> Future<'_, ()> {
        self.kept.publish(entry)
    }

    fn recent(&self, n: usize, options: BackendRecentOptions) -> Future<'_, Vec<StatusEntry>> {
        self.kept.recent(n, options)
    }

    fn subscribe(&self, callback: Callback) -> Future<'_, ()> {
        Box::pin(async move {
            self.asked.send_replace(true);
            drop(self.answer.subscribe().wait_for(|answer| *answer).await);
            self.kept.subscribe(callback).await;
        })
    }

    fn unsubscribe(&self, callback: Callback) -> Future<'_, ()> {
        self.kept.unsubscribe(callback)
    }
}

/// A kit over `backend`, an SMS transport on it.
#[expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
fn late_kit(backend: &Arc<Answered>) -> RoomKit {
    use roomkit::model::{ChannelId, ChannelType};
    use roomkit::testing::{RecordingTransport, RecordingTransportOptions};

    let held: Arc<dyn StatusBackend> = Arc::<Answered>::clone(backend);
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(StatusBus::new(StatusBusOptions {
            backend: Some(held),
            ..StatusBusOptions::default()
        })),
        ..RoomKitOptions::default()
    });
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit
}

/// A room `r` the store holds, and in it two events `id`, the kit idle: no
/// call asked for the subscription yet.
#[expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
async fn seeded(kit: &RoomKit) -> Vec<roomkit::model::EventId> {
    use roomkit::ConversationStore;
    use roomkit::model::{
        ChannelId, ChannelType, EventSource, Room, RoomEvent, RoomId, TextContent,
    };

    let room = RoomId::new("r").unwrap();
    kit.store()
        .create_room(Room::new(room.clone()))
        .await
        .unwrap();
    let source = EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
    let mut ids = Vec::new();
    for index in 0..2 {
        let event = RoomEvent {
            index,
            ..RoomEvent::new(room.clone(), source.clone(), TextContent::new("hi"))
        };
        ids.push(event.id.clone());
        kit.store().add_event(event).await.unwrap();
    }
    ids
}

// Python: tests/test_status_posted_without_context_manager.py::test_a_message_waiting_for_the_subscription_is_cut_by_the_close
// The call the wait held before its room's lock is cut: nothing of it is
// stored, no handle for a deferred one. The door it passed refuses nothing
// any more (RMK-1255 item 3); its wait ends at the close's start, where
// Python's has the close's grace (item 4, HITL below).
#[tokio::test(start_paused = true)]
async fn a_message_waiting_for_the_subscription_is_cut_by_the_close() {
    use roomkit::ProcessInboundOptions;
    use roomkit::model::{ChannelId, InboundMessage, RoomId, TextContent};

    for (named, defer_delivery) in [(false, false), (true, false), (false, true), (true, true)] {
        let backend = Arc::new(Answered::default());
        let kit = late_kit(&backend);
        let message =
            InboundMessage::new(ChannelId::new("sms").unwrap(), "u1", TextContent::new("hi"));
        let options = ProcessInboundOptions {
            room_id: named.then(|| RoomId::new("r1").unwrap()),
            defer_delivery,
            ..ProcessInboundOptions::default()
        };
        let calling = kit.clone();
        let call = tokio::spawn(async move { calling.process_inbound(message, options).await });
        backend.until_waited_for().await;

        kit.close().await.unwrap();
        let result = call.await.unwrap().unwrap();

        let read = (
            result.blocked,
            result.reason.as_deref(),
            result.cancellation_reason.as_deref(),
            result.event.is_some(),
            result.delivery.is_some(),
        );
        assert_eq!(
            read,
            (true, Some("kit_closed"), Some("kit_closed"), false, false),
            "named {named}, deferred {defer_delivery}"
        );
    }
}

// Python: tests/test_status_posted_without_context_manager.py::test_a_call_waiting_for_the_subscription_goes_on_within_the_close_s_grace
// A call past its door that met its room's lock only after the wait keeps
// what Python's call holds as the close starts: its right to end its work
// within the grace, rather than a cut at the lock.
#[tokio::test(start_paused = true)]
async fn a_call_waiting_for_the_subscription_goes_on_within_the_close_s_grace() {
    use roomkit::model::{ChannelId, EventContent, RoomEvent, RoomId, RoomStatus, TextContent};
    use roomkit::{
        AttachChannelOptions, CloseRoomOptions, ConversationStore, CreateRoomOptions,
        DeleteEventOptions, HookExecution, HookOptions, HookTrigger, PauseRoomOptions,
        UpdateEventOptions,
    };

    for name in [
        "create_room",
        "pause_room",
        "close_room",
        "attach_channel",
        "update_event",
        "delete_event",
    ] {
        let backend = Arc::new(Answered::default());
        let kit = late_kit(&backend);
        let ids = seeded(&kit).await;
        for trigger in [HookTrigger::OnEventUpdated, HookTrigger::OnEventDeleted] {
            let options = HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            };
            kit.hook(trigger, |_event: RoomEvent, _context| async {}, options)
                .unwrap();
        }
        let room = RoomId::new("r").unwrap();
        let (calling, in_room, event) = (kit.clone(), room.clone(), ids[0].clone());
        let call = tokio::spawn(async move {
            match name {
                "create_room" => {
                    let options = CreateRoomOptions {
                        room_id: Some(RoomId::new("new").unwrap()),
                        ..CreateRoomOptions::default()
                    };
                    calling.create_room(options).await.map(drop)
                }
                "pause_room" => calling
                    .pause_room(&in_room, PauseRoomOptions::default())
                    .await
                    .map(drop),
                "close_room" => calling
                    .close_room(&in_room, CloseRoomOptions::default())
                    .await
                    .map(drop),
                "attach_channel" => {
                    let sms = ChannelId::new("sms").unwrap();
                    calling
                        .attach_channel(&in_room, &sms, AttachChannelOptions::default())
                        .await
                        .map(drop)
                }
                "update_event" => {
                    let options = UpdateEventOptions {
                        content: Some(EventContent::Text(TextContent::new("new"))),
                        ..UpdateEventOptions::default()
                    };
                    calling
                        .update_event(&in_room, &event, options)
                        .await
                        .map(drop)
                }
                _ => calling
                    .delete_event(&in_room, &event, DeleteEventOptions::default())
                    .await
                    .map(drop),
            }
        });
        backend.until_waited_for().await;

        let closing = kit.clone();
        let closing = tokio::spawn(async move { closing.close().await });
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        backend.answer();
        call.await
            .unwrap()
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        closing.await.unwrap().unwrap();

        let stored = kit.store();
        let got = stored.get_room(&room).await.unwrap().unwrap();
        let written = match name {
            "create_room" => stored
                .get_room(&RoomId::new("new").unwrap())
                .await
                .unwrap()
                .is_some(),
            "pause_room" => got.status == RoomStatus::Paused,
            "close_room" => got.status == RoomStatus::Closed,
            "attach_channel" => {
                let sms = ChannelId::new("sms").unwrap();
                stored.get_binding(&room, &sms).await.unwrap().is_some()
            }
            "update_event" => {
                let event = stored.get_event(&ids[0]).await.unwrap().unwrap();
                matches!(event.content, EventContent::Text(text) if text.body == "new")
            }
            _ => stored.get_event(&ids[0]).await.unwrap().is_none(),
        };
        assert!(written, "{name}");
    }
}

// Python: tests/test_status_posted_without_context_manager.py::test_a_change_of_an_event_subscribes_only_for_a_hook_that_reads_its_context
// RMK-1255 item 5: event_ops.py builds a change's context only for a hook.
#[tokio::test]
async fn a_change_of_an_event_subscribes_only_for_a_hook_that_reads_its_context() {
    use roomkit::model::{EventContent, RoomEvent, RoomId, TextContent};
    use roomkit::{
        DeleteEventOptions, HookExecution, HookOptions, HookTrigger, UpdateEventOptions,
    };

    let backend = Arc::new(Answered::default());
    let kit = late_kit(&backend);
    let ids = seeded(&kit).await;
    let room = RoomId::new("r").unwrap();
    let changed = |body: &str| UpdateEventOptions {
        content: Some(EventContent::Text(TextContent::new(body))),
        ..UpdateEventOptions::default()
    };
    let deleted = DeleteEventOptions::default;

    // No hook: neither change waits for a subscription nobody answered.
    let bound = std::time::Duration::from_secs(1);
    let update = kit.update_event(&room, &ids[0], changed("changed"));
    tokio::time::timeout(bound, update).await.unwrap().unwrap();
    let delete = kit.delete_event(&room, &ids[1], deleted());
    tokio::time::timeout(bound, delete).await.unwrap().unwrap();

    // A hook on the trigger: each waits for it.
    let options = || HookOptions {
        execution: HookExecution::Async,
        ..HookOptions::default()
    };
    let noted = |_event: RoomEvent, _context| async {};
    kit.hook(HookTrigger::OnEventUpdated, noted, options())
        .unwrap();
    kit.hook(HookTrigger::OnEventDeleted, noted, options())
        .unwrap();
    let waits = std::time::Duration::from_millis(50);
    let update = kit.update_event(&room, &ids[0], changed("again"));
    assert!(tokio::time::timeout(waits, update).await.is_err());
    let delete = kit.delete_event(&room, &ids[0], deleted());
    assert!(tokio::time::timeout(waits, delete).await.is_err());
    backend.answer();
    let update = kit.update_event(&room, &ids[0], changed("again"));
    tokio::time::timeout(bound, update).await.unwrap().unwrap();
    kit.close().await.unwrap();
}

// RMK-1201 (1): the close does not leave a first activity waiting for a
// subscription that never ends: the wait ends at the close's start and the
// call goes on under the close's rules, as Python's close cuts the call
// (Python waits its grace first, where Rust does not).
#[tokio::test]
async fn the_close_ends_an_activity_waiting_for_its_subscription() {
    let held: Arc<dyn StatusBackend> = Arc::new(FirstTryHangs::default());
    let bus = StatusBus::new(StatusBusOptions {
        backend: Some(held),
        ..StatusBusOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(bus),
        ..RoomKitOptions::default()
    });
    let waiting = kit.clone();
    let activity = tokio::spawn(async move {
        waiting
            .create_room(roomkit::CreateRoomOptions::default())
            .await
    });
    tokio::task::yield_now().await;

    kit.close().await.unwrap();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(2), activity).await;

    // Admitted before the close, the call ends under its rules: it creates
    // its room, as Python's does when its subscription ends within the grace.
    let created = ended.map(|joined| joined.is_ok_and(|created| created.is_ok()));
    assert_eq!(created, Ok(true));
}
