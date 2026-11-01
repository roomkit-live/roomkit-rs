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
