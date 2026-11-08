//! A conformance idle waits for the status publications it reads afterwards
//! (RMK-1548). `StatusBus::post` and the kit's close stay non-blocking with
//! respect to the bus's own publication, as Python's `status_bus.py`.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use roomkit::orchestration::backend::{RecentOptions as BackendRecentOptions, StatusBackend};
use roomkit::orchestration::{
    InMemoryStatusBackend, PostOptions, RecentOptions, StatusBus, StatusBusOptions, StatusEntry,
    StatusLevel,
};
use roomkit::{RoomKit, RoomKitOptions};
use tokio::sync::Notify;

type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type Callback = Arc<dyn Fn(StatusEntry) -> Pending<'static, ()> + Send + Sync>;

/// Each publication waits for one explicit release, including posts made
/// by a subscriber. No timer decides when the backend finishes.
#[derive(Default)]
struct Held {
    kept: InMemoryStatusBackend,
    entered: Notify,
    release: Notify,
    closed: AtomicBool,
}

impl StatusBackend for Held {
    fn publish(&self, entry: StatusEntry) -> Pending<'_, ()> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            self.kept.publish(entry).await;
        })
    }

    fn recent(&self, n: usize, options: BackendRecentOptions) -> Pending<'_, Vec<StatusEntry>> {
        self.kept.recent(n, options)
    }

    fn subscribe(&self, callback: Callback) -> Pending<'_, ()> {
        self.kept.subscribe(callback)
    }

    fn unsubscribe(&self, callback: Callback) -> Pending<'_, ()> {
        self.kept.unsubscribe(callback)
    }

    fn close(&self) -> Pending<'_, ()> {
        self.closed.store(true, Ordering::SeqCst);
        Box::pin(async {})
    }
}

#[expect(
    clippy::unwrap_used,
    reason = "a test fails if its fixture cannot finish"
)]
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap()
}

#[expect(
    clippy::unwrap_used,
    reason = "a test fails if its fixture cannot start"
)]
async fn kit() -> (RoomKit, Arc<Held>) {
    let held = Arc::new(Held::default());
    let backend = Arc::clone(&held);
    let kit = RoomKit::new(RoomKitOptions {
        status_bus: Some(StatusBus::new(StatusBusOptions {
            backend: Some(backend),
            ..StatusBusOptions::default()
        })),
        ..RoomKitOptions::default()
    });
    kit.start().await.unwrap();
    bounded(roomkit::testing::idle(&kit)).await;
    (kit, held)
}

async fn still_pending(mut future: Pin<&mut impl Future<Output = ()>>) {
    let polled = poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
    assert!(
        polled.is_pending(),
        "idle returned while the publication was held"
    );
}

#[expect(
    clippy::unwrap_used,
    reason = "a test fails if its fixture cannot close"
)]
async fn waits_for_a_post() {
    let (kit, held) = kit().await;
    let entry = kit.status_bus().post(
        "worker",
        "done",
        StatusLevel::Completed,
        PostOptions::default(),
    );
    bounded(held.entered.notified()).await;
    let idle = roomkit::testing::idle(&kit);
    tokio::pin!(idle);
    still_pending(idle.as_mut()).await;
    held.release.notify_one();
    bounded(idle).await;
    assert_eq!(
        kit.status_bus().recent(RecentOptions::default()).await,
        [entry]
    );
    kit.close().await.unwrap();
}

// The shared background-worker scenarios read status.recent after idle.
#[tokio::test]
async fn idle_waits_for_a_status_post_on_one_thread() {
    waits_for_a_post().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_waits_for_a_status_post_on_four_workers() {
    waits_for_a_post().await;
}

#[tokio::test]
async fn idle_waits_for_a_post_made_by_a_subscriber() {
    let (kit, held) = kit().await;
    let posting = kit.status_bus().clone();
    let callback: Callback = Arc::new(move |entry| {
        if entry.action == "first" {
            posting.post(
                "worker",
                "second",
                StatusLevel::Completed,
                PostOptions::default(),
            );
        }
        Box::pin(async {})
    });
    kit.status_bus().subscribe(Arc::clone(&callback)).await;
    kit.status_bus()
        .post("worker", "first", StatusLevel::Info, PostOptions::default());
    bounded(held.entered.notified()).await;
    let idle = roomkit::testing::idle(&kit);
    tokio::pin!(idle);
    still_pending(idle.as_mut()).await;
    held.release.notify_one();
    bounded(held.entered.notified()).await;
    still_pending(idle.as_mut()).await;
    held.release.notify_one();
    bounded(idle).await;
    let entries = kit.status_bus().recent(RecentOptions::default()).await;
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    kit.status_bus().unsubscribe(callback).await;
    kit.close().await.unwrap();
}

#[tokio::test]
async fn idle_waits_for_the_work_and_posts_a_status_hook_starts() {
    use roomkit::{CreateRoomOptions, HookExecution, HookOptions, HookTrigger};

    let (kit, held) = kit().await;
    let room = kit.create_room(CreateRoomOptions::default()).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (entering, releasing) = (Arc::clone(&entered), Arc::clone(&release));
    let posting = kit.status_bus().clone();
    kit.hook(
        HookTrigger::OnStatusPosted,
        move |entry: StatusEntry, _| {
            let (entered, release) = (Arc::clone(&entering), Arc::clone(&releasing));
            let posting = posting.clone();
            async move {
                if entry.action == "first" {
                    posting.post(
                        "worker",
                        "done",
                        StatusLevel::Completed,
                        PostOptions {
                            metadata: Some(entry.metadata),
                            ..PostOptions::default()
                        },
                    );
                    return;
                }
                entered.notify_one();
                release.notified().await;
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    let mut metadata = roomkit::model::Metadata::new();
    metadata.insert("room_id".to_owned(), room.id.to_string().into());
    kit.status_bus().post(
        "worker",
        "first",
        StatusLevel::Completed,
        PostOptions {
            metadata: Some(metadata),
            ..PostOptions::default()
        },
    );
    bounded(held.entered.notified()).await;
    let idle = roomkit::testing::idle(&kit);
    tokio::pin!(idle);
    still_pending(idle.as_mut()).await;
    held.release.notify_one();
    bounded(held.entered.notified()).await;
    still_pending(idle.as_mut()).await;
    held.release.notify_one();
    bounded(entered.notified()).await;
    still_pending(idle.as_mut()).await;
    release.notify_one();
    bounded(idle).await;
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_bus_publication_does_not_become_work_the_kit_s_close_waits_for() {
    let (kit, held) = kit().await;
    let entry = kit.status_bus().post(
        "worker",
        "done",
        StatusLevel::Completed,
        PostOptions::default(),
    );
    bounded(held.entered.notified()).await;
    bounded(kit.close()).await.unwrap();
    assert!(held.closed.load(Ordering::SeqCst));
    assert_eq!(kit.status_bus().recent(RecentOptions::default()).await, []);
    held.release.notify_one();
    bounded(roomkit::testing::idle(&kit)).await;
    assert_eq!(
        kit.status_bus().recent(RecentOptions::default()).await,
        [entry]
    );
}
