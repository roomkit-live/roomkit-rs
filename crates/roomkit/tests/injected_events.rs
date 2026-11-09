//! Hook-injection boundaries that the shared scenario format cannot express.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "support/warnings.rs"]
mod warnings;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelId, ChannelType, EventSource, HookResult, InboundMessage, InjectedEvent, RoomEvent,
    RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DeliverOptions, HookExecution, HookOptions,
    HookTrigger, InMemoryLockManager, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
    RoomLock, RoomLockManager,
};
use tokio::sync::{Notify, watch};

const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}
fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn notice() -> InjectedEvent {
    InjectedEvent {
        target_channel_ids: Some(vec![channel("ws")]),
        ..InjectedEvent::new(RoomEvent::new(
            room(),
            EventSource::new(channel("system"), ChannelType::SYSTEM),
            TextContent::new("notice"),
        ))
    }
}

async fn opened(options: RoomKitOptions) -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new(options);
    let ws = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, ws)
}

// Python: tests/test_injected_events.py::TestAnotherTrigger::test_drops_them_with_a_warning
#[tokio::test]
async fn another_trigger_drops_injections_with_a_warning() {
    let (kit, ws) = opened(RoomKitOptions::default()).await;
    kit.hook(
        HookTrigger::BeforeDeliver,
        |_: RoomEvent, _| async {
            HookResult::allow(AllowOptions {
                injected: Some(vec![notice()]),
            })
        },
        HookOptions {
            name: "inject_on_deliver".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let (warnings, _capturing) = warnings::Warnings::capture();
    kit.deliver(
        &room(),
        "ready",
        DeliverOptions {
            channel_id: Some(channel("sms")),
            ..DeliverOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(ws.texts(), ["ready"]);
    assert!(
        warnings
            .containing("inject_on_deliver")
            .iter()
            .any(|line| line.starts_with("WARN")
                && line.contains("before_deliver")
                && line.contains("dropped"))
    );
    kit.close().await.unwrap();
}

/// Count requests before awaiting the lock, so the test establishes the
/// cause, external holder and queued injection order without a sleep.
struct WatchedLocks {
    inner: InMemoryLockManager,
    requests: watch::Sender<usize>,
}

type LockAnswer<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>>;

impl RoomLockManager for WatchedLocks {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> LockAnswer<'a> {
        Box::pin(async move {
            self.requests.send_modify(|requests| *requests += 1);
            self.inner.locked(room_id).await
        })
    }
}

impl WatchedLocks {
    async fn reached(&self, count: usize) {
        tokio::time::timeout(
            BOUND,
            self.requests
                .subscribe()
                .wait_for(|requests| *requests >= count),
        )
        .await
        .unwrap()
        .unwrap();
    }
}

fn notifier(kit: &RoomKit, entered: Arc<Notify>, release: Arc<Notify>, noticed: Arc<Notify>) {
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            async move {
                if event.content != TextContent::new("hello").into() {
                    return HookResult::allow(AllowOptions::default());
                }
                entered.notify_one();
                release.notified().await;
                HookResult::block(
                    "held",
                    BlockOptions {
                        injected: Some(vec![notice()]),
                        ..BlockOptions::default()
                    },
                )
            }
        },
        HookOptions {
            name: "notify".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let noticed = Arc::clone(&noticed);
            async move {
                if event.content == TextContent::new("notice").into() {
                    noticed.notify_one();
                }
            }
        },
        HookOptions {
            name: "seen".to_owned(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
}

async fn caller_cancelled_waiting_for_the_lock() {
    let (requests, _) = watch::channel(0);
    let locks = Arc::new(WatchedLocks {
        inner: InMemoryLockManager::default(),
        requests,
    });
    let (kit, ws) = opened(RoomKitOptions {
        lock_manager: Some(locks.clone()),
        ..RoomKitOptions::default()
    })
    .await;
    locks.requests.send_replace(0);
    let (entered, release, noticed) = (
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    );
    notifier(
        &kit,
        Arc::clone(&entered),
        Arc::clone(&release),
        Arc::clone(&noticed),
    );
    let cause = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(
                InboundMessage::new(channel("sms"), "u", TextContent::new("hello")),
                ProcessInboundOptions {
                    room_id: Some(room()),
                    ..ProcessInboundOptions::default()
                },
            )
            .await
        })
    };
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    let holder = {
        let locks = Arc::clone(&locks);
        tokio::spawn(async move { locks.locked(&room()).await.unwrap() })
    };
    locks.reached(2).await;
    release.notify_one();
    let held = tokio::time::timeout(BOUND, holder).await.unwrap().unwrap();
    locks.reached(3).await;
    assert!(
        !cause.is_finished(),
        "the caller must still await its injection"
    );
    cause.abort();
    assert!(cause.await.unwrap_err().is_cancelled());
    held.release().await.unwrap();
    tokio::time::timeout(BOUND, noticed.notified())
        .await
        .unwrap();
    assert_eq!(ws.texts(), ["notice"]);
    kit.close().await.unwrap();
}

// Python: tests/test_injected_events.py::TestItsPass::test_is_committed_when_its_caller_is_cancelled_waiting_for_the_lock
#[tokio::test]
async fn is_committed_when_its_caller_is_cancelled_waiting_for_the_lock() {
    caller_cancelled_waiting_for_the_lock().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn is_committed_when_its_caller_is_cancelled_waiting_for_the_lock_on_four_workers() {
    caller_cancelled_waiting_for_the_lock().await;
}
