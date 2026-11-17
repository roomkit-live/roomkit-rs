//! Python's public detach/publication ordering (RFC §10.6.7).

#![expect(clippy::unwrap_used, reason = "a broken fixture fails its test")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
#[path = "local_presence_order/store.rs"]
mod store;
#[path = "local_presence_order/support.rs"]
mod support;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DetachChannelOptions, RoomKit, RoomKitOptions,
};
use support::Agent;
use tokio::sync::Notify;

#[derive(Default)]
struct HeldPresence {
    started: Notify,
    release: Notify,
    calls: std::sync::atomic::AtomicUsize,
    published: Mutex<Vec<(EphemeralEventType, String)>>,
    inner: InMemoryRealtime,
}

impl RealtimeBackend for HeldPresence {
    fn publish<'a>(&'a self, channel: &'a str, event: EphemeralEvent) -> RealtimeFuture<'a, ()> {
        Box::pin(async move {
            if event.r#type == EphemeralEventType::PresenceOnline {
                if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                self.published
                    .lock()
                    .unwrap()
                    .push((event.r#type, event.user_id.clone()));
            }
            self.inner.publish(channel, event).await;
        })
    }

    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.inner.subscribe(channel, callback)
    }

    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        self.inner.unsubscribe(subscription_id)
    }

    fn close(&self) -> RealtimeFuture<'_, ()> {
        self.inner.close()
    }
}

async fn detach_waits_then_reattach_publishes(refuses: bool) {
    let backend = Arc::new(HeldPresence::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<HeldPresence>::clone(&backend)),
        ..Default::default()
    });
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let channel: roomkit::model::ChannelId = "helper".parse().unwrap();
    let called = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    kit.register_channel(Agent {
        refuses,
        called: Arc::clone(&called),
    })
    .unwrap();
    let removed = Arc::new(Notify::new());
    let announced = Arc::clone(&removed);
    kit.on("room_channel_detached", move |_| {
        let announced = Arc::clone(&announced);
        async move {
            announced.notify_one();
        }
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (owner, held_room, held_channel) = (kit.clone(), room.clone(), channel.clone());
    let attaching = tokio::spawn(async move {
        owner
            .attach_channel(&held_room, &held_channel, AttachChannelOptions::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
        .await
        .unwrap();
    let (owner, held_room, held_channel) = (kit.clone(), room.clone(), channel.clone());
    let mut detaching = tokio::spawn(async move {
        owner
            .detach_channel(&held_room, &held_channel, DetachChannelOptions::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), removed.notified())
        .await
        .unwrap();
    let early = tokio::time::timeout(Duration::from_millis(50), &mut detaching)
        .await
        .ok();
    let returned_before_publication = early.is_some();
    backend.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), attaching)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let detached = match early {
        Some(result) => result,
        None => tokio::time::timeout(Duration::from_secs(2), detaching)
            .await
            .unwrap(),
    }
    .unwrap()
    .unwrap();
    kit.attach_channel(&room, &channel, AttachChannelOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert!(detached);
    assert_eq!(called.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        !returned_before_publication,
        "detach returned before the earlier presence publication"
    );
    assert_eq!(
        *backend.published.lock().unwrap(),
        [
            (EphemeralEventType::PresenceOnline, "helper".into()),
            (EphemeralEventType::PresenceOnline, "helper".into()),
        ]
    );
}

#[tokio::test]
async fn detach_waits_for_an_earlier_presence_then_reattach_publishes_again() {
    for refuses in [false, true] {
        detach_waits_then_reattach_publishes(refuses).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn several_workers_detach_waits_for_an_earlier_presence_then_reattach_publishes_again() {
    for refuses in [false, true] {
        detach_waits_then_reattach_publishes(refuses).await;
    }
}

// Python: test_a_failed_detach_state_cleanup_still_forgets_the_presence_claim.
#[tokio::test]
async fn a_failed_detach_state_cleanup_still_forgets_the_presence_claim() {
    use roomkit::ConversationStore;
    use roomkit::store::PatchRoomMetadataOptions;
    use serde_json::json;
    let backend = Arc::new(HeldPresence::default());
    backend.calls.store(1, std::sync::atomic::Ordering::SeqCst);
    let mut options = RoomKitOptions::with_store(store::FailedCleanupStore::default());
    let store = Arc::clone(&options.store);
    options.realtime = Some(Arc::<HeldPresence>::clone(&backend));
    let kit = RoomKit::new(options);
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let channel: roomkit::model::ChannelId = "helper".parse().unwrap();
    kit.register_channel(Agent {
        refuses: false,
        called: Arc::default(),
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &channel, AttachChannelOptions::default())
        .await
        .unwrap();
    store
        .patch_room_metadata(
            &room,
            json!({"roomkit:channel_state:helper": {"model": "kept"}})
                .as_object()
                .unwrap()
                .clone(),
            PatchRoomMetadataOptions::default(),
        )
        .await
        .unwrap();
    store.fault.store(true, std::sync::atomic::Ordering::SeqCst);
    let error = kit
        .detach_channel(&room, &channel, DetachChannelOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(error, roomkit::RoomKitError::Channel { channel_id, error }
        if channel_id == channel && error.error_type == "StoreError" && error.message == "the store failed")
    );
    assert!(store.get_binding(&room, &channel).await.unwrap().is_none());
    kit.attach_channel(&room, &channel, AttachChannelOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        *backend.published.lock().unwrap(),
        [
            (EphemeralEventType::PresenceOnline, "helper".into()),
            (EphemeralEventType::PresenceOnline, "helper".into()),
        ]
    );
}

// Python: test_a_detach_that_removes_nothing_preserves_the_presence_claim.
#[tokio::test]
async fn a_detach_that_removes_nothing_preserves_the_presence_claim() {
    use roomkit::ConversationStore;
    for missing_binding in [false, true] {
        let backend = Arc::new(HeldPresence::default());
        backend.calls.store(1, std::sync::atomic::Ordering::SeqCst);
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<HeldPresence>::clone(&backend)),
            ..Default::default()
        });
        let room: roomkit::model::RoomId = "r1".parse().unwrap();
        let channel: roomkit::model::ChannelId = "helper".parse().unwrap();
        kit.register_channel(Agent {
            refuses: false,
            called: Arc::default(),
        })
        .unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await
            .unwrap();
        if missing_binding {
            assert!(kit.store().remove_binding(&room, &channel).await.unwrap());
            assert!(
                !kit.detach_channel(&room, &channel, DetachChannelOptions::default())
                    .await
                    .unwrap()
            );
        } else {
            let error = kit
                .detach_channel(
                    &room,
                    &channel,
                    DetachChannelOptions {
                        organization_id: Some("other".into()),
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(error, roomkit::RoomKitError::RoomNotFound(id) if id == room));
        }
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await
            .unwrap();
        kit.close().await.unwrap();
        assert_eq!(
            *backend.published.lock().unwrap(),
            [(EphemeralEventType::PresenceOnline, "helper".into())]
        );
    }
}
