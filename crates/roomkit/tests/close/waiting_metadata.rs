//! RFC §12.10.4: a metadata call waiting for the room lock is cut at once.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pretty_assertions::assert_eq;
use roomkit::model::{RoomId, RoomStatus};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, GetRoomOptions, InMemoryLockManager, RoomKit,
    RoomKitError, RoomKitOptions, RoomLock, RoomLockManager, UpdateRoomMetadataOptions,
};
use serde_json::json;
use tokio::sync::Notify;

use crate::at_once::locking_hook;
use crate::held::{AT_ONCE, Holding, closed, room, spawn_close, spawn_said, stored};
use crate::support::channel;

/// Signal an actual Pending poll of the real manager, not a timer after spawn.
#[derive(Default)]
struct WaitingLocks {
    inner: InMemoryLockManager,
    armed: AtomicBool,
    waiting: Notify,
}

type Locked<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>>;

impl RoomLockManager for WaitingLocks {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> Locked<'a> {
        let mut held = self.inner.locked(room_id);
        Box::pin(poll_fn(move |cx| {
            let result = held.as_mut().poll(cx);
            if result.is_pending()
                && room_id.as_str() == "r"
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.waiting.notify_one();
            }
            result
        }))
    }
}

async fn kit_with(locks: Arc<WaitingLocks>, target: &Holding) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        lock_manager: Some(locks),
        ..Default::default()
    });
    kit.register_channel(Holding::new("sms", [])).unwrap();
    kit.register_channel(target.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room("r")),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws"] {
        kit.attach_channel(&room("r"), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

// Python: tests/test_kit_close.py::test_a_call_waiting_for_its_room_lock_is_cut_at_once[update_room_metadata]
#[tokio::test(start_paused = true)]
async fn metadata_waiting_for_the_lock_is_cut_before_its_locker_ends() {
    let locks = Arc::new(WaitingLocks::default());
    let target = Holding::new("ws", []);
    let kit = kit_with(Arc::clone(&locks), &target).await;
    let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    locking_hook(&kit, &entered, &release, true);
    let locker = spawn_said(&kit, "locker", "r");
    entered.notified().await;
    locks.armed.store(true, Ordering::SeqCst);
    let waiter = tokio::spawn({
        let kit = kit.clone();
        async move {
            kit.update_room_metadata(
                &room("r"),
                [("waiter".to_owned(), json!(true))].into_iter().collect(),
                UpdateRoomMetadataOptions::default(),
            )
            .await
        }
    });
    locks.waiting.notified().await;
    assert!(!waiter.is_finished());
    let close = spawn_close(&kit);
    let result = tokio::time::timeout(AT_ONCE, waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(RoomKitError::KitClosed)));
    assert!(!locker.is_finished());
    release.notify_one();
    closed(close).await;
    let original = locker.await.unwrap().unwrap();
    assert!(original.event.is_some());
    assert_eq!(original.cancellation_reason.as_deref(), Some("kit_closed"));
    let kept = kit
        .get_room(&room("r"), GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(kept.status, RoomStatus::Active);
    assert!(!kept.metadata.contains_key("waiter"));
    assert_eq!(stored(&kit, "r").await, ["locker"]);
    assert_eq!(target.sent(), Vec::<String>::new());
}
