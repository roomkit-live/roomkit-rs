//! A room lock manager written outside the engine wraps its acquisition in
//! [`roomkit::track_host_room_lock`] (RFC §13.5): a host holding its guard
//! calls the kit for that room and enters under it, as it does with the
//! built-in manager. Without the wrapper, the kit's call waits for the host.

use std::collections::HashMap;

use roomkit::{RoomLock, RoomLockManager};
use tokio::sync::{Mutex as RoomMutex, OwnedMutexGuard};

use super::*;
use pretty_assertions::assert_eq;

/// One tokio mutex per room, its acquisition wrapped in
/// `track_host_room_lock` when `tracked`.
#[derive(Default)]
struct OwnLocks {
    rooms: Mutex<HashMap<RoomId, Arc<RoomMutex<()>>>>,
    tracked: bool,
}

struct OwnLock(#[expect(dead_code, reason = "held for its drop")] OwnedMutexGuard<()>);

impl RoomLock for OwnLock {
    fn release(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + 'static>> {
        Box::pin(ready(Ok(())))
    }
}

impl RoomLockManager for OwnLocks {
    fn locked<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>> {
        let room = Arc::clone(
            self.rooms
                .lock()
                .unwrap()
                .entry(room_id.clone())
                .or_default(),
        );
        let acquire = async move {
            let lock: Box<dyn RoomLock> = Box::new(OwnLock(room.lock_owned().await));
            Ok(lock)
        };
        if self.tracked {
            Box::pin(roomkit::track_host_room_lock(self, room_id, acquire))
        } else {
            Box::pin(acquire)
        }
    }
}

pub(super) async fn a_host_holding_its_guard_enters_once_tracked(tracked: bool) {
    let locks = Arc::new(OwnLocks {
        tracked,
        ..OwnLocks::default()
    });
    let manager: Arc<dyn RoomLockManager> = Arc::<OwnLocks>::clone(&locks);
    let (kit, _) = room_with(RoomKitOptions {
        lock_manager: Some(manager),
        ..Default::default()
    })
    .await;
    let held = locks.locked(&room_id()).await.unwrap();

    let entered = tokio::time::timeout(
        Duration::from_millis(50),
        kit.process_inbound(message("hello"), in_room(room_id())),
    )
    .await;

    assert_eq!(entered.is_ok(), tracked, "tracked={tracked}");
    held.release().await.unwrap();
    let stored: &[&str] = if tracked { &["hello"] } else { &[] };
    assert_eq!(message_texts(&kit).await, stored);
    kit.close().await.unwrap();
}
