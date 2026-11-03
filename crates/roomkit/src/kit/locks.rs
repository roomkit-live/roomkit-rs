//! The kit's lock manager (RFC §13.5): Python's `kit.lock_manager`, and its
//! close with the kit's.

use std::sync::Arc;

use super::{Inner, RoomKit};
use crate::lock_manager::RoomLockManager;

impl<S> RoomKit<S> {
    /// Python `lock_manager`: the lock manager the kit takes its room locks
    /// from, the one [`RoomKitOptions::lock_manager`](crate::RoomKitOptions::lock_manager)
    /// gave, else the kit's own `InMemoryLockManager`.
    #[must_use]
    pub fn lock_manager(&self) -> Arc<dyn RoomLockManager> {
        Arc::clone(self.inner.locks.manager())
    }
}

impl<S> Inner<S> {
    /// Closes the lock manager, and the delivery claim manager the host
    /// handed when it is another, as Python's close does after the store's:
    /// a failure is logged, the close goes on.
    pub(super) async fn close_lock_managers(&self) {
        let managers = std::iter::once(self.locks.manager()).chain(self.claim_manager.as_ref());
        for manager in managers {
            if let Err(error) = manager.close().await {
                tracing::warn!(target: "roomkit::locks", %error, "closing a lock manager failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use roomkit_core::model::{
        ChannelBinding, ChannelId, ChannelType, IdentityId, Room, RoomId, TextContent,
    };

    use crate::lock_manager::{InMemoryLockManager, RoomLock, RoomLockManager};
    use crate::store::ConversationStore;
    use crate::testing::{RecordingTransport, RecordingTransportOptions};
    use crate::{
        AttachChannelOptions, CreateRoomOptions, RequestVerificationOptions, RoomKit, RoomKitError,
        RoomKitOptions, SendEventOptions,
    };

    // Python: tests/test_lock_release.py::TestTheKitsHold::test_the_kit_s_lock_manager_is_the_one_given
    #[tokio::test]
    async fn the_kit_s_lock_manager_is_the_one_given() {
        let given: Arc<dyn RoomLockManager> = Arc::new(InMemoryLockManager::default());
        let kit = RoomKit::new(RoomKitOptions {
            lock_manager: Some(Arc::clone(&given)),
            ..RoomKitOptions::default()
        });
        assert!(Arc::ptr_eq(&kit.lock_manager(), &given));
        kit.close().await.unwrap();
    }

    /// A manager recording the key of each lock released through it.
    #[derive(Default)]
    struct Recording {
        inner: InMemoryLockManager,
        asked: Arc<Mutex<Vec<String>>>,
        released: Arc<Mutex<Vec<String>>>,
    }

    struct Recorded {
        lock: Box<dyn RoomLock>,
        key: String,
        released: Arc<Mutex<Vec<String>>>,
    }

    type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, RoomKitError>> + Send + 'a>>;

    impl RoomLockManager for Recording {
        fn locked<'a>(&'a self, room_id: &'a RoomId) -> Answer<'a, Box<dyn RoomLock>> {
            Box::pin(async move {
                self.asked.lock().unwrap().push(room_id.to_string());
                let lock: Box<dyn RoomLock> = Box::new(Recorded {
                    lock: self.inner.locked(room_id).await?,
                    key: room_id.to_string(),
                    released: Arc::clone(&self.released),
                });
                Ok(lock)
            })
        }
    }

    impl RoomLock for Recorded {
        fn release(self: Box<Self>) -> Answer<'static, ()> {
            self.released.lock().unwrap().push(self.key.clone());
            self.lock.release()
        }
    }

    // Python: tests/test_lock_release.py (a release is the manager's on every
    // path): a lock the kit lets go of unreleased, an identity's, is
    // released through its manager all the same.
    #[tokio::test]
    async fn a_lock_the_kit_lets_go_of_is_released_through_its_manager() {
        let locks = Arc::new(Recording::default());
        let released = Arc::clone(&locks.released);
        let kit = RoomKit::new(RoomKitOptions {
            lock_manager: Some(locks),
            ..RoomKitOptions::default()
        });
        let room = RoomId::new("r1").unwrap();
        let create = CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        let alice = IdentityId::new("alice").unwrap();
        let sms = ChannelId::new("sms").unwrap();
        let options = RequestVerificationOptions::default();
        kit.request_verification(&alice, &room, &sms, options)
            .await
            .unwrap();
        let keys = released.lock().unwrap().clone();
        assert!(
            keys.contains(&"\0verification::alice".to_owned()),
            "{keys:?}"
        );
        kit.close().await.unwrap();
    }

    // Python: tests/test_locks.py::test_a_room_id_with_a_nul_character_is_refused
    #[tokio::test]
    async fn a_room_id_with_a_nul_character_is_refused() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let create = CreateRoomOptions {
            room_id: Some(RoomId::new("\0__delivery__:r1").unwrap()),
            ..CreateRoomOptions::default()
        };
        let refused = kit.create_room(create).await.unwrap_err();
        assert!(matches!(refused, RoomKitError::Value(_)), "{refused:?}");
        kit.close().await.unwrap();
    }

    /// A room no `create_room` made, written to the store straight, whose id
    /// holds a NUL character: in a shared manager its lock would be another
    /// lock's key, `\0__delivery__:r1` r1's delivery claim (security review
    /// of RMK-1281, HITL 5c17ea1c). Its lock raises `ValueError`, which the
    /// kit reports as the lock's failure, as Python raises it from the lock.
    // Python: tests/test_lanes_multiworker.py::test_a_stored_room_whose_id_holds_nul_takes_no_room_lock
    #[tokio::test]
    async fn a_stored_room_whose_id_holds_nul_takes_no_room_lock() {
        let locks = Arc::new(Recording::default());
        let asked = Arc::clone(&locks.asked);
        let kit = RoomKit::new(RoomKitOptions {
            lock_manager: Some(locks),
            ..RoomKitOptions::default()
        });
        let src = ChannelId::new("src").unwrap();
        kit.register_channel(RecordingTransport::new(
            src.clone(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        let nul_room = RoomId::new("\0__delivery__:r1").unwrap();
        kit.store()
            .create_room(Room::new(nul_room.clone()))
            .await
            .unwrap();
        let refused = kit
            .attach_channel(&nul_room, &src, AttachChannelOptions::default())
            .await
            .unwrap_err();
        assert_eq!(refused.class_name(), "ValueError", "{refused:?}");
        let binding = ChannelBinding::new(src.clone(), nul_room.clone(), ChannelType::SMS);
        kit.store().add_binding(binding).await.unwrap();
        let refused = kit
            .send_event(
                &nul_room,
                &src,
                TextContent::new("hi"),
                SendEventOptions::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.class_name(), "ValueError", "{refused:?}");
        let asked = asked.lock().unwrap().clone();
        assert!(!asked.contains(&nul_room.to_string()), "{asked:?}");
        kit.close().await.unwrap();
    }
}
