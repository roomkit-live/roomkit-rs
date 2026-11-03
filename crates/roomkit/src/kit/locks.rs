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

    use roomkit_core::model::RoomId;

    use crate::lock_manager::{InMemoryLockManager, RoomLockManager};
    use crate::{CreateRoomOptions, RoomKit, RoomKitError, RoomKitOptions};

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
}
