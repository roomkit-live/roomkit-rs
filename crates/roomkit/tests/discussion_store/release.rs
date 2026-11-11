//! Failed selection and queue-edit releases keep their first error (§13.5).

use super::*;
use roomkit::{InMemoryLockManager, RoomKitError, RoomLock};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, RoomKitError>> + Send + 'a>>;

#[derive(Default)]
struct Releases {
    inner: InMemoryLockManager,
    script: Arc<Mutex<VecDeque<Option<&'static str>>>>,
}

impl RoomLockManager for Releases {
    fn locked<'a>(&'a self, room: &'a RoomId) -> Answer<'a, Box<dyn RoomLock>> {
        Box::pin(async move {
            let lock: Box<dyn RoomLock> = Box::new(Release {
                lock: self.inner.locked(room).await?,
                script: Arc::clone(&self.script),
            });
            Ok(lock)
        })
    }
}

struct Release {
    lock: Box<dyn RoomLock>,
    script: Arc<Mutex<VecDeque<Option<&'static str>>>>,
}

impl RoomLock for Release {
    fn release(self: Box<Self>) -> Answer<'static, ()> {
        Box::pin(async move {
            self.lock.release().await?;
            match self.script.lock().unwrap().pop_front().flatten() {
                Some(message) => Err(RoomKitError::Lock(ErrorRecord::new(
                    "LockReleaseFailedError",
                    message,
                ))),
                None => Ok(()),
            }
        })
    }
}

#[tokio::test]
async fn both_regeneration_locks_are_released_and_the_first_failure_is_kept() {
    for (selection, expected) in [
        (None, "queue release"),
        (Some("selection release"), "selection release"),
    ] {
        let locks = Arc::new(Releases::default());
        let manager: Arc<dyn RoomLockManager> = Arc::<Releases>::clone(&locks);
        let (kit, store, _) = followed_with(manager).await;
        *locks.script.lock().unwrap() = VecDeque::from([selection, Some("queue release")]);
        let result = kit
            .regenerate_response(
                &RoomId::new("r1").unwrap(),
                RegenerateResponseOptions::default(),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(result.event.is_some());
        assert!(!result.blocked);
        assert_eq!(result.response_events, Vec::<RoomEvent>::new());
        assert_eq!(result.error.unwrap().message, expected);
        assert_eq!(*locks.script.lock().unwrap(), VecDeque::new());
        let room = store
            .get_room(&RoomId::new("r1").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            room.metadata.get("_speak_queue").unwrap()["entries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        kit.close().await.unwrap();
    }
}
