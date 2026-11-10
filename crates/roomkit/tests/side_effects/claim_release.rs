//! RFC §13.5: a claim's failed release changes neither a completed set
//! nor a cut set. The claim manager is separate from the room lock manager.

use std::pin::Pin;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::ErrorRecord;
use roomkit::{GetRoomOptions, InMemoryLockManager, RoomKitError, RoomLock, RoomLockManager};

use super::*;

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, RoomKitError>> + Send + 'a>>;

struct FailingClaims {
    inner: InMemoryLockManager,
    armed: Arc<AtomicBool>,
    failures: Arc<AtomicUsize>,
}

struct Claim {
    inner: Box<dyn RoomLock>,
    armed: Arc<AtomicBool>,
    failures: Arc<AtomicUsize>,
}

impl RoomLockManager for FailingClaims {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> Answer<'a, Box<dyn RoomLock>> {
        Box::pin(async move {
            let held: Box<dyn RoomLock> = Box::new(Claim {
                inner: self.inner.locked(room_id).await?,
                armed: Arc::clone(&self.armed),
                failures: Arc::clone(&self.failures),
            });
            Ok(held)
        })
    }
}

impl RoomLock for Claim {
    fn release(self: Box<Self>) -> Answer<'static, ()> {
        Box::pin(async move {
            self.inner.release().await?;
            if self.armed.load(Ordering::SeqCst) {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return Err(RoomKitError::Lock(ErrorRecord::new(
                    "LockReleaseFailedError",
                    "the delivery claim's connection was lost",
                )));
            }
            Ok(())
        })
    }
}

fn kit() -> (RoomKit, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let armed = Arc::new(AtomicBool::new(false));
    let failures = Arc::new(AtomicUsize::new(0));
    let kit = RoomKit::new(RoomKitOptions {
        delivery_claim_lock_manager: Some(Arc::new(FailingClaims {
            inner: InMemoryLockManager::default(),
            armed: Arc::clone(&armed),
            failures: Arc::clone(&failures),
        })),
        ..RoomKitOptions::default()
    });
    (kit, armed, failures)
}

// Python: tests/test_lanes.py::test_a_round_whose_claim_release_fails_finishes_the_sets_it_ran
#[tokio::test]
async fn a_failed_claim_release_keeps_a_completed_sets_aftermath() {
    let (kit, armed, failures) = kit();
    let target = RecordingTransport::new(
        channel("ws1"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    sms_and(&kit, target.clone()).await;
    gives_task(&kit, "hello", "t1");
    let created = notes_created(&kit);
    let after = aftermath(&kit);
    armed.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(5), hello_from(&kit))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    roomkit::testing::idle(&kit).await;
    let room = kit
        .get_room(&room_id(), GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(result.cancellation_reason, None);
    assert_eq!(target.texts(), ["hello"]);
    assert_eq!(*created.lock().unwrap(), ["t1"]);
    assert_eq!(*after.lock().unwrap(), ["hello"]);
    assert_eq!(
        room.delivered_index,
        i64::try_from(result.event.unwrap().index).unwrap()
    );
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    assert!(
        kit.store()
            .get_task(&TaskId::new("t1").unwrap())
            .await
            .unwrap()
            .is_some()
    );
    kit.close().await.unwrap();
}

// Python: tests/test_lanes.py::test_a_cut_round_whose_claim_release_fails_runs_no_aftermath
// Rust finishes each set separately: a prior completed set stays completed.
#[tokio::test(start_paused = true)]
async fn a_failed_claim_release_does_not_revive_a_cut_set() {
    let (kit, armed, failures) = kit();
    let reached = Arc::new(tokio::sync::Notify::new());
    sms_and(
        &kit,
        Stuck {
            reached: Arc::clone(&reached),
        },
    )
    .await;
    let after = aftermath(&kit);
    armed.store(true, Ordering::SeqCst);
    let caller = kit.clone();
    let sending = tokio::spawn(async move {
        caller
            .process_inbound(
                sms("Hold on"),
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    // The kit grants five seconds; the watchdog must leave it time to cut.
    tokio::time::timeout(Duration::from_secs(10), kit.close())
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
    assert_eq!(*after.lock().unwrap(), Vec::<String>::new());
    assert_eq!(failures.load(Ordering::SeqCst), 1);
}
