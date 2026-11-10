//! RFC §10.2, §12.10.4 and §13.5: the lane's lock and close boundaries.

use super::*;
use crate::context::ExecutionContext;
use crate::lock_manager::SharedLockManager;
use crate::store::CommitEventOptions;
use crate::{InMemoryLockManager, RoomLockManager};
use pretty_assertions::assert_eq;
use roomkit_core::model::EventSource;
use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

async fn room_with(kit: &RoomKit, target: impl Channel) {
    let target_id = target.channel_id();
    kit.register_channel(target).unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in [channel("sms"), target_id] {
        kit.attach_channel(&room_id(), &id, AttachChannelOptions::default())
            .await
            .unwrap();
    }
}

struct Probe {
    locks: Arc<dyn RoomLockManager>,
    seen: Arc<AtomicUsize>,
}

impl Channel for Probe {
    fn channel_id(&self) -> ChannelId {
        channel("probe")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    async fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let context = ExecutionContext::current();
        assert!(!context.holds_a_room_lock());
        assert!(context.lease(&room_id()).is_none());
        // Ask the real manager, bypassing the driver's reentrant lease.
        // A delivery claim in this manager must not hold the room's key.
        let id = room_id();
        let held = tokio::time::timeout(BOUND, self.locks.locked(&id))
            .await
            .unwrap()
            .unwrap();
        held.release().await.unwrap();
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(ChannelOutput::default())
    }
}

async fn unlocked_delivery(locks: Arc<dyn RoomLockManager>) {
    let kit = RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::clone(&locks)),
        ..Default::default()
    });
    let seen = Arc::new(AtomicUsize::new(0));
    room_with(
        &kit,
        Probe {
            locks,
            seen: Arc::clone(&seen),
        },
    )
    .await;
    let result = tokio::time::timeout(
        BOUND,
        kit.process_inbound(message("hello"), ProcessInboundOptions::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.error.is_none());
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    kit.close().await.unwrap();
}

// Python: tests/test_lanes_multiworker.py::test_delivery_runs_outside_the_room_lock
#[tokio::test(start_paused = true)]
async fn delivery_holds_no_room_lock_with_the_memory_manager() {
    unlocked_delivery(Arc::new(InMemoryLockManager::default())).await;
}

#[tokio::test(start_paused = true)]
async fn delivery_holds_no_room_lock_with_a_shared_claim_manager() {
    unlocked_delivery(Arc::new(SharedLockManager::default())).await;
}

async fn close_after_hole(filled_after_close: bool) {
    let kit = RoomKit::new(RoomKitOptions {
        delivery_gap_timeout: Duration::from_secs(60),
        ..Default::default()
    });
    let target = RecordingTransport::new(
        channel("probe"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    room_with(&kit, target.clone()).await;
    let hole = kit
        .store()
        .commit_event(
            &room_id(),
            RoomEvent::new(
                room_id(),
                EventSource::new(channel("sms"), ChannelType::SMS),
                TextContent::new("record only"),
            ),
            CommitEventOptions::default(),
        )
        .await
        .unwrap();
    let result = kit
        .process_inbound(
            message("hello"),
            ProcessInboundOptions {
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let handle = result.delivery.unwrap();
    assert!(!handle.done());
    assert!(target.texts().is_empty());
    let closer = kit.clone();
    let closing = tokio::spawn(async move { closer.close().await });
    if filled_after_close {
        let mut phases = kit.inner.tasks.phases();
        tokio::time::timeout(
            BOUND,
            phases.wait_for(|phase| *phase != crate::tasks::Phase::Open),
        )
        .await
        .unwrap()
        .unwrap();
    }
    // Python's late CursorEntry: a record with no delivery set tells the
    // same kit its index is over, even after admission has closed.
    kit.inner.cursors.over(&room_id(), [hole.index]);
    kit.inner.cursors.catch_up(kit.store(), &room_id()).await;
    // Within the five-second grace, not after its forced cut.
    tokio::time::timeout(Duration::from_secs(1), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let finished = handle.wait().await.unwrap();
    // The lane keeps and delivers its first set. The enclosing kit's
    // close still marks every unfinished call kit_closed (§12.10.4).
    assert_eq!(finished.cancellation_reason.as_deref(), Some("kit_closed"));
    assert_eq!(target.texts(), ["hello"]);
    assert_eq!(
        finished
            .delivery_results
            .get(&channel("probe"))
            .unwrap()
            .status,
        roomkit_core::model::DeliveryResultStatus::Sent
    );
}

// Python: tests/test_lanes.py::test_close_runs_the_entry_it_keeps_once_its_index_hole_is_filled
#[tokio::test(start_paused = true)]
async fn close_keeps_the_first_set_when_its_hole_is_filled_before_close() {
    close_after_hole(false).await;
}

#[tokio::test(start_paused = true)]
async fn close_keeps_the_first_set_when_its_hole_is_filled_during_close() {
    close_after_hole(true).await;
}
