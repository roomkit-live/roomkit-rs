//! A failed target still completes its committed set's index (RFC §13.5).
//! A rerun's failed context read, before any target, is covered separately
//! in roomkit-core/tests/ops/rerun.rs: it owns no new index to advance.

use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

struct FailingAgent {
    reached: Arc<tokio::sync::Notify>,
    opened: Arc<tokio::sync::Notify>,
}

impl Channel for FailingAgent {
    fn channel_id(&self) -> ChannelId {
        channel("ws1")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::default()))
    }
    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.reached.notify_one();
        self.opened.notified().await;
        Err("turn exploded".into())
    }
}

// Python: tests/test_lanes.py::test_execution_failure_resolves_wait_and_records_error
// Its cursor guarantee at a real indexed execution failure; a target's
// error does not suppress AFTER_BROADCAST, unlike the fake host's global abort.
#[tokio::test(start_paused = true)]
async fn a_failed_agent_turn_advances_the_completed_sets_cursor() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let reached = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::new(tokio::sync::Notify::new());
    sms_and(
        &kit,
        FailingAgent {
            reached: Arc::clone(&reached),
            opened: Arc::clone(&opened),
        },
    )
    .await;
    let after = aftermath(&kit);
    let result = kit.process_inbound(sms("hello"), deferred()).await.unwrap();
    let event = result.event.unwrap();
    let bound = Duration::from_secs(5);
    tokio::time::timeout(bound, reached.notified())
        .await
        .unwrap();
    assert!(
        kit.store().get_delivered_index(&room_id()).await.unwrap()
            < i64::try_from(event.index).unwrap()
    );
    opened.notify_one();
    let finished = tokio::time::timeout(bound, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    let error = finished.error.unwrap();
    assert_eq!(error.error_type, "ChannelError");
    assert_eq!(error.message, "turn exploded");
    assert_eq!(
        finished
            .delivery_results
            .get(&channel("ws1"))
            .unwrap()
            .status,
        roomkit::model::DeliveryResultStatus::Failed
    );
    assert_eq!(finished.cancellation_reason, None);
    assert!(finished.response_events.is_empty());
    // Read before idle or another call can move the cursor for this set.
    assert_eq!(
        kit.store().get_delivered_index(&room_id()).await.unwrap(),
        i64::try_from(event.index).unwrap()
    );
    assert_eq!(
        kit.store()
            .get_room(&room_id())
            .await
            .unwrap()
            .unwrap()
            .latest_index,
        Some(event.index)
    );
    roomkit::testing::idle(&kit).await;
    assert_eq!(*after.lock().unwrap(), ["hello"]);
    kit.close().await.unwrap();
}
