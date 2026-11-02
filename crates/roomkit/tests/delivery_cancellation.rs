//! A completion handle's cancel past its cleanup budget (RFC §10.1
//! "Explicit delivery cancellation"): what no scenario can bound, a cleanup
//! that outlasts the cancel's `timeout`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CancelOptions, Channel, ChannelError, CreateRoomOptions, DeliveryHandle,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(2);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// How long a turn's code runs on, its thread blocked, once it started:
/// a cut lands only once it yields (Python's finalizer that outlasts the
/// cancel's budget).
const FINALIZER: Duration = Duration::from_millis(200);

/// An agent whose turn never answers: it tells `started` it runs, keeps
/// its thread for [`FINALIZER`], then waits until it is cut.
struct Stalled {
    started: Arc<Notify>,
}

impl Channel for Stalled {
    fn channel_id(&self) -> ChannelId {
        channel("ai")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's method is a future"
    )]
    async fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        Err(ChannelError::from("an agent receives no message"))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let started = Arc::clone(&self.started);
        async move {
            started.notify_one();
            std::thread::sleep(FINALIZER);
            std::future::pending().await
        }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's method is a future"
    )]
    async fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        Ok(ChannelOutput::empty())
    }
}

/// A kit whose agent's turn runs on the deferred message it returns the
/// handle of, once that turn started.
async fn running_turn() -> (RoomKit, DeliveryHandle) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let started = Arc::new(Notify::new());
    kit.register_channel(Stalled {
        started: Arc::clone(&started),
    })
    .unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for id in ["sms", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("task A"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    let decided = kit.process_inbound(message, options).await.unwrap();
    tokio::time::timeout(BOUND, started.notified())
        .await
        .unwrap();
    (kit, decided.delivery.unwrap())
}

// Python: tests/test_delivery_cancellation.py::test_cancel_timeout_reports_incomplete_cleanup_and_can_be_joined
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_past_its_timeout_reports_incomplete_cleanup_and_can_be_joined() {
    let (kit, handle) = running_turn().await;
    let options = CancelOptions {
        reason: "stopped".to_owned(),
        timeout: Duration::from_millis(20),
    };

    let cancelled = handle.cancel(options).await;

    assert!(
        matches!(cancelled, Err(RoomKitError::Timeout)),
        "{cancelled:?}"
    );
    let joined = tokio::time::timeout(BOUND, handle.wait()).await.unwrap();
    assert_eq!(
        joined.unwrap().cancellation_reason.as_deref(),
        Some("stopped")
    );
    kit.close().await.unwrap();
}

// Python: tests/test_delivery_cancellation.py::test_cancelling_cancel_waiter_still_finishes_cleanup
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_dropped_before_it_returns_still_ends_its_turn() {
    let (kit, handle) = running_turn().await;

    let dropped = tokio::time::timeout(
        Duration::from_millis(20),
        handle.cancel(CancelOptions::default()),
    )
    .await;

    assert!(
        dropped.is_err(),
        "the cancel returned before its turn ended"
    );
    let joined = tokio::time::timeout(BOUND, handle.wait()).await.unwrap();
    assert_eq!(
        joined.unwrap().cancellation_reason.as_deref(),
        Some("caller_cancelled")
    );
    kit.close().await.unwrap();
}
