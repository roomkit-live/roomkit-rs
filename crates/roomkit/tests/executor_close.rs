//! Python: `tests/test_executor_close_contract.py`; independent executor lifecycle.
#![expect(clippy::unwrap_used, reason = "tests fail on invalid fixtures")]

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
    RoomId,
};
use roomkit::turns::TakesOptions;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, RoomKit, RoomKitOptions,
};

struct ClosingChannel(Arc<AtomicUsize>);
impl Channel for ClosingChannel {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("channel").unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
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
        ready(Ok(ChannelOutput::empty()))
    }
    async fn close(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

// Python: test_closing_the_local_executor_keeps_the_kits_borrowed_channels
#[tokio::test]
async fn closing_the_local_executor_keeps_the_kits_borrowed_channels() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let closes = Arc::new(AtomicUsize::new(0));
    let channel = ClosingChannel(Arc::clone(&closes));
    let id = channel.channel_id();
    kit.register_channel(channel).unwrap();
    let executor = kit.turn_executor();
    assert!(!executor.closed());
    assert!(executor.declares(&id));
    assert!(executor.takes(&id, TakesOptions::default()));
    executor.close().await;
    executor.close().await;
    assert!(executor.closed());
    assert!(!executor.declares(&id));
    assert!(!executor.takes(&id, TakesOptions::default()));
    assert!(executor.hosts_channel(&id));
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    let room = RoomId::new("still-open").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &id, AttachChannelOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct Held {
    entered: tokio::sync::Notify,
    cleaned: std::sync::atomic::AtomicBool,
}
struct Cleanup(Arc<Held>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleaned.store(true, Ordering::SeqCst);
    }
}
struct HeldAgent(Arc<Held>);
impl Channel for HeldAgent {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("agent").unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> roomkit::model::ChannelCategory {
        roomkit::model::ChannelCategory::Intelligence
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
        ready(Ok(ChannelOutput::empty()))
    }
    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let _cleanup = Cleanup(Arc::clone(&self.0));
        self.0.entered.notify_one();
        std::future::pending().await
    }
}

// Python: test_executor_close_contract.py::test_later_external_close_cuts_a_turn_previously_spared_by_its_own_close
// Its cleanup is done when close returns, before the inbound caller is joined.
#[tokio::test]
async fn closing_waits_until_the_turns_future_has_been_dropped() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let state = Arc::new(Held::default());
    kit.register_channel(ClosingChannel(Arc::default()))
        .unwrap();
    kit.register_channel(HeldAgent(Arc::clone(&state))).unwrap();
    let room = RoomId::new("room").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["channel", "agent"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(id).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let sender = kit.clone();
    let running = tokio::spawn(async move {
        sender
            .process_inbound(
                InboundMessage::new(
                    ChannelId::new("channel").unwrap(),
                    "alice",
                    roomkit::model::TextContent::new("hello"),
                ),
                roomkit::ProcessInboundOptions {
                    room_id: Some(room),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), state.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        kit.turn_executor().close(),
    )
    .await
    .unwrap();
    assert!(state.cleaned.load(Ordering::SeqCst));
    assert!(running.await.unwrap().unwrap().error.is_some());
    kit.close().await.unwrap();
}
