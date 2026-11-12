//! The ACP channel's own steering method addresses only its named turn.

use super::{Agent, PATIENCE, Piped, channel};
use crate::AcpChannel;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, current_turn_host,
    model::{
        Cancel, ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
        InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
    },
    testing::{RecordingTransport, RecordingTransportOptions},
};
use serde_json::json;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

/// Capture the public turn host while forwarding to the actual ACP channel.
struct Addressed {
    inner: AcpChannel<Piped>,
    turn: Arc<Mutex<Option<String>>>,
}
impl Channel for Addressed {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id()
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
        self.inner.handle_inbound(message, context)
    }
    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.inner.deliver(event, binding, context)
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        *self.turn.lock().unwrap() = Some(current_turn_host().unwrap().turn_id().to_owned());
        self.inner.on_event(event, binding, context).await
    }
    async fn close(&self) {
        self.inner.close().await;
    }
}

// Python: tests/test_channels/test_acp_backend.py::test_a_steering_cancel_reaches_the_turn_it_names_only
#[tokio::test]
async fn test_a_steering_cancel_reaches_the_turn_it_names_only() {
    let (channel, mut agents, _) = channel();
    let address = Arc::new(Mutex::new(None));
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(Addressed {
        inner: channel.clone(),
        turn: Arc::clone(&address),
    })
    .unwrap();
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for name in ["ws", "claude"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(name).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let running = tokio::spawn({
        let kit = Arc::clone(&kit);
        async move {
            kit.process_inbound(
                InboundMessage::new(
                    ChannelId::new("ws").unwrap(),
                    "alice",
                    TextContent::new("hello"),
                ),
                ProcessInboundOptions {
                    room_id: Some(room),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        }
    });
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let initialize = agent.read().await;
    agent
        .answer(
            &initialize,
            json!({"protocolVersion":1,"agentCapabilities":{}}),
        )
        .await;
    let opening = agent.read().await;
    assert_eq!(opening["method"], "session/new");
    agent.answer(&opening, json!({"sessionId":"s1"})).await;
    let prompt = agent.read().await;
    assert_eq!(prompt["method"], "session/prompt");
    let cancel = Cancel::default().into();
    assert_eq!(channel.steer(&cancel, "another-turn").await.unwrap(), None);
    let turn = address.lock().unwrap().clone().unwrap();
    assert_eq!(channel.steer(&cancel, &turn).await.unwrap(), Some(1));
    let cancelled = agent.read().await;
    assert_eq!(cancelled["method"], "session/cancel");
    assert_eq!(cancelled["params"]["sessionId"], "s1");
    agent
        .answer(&prompt, json!({"stopReason":"cancelled"}))
        .await;
    assert!(
        tokio::time::timeout(PATIENCE, running)
            .await
            .unwrap()
            .unwrap()
            .error
            .is_none()
    );
    assert_eq!(channel.steer(&cancel, &turn).await.unwrap(), None);
    kit.close().await.unwrap();
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}
