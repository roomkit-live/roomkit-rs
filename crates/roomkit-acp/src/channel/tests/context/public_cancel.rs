//! Public owner cancellation while ACP is still obtaining host context.

use std::sync::Arc;

use roomkit::model::{Cancel, ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, SteerOptions,
};
use tokio::sync::Notify;

use super::{AcpChannelOptions, AcpContextContributor, PATIENCE, channel, transport};

async fn room(agent: impl Channel) -> Arc<RoomKit> {
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(agent).unwrap();
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("room-1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for name in ["ws", "agent"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(name).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}

// Python: test_acp_backend::test_a_steering_cancel_before_the_agent_is_reached_cuts_the_turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_steering_cancel_before_the_agent_is_reached_cuts_the_turn() {
    let entered = Arc::new(Notify::new());
    let released = Arc::new(Notify::new());
    let contributor = held_contributor(Arc::clone(&entered), Arc::clone(&released));
    let (transport, mut opened) = transport();
    let kit = room(channel(AcpChannelOptions {
        transport: Some(transport),
        context_contributor: Some(contributor),
        ..AcpChannelOptions::default()
    }))
    .await;
    let running = tokio::spawn(send(Arc::clone(&kit)));
    tokio::time::timeout(PATIENCE, entered.notified())
        .await
        .unwrap();
    assert_eq!(
        kit.steer(
            &RoomId::new("room-1").unwrap(),
            &ChannelId::new("agent").unwrap(),
            Cancel::default(),
            SteerOptions::default()
        )
        .await
        .unwrap(),
        1
    );
    released.notify_one();
    tokio::time::timeout(PATIENCE, running)
        .await
        .unwrap()
        .unwrap();
    assert!(
        opened.try_recv().is_err(),
        "no agent connection or prompt before owner cancellation"
    );
    kit.close().await.unwrap();
}

fn held_contributor(entered: Arc<Notify>, released: Arc<Notify>) -> AcpContextContributor {
    AcpContextContributor::new({
        let entered = Arc::clone(&entered);
        let released = Arc::clone(&released);
        move |_, _| {
            let entered = Arc::clone(&entered);
            let released = Arc::clone(&released);
            async move {
                entered.notify_one();
                released.notified().await;
                Ok(vec!["note".to_owned()])
            }
        }
    })
}

async fn send(kit: Arc<RoomKit>) {
    kit.process_inbound(
        InboundMessage::new(
            ChannelId::new("ws").unwrap(),
            "alice",
            TextContent::new("hello"),
        ),
        ProcessInboundOptions {
            room_id: Some(RoomId::new("room-1").unwrap()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}
