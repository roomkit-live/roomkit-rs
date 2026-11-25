//! The process's beat is its thread's (RFC §10.6.8): a turn or a control writes none.
use std::sync::Arc;
use std::time::Duration;

use super::super::test_support::Services;
use super::super::{DistributedTurnBackend, DistributedTurnBackendOptions};
use crate::model::{ChannelBinding, ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use crate::turns::{ChannelControl, TurnBackend, TurnBackendHandle};
use crate::{ProcessInboundOptions, RoomKit, RoomKitOptions, TurnTimeouts};

/// The instant of this owner's last beat, in milliseconds of the registry's clock.
async fn last_beat(node: &DistributedTurnBackend) -> i64 {
    node.registry().view(i64::MAX).await.unwrap().owners[node.node_id()]
}

/// Returns once the registry's clock passed `instant`: a beat written from now on carries a later one.
async fn clock_passes(node: &DistributedTurnBackend, instant: i64) {
    while node.registry().view(i64::MAX).await.unwrap().now <= instant {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The beat thread alone refreshes the process's record, as in Python.
#[tokio::test]
async fn a_turn_and_a_control_write_no_beat() {
    let options = DistributedTurnBackendOptions {
        // The beat thread stays silent for the whole test.
        timeouts: TurnTimeouts {
            heartbeat_interval: Duration::from_secs(60),
            ..Default::default()
        },
        ..Default::default()
    };
    let node = Services::default().node(options);
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(TurnBackendHandle::new(Arc::new(node.clone()))),
        ..Default::default()
    });
    let room: RoomId = "r".parse().unwrap();
    let agent: ChannelId = "agent".parse().unwrap();
    let text: ChannelId = "text".parse().unwrap();
    super::register_channels(&kit, &text, &agent, true);
    super::attach_agents_room(&kit, &room, &text, &agent).await;
    // The backend's first use writes its beat.
    super::assert_room_reply(&kit, &room, &text, &agent).await;
    let first = last_beat(&node).await;
    clock_passes(&node, first).await;

    let turn = kit
        .process_inbound(
            InboundMessage::new(text, "person", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(turn.error, None);
    let attached = ChannelControl::RoomAttached {
        channel_id: agent.clone(),
        room_id: room.clone(),
        organization_id: None,
        binding: Box::new(ChannelBinding::new(agent, room, ChannelType::AI)),
    };
    TurnBackend::control(&node, attached).await.unwrap();

    assert_eq!(last_beat(&node).await, first);
    kit.close().await.unwrap();
}
