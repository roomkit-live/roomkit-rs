//! The process's beat is its thread's (RFC §10.6.8): a turn or a control writes none.
use std::sync::Arc;
use std::time::Duration;

use super::super::{RedisTurnBackend, RedisTurnBackendOptions};
use crate::model::{ChannelBinding, ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use crate::turns::{ChannelControl, TurnBackend, TurnBackendHandle};
use crate::{ProcessInboundOptions, RoomKit, RoomKitOptions, TurnTimeouts};

/// The instant of this owner's last beat, in milliseconds of Redis's clock.
async fn last_beat(node: &RedisTurnBackend) -> i64 {
    let mut connection = node.connection().await.unwrap();
    redis::cmd("ZSCORE")
        .arg(node.key("owners"))
        .arg(node.node_id())
        .query_async(&mut connection)
        .await
        .unwrap()
}

/// Returns once Redis's clock passed `instant`: a beat written from now on carries a later one.
async fn clock_passes(node: &RedisTurnBackend, instant: i64) {
    let mut connection = node.connection().await.unwrap();
    loop {
        let (seconds, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut connection)
            .await
            .unwrap();
        if seconds * 1000 + micros / 1000 > instant {
            return;
        }
    }
}

/// The beat thread alone refreshes the process's record, as in Python.
#[tokio::test]
async fn a_turn_and_a_control_write_no_beat() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    let options = RedisTurnBackendOptions {
        key_prefix: format!("roomkit:beats:{}", uuid::Uuid::new_v4()),
        // The beat thread stays silent for the whole test.
        timeouts: TurnTimeouts {
            heartbeat_interval: Duration::from_secs(60),
            ..Default::default()
        },
        ..Default::default()
    };
    let node = RedisTurnBackend::new(Some(&url), options).unwrap();
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
