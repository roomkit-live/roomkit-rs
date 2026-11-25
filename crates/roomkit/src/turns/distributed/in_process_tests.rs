//! Placement across two nodes of one process: the backend on the broker
//! and the registry of this process.
use std::sync::Arc;
use std::time::Duration;

use super::test_support::{assert_room_reply, attach_agents_room, register_channels, worker};
use super::{
    DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker, InMemoryTurnRegistry,
};
use crate::model::{ChannelId, RoomId};
use crate::turns::TurnBackendHandle;
use crate::{DetachChannelOptions, RoomKit, RoomKitOptions, TurnTimeouts};

fn nodes(loop_back: bool) -> (DistributedTurnBackend, DistributedTurnBackend) {
    let broker = Arc::new(InMemoryTurnBroker::new());
    let registry = Arc::new(InMemoryTurnRegistry::new());
    let node = || {
        let options = DistributedTurnBackendOptions {
            timeouts: TurnTimeouts {
                heartbeat_interval: Duration::from_millis(50),
                ..Default::default()
            },
            _loop_back: loop_back,
        };
        let broker = Arc::<InMemoryTurnBroker>::clone(&broker);
        let registry = Arc::<InMemoryTurnRegistry>::clone(&registry);
        DistributedTurnBackend::new(broker, registry, options)
    };
    (node(), node())
}

async fn a_room_turn(loop_back: bool, served_elsewhere: bool) {
    let (node, peer) = nodes(loop_back);
    let serving = if served_elsewhere { &peer } else { &node };
    serving.serve(worker("in-process")).await.unwrap();
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(TurnBackendHandle::new(Arc::new(node.clone()))),
        ..Default::default()
    });
    let room: RoomId = "r".parse().unwrap();
    let agent: ChannelId = "agent".parse().unwrap();
    let text: ChannelId = "text".parse().unwrap();
    register_channels(&kit, &text, &agent, false);
    attach_agents_room(&kit, &room, &text, &agent).await;

    assert_room_reply(&kit, &room, &text, &agent).await;

    // Every owner reads who holds the room's state (RFC §10.6.7).
    let holder = peer.registry().holder("agent", "r").await.unwrap();
    assert_eq!(
        holder,
        Some(format!("{}:in-process", serving.node_id())),
        "the executor that took the turn holds the room"
    );
    // A detach leaves nobody holding the room's state.
    kit.detach_channel(&room, &agent, DetachChannelOptions::default())
        .await
        .unwrap();
    assert_eq!(peer.registry().holder("agent", "r").await.unwrap(), None);
    kit.close().await.unwrap();
    peer.close().await.unwrap();
}

#[tokio::test]
async fn a_room_turn_reaches_the_worker_served_on_another_node_of_this_process() {
    a_room_turn(false, true).await;
}

#[tokio::test]
async fn a_room_turn_reaches_the_worker_served_on_its_owners_node() {
    a_room_turn(false, false).await;
}

#[tokio::test]
async fn loop_back_carries_a_locally_served_turn_through_the_nodes_inbox() {
    a_room_turn(true, false).await;
}

#[tokio::test]
async fn an_executor_served_on_another_node_is_listed_online_with_its_node() {
    let (node, peer) = nodes(false);
    peer.serve(worker("in-process")).await.unwrap();

    let listed = node.executors().await.unwrap();

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].executor_id, "in-process");
    assert_eq!(listed[0].node.as_deref(), Some(peer.node_id()));
    assert!(listed[0].online);
    peer.close().await.unwrap();
    assert_eq!(node.executors().await.unwrap(), []);
    node.close().await.unwrap();
}
