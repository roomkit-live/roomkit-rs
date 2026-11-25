//! Placement across the nodes of one deployment and admission by the chosen local worker.
use std::collections::BTreeSet;

use super::DistributedTurnBackend;
use super::test_support::{
    assert_room_reply, attach_agents_room, nodes_with_loop_back, register_channels, served_nodes,
    worker,
};
use crate::model::{ChannelId, RoomId};
use crate::testing::{EchoAgent, EchoAgentOptions};
use crate::{
    AgentIdentity, ChannelDeclaration, DetachChannelOptions, RoomKit, RoomKitOptions, TurnExecutor,
    TurnExecutorOptions,
};

fn nodes() -> (DistributedTurnBackend, DistributedTurnBackend) {
    nodes_with_loop_back(false)
}

async fn choose(node: &DistributedTurnBackend, room: &str) -> String {
    match node
        .place(
            &"agent".parse().unwrap(),
            room,
            &[],
            None,
            &BTreeSet::new(),
            true,
        )
        .await
        .unwrap()
    {
        super::placement::Placed::Here(worker) => worker.executor_id().to_owned(),
        super::placement::Placed::Elsewhere(target) => target.executor_id,
    }
}

#[tokio::test]
async fn the_holder_precedes_the_first_local_worker_in_registration_order() {
    let (owner, remote) = nodes();
    remote.serve(worker("remote")).await.unwrap();
    assert_eq!(choose(&owner, "already-held").await, "remote");
    owner.serve(worker("z-first")).await.unwrap();
    owner.serve(worker("a-second")).await.unwrap();

    assert_eq!(choose(&owner, "already-held").await, "remote");
    let mut new_rooms = Vec::new();
    for room in ["one", "two", "three"] {
        new_rooms.push(choose(&owner, room).await);
    }
    assert_eq!(new_rooms, ["z-first"; 3]);

    // Serving an id again moves it after the other local candidates, as in Python.
    owner.serve(worker("z-first")).await.unwrap();
    assert_eq!(choose(&owner, "one").await, "z-first");
    assert_eq!(choose(&owner, "four").await, "a-second");
    assert_eq!(choose(&remote, "four").await, "a-second");
    owner.close().await.unwrap();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn a_room_turn_reaches_the_worker_served_on_its_owners_backend() {
    a_room_turn(false, Served::Here).await;
}

#[tokio::test]
async fn a_room_turn_reaches_the_worker_served_on_another_node() {
    a_room_turn(false, Served::Elsewhere).await;
}

#[tokio::test]
async fn loop_back_carries_a_locally_served_turn_through_the_nodes_inbox() {
    a_room_turn(true, Served::Here).await;
}

#[tokio::test]
async fn loop_back_carries_the_kits_own_turn_without_advertising_its_channel() {
    a_room_turn(true, Served::ByTheKit).await;
}

#[tokio::test]
async fn the_kits_own_turn_records_its_executor_as_the_rooms_holder() {
    a_room_turn(false, Served::ByTheKit).await;
}

/// Where the agent of a room turn is served.
#[derive(Clone, Copy, PartialEq)]
enum Served {
    /// By a worker of the owner's node.
    Here,
    /// By a worker of the other node.
    Elsewhere,
    /// By the kit's own executor, as its registered channel.
    ByTheKit,
}

async fn a_room_turn(loop_back: bool, served: Served) {
    let (services, node, peer) = served_nodes(loop_back);
    let own_channel = served == Served::ByTheKit;
    let serving = if served == Served::Elsewhere {
        &peer
    } else {
        &node
    };
    if !own_channel {
        serving.serve(worker("in-process")).await.unwrap();
    }
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(crate::turns::TurnBackendHandle::new(std::sync::Arc::new(
            node.clone(),
        ))),
        ..Default::default()
    });
    let room: RoomId = "r".parse().unwrap();
    let agent: ChannelId = "agent".parse().unwrap();
    let text: ChannelId = "text".parse().unwrap();
    register_channels(&kit, &text, &agent, own_channel);
    attach_agents_room(&kit, &room, &text, &agent).await;
    assert_room_reply(&kit, &room, &text, &agent).await;
    let through_the_inbox = !services.broker.posted(node.node_id()).is_empty();
    assert_eq!(
        through_the_inbox,
        loop_back || served == Served::Elsewhere,
        "a turn served on its owner's node uses the inbox only looped back"
    );
    // Every owner reads who holds the room's state (RFC §10.6.7).
    let holder = peer.registry().holder("agent", "r").await.unwrap();
    let executor = if own_channel {
        kit.turn_executor().executor_id().to_owned()
    } else {
        "in-process".to_owned()
    };
    assert_eq!(holder, Some(format!("{}:{executor}", serving.node_id())));
    if own_channel {
        assert_eq!(
            peer.executors().await.unwrap(),
            Vec::<crate::turns::ExecutorInfo>::new()
        );
    }
    // A detach leaves nobody holding the room's state (RFC §10.6.7).
    kit.detach_channel(&room, &agent, DetachChannelOptions::default())
        .await
        .unwrap();
    assert_eq!(peer.registry().holder("agent", "r").await.unwrap(), None);
    peer.close().await.unwrap();
    kit.close().await.unwrap();
}

#[tokio::test]
async fn an_executor_served_on_another_node_is_listed_online_with_its_node() {
    let (node, peer) = nodes();
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

mod beats;
mod collision;
mod named;
mod owner;

#[tokio::test]
async fn a_local_only_replacement_withdraws_its_own_advertisement_and_preserves_a_peer_of_the_same_id()
 {
    for predicate in [true, false] {
        let (host, peer) = nodes();
        let shared = worker("same");
        let other = worker("same");
        let make = |id| {
            std::future::ready(Ok(Some(EchoAgent::new(
                id,
                "local",
                EchoAgentOptions::default(),
            ))))
        };
        let options = TurnExecutorOptions {
            executor_id: Some("same".into()),
            ..Default::default()
        };
        let local = if predicate {
            TurnExecutor::new_with_host_predicate(make, |id| id.as_str() == "local-agent", options)
        } else {
            TurnExecutor::new(make, options)
        }
        .unwrap();

        host.serve(shared.clone()).await.unwrap();
        peer.serve(other.clone()).await.unwrap();
        assert_eq!(peer.executors().await.unwrap().len(), 2);
        host.serve(local.clone()).await.unwrap();
        let seen = peer.executors().await.unwrap();
        let observed: Vec<_> = seen
            .iter()
            .map(|row| (row.node.as_deref(), row.executor_id.as_str()))
            .collect();
        assert_eq!(observed, [(Some(peer.node_id()), "same")]);
        assert!(
            host.executors()
                .await
                .unwrap()
                .iter()
                .any(|row| row.node.as_deref() == Some(host.node_id())
                    && row.covers("local-agent", &[], None).unwrap())
        );
        assert!(
            host.receiving_worker("same", peer.node_id()).is_none(),
            "a handover already addressed to the old advertisement must be refused"
        );

        host.close().await.unwrap();
        peer.close().await.unwrap();
        shared.close().await;
        other.close().await;
        local.close().await;
    }
}
