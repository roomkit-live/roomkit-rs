//! Placement across real Redis nodes and admission by the chosen local worker.
use std::collections::BTreeSet;
use std::time::Duration;

use super::{RedisTurnBackend, RedisTurnBackendOptions};
use crate::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId,
    TextContent,
};
use crate::store::ListEventsOptions;
use crate::testing::{EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions};
use crate::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitOptions, TurnExecutor, TurnExecutorOptions,
    TurnTimeouts,
};

fn nodes() -> Option<(RedisTurnBackend, RedisTurnBackend)> {
    nodes_with_loop_back(false)
}

fn nodes_with_loop_back(loop_back: bool) -> Option<(RedisTurnBackend, RedisTurnBackend)> {
    let url = std::env::var("REDIS_URL").ok()?;
    let options = RedisTurnBackendOptions {
        _loop_back: loop_back,
        key_prefix: format!("roomkit:placement:{}", uuid::Uuid::new_v4()),
        timeouts: TurnTimeouts {
            heartbeat_interval: Duration::from_millis(50),
            ..Default::default()
        },
        ..Default::default()
    };
    Some((
        RedisTurnBackend::new(Some(&url), options.clone()).unwrap(),
        RedisTurnBackend::new(Some(&url), options).unwrap(),
    ))
}

fn worker(name: &'static str) -> TurnExecutor {
    TurnExecutor::new(
        move |id| async move { Ok(Some(EchoAgent::new(id, name, EchoAgentOptions::default()))) },
        TurnExecutorOptions {
            executor_id: Some(name.into()),
            hosts: Some(BTreeSet::from(["agent".into()])),
            ..Default::default()
        },
    )
    .unwrap()
}

async fn choose(node: &RedisTurnBackend, room: &str) -> String {
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
    let Some((owner, remote)) = nodes() else {
        return;
    };
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
    room_turn_on_local_node(false, false).await;
}

#[tokio::test]
async fn loop_back_carries_a_locally_served_turn_through_the_nodes_inbox() {
    room_turn_on_local_node(true, false).await;
}

#[tokio::test]
async fn loop_back_carries_the_kits_own_turn_without_advertising_its_channel() {
    room_turn_on_local_node(true, true).await;
}

async fn room_turn_on_local_node(loop_back: bool, own_channel: bool) {
    let Some((node, peer)) = nodes_with_loop_back(loop_back) else {
        return;
    };
    if !own_channel {
        node.serve(worker("in-process")).await.unwrap();
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
    // A consumed XADD leaves the inbox key behind.
    let mut connection = node.connection().await.unwrap();
    let inbox: bool = redis::cmd("EXISTS")
        .arg(node.key(&format!("inbox:{}", node.node_id())))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(inbox, loop_back, "only the looped-back turn uses the inbox");
    if own_channel {
        assert_eq!(
            peer.executors().await.unwrap(),
            Vec::<crate::turns::ExecutorInfo>::new()
        );
    }
    peer.close().await.unwrap();
    kit.close().await.unwrap();
}

fn register_channels(kit: &RoomKit, tx: &ChannelId, agent: &ChannelId, own_channel: bool) {
    kit.register_channel(RecordingTransport::new(
        tx.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    if own_channel {
        kit.register_channel(EchoAgent::new(
            agent.clone(),
            "in-process",
            EchoAgentOptions::default(),
        ))
        .unwrap();
    } else {
        kit.register_channel(ChannelDeclaration {
            identity: Some(AgentIdentity::default()),
            ..ChannelDeclaration::new(agent.clone())
        })
        .unwrap();
    }
}

async fn attach_agents_room(kit: &RoomKit, room: &RoomId, tx: &ChannelId, agent: &ChannelId) {
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(room, tx, AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(
        room,
        agent,
        AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

async fn assert_room_reply(kit: &RoomKit, room: &RoomId, tx: &ChannelId, agent: &ChannelId) {
    let result = kit
        .process_inbound(
            InboundMessage::new(tx.clone(), "person", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.error, None);
    let rows = kit
        .store()
        .list_events(room, ListEventsOptions::default())
        .await
        .unwrap();
    let replies: Vec<_> = rows
        .iter()
        .filter(|row| &row.source.channel_id == agent && row.r#type == EventType::Message)
        .collect();
    assert_eq!(replies.len(), 1);
    assert!(matches!(&replies[0].content, EventContent::Text(text) if text.body == "in-process"));
}

mod collision;
mod owner;

#[tokio::test]
async fn a_local_only_replacement_withdraws_its_own_advertisement_and_preserves_a_peer_of_the_same_id()
 {
    for predicate in [true, false] {
        let Some((host, peer)) = nodes() else { return };
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
