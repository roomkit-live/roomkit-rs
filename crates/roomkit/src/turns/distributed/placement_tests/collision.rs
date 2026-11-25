//! Same-node loopback still distinguishes a kit's reserved channels from a
//! separately served executor reusing its public identifier.
use super::*;
use std::sync::Arc;

#[tokio::test]
async fn loop_back_keeps_a_same_id_workers_declaration() {
    let (node, peer) = nodes_with_loop_back(true);
    let (kit, _, _, _) = setup(&node).await;
    let id = kit.turn_executor().executor_id().to_owned();
    node.serve(replacement(&id, true)).await.unwrap();
    let published = peer.executors().await.unwrap();
    assert!(published.iter().any(|info| {
        info.executor_id == id
            && info
                .hosts
                .as_ref()
                .is_some_and(|names| names.contains("agent"))
    }));
    node.serve(replacement(&id, false)).await.unwrap();
    assert_eq!(
        peer.executors().await.unwrap(),
        Vec::<crate::turns::ExecutorInfo>::new()
    );
    peer.close().await.unwrap();
    kit.close().await.unwrap();
}

#[tokio::test]
async fn loop_back_routes_same_id_workers_by_channel() {
    for shared in [true, false] {
        let (node, peer) = nodes_with_loop_back(true);
        let (kit, text, home, agent) = setup(&node).await;
        let id = kit.turn_executor().executor_id().to_owned();
        node.serve(replacement(&id, shared)).await.unwrap();
        for channel in [agent, home] {
            let room: RoomId = channel.as_str().parse().unwrap();
            attach_agents_room(&kit, &room, &text, &channel).await;
            assert_room_reply(&kit, &room, &text, &channel).await;
        }
        peer.close().await.unwrap();
        kit.close().await.unwrap();
    }
}

pub(super) fn replacement(id: &str, shared: bool) -> TurnExecutor {
    let options = TurnExecutorOptions {
        executor_id: Some(id.into()),
        hosts: shared.then(|| BTreeSet::from(["agent".into()])),
        ..Default::default()
    };
    TurnExecutor::new(
        move |id: ChannelId| async move {
            let reply = if id.as_str() == "agent" {
                "in-process"
            } else {
                "wrong executor"
            };
            Ok(Some(EchoAgent::new(id, reply, EchoAgentOptions::default())))
        },
        options,
    )
    .unwrap()
}

async fn setup(node: &DistributedTurnBackend) -> (RoomKit, ChannelId, ChannelId, ChannelId) {
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(crate::turns::TurnBackendHandle::new(Arc::new(node.clone()))),
        ..Default::default()
    });
    let text = "text".parse().unwrap();
    let home = "home".parse().unwrap();
    let agent: ChannelId = "agent".parse().unwrap();
    register_channels(&kit, &text, &home, true);
    kit.register_channel(ChannelDeclaration {
        identity: Some(AgentIdentity::default()),
        ..ChannelDeclaration::new(agent.clone())
    })
    .unwrap();
    let room = "before-replacement".parse().unwrap();
    attach_agents_room(&kit, &room, &text, &home).await;
    assert_room_reply(&kit, &room, &text, &home).await;
    (kit, text, home, agent)
}
