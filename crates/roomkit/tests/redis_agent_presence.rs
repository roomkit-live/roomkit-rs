//! Cross-owner presence claims and publication using a real Redis registry.
#![cfg(feature = "redis")]
#![expect(clippy::unwrap_used, reason = "a broken test fixture fails the test")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{ChannelCategory, ChannelId, RoomId};
use roomkit::realtime::{EphemeralEventType, InMemoryRealtime, RealtimeBackend};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, CreateRoomOptions, InMemoryStore,
    RoomKit, RoomKitOptions, TurnExecutor, TurnExecutorOptions, TurnTimeouts,
};
use roomkit::{RedisTurnBackend, RedisTurnBackendOptions};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

#[tokio::test]
async fn two_owners_claim_each_change_once_and_share_the_python_key() {
    let Some((prefix, node)) = test_nodes(Duration::from_millis(30)) else {
        return;
    };
    let (first, second) = (node(), node());
    let agent = "a:b";
    let room = "r/É";
    let (one, two) = tokio::join!(
        first.claim_presence(agent, room, Some(true)),
        second.claim_presence(agent, room, Some(true)),
    );
    assert_ne!(one.unwrap(), two.unwrap());
    assert!(
        second
            .claim_presence(agent, room, Some(false))
            .await
            .unwrap()
    );
    assert!(
        !first
            .claim_presence(agent, room, Some(false))
            .await
            .unwrap()
    );
    let key = format!("{prefix}:presence:r%2F%C3%89:a%3Ab");
    let url = std::env::var("REDIS_URL").unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let (state, ttl): (String, i64) = redis::pipe()
        .cmd("GET")
        .arg(&key)
        .cmd("PTTL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(state, "offline");
    assert!((6 * 24 * 60 * 60 * 1000..=7 * 24 * 60 * 60 * 1000).contains(&ttl));
    assert!(!second.claim_presence(agent, room, None).await.unwrap());
    assert!(
        first
            .claim_presence(agent, room, Some(false))
            .await
            .unwrap()
    );
    assert!(!first.claim_presence(agent, room, None).await.unwrap());
    first.close().await.unwrap();
    assert!(first.claim_presence(agent, room, Some(true)).await.is_err());
    second.close().await.unwrap();
}

struct Room {
    id: RoomId,
    agent: ChannelId,
    realtime: Arc<InMemoryRealtime>,
    store: Arc<InMemoryStore>,
}

impl Room {
    fn new() -> Self {
        Self {
            id: "r".parse().unwrap(),
            agent: "worker".parse().unwrap(),
            realtime: Arc::new(InMemoryRealtime::default()),
            store: Arc::new(InMemoryStore::new()),
        }
    }

    fn owner(&self, node: RedisTurnBackend) -> RoomKit {
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<InMemoryRealtime>::clone(&self.realtime)),
            store: Arc::clone(&self.store),
            turn_backend: Some(std::sync::Arc::new(node)),
            ..Default::default()
        });
        kit.register_channel(ChannelDeclaration {
            identity: Some(AgentIdentity::default()),
            ..ChannelDeclaration::new(self.agent.clone())
        })
        .unwrap();
        kit
    }

    async fn create(&self, kit: &RoomKit) -> UnboundedReceiver<EphemeralEventType> {
        let (send, events) = unbounded_channel();
        self.realtime
            .subscribe_to_room(
                &self.id,
                Arc::new(move |event| {
                    if event.user_id == "worker"
                        && matches!(
                            event.r#type,
                            EphemeralEventType::PresenceOffline
                                | EphemeralEventType::PresenceOnline
                        )
                    {
                        send.send(event.r#type).unwrap();
                    }
                    Box::pin(async {})
                }),
            )
            .await;
        kit.create_room(CreateRoomOptions {
            room_id: Some(self.id.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        self.attach(kit).await;
        events
    }

    async fn attach(&self, kit: &RoomKit) {
        kit.attach_channel(
            &self.id,
            &self.agent,
            AttachChannelOptions {
                category: Some(ChannelCategory::Intelligence),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
}

fn worker(id: &str) -> TurnExecutor {
    TurnExecutor::new(
        |_id| async { Ok(None::<EchoAgent>) },
        TurnExecutorOptions {
            executor_id: Some(id.into()),
            hosts: Some(BTreeSet::from(["worker".into()])),
            ..Default::default()
        },
    )
    .unwrap()
}

fn test_nodes(interval: Duration) -> Option<(String, impl Fn() -> RedisTurnBackend)> {
    let url = std::env::var("REDIS_URL").ok()?;
    let prefix = format!("roomkit:test:agent-presence:{}", uuid::Uuid::new_v4());
    Some((prefix.clone(), move || {
        RedisTurnBackend::new(
            Some(&url),
            RedisTurnBackendOptions {
                key_prefix: prefix.clone(),
                timeouts: TurnTimeouts {
                    heartbeat_interval: interval,
                    heartbeat_timeout: Duration::from_secs(30),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap()
    }))
}

async fn expect_event(
    events: &mut UnboundedReceiver<EphemeralEventType>,
    kind: EphemeralEventType,
) {
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .unwrap(),
        Some(kind)
    );
}

#[tokio::test]
async fn two_room_owners_publish_each_remote_worker_change_once() {
    use EphemeralEventType::{PresenceOffline, PresenceOnline};
    let interval = Duration::from_millis(30);
    let Some((_, node)) = test_nodes(interval) else {
        return;
    };
    let room = Room::new();
    let (first, second) = (room.owner(node()), room.owner(node()));
    let peer = node();
    let worker = worker("host-1");
    peer.serve(worker.clone()).await.unwrap();
    let mut events = room.create(&first).await;
    expect_event(&mut events, PresenceOnline).await;
    peer.close().await.unwrap();
    expect_event(&mut events, PresenceOffline).await;
    tokio::time::sleep(3 * interval).await;
    assert!(events.try_recv().is_err());
    first
        .detach_channel(
            &room.id,
            &room.agent,
            roomkit::DetachChannelOptions::default(),
        )
        .await
        .unwrap();
    room.attach(&second).await;
    expect_event(&mut events, PresenceOffline).await;
    worker.close().await;
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn a_local_serve_and_close_publish_without_waiting_for_a_heartbeat() {
    use EphemeralEventType::{PresenceOffline, PresenceOnline};
    let Some((_, node)) = test_nodes(Duration::from_secs(6)) else {
        return;
    };
    let backend = node();
    let room = Room::new();
    let kit = room.owner(backend.clone());
    let mut events = room.create(&kit).await;
    expect_event(&mut events, PresenceOffline).await;
    let worker = worker("host-1");
    backend.serve(worker.clone()).await.unwrap();
    assert!(
        !backend
            .claim_presence("worker", "r", Some(true))
            .await
            .unwrap()
    );
    expect_event(&mut events, PresenceOnline).await;
    worker.close().await;
    assert!(
        !backend
            .claim_presence("worker", "r", Some(false))
            .await
            .unwrap()
    );
    expect_event(&mut events, PresenceOffline).await;
    tokio::time::timeout(Duration::from_secs(2), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
    kit.close().await.unwrap();
}

#[tokio::test]
async fn each_node_lists_local_workers_directly_and_only_advertised_peers() {
    let Some((_, node)) = test_nodes(Duration::from_millis(50)) else {
        return;
    };
    let (hosting, elsewhere) = (node(), node());
    let named = worker("host-1");
    let dynamic = TurnExecutor::new_with_host_predicate(
        |_| async { Ok(None::<EchoAgent>) },
        |id| id.as_str().starts_with("agent-"),
        TurnExecutorOptions {
            executor_id: Some("dynamic".into()),
            ..Default::default()
        },
    )
    .unwrap();
    hosting.serve(named.clone()).await.unwrap();
    hosting.serve(dynamic.clone()).await.unwrap();
    let here = hosting.executors().await.unwrap();
    let remote = elsewhere.executors().await.unwrap();
    dynamic.close().await;
    let after = hosting.executors().await.unwrap();
    named.close().await;
    hosting.close().await.unwrap();
    elsewhere.close().await.unwrap();

    assert_eq!(here.len(), 2);
    assert!(
        here.iter()
            .all(|row| row.node.as_deref() == Some(hosting.node_id())
                && row.last_beat.is_none()
                && row.online)
    );
    let local = here
        .iter()
        .find(|row| row.executor_id == "dynamic")
        .unwrap();
    assert!(local.hosts.is_none());
    assert_eq!(remote.len(), 1);
    let peer = remote.first().unwrap();
    assert_eq!(peer.executor_id, named.executor_id());
    assert!(peer.last_beat.is_some() && peer.covers("worker", &[], None).unwrap());
    assert_eq!(after.len(), 1);
    assert_eq!(after.first().unwrap().executor_id, named.executor_id());
}

#[tokio::test]
async fn the_catalogue_excludes_only_the_attached_owners_executor() {
    let Some((_, node)) = test_nodes(Duration::from_millis(50)) else {
        return;
    };
    let backend = node();
    let room = Room::new();
    let owner = room.owner(backend.clone());
    let _events = room.create(&owner).await;
    let own = owner.turn_executor();
    let other = RoomKit::new(RoomKitOptions::default());
    let other_worker = other.turn_executor();

    backend.serve(own.clone()).await.unwrap();
    let own_listing = owner.executors().await.unwrap();

    backend.serve(other_worker.clone()).await.unwrap();
    let foreign_listing = owner.executors().await.unwrap();

    let same_id = worker(own.executor_id());
    backend.serve(same_id.clone()).await.unwrap();
    let replaced = owner.executors().await.unwrap();
    owner.close().await.unwrap();
    other.close().await.unwrap();
    same_id.close().await;

    assert!(own_listing.is_empty(), "{own_listing:?}");
    assert_eq!(foreign_listing.len(), 1);
    assert_eq!(
        foreign_listing.first().unwrap().executor_id,
        other_worker.executor_id()
    );
    assert_eq!(replaced.len(), 2);
    assert!(
        replaced
            .iter()
            .any(|info| info.executor_id == other_worker.executor_id())
    );
    assert!(
        replaced
            .iter()
            .any(|info| info.executor_id == own.executor_id()
                && info.covers("worker", &[], None).unwrap())
    );
}

#[tokio::test]
async fn attaching_a_live_agent_identifies_an_executor_already_served() {
    let Some((_, node)) = test_nodes(Duration::from_millis(50)) else {
        return;
    };
    let backend = node();
    let owner = RoomKit::new(RoomKitOptions {
        turn_backend: Some(std::sync::Arc::new(backend.clone())),
        ..Default::default()
    });
    let own = owner.turn_executor();
    backend.serve(own.clone()).await.unwrap();
    let before = owner.executors().await.unwrap();
    let room = Room::new();
    owner
        .register_channel(EchoAgent::new(
            room.agent.clone(),
            "hi",
            EchoAgentOptions::default(),
        ))
        .unwrap();
    let _events = room.create(&owner).await;
    let after = owner.executors().await.unwrap();
    owner.close().await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before.first().unwrap().executor_id, own.executor_id());
    assert!(after.is_empty(), "{after:?}");
}
