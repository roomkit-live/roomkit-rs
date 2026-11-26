//! Cross-owner presence claims and publication: the owners are nodes of
//! one distributed backend, on a broker and a registry of this process.
#![expect(clippy::unwrap_used, reason = "a broken test fixture fails the test")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{ChannelCategory, ChannelId, RoomId};
use roomkit::realtime::{EphemeralEventType, InMemoryRealtime, RealtimeBackend};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::{
    DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker, InMemoryTurnRegistry,
};
use roomkit::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, CreateRoomOptions, InMemoryStore,
    RoomKit, RoomKitOptions, TurnExecutor, TurnExecutorOptions, TurnTimeouts,
};
use roomkit_core::ops::{Environment, SystemEnvironment};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

#[tokio::test]
async fn two_owners_claim_each_change_once() {
    let node = test_nodes(Duration::from_millis(30));
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

    fn owner(&self, node: DistributedTurnBackend) -> RoomKit {
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<InMemoryRealtime>::clone(&self.realtime)),
            store: Arc::clone(&self.store),
            turn_backend: Some(roomkit::turns::TurnBackendHandle::new(Arc::new(node))),
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

/// The nodes of one deployment, beating every `interval`.
fn test_nodes(interval: Duration) -> impl Fn() -> DistributedTurnBackend {
    let broker = Arc::new(InMemoryTurnBroker::new());
    let registry = Arc::new(InMemoryTurnRegistry::new());
    move || {
        DistributedTurnBackend::new(
            Arc::<InMemoryTurnBroker>::clone(&broker),
            Arc::<InMemoryTurnRegistry>::clone(&registry),
            DistributedTurnBackendOptions {
                timeouts: TurnTimeouts {
                    heartbeat_interval: interval,
                    heartbeat_timeout: Duration::from_secs(30),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
    }
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
    let node = test_nodes(interval);
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

// The room can observe a worker that comes and goes between registry polls.
#[tokio::test]
async fn an_attached_room_reports_a_worker_that_leaves_between_polls() {
    use EphemeralEventType::{PresenceOffline, PresenceOnline};
    let node = test_nodes(Duration::from_secs(1));
    let room = Room::new();
    let owner = node();
    let kit = room.owner(owner.clone());
    let peer = node();
    // Serving a different channel runs an initial coverage check while the
    // room's agent is still absent. Closing it below triggers the next one.
    let other = dynamic_worker("other", "other-");
    owner.serve(other.clone()).await.unwrap();
    let agent = worker("host-1");
    peer.serve(agent.clone()).await.unwrap();
    let mut events = room.create(&kit).await;
    expect_event(&mut events, PresenceOnline).await;
    peer.close().await.unwrap();
    other.close().await;
    expect_event(&mut events, PresenceOffline).await;
    agent.close().await;
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_local_serve_and_close_publish_without_waiting_for_a_heartbeat() {
    use EphemeralEventType::{PresenceOffline, PresenceOnline};
    let node = test_nodes(Duration::from_secs(6));
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
    let node = test_nodes(Duration::from_millis(50));
    let (hosting, elsewhere) = (node(), node());
    let named = worker("host-1");
    let dynamic = dynamic_worker("dynamic", "agent-");
    let serving = unix_millis();
    hosting.serve(named.clone()).await.unwrap();
    hosting.serve(dynamic.clone()).await.unwrap();
    let here = hosting.executors().await.unwrap();
    let remote = elsewhere.executors().await.unwrap();
    let listed = unix_millis();
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
    let mut local_named = here
        .iter()
        .find(|row| row.executor_id == named.executor_id())
        .unwrap()
        .clone();
    local_named.hosts = Some(BTreeSet::new());
    assert!(local_named.covers("worker", &[], None).unwrap());
    assert_eq!(remote.len(), 1);
    let peer = remote.first().unwrap();
    assert_eq!(peer.executor_id, named.executor_id());
    assert!(peer.covers("worker", &[], None).unwrap());
    // Its last beat is the time it beat, as on a server's registry.
    let beat = peer.last_beat.unwrap().as_unix_millis();
    assert!(
        (serving - 1000..=listed + 1000).contains(&beat),
        "last beat {beat} ms, read between {serving} and {listed}"
    );
    assert_eq!(after.len(), 1);
    assert_eq!(after.first().unwrap().executor_id, named.executor_id());
}

fn dynamic_worker(id: &str, prefix: &'static str) -> TurnExecutor {
    TurnExecutor::new_with_host_predicate(
        |_| async { Ok(None::<EchoAgent>) },
        move |id| id.as_str().starts_with(prefix),
        TurnExecutorOptions {
            executor_id: Some(id.into()),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn a_local_predicate_is_visible_only_on_the_serving_node() {
    let node = test_nodes(Duration::from_millis(50));
    let (hosting, elsewhere) = (node(), node());
    let worker = dynamic_worker("predicate", "agent-");
    hosting.serve(worker.clone()).await.unwrap();
    let info = hosting.executors().await.unwrap().pop().unwrap();
    let repeated = hosting.executors().await.unwrap().pop().unwrap();
    let cloned = info.clone();
    let data = serde_json::to_value(&info).unwrap();
    let restored: roomkit::turns::ExecutorInfo = serde_json::from_value(data.clone()).unwrap();
    worker.close().await;
    let after_close = hosting.executors().await.unwrap();

    let replacement = dynamic_worker("predicate", "blue-");
    hosting.serve(replacement.clone()).await.unwrap();
    let replaced = hosting.executors().await.unwrap().pop().unwrap();
    let remote = elsewhere.executors().await.unwrap();
    replacement.close().await;
    hosting.close().await.unwrap();
    elsewhere.close().await.unwrap();

    assert!(remote.is_empty() && after_close.is_empty());
    assert!(info.hosts.is_none() && info.last_beat.is_none());
    assert!(cloned.covers("agent-one", &[], None).unwrap());
    assert!(!info.covers("blue-one", &[], None).unwrap());
    assert!(!info.covers("agent-one", &["missing".into()], None).unwrap());
    assert!(!info.covers("agent-one", &[], Some("other")).unwrap());
    assert!(!restored.covers("agent-one", &[], None).unwrap());
    assert!(replaced.covers("blue-one", &[], None).unwrap());
    assert!(!replaced.covers("agent-one", &[], None).unwrap());
    assert_eq!(data, serde_json::to_value(&replaced).unwrap());
    assert_eq!(info, cloned);
    assert_eq!(info, repeated);
    assert_ne!(info, restored);
    assert_ne!(info, replaced);
}

#[tokio::test]
async fn a_declared_agents_presence_lists_each_matching_local_predicate() {
    let node = test_nodes(Duration::from_millis(50));
    let backend = node();
    let room = Room::new();
    let kit = room.owner(backend.clone());
    let _events = room.create(&kit).await;
    let first = dynamic_worker("first", "wor");
    let second = dynamic_worker("second", "work");
    let third = dynamic_worker("third", "other");
    for worker in [&first, &second, &third] {
        backend.serve(worker.clone()).await.unwrap();
    }
    let before = kit
        .agent_presence(&room.id, roomkit::AgentPresenceOptions::default())
        .await
        .unwrap();
    first.close().await;
    let after = kit
        .agent_presence(&room.id, roomkit::AgentPresenceOptions::default())
        .await
        .unwrap();
    second.close().await;
    third.close().await;
    kit.close().await.unwrap();

    assert_eq!(before.first().unwrap().executors, ["first", "second"]);
    assert_eq!(after.first().unwrap().executors, ["second"]);
}

#[tokio::test]
async fn the_catalogue_excludes_only_the_attached_owners_executor() {
    let node = test_nodes(Duration::from_millis(50));
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
    let node = test_nodes(Duration::from_millis(50));
    let backend = node();
    let owner = RoomKit::new(RoomKitOptions {
        turn_backend: Some(roomkit::turns::TurnBackendHandle::new(Arc::new(
            backend.clone(),
        ))),
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
    let weak = owner.downgrade();
    drop(before);
    drop(own);
    drop(owner);
    assert!(weak.upgrade().is_none());
}

fn unix_millis() -> i64 {
    SystemEnvironment.now().as_unix_millis()
}
