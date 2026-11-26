//! What the backend's tests share: the services of one test, kept in this
//! process, the nodes on them, a worker that answers its name, a room with
//! a transport and an agent, and the reply a turn leaves there.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{
    BrokerMessage, DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker,
    InMemoryTurnRegistry, TurnBroker, TurnInbox, TurnRegistry,
};
use crate::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId,
    TextContent,
};
use crate::store::ListEventsOptions;
use crate::testing::{EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions};
use crate::turns::{TurnFuture, lock};
use crate::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, TurnExecutor, TurnExecutorOptions, TurnTimeouts,
};

/// A test's broker: this process's, which keeps what was posted to each
/// node and refuses the nodes a test names, as a service that cannot write
/// one inbox.
#[derive(Default)]
pub(super) struct WatchedBroker {
    carried: InMemoryTurnBroker,
    posted: Mutex<Vec<(String, BrokerMessage)>>,
    /// The nodes whose posts are refused, with how many were.
    refused: Mutex<BTreeMap<String, usize>>,
}

impl WatchedBroker {
    /// Every message `node`'s inbox received, in the order it did.
    pub(super) fn posted(&self, node: &str) -> Vec<BrokerMessage> {
        lock(&self.posted)
            .iter()
            .filter(|(to, _)| to == node)
            .map(|(_, message)| message.clone())
            .collect()
    }

    /// Has each post to `node` fail until [`Self::accept`].
    pub(super) fn refuse(&self, node: &str) {
        lock(&self.refused).entry(node.to_owned()).or_default();
    }

    /// Has posts to `node` succeed again.
    pub(super) fn accept(&self, node: &str) {
        lock(&self.refused).remove(node);
    }

    /// How many posts to `node` failed since it was last refused.
    pub(super) fn refusals(&self, node: &str) -> usize {
        lock(&self.refused).get(node).copied().unwrap_or(0)
    }

    /// Returns once a post to `node` failed: the backend tried it.
    pub(super) async fn refused_once(&self, node: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.refusals(node) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the backend posts to the refused node");
    }
}

impl TurnBroker for WatchedBroker {
    fn post<'a>(
        &'a self,
        node: &'a str,
        messages: &'a [BrokerMessage],
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            if let Some(refusals) = lock(&self.refused).get_mut(node) {
                *refusals += 1;
                return Err(RoomKitError::Runtime(format!(
                    "the test broker refuses the inbox of {node}"
                )));
            }
            self.carried.post(node, messages).await?;
            let posted = messages
                .iter()
                .cloned()
                .map(|message| (node.to_owned(), message));
            lock(&self.posted).extend(posted);
            Ok(())
        })
    }

    fn inbox<'a>(
        &'a self,
        node: &'a str,
        block: Duration,
    ) -> TurnFuture<'a, Result<Box<dyn TurnInbox>, RoomKitError>> {
        self.carried.inbox(node, block)
    }

    fn acknowledge<'a>(
        &'a self,
        node: &'a str,
        ids: &'a [String],
    ) -> TurnFuture<'a, Result<(), RoomKitError>> {
        self.carried.acknowledge(node, ids)
    }
}

/// The services the nodes of one test share: no other test reaches them.
#[derive(Clone, Default)]
pub(super) struct Services {
    pub(super) broker: Arc<WatchedBroker>,
    pub(super) registry: Arc<InMemoryTurnRegistry>,
}

impl Services {
    /// A node of these services.
    pub(super) fn node(&self, options: DistributedTurnBackendOptions) -> DistributedTurnBackend {
        let broker: Arc<dyn TurnBroker> = Arc::<WatchedBroker>::clone(&self.broker);
        let registry: Arc<dyn TurnRegistry> = Arc::<InMemoryTurnRegistry>::clone(&self.registry);
        DistributedTurnBackend::new(broker, registry, options)
    }

    /// A node with the backend's default options.
    pub(super) fn default_node(&self) -> DistributedTurnBackend {
        self.node(DistributedTurnBackendOptions::default())
    }
}

/// Two nodes of one deployment, beating every 50 ms.
pub(super) fn nodes_with_loop_back(
    loop_back: bool,
) -> (DistributedTurnBackend, DistributedTurnBackend) {
    let (_, node, peer) = served_nodes(loop_back);
    (node, peer)
}

/// Two nodes of one deployment, with the services they share.
pub(super) fn served_nodes(
    loop_back: bool,
) -> (Services, DistributedTurnBackend, DistributedTurnBackend) {
    let services = Services::default();
    let options = DistributedTurnBackendOptions {
        timeouts: TurnTimeouts {
            heartbeat_interval: Duration::from_millis(50),
            ..Default::default()
        },
        _loop_back: loop_back,
    };
    let (node, peer) = (services.node(options.clone()), services.node(options));
    (services, node, peer)
}

pub(super) fn worker(name: &'static str) -> TurnExecutor {
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

pub(super) fn register_channels(
    kit: &RoomKit,
    tx: &ChannelId,
    agent: &ChannelId,
    own_channel: bool,
) {
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

pub(super) async fn attach_agents_room(
    kit: &RoomKit,
    room: &RoomId,
    tx: &ChannelId,
    agent: &ChannelId,
) {
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

pub(super) async fn assert_room_reply(
    kit: &RoomKit,
    room: &RoomId,
    tx: &ChannelId,
    agent: &ChannelId,
) {
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
