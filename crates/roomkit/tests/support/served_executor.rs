//! The same worker contract across processes over a kit with a concrete durable store.
use std::future::ready;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventType, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::turns::{
    DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker,
    InMemoryTurnRegistry, TurnBackend,
};
use roomkit::{
    AgentIdentity, AttachChannelOptions, Channel, ChannelDeclaration, ChannelError,
    ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
    TurnTimeouts, WeakRoomKit,
};

struct StoreReader<S> {
    worker: WeakRoomKit<S>,
    room: RoomId,
    channel: ChannelId,
}

impl<S: ConversationStore> Channel for StoreReader<S> {
    fn channel_id(&self) -> ChannelId {
        self.channel.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        ctx: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &ctx)))
    }

    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        ctx: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let worker = self.worker.upgrade().unwrap();
        let room = worker.store().get_room(&self.room).await.unwrap().unwrap();
        EchoAgent::new(
            self.channel.clone(),
            room.id.to_string(),
            EchoAgentOptions::default(),
        )
        .on_event(event, binding, ctx)
        .await
    }
}

/// A node of a distributed backend, on a broker and a registry of this process.
fn backend() -> DistributedTurnBackend {
    DistributedTurnBackend::new(
        Arc::new(InMemoryTurnBroker::new()),
        Arc::new(InMemoryTurnRegistry::new()),
        DistributedTurnBackendOptions {
            timeouts: TurnTimeouts {
                heartbeat_interval: Duration::from_millis(50),
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

pub(crate) async fn exercise<S: ConversationStore>(worker: RoomKit<S>) {
    let node = backend();
    let agent = ChannelId::new("agent").unwrap();
    // Only this worker's store contains the answer. Its real channel is reachable
    // through its kit's executor, while the owner holds the declaration alone.
    let answer = worker
        .create_room(CreateRoomOptions::default())
        .await
        .unwrap()
        .id;
    worker
        .register_channel(StoreReader {
            worker: worker.downgrade(),
            room: answer.clone(),
            channel: agent.clone(),
        })
        .unwrap();
    let executor = worker.turn_executor();
    TurnBackend::serve(&node, Arc::new(executor.clone()))
        .await
        .unwrap();
    assert!(
        node.executors()
            .await
            .unwrap()
            .iter()
            .any(|row| row.executor_id == executor.executor_id())
    );

    let owner = RoomKit::new(RoomKitOptions {
        turn_backend: Some(roomkit::turns::TurnBackendHandle::new(Arc::new(
            node.clone(),
        ))),
        ..Default::default()
    });
    let (room, source) = owner_room(&owner, &agent).await;
    let inbound = owner
        .process_inbound(
            InboundMessage::new(source, "person", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(inbound.error, None);
    let events = owner
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await
        .unwrap();
    let replies: Vec<_> = events
        .iter()
        .filter(|row| row.source.channel_id == agent && row.r#type == EventType::Message)
        .map(|row| &row.content)
        .collect();
    assert!(
        matches!(replies.as_slice(), [EventContent::Text(text)] if text.body == answer.as_str())
    );
    executor.close().await;
    assert!(
        node.executors()
            .await
            .unwrap()
            .iter()
            .all(|row| row.executor_id != executor.executor_id())
    );
    owner.close().await.unwrap();
    worker.close().await.unwrap();
}

async fn owner_room(owner: &RoomKit, agent: &ChannelId) -> (RoomId, ChannelId) {
    let source = ChannelId::new("text").unwrap();
    owner
        .register_channel(RecordingTransport::new(
            source.clone(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
    owner
        .register_channel(ChannelDeclaration {
            identity: Some(AgentIdentity::default()),
            ..ChannelDeclaration::new(agent.clone())
        })
        .unwrap();
    let room = owner
        .create_room(CreateRoomOptions::default())
        .await
        .unwrap()
        .id;
    owner
        .attach_channel(&room, &source, AttachChannelOptions::default())
        .await
        .unwrap();
    owner
        .attach_channel(
            &room,
            agent,
            AttachChannelOptions {
                category: Some(ChannelCategory::Intelligence),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (room, source)
}
