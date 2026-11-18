//! A serving process publishes changes before serve and worker close return.
use super::{Arc, InMemoryTurnBackend, TurnBackend, echo};
use roomkit::model::ChannelCategory;
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ChannelError, CreateRoomOptions,
};
use roomkit::{RoomKit, RoomKitOptions};
use tokio::sync::mpsc;

struct Publications {
    room: InMemoryRealtime,
    events: mpsc::UnboundedSender<EphemeralEventType>,
}

impl RealtimeBackend for Publications {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), ChannelError>> {
        if event.user_id == "ai"
            && matches!(
                event.r#type,
                EphemeralEventType::PresenceOnline | EphemeralEventType::PresenceOffline
            )
        {
            self.events.send(event.r#type).unwrap();
        }
        self.room.publish(channel, event)
    }
    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.room.subscribe(channel, callback)
    }
    fn unsubscribe<'a>(&'a self, id: &'a str) -> RealtimeFuture<'a, bool> {
        self.room.unsubscribe(id)
    }
}

#[tokio::test(start_paused = true)]
async fn the_public_backend_publishes_each_agent_transition_before_returning() {
    let backend = Arc::new(InMemoryTurnBackend::new());
    let (sent, mut events) = mpsc::unbounded_channel();
    let realtime = Arc::new(Publications {
        room: InMemoryRealtime::default(),
        events: sent,
    });
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(Arc::<InMemoryTurnBackend>::clone(&backend)),
        realtime: Some(realtime),
        ..Default::default()
    });
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let agent: roomkit::model::ChannelId = "ai".parse().unwrap();
    kit.register_channel(ChannelDeclaration {
        identity: Some(AgentIdentity::default()),
        ..ChannelDeclaration::new(agent.clone())
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room,
        &agent,
        AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        events.try_recv().unwrap(),
        EphemeralEventType::PresenceOffline
    );
    tokio::task::yield_now().await;
    let worker = echo("worker");
    backend
        .serve(Arc::<roomkit::TurnExecutor>::clone(&worker))
        .await
        .unwrap();
    assert_eq!(
        events.try_recv().unwrap(),
        EphemeralEventType::PresenceOnline
    );
    backend
        .serve(Arc::<roomkit::TurnExecutor>::clone(&worker))
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
    worker.close().await;
    assert_eq!(
        events.try_recv().unwrap(),
        EphemeralEventType::PresenceOffline
    );
    assert!(events.try_recv().is_err());
    kit.close().await.unwrap();
}
