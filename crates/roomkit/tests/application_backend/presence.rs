//! A transport with the four required operations inherits per-backend presence.
use roomkit::model::{ChannelCategory, ChannelId, RoomId};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ServedExecutor, TurnBackend, TurnBackendHandle,
    TurnClient, TurnClientOptions, TurnFuture, TurnLink, TurnRequest,
};
use roomkit::{
    AttachChannelOptions, ChannelDeclaration, CreateRoomOptions, DetachChannelOptions, RoomKit,
    RoomKitError, RoomKitOptions,
};
use std::sync::Arc;

struct App;

impl TurnBackend for App {
    fn submit(&self, _: TurnRequest) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async { Err(RoomKitError::KitClosed) })
    }
    fn serve(&self, _: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
    fn control(
        &self,
        _: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
}

async fn attach_agent(kit: &RoomKit, room: &RoomId, agent: &ChannelId) {
    kit.register_channel(EchoAgent::new(
        agent.clone(),
        "reply",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
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

async fn claim(backend: &TurnBackendHandle, online: Option<bool>) -> bool {
    backend
        .claim_presence("agent", "room", online)
        .await
        .unwrap()
}

#[tokio::test]
async fn kit_client_and_direct_calls_share_only_their_backends_claims() {
    let backend = TurnBackendHandle::new(Arc::new(App));
    let other = TurnBackendHandle::new(Arc::new(App));
    let client = TurnClient::new(backend.clone(), TurnClientOptions::default());
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(backend.clone()),
        ..Default::default()
    });
    let room = RoomId::new("room").unwrap();
    let agent = ChannelId::new("agent").unwrap();
    attach_agent(&kit, &room, &agent).await;
    assert!(!claim(client.backend(), Some(true)).await);
    assert!(claim(&backend, Some(false)).await);
    assert!(claim(&other, Some(false)).await);

    kit.detach_channel(&room, &agent, DetachChannelOptions::default())
        .await
        .unwrap();
    assert!(claim(client.backend(), Some(false)).await);
    assert!(!claim(&backend, None).await);
    assert!(claim(client.backend(), Some(false)).await);
    assert!(!claim(&other, Some(false)).await);
    client.close().await.unwrap();
    kit.close().await.unwrap();
}

/// Application transport which supplies an authoritative, empty catalogue.
struct EmptyCatalogue;

impl TurnBackend for EmptyCatalogue {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        App.submit(request)
    }
    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        App.serve(executor)
    }
    fn control(
        &self,
        request: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        App.control(request)
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        App.close()
    }
    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        Some(Box::pin(async { Ok(Vec::new()) }))
    }
}

#[tokio::test]
async fn only_a_backend_with_a_catalogue_announces_that_a_declared_agent_is_offline() {
    let transports: [(Arc<dyn TurnBackend>, bool); 2] =
        [(Arc::new(App), false), (Arc::new(EmptyCatalogue), true)];
    for (transport, has_catalogue) in transports {
        let backend = TurnBackendHandle::new(transport);
        let kit = RoomKit::new(RoomKitOptions {
            turn_backend: Some(backend.clone()),
            ..Default::default()
        });
        let room = RoomId::new("room").unwrap();
        let channel = ChannelId::new("agent").unwrap();
        kit.register_channel(ChannelDeclaration::new(channel.clone()))
            .unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await
            .unwrap();
        assert_eq!(kit.executors().await.unwrap(), []);
        assert_eq!(backend.executors().await.unwrap(), []);
        // Attachment claims offline only if the transport actually supplied the empty list.
        assert_eq!(claim(&backend, Some(false)).await, !has_catalogue);
        kit.close().await.unwrap();
    }
}
