//! A transport with the four required operations inherits per-backend presence.
use roomkit::model::{ChannelCategory, ChannelId, RoomId};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::{
    ChannelControl, ChannelResult, ServedExecutor, TurnBackend, TurnBackendHandle, TurnClient,
    TurnClientOptions, TurnFuture, TurnLink, TurnRequest,
};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DetachChannelOptions, RoomKit, RoomKitError,
    RoomKitOptions,
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
