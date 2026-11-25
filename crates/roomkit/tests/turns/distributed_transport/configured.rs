//! An ordinary kit's first channel control announces its executor to the registry.
use std::sync::Arc;

use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId,
    TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::turns::TurnBackendHandle;
use roomkit::turns::wire::Frame;
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, TurnExecutor, TurnExecutorOptions,
};
use uuid::Uuid;

fn kit(backend: &TurnBackendHandle, name: &str) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(backend.clone()),
        ..Default::default()
    });
    let request = super::request();
    kit.register_channel(EchoAgent::new(
        request.channel_id.clone(),
        name,
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit
}

async fn attach(kit: &RoomKit, name: &str) {
    let request = super::request();
    let room = RoomId::new(name).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room,
        &request.channel_id,
        AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

async fn assert_chosen(backend: &TurnBackendHandle, expected: &str) {
    let mut request = super::request();
    request.turn_id = Uuid::new_v4().to_string();
    let link = backend.submit(request).await.unwrap();
    assert_eq!(
        link.receive().await.unwrap(),
        (
            1,
            Frame::Accepted {
                executor_id: expected.into()
            }
        )
    );
    while !matches!(link.receive().await.unwrap().1, Frame::End) {}
    link.release();
}

#[tokio::test]
async fn a_kits_first_attach_reserves_its_live_channel_even_with_other_workers() {
    let backend = super::deployment()();
    let peer = TurnExecutor::new(
        |id| async move {
            Ok(Some(EchoAgent::new(
                id,
                "peer",
                EchoAgentOptions::default(),
            )))
        },
        TurnExecutorOptions {
            hosts: Some(["ai".into()].into()),
            executor_id: Some("already-served".into()),
            ..Default::default()
        },
    )
    .unwrap();
    backend.serve(Arc::new(peer.clone())).await.unwrap();
    assert_chosen(&backend, "already-served").await;
    let first = kit(&backend, "first");
    attach(&first, "first").await;
    let second = kit(&backend, "second");
    attach(&second, "second").await;
    assert_reply(&second, "second", "first").await;
    assert_chosen(&backend, first.turn_executor().executor_id()).await;
    let rows = backend.executors().await.unwrap();
    assert!(
        rows.iter()
            .any(|row| row.executor_id == second.turn_executor().executor_id())
    );
    assert!(
        rows.iter()
            .all(|row| row.executor_id != first.turn_executor().executor_id())
    );
    peer.close().await;
    first.close().await.unwrap();
    second.close().await.unwrap();
}

async fn assert_reply(kit: &RoomKit, room: &str, answer: &str) {
    let room = RoomId::new(room).unwrap();
    let source = ChannelId::new("text").unwrap();
    kit.register_channel(RecordingTransport::new(
        source.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.attach_channel(&room, &source, AttachChannelOptions::default())
        .await
        .unwrap();
    let result = kit
        .process_inbound(
            InboundMessage::new(source, "person", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.error, None);
    let events = kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await
        .unwrap();
    let answers = events
        .iter()
        .filter(|row| row.r#type == EventType::Message && row.source.channel_id.as_str() == "ai")
        .map(|row| &row.content)
        .collect::<Vec<_>>();
    assert!(matches!(answers.as_slice(), [EventContent::Text(text)] if text.body == answer));
}
