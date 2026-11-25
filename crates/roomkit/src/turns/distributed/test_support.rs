//! What the placement tests share, on any broker and registry: a worker
//! that answers its name, a room with a transport and an agent, and the
//! reply a turn leaves there.
use std::collections::BTreeSet;

use crate::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId,
    TextContent,
};
use crate::store::ListEventsOptions;
use crate::testing::{EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions};
use crate::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, TurnExecutor, TurnExecutorOptions,
};

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
