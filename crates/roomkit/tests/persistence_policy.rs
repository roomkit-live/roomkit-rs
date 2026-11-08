//! The kit's persistence policy (RFC §14.3), as the store holds what it
//! keeps: what the shared scenarios cannot read, since they leave the
//! lifecycle records out of every index (roomkit-specs
//! `conformance/README.md`, "Normalization").

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeSet;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventType, InboundMessage, PersistencePolicy, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A kit storing messages alone, its room `r1` binding the transports `a`
/// and `b`: the attaches' lifecycle records are not stored.
async fn a_room_storing_messages_alone() -> RoomKit {
    let policy = PersistencePolicy {
        persist_types: Some(BTreeSet::from([EventType::Message])),
        ..PersistencePolicy::default()
    };
    let kit = RoomKit::new(RoomKitOptions {
        persistence_policy: Some(policy),
        ..Default::default()
    });
    let options = CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    for id in ["a", "b"] {
        let transport = RecordingTransport::new(
            ChannelId::new(id).unwrap(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport.clone()).unwrap();
        kit.attach_channel(
            &room_id(),
            transport.channel_id(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}

// Python: tests/test_latest_index.py::TestAnEmptyRoom::test_takes_its_first_event_s_index_as_latest
#[tokio::test]
async fn takes_its_first_event_s_index_as_latest() {
    let kit = a_room_storing_messages_alone().await;
    let message = InboundMessage::new(ChannelId::new("a").unwrap(), "u", TextContent::new("hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap();

    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    let every_row = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    };
    let timeline = kit
        .store()
        .list_events(&room_id(), every_row)
        .await
        .unwrap();
    let types: Vec<EventType> = timeline.iter().map(|event| event.r#type).collect();
    assert_eq!(
        (room.event_count, room.latest_index, types),
        (1, Some(0), vec![EventType::Message])
    );
    kit.close().await.unwrap();
}
