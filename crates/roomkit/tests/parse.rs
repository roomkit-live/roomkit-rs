//! What the inbound pipeline keeps of a channel's parse on the tokio
//! runtime (RFC §10.1 step 4): a field the channel set stands, and the
//! message's fills only the ones it left unset, as Python's
//! `_apply_message_fields` does. The conformance scenarios
//! `inbound/a-field-the-parse-set-stands` and
//! `inbound/a-key-read-off-the-wire-replays-a-redelivery` pin the rule for
//! any channel; this pins what a `TransportChannel` reads itself.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, EventType, InboundMessage, MediaContent, RoomId};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, TransportChannel, TransportChannelOptions,
};

/// A kit whose room `room_1` binds an SMS transport, `sms`.
async fn sms_room() -> (RoomKit, RoomId) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = ChannelId::new("sms").unwrap();
    let room = RoomId::new("room_1").unwrap();
    kit.register_channel(TransportChannel::new(
        sms.clone(),
        ChannelType::SMS,
        TransportChannelOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &sms, AttachChannelOptions::default())
        .await
        .unwrap();
    (kit, room)
}

/// Python: `TransportChannel.handle_inbound` (roomkit
/// `src/roomkit/channels/transport.py`), which sources an SMS with media
/// as `MMS`; no Python test pins it.
#[tokio::test]
async fn an_sms_with_media_is_stored_as_an_mms() {
    let (kit, room) = sms_room().await;
    let photo = MediaContent::new("https://example.com/photo.jpg", "image/jpeg").unwrap();
    let message = InboundMessage::new(ChannelId::new("sms").unwrap(), "+15551234567", photo);
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let stored = kit
        .store()
        .list_events(&room, roomkit::store::ListEventsOptions::default())
        .await;
    let types: Vec<ChannelType> = stored
        .unwrap()
        .into_iter()
        .filter(|event| event.r#type == EventType::Message)
        .map(|event| event.source.channel_type)
        .collect();
    assert_eq!(types, [ChannelType::MMS]);
    kit.close().await.unwrap();
}
