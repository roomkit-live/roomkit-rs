//! The addresses a host gives a binding, written by the channel's own
//! methods on the tokio runtime (RFC §5.7, §10.4): the correspondent in the
//! form the channel writes an inbound sender in, the recipient where the
//! channel reads it, and a binding that names no one but delivers to an
//! address naming that person. The conformance scenarios
//! `rooms/an-attach-names-its-correspondent-in-the-channel-s-form`,
//! `routing/a-sender-is-read-in-the-channel-s-own-form`,
//! `routing/a-message-is-routed-by-the-channel-s-conversation`,
//! `routing/a-routed-sender-is-recorded-where-the-channel-replies` and
//! `streaming/a-channel-streams-in-the-rooms-it-says` pin where the kit
//! calls each method for any channel; this pins a `TransportChannel`'s.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, Metadata, ParticipantId, RoomId};
use roomkit::{
    AttachChannelOptions, CountryCode, CreateRoomOptions, RoomKit, RoomKitOptions,
    TransportChannel, TransportChannelOptions,
};

const ALICE: &str = "+15550000001";
const BOB: &str = "+15550000002";

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// Python: `tests/test_transport_recipient.py::test_a_host_address_is_written_in_e164`,
/// its attaches (roomkit-rs has no `add_member`).
#[tokio::test]
async fn a_host_address_is_written_in_e164() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = ChannelId::new("sms").unwrap();
    kit.register_channel(TransportChannel::new(
        sms.clone(),
        ChannelType::SMS,
        TransportChannelOptions {
            country_code: Some(CountryCode::new("1").unwrap()),
            ..TransportChannelOptions::default()
        },
    ))
    .unwrap();
    for id in ["r1", "r2"] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room(id)),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
    }
    let binding = kit
        .attach_channel(
            &room("r1"),
            &sms,
            AttachChannelOptions {
                participant_id: Some(ParticipantId::new("555-000-0001").unwrap()),
                ..AttachChannelOptions::default()
            },
        )
        .await
        .unwrap();
    let mut metadata = Metadata::new();
    metadata.insert("phone_number".to_owned(), "555.000.0002".into());
    let recipient_only = kit
        .attach_channel(
            &room("r2"),
            &sms,
            AttachChannelOptions {
                metadata,
                ..AttachChannelOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (
            binding.participant_id.map(String::from),
            recipient_only.participant_id.map(String::from),
            recipient_only.metadata.get("phone_number").cloned()
        ),
        (
            Some(ALICE.to_owned()),
            Some(BOB.to_owned()),
            Some(BOB.into())
        )
    );
    kit.close().await.unwrap();
}
