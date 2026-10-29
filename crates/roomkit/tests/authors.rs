//! The record of a turn's author and the room's register of authors on the
//! tokio runtime (RFC §6.4, §10.1 step 12, §14.1, gap G-089): the same
//! messages as probe 9 of docs/steps/step-03.md, sent to the Python
//! implementation (roomkit 2d6e6267), give the same records, digest for
//! digest and rank for rank, and the same register, which the store keeps
//! apart from the room's metadata.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, EventType, InboundMessage, RoomId, TextContent};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};
use serde_json::{Value, json};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

/// A record of an author: its name, rank and source.
type Record = (&'static str, u64, &'static str);

/// What each message is, and the record Python committed it with.
const SENT: [(&str, &str, Option<&str>, Option<Record>); 11] = [
    (
        "sms",
        "+15550001",
        Some("Alice"),
        Some(("Alice", 1, "b53db63d2136fd6f")),
    ),
    (
        "ws",
        "u-2",
        Some("ALICE"),
        Some(("ALICE", 2, "f0a09d682358e721")),
    ),
    (
        "sms",
        "+15550003",
        Some("\u{410}lice"),
        Some(("\u{410}lice", 3, "ca20ab0afe7d2a54")),
    ),
    (
        "sms",
        "+15550001",
        Some("Alice"),
        Some(("Alice", 1, "b53db63d2136fd6f")),
    ),
    (
        "ws",
        "u-5",
        Some("Bob"),
        Some(("Bob", 1, "b8e5274fc03bd858")),
    ),
    (
        "ws",
        "u-6",
        Some("Lan"),
        Some(("Lan", 1, "9ee799f0eaf8ebc3")),
    ),
    (
        "ws",
        "u-7",
        Some("Ian"),
        Some(("Ian", 2, "e651944b10264c12")),
    ),
    (
        "ws",
        "u-8",
        Some("ian"),
        Some(("ian", 3, "eefc94fb4355ccab")),
    ),
    (
        "ws",
        "u-9",
        Some("  Jean-Luc\u{200b}  "),
        Some(("Jean-Luc", 1, "0aa75761374fd7f9")),
    ),
    (
        "ws",
        "u-10",
        Some("Jean Luc"),
        Some(("Jean Luc", 2, "9f511ab940ace609")),
    ),
    ("sms", "+15550011", None, None),
];

#[tokio::test]
async fn names_that_read_alike_are_ranked_as_the_python_implementation_ranks_them() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("room_1").unwrap();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room, &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    for (from, sender, name, _) in SENT {
        let mut message = InboundMessage::new(channel(from), sender, TextContent::new("hi"));
        if let Some(name) = name {
            message
                .metadata
                .insert("sender_name".to_owned(), name.into());
        }
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    let records: Vec<Option<Value>> = kit
        .store()
        .list_events(&room, &every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.r#type == EventType::Message)
        .map(|event| event.metadata.get("author").cloned())
        .collect();
    let expected: Vec<Option<Value>> = SENT
        .iter()
        .map(|(_, _, _, record)| {
            record
                .map(|(name, rank, source)| json!({ "name": name, "rank": rank, "source": source }))
        })
        .collect();
    let register = kit
        .store()
        .get_private_state(&room, "author_register")
        .await
        .unwrap()
        .map(Value::Object);
    let stored = kit.store().get_room(&room).await.unwrap().unwrap();
    assert_eq!(
        (
            records,
            register,
            stored.metadata.contains_key("author_register")
        ),
        (
            expected,
            Some(json!({
                "names": {
                    "0342973908f267c0": "b53db63d2136fd6f:1 f0a09d682358e721:2 ca20ab0afe7d2a54:3",
                    "3d5f15e1b15907af": "e651944b10264c12:2 eefc94fb4355ccab:3",
                    "87bd064c9cadb523": "f0a09d682358e721:2",
                    "af42c677a85cf601": "b8e5274fc03bd858:1",
                    "cdb9781382facb1e": "9ee799f0eaf8ebc3:1 e651944b10264c12:2",
                    "d3477034863f2409": "0aa75761374fd7f9:1 9f511ab940ace609:2"
                },
                "version": 3
            })),
            false
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
