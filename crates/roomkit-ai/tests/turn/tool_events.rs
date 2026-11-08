//! A round's tool calls on the room's realtime (RFC §6.4, §8.4): what a
//! subscriber can read of them in the store when their events arrive.

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::EventType;
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::store::ConversationStore;
use roomkit::{InMemoryStore, RoomKit, RoomKitOptions, SubscribeRoomOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};

use super::support::{Room, channel, every_row, room_id, served};

/// A tool call event's type and how many rows of its kind the store held
/// when it arrived.
type Read = (EphemeralEventType, usize);

/// A subscriber that reads the stored tool rows on each tool call event
/// finds the row of its kind stored: the START row on `TOOL_CALL_START`,
/// the END row on `TOOL_CALL_END`, which is published once the reader took
/// the round's end rows, as Python's is once its end markers are consumed.
#[tokio::test]
async fn a_call_s_end_row_is_stored_when_its_realtime_end_arrives() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "1"}"#)]),
        ScriptedRound::answering("Shipped."),
    ]);
    let options = AiChannelOptions {
        tools: served().into(),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), model, options).unwrap();
    let store = Arc::new(InMemoryStore::new());
    let kit = RoomKit::new(RoomKitOptions {
        store: Arc::clone(&store),
        ..RoomKitOptions::default()
    });
    let room = Room::on_kit(kit, agent).await;
    let read: Arc<Mutex<Vec<Read>>> = Arc::default();
    let seen = Arc::clone(&read);
    let record = move |event: EphemeralEvent| {
        let (seen, store) = (Arc::clone(&seen), Arc::clone(&store));
        async move {
            let kind = if event.r#type == EphemeralEventType::ToolCallStart {
                EventType::ToolCallStart
            } else if event.r#type == EphemeralEventType::ToolCallEnd {
                EventType::ToolCallEnd
            } else {
                return;
            };
            let rows = store.list_events(&room_id(), every_row()).await.unwrap();
            let stored = rows.iter().filter(|row| row.r#type == kind).count();
            seen.lock().unwrap().push((event.r#type, stored));
        }
    };
    room.kit
        .subscribe_room(&room_id(), record, SubscribeRoomOptions::default())
        .await
        .unwrap();
    room.says("Where is order 1?").await;
    roomkit::testing::idle(&room.kit).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    room.kit.close().await.unwrap();
    assert_eq!(
        read.lock().unwrap().clone(),
        [
            (EphemeralEventType::ToolCallStart, 1),
            (EphemeralEventType::ToolCallEnd, 1),
        ]
    );
}
