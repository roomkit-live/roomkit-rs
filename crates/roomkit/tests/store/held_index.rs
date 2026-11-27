//! Python: `tests/test_store_memory.py::TestEventOperations::test_an_index_an_update_moved_an_event_to_is_held`.

use roomkit::model::{
    ChannelId, ChannelType, EventId, EventSource, Room, RoomEvent, RoomId, TextContent,
};
use roomkit::{ConversationStore, InMemoryStore, StoreError};

/// `id` of `room_1` at `index`, a text from SMS.
fn event(id: &str, index: u64) -> RoomEvent {
    let mut event = RoomEvent {
        id: EventId::new(id).unwrap(),
        ..RoomEvent::new(
            RoomId::new("room_1").unwrap(),
            EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
            TextContent::new("hi"),
        )
    };
    event.index = index;
    event
}

/// RFC §8.1: the index an update writes is held like any other.
#[tokio::test]
async fn an_index_an_update_moved_an_event_to_is_held() {
    let store = InMemoryStore::new();
    store
        .create_room(Room::new(RoomId::new("room_1").unwrap()))
        .await
        .unwrap();
    store.add_event(event("evt_0", 0)).await.unwrap();
    store.update_event(event("evt_0", 10)).await.unwrap();
    assert_eq!(
        store.add_event(event("evt_1", 10)).await.map(drop),
        Err(StoreError::EventIndexTaken {
            room_id: RoomId::new("room_1").unwrap(),
            index: 10,
        })
    );
}
