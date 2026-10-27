//! The store suite: what every [`ConversationStore`] answers (RFC §14),
//! run on the store the including crate names.
//!
//! The crate that includes this module defines `Store`, the store's type,
//! and `store()`, which opens an empty one: the in-memory store here
//! (`tests/store/main.rs`), the SQLite store in roomkit-sqlite, whose suite
//! includes this directory by path.

mod events;
mod identities;
mod members;
mod rooms;
mod routing;
mod timeline;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelType, EditContent,
    EventContent, EventDirection, EventId, EventSource, EventStatus, EventType, ParticipantId,
    Room, RoomEvent, RoomId, RoomUpdates, Timestamp,
};
use roomkit::{ConversationStore, RoomCounters};

use crate::Store;

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn event_id(id: &str) -> EventId {
    EventId::new(id).unwrap()
}

/// `evt_{index}` of `room_1`, a text from SMS.
pub(crate) fn event(index: u64) -> RoomEvent {
    let mut event = RoomEvent::new(
        event_id(&format!("evt_{index}")),
        room_id(),
        EventType::Message,
        EventSource::new(channel("sms"), ChannelType::SMS, EventDirection::Inbound),
        EventContent::text("hi"),
        Timestamp::UNIX_EPOCH,
    );
    event.index = index;
    event
}

/// `evt_{index}` stored `status`.
pub(crate) fn event_stored(index: u64, status: EventStatus) -> RoomEvent {
    let mut event = event(index);
    event.status = status;
    event
}

/// `evt_{index}`, a reply in the thread of `evt_{parent}`.
pub(crate) fn reply(index: u64, parent: u64) -> RoomEvent {
    let mut reply = event(index);
    reply.parent_event_id = Some(event_id(&format!("evt_{parent}")));
    reply
}

/// `evt_{index}`, an edit of `evt_{target}`.
pub(crate) fn edit_of(index: u64, target: u64) -> RoomEvent {
    let mut edit = event(index);
    edit.event_type = EventType::Edit;
    edit.content = EventContent::Edit(EditContent::new(
        event_id(&format!("evt_{target}")),
        EventContent::text("corrected"),
    ));
    edit
}

/// The counters of a room once `index` is committed after every index
/// before it.
pub(crate) fn counters(index: u64) -> RoomCounters {
    RoomCounters {
        event_count: index + 1,
        latest_index: index,
        last_activity_at: None,
        room: RoomUpdates::default(),
    }
}

/// An empty store holding `room_1`.
pub(crate) async fn with_room() -> Store {
    let store = crate::store().await;
    store
        .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
        .await
        .unwrap();
    store
}

/// A store holding `room_1` and `events`, committed in their order.
pub(crate) async fn with_events(events: Vec<RoomEvent>) -> Store {
    let store = with_room().await;
    for committed in events {
        let index = committed.index;
        store
            .commit(committed, counters(index), None)
            .await
            .unwrap();
    }
    store
}

/// The ids of `events`.
pub(crate) fn ids(events: &[RoomEvent]) -> Vec<String> {
    events.iter().map(|event| event.id.to_string()).collect()
}

/// `room_1`'s binding of an SMS channel `id`, naming no one.
pub(crate) fn binding(id: &str) -> ChannelBinding {
    ChannelBinding::new(
        channel(id),
        room_id(),
        ChannelType::SMS,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
        Timestamp::UNIX_EPOCH,
    )
}

pub(crate) fn participant_id(id: &str) -> ParticipantId {
    ParticipantId::new(id).unwrap()
}
