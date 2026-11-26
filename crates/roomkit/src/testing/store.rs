//! What every [`ConversationStore`] answers (RFC §14), checked on any
//! store: the core's own, in its tests, and a store written outside this
//! crate, by its own tests.
//!
//! Each check panics, saying what, where the contract is not kept. Each
//! opens an empty store of its own, and writes the room `room_1`.
#![expect(
    clippy::unwrap_used,
    reason = "a check fails its test by panicking, as an assertion does"
)]

mod events;
mod identities;
mod members;
mod rooms;
mod routing;
mod timeline;

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;

use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EditContent, EventContent, EventId, EventSource,
    EventStatus, EventType, ParticipantId, Room, RoomEvent, RoomId, TextContent, Timestamp,
};
use crate::store::CommitEventOptions;
use crate::{ConversationStore, StoreError};

/// What a check opens an empty store with.
type Open<'a, T> = &'a (dyn Fn() -> Pin<Box<dyn Future<Output = T> + 'a>> + 'a);

/// What `open` gives: the store, or what holds it while the check runs
/// (a store on a file or a schema of its own, removed with it).
trait Held: Deref<Target = Self::Store> {
    /// The store it holds.
    type Store: ConversationStore + ?Sized;
}

impl<T> Held for T
where
    T: Deref,
    T::Target: ConversationStore,
{
    type Store = T::Target;
}

/// Checks that the store `open` opens keeps the contract of
/// [`ConversationStore`] (RFC §14): its rooms, their events and timeline,
/// their members, its identities and its routing lookups are read as they
/// were written, and its commits and changes refuse what the contract
/// refuses. `open` opens an empty store at each call, and gives the store
/// or what holds it while a check runs (a `Box` of an in-memory store, a
/// store on a file of its own removed with it): each check opens its own,
/// and drops it when it is done.
///
/// # Panics
///
/// Where the store does not keep the contract, saying what.
pub async fn check_conversation_store<T, F, Fut>(open: F)
where
    T: Deref,
    T::Target: ConversationStore,
    F: Fn() -> Fut,
    Fut: Future<Output = T>,
{
    let open = || -> Pin<Box<dyn Future<Output = T> + '_>> { Box::pin(open()) };
    let open: Open<'_, T> = &open;
    Box::pin(rooms::check(open)).await;
    Box::pin(events::check(open)).await;
    Box::pin(timeline::check(open)).await;
    Box::pin(members::check(open)).await;
    Box::pin(identities::check(open)).await;
    Box::pin(routing::check(open)).await;
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn event_id(id: &str) -> EventId {
    EventId::new(id).unwrap()
}

/// `evt_{index}` of `room_1`, a text from SMS.
fn event(index: u64) -> RoomEvent {
    let mut event = RoomEvent {
        id: event_id(&format!("evt_{index}")),
        created_at: Timestamp::UNIX_EPOCH,
        ..RoomEvent::new(
            room_id(),
            EventSource::new(channel("sms"), ChannelType::SMS),
            TextContent::new("hi"),
        )
    };
    event.index = index;
    event
}

/// `evt_{index}` stored `status`.
fn event_stored(index: u64, status: EventStatus) -> RoomEvent {
    let mut event = event(index);
    event.status = status;
    event
}

/// `evt_{index}`, a reply in the thread of `evt_{parent}`.
fn reply(index: u64, parent: u64) -> RoomEvent {
    let mut reply = event(index);
    reply.parent_event_id = Some(event_id(&format!("evt_{parent}")));
    reply
}

/// `evt_{index}`, an edit of `evt_{target}`.
fn edit_of(index: u64, target: u64) -> RoomEvent {
    let mut edit = event(index);
    edit.r#type = EventType::Edit;
    edit.content = EventContent::Edit(EditContent::new(
        event_id(&format!("evt_{target}")),
        TextContent::new("corrected"),
    ));
    edit
}

/// A commit that is no activity of the room: its counters move, its
/// activity and status stay (RFC §5.1).
fn no_activity() -> CommitEventOptions {
    CommitEventOptions {
        activity: false,
        ..CommitEventOptions::default()
    }
}

/// Commits `event` to its room, no activity of the room, the change's
/// `target` as it leaves it.
async fn commit<T: Held>(
    store: &T,
    event: RoomEvent,
    target: Option<RoomEvent>,
) -> Result<RoomEvent, StoreError> {
    let room_id = event.room_id.clone();
    let options = CommitEventOptions {
        target,
        ..no_activity()
    };
    store.commit_event(&room_id, event, options).await
}

/// An empty store holding `room_1`.
async fn with_room<T: Held>(open: Open<'_, T>) -> T {
    let store = open().await;
    store.create_room(Room::new(room_id())).await.unwrap();
    store
}

/// A store holding `room_1` and `events`, committed in their order.
async fn with_events<T: Held>(open: Open<'_, T>, events: Vec<RoomEvent>) -> T {
    let store = with_room(open).await;
    for committed in events {
        let room_id = committed.room_id.clone();
        store
            .commit_event(&room_id, committed, no_activity())
            .await
            .unwrap();
    }
    store
}

/// The ids of `events`.
fn ids(events: &[RoomEvent]) -> Vec<String> {
    events.iter().map(|event| event.id.to_string()).collect()
}

/// `room_1`'s binding of an SMS channel `id`, naming no one.
fn binding(id: &str) -> ChannelBinding {
    ChannelBinding {
        attached_at: Timestamp::UNIX_EPOCH,
        ..ChannelBinding::new(channel(id), room_id(), ChannelType::SMS)
    }
}

fn participant_id(id: &str) -> ParticipantId {
    ParticipantId::new(id).unwrap()
}
