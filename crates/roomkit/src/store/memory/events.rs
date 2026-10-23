//! The in-memory store's hard delete (RFC §14.1 `delete_event`, §10.3):
//! the event, the replies of its thread, and the edits and deletes of what
//! it removes.

use std::collections::BTreeSet;

use roomkit_core::model::{EventId, RoomEvent};
use roomkit_core::rules::edits;

/// Removes `event_id` from `events`, with what goes with it (gap G-063):
///
/// - with `cascade_replies`, every event whose thread it roots, a reply's
///   replies included, since nothing else protects them (RFC §10.3);
/// - whatever `cascade_replies` says, every edit and delete of an event
///   removed: an edit holds its target's content, and a change of an event
///   that no longer exists changes nothing.
///
/// Returns the events removed, the target first, then the others in
/// timeline order; none when `events` does not hold the target.
pub(super) fn remove(
    events: &mut Vec<RoomEvent>,
    event_id: &EventId,
    cascade_replies: bool,
) -> Vec<RoomEvent> {
    let Some(at) = events.iter().position(|event| &event.id == event_id) else {
        return Vec::new();
    };
    let mut removed = vec![events.remove(at)];
    let mut gone = BTreeSet::from([event_id.clone()]);
    while let Some(next) = events
        .iter()
        .position(|event| goes_with(event, &gone, cascade_replies))
    {
        let next = events.remove(next);
        gone.insert(next.id.clone());
        removed.push(next);
    }
    removed.sort_by_key(|event| (event.id != *event_id, event.index));
    removed
}

/// Whether `event` goes with the events `gone`: a reply in their thread when
/// replies cascade, an edit or a delete of one of them always.
fn goes_with(event: &RoomEvent, gone: &BTreeSet<EventId>, cascade_replies: bool) -> bool {
    let reply = cascade_replies
        && event
            .parent_event_id
            .as_ref()
            .is_some_and(|parent| gone.contains(parent));
    let change = edits::mutation(event)
        .ok()
        .flatten()
        .is_some_and(|mutation| gone.contains(mutation.target_event_id()));
    reply || change
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType, RoomId,
        Timestamp,
    };

    use super::*;

    fn event(index: u64, parent: Option<u64>) -> RoomEvent {
        let mut event = RoomEvent::new(
            EventId::new(format!("evt_{index}")).unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::Sms,
                EventDirection::Inbound,
            ),
            EventContent::text("hi"),
            Timestamp::UNIX_EPOCH,
        );
        event.index = index;
        event.parent_event_id = parent.map(|parent| EventId::new(format!("evt_{parent}")).unwrap());
        event
    }

    fn ids(events: &[RoomEvent]) -> Vec<String> {
        events.iter().map(|event| event.id.to_string()).collect()
    }

    /// `evt_0`, its replies `evt_1` and `evt_3`, `evt_2` apart, and
    /// `evt_4`, a reply to `evt_3`.
    fn timeline() -> Vec<RoomEvent> {
        vec![
            event(0, None),
            event(1, Some(0)),
            event(2, None),
            event(3, Some(0)),
            event(4, Some(3)),
        ]
    }

    #[test]
    fn a_delete_takes_the_thread_with_it_the_target_first() {
        let mut events = timeline();
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), true);
        assert_eq!(
            (ids(&removed), ids(&events)),
            (
                ["evt_0", "evt_1", "evt_3", "evt_4"]
                    .map(str::to_owned)
                    .to_vec(),
                vec!["evt_2".to_owned()]
            )
        );
    }

    #[test]
    fn a_delete_without_cascade_takes_the_target_only() {
        let mut events = timeline();
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), false);
        assert_eq!((ids(&removed), events.len()), (vec!["evt_0".to_owned()], 4));
    }

    #[test]
    fn a_delete_of_a_reply_keeps_its_root() {
        let mut events = timeline();
        let removed = remove(&mut events, &EventId::new("evt_3").unwrap(), true);
        assert_eq!(
            ids(&removed),
            ["evt_3", "evt_4"].map(str::to_owned).to_vec()
        );
    }

    /// The store's event methods, on a room `room_1` holding `evt_0` and its
    /// reply `evt_1`.
    mod the_store {
        use pretty_assertions::assert_eq;
        use roomkit_core::model::{EventStatus, EventUpdates, Room, RoomUpdates};
        use roomkit_core::ops::{RoomCounters, TargetUpdate};

        use super::*;
        use crate::store::{ConversationStore, EventFilter, InMemoryStore, StoreError};

        fn counters(index: u64) -> RoomCounters {
            RoomCounters {
                event_count: index + 1,
                latest_index: index,
                last_activity_at: None,
                room: RoomUpdates::default(),
            }
        }

        fn room_id() -> RoomId {
            RoomId::new("room_1").unwrap()
        }

        async fn store() -> InMemoryStore {
            let store = InMemoryStore::new();
            store
                .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
                .await
                .unwrap();
            for committed in [event(0, None), event(1, Some(0))] {
                let index = committed.index;
                store
                    .commit(committed, counters(index), None)
                    .await
                    .unwrap();
            }
            store
        }

        fn edited(text: &str) -> TargetUpdate {
            TargetUpdate {
                event_id: EventId::new("evt_0").unwrap(),
                updates: EventUpdates {
                    content: Some(EventContent::text(text)),
                    ..EventUpdates::default()
                },
            }
        }

        /// RFC §10.1 step 12: the target and the edit are one write.
        #[tokio::test]
        async fn a_commit_updates_its_target_in_the_same_write() {
            let store = store().await;
            store
                .commit(event(2, None), counters(2), Some(edited("hello")))
                .await
                .unwrap();
            let target = store
                .get_event(&EventId::new("evt_0").unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(target.content, EventContent::text("hello"));
        }

        #[tokio::test]
        async fn a_commit_whose_target_is_gone_writes_nothing() {
            let store = store().await;
            store
                .delete_event(&room_id(), &EventId::new("evt_0").unwrap(), false)
                .await
                .unwrap();
            let committed = store
                .commit(event(2, None), counters(2), Some(edited("hello")))
                .await;
            let room = store.get_room(&room_id()).await.unwrap().unwrap();
            assert_eq!(
                (
                    committed,
                    room.event_count,
                    store.get_event_count(&room_id(), None).await
                ),
                (
                    Err(StoreError::EventNotFound(EventId::new("evt_0").unwrap())),
                    2,
                    Ok(1)
                )
            );
        }

        /// Gap G-064: a hard-deleted index is never given again.
        #[tokio::test]
        async fn a_hard_deleted_index_is_refused() {
            let store = store().await;
            store
                .delete_event(&room_id(), &EventId::new("evt_1").unwrap(), true)
                .await
                .unwrap();
            let again = store.commit(event(1, None), counters(1), None).await;
            assert_eq!(
                again,
                Err(StoreError::IndexTaken {
                    room_id: room_id(),
                    index: 1
                })
            );
        }

        /// Gap G-064: the counters stay, the timeline shrinks.
        #[tokio::test]
        async fn a_hard_delete_leaves_the_counters_and_lowers_the_count() {
            let store = store().await;
            store
                .delete_event(&room_id(), &EventId::new("evt_0").unwrap(), true)
                .await
                .unwrap();
            let room = store.get_room(&room_id()).await.unwrap().unwrap();
            assert_eq!(
                (
                    room.event_count,
                    room.latest_index,
                    store.get_event_count(&room_id(), None).await
                ),
                (2, Some(1), Ok(0))
            );
        }

        /// RFC §14.1: a count follows the rule of the page it stands for.
        #[tokio::test]
        async fn a_filtered_count_leaves_refused_events_out_as_a_timeline_read_does() {
            let store = store().await;
            let mut refused = event(2, None);
            refused.status = EventStatus::Blocked;
            store.commit(refused, counters(2), None).await.unwrap();
            let counts = (
                store.get_event_count(&room_id(), None).await,
                store
                    .get_event_count(&room_id(), Some(EventFilter::default()))
                    .await,
                store
                    .get_event_count(
                        &room_id(),
                        Some(EventFilter {
                            include_blocked: true,
                        }),
                    )
                    .await,
                store
                    .list_events(&room_id(), false)
                    .await
                    .map(|events| events.len()),
            );
            assert_eq!(counts, (Ok(3), Ok(2), Ok(3), Ok(2)));
        }

        #[tokio::test]
        async fn an_update_writes_what_it_names_and_returns_the_event() {
            let store = store().await;
            let updated = store
                .update_event(&EventId::new("evt_0").unwrap(), edited("hello").updates)
                .await
                .unwrap();
            assert_eq!(
                (updated.content, updated.index),
                (EventContent::text("hello"), 0)
            );
        }

        #[tokio::test]
        async fn an_update_of_a_missing_event_is_refused() {
            let store = store().await;
            let updated = store
                .update_event(&EventId::new("evt_9").unwrap(), EventUpdates::default())
                .await;
            assert_eq!(
                updated,
                Err(StoreError::EventNotFound(EventId::new("evt_9").unwrap()))
            );
        }

        #[tokio::test]
        async fn a_delete_in_another_room_removes_nothing() {
            let store = store().await;
            let other = RoomId::new("room_2").unwrap();
            store
                .create_room(Room::new(other.clone(), Timestamp::UNIX_EPOCH))
                .await
                .unwrap();
            let removed = store
                .delete_event(&other, &EventId::new("evt_0").unwrap(), true)
                .await
                .unwrap();
            assert_eq!(
                (removed.len(), store.get_event_count(&room_id(), None).await),
                (0, Ok(2))
            );
        }
    }

    fn edit_of(index: u64, target: u64) -> RoomEvent {
        let mut edit = event(index, None);
        edit.event_type = EventType::Edit;
        edit.content = EventContent::Edit(roomkit_core::model::EditContent {
            target_event_id: EventId::new(format!("evt_{target}")).unwrap(),
            new_content: Box::new(EventContent::text("corrected")),
            edit_source: None,
        });
        edit
    }

    /// Gap G-063: an edit holds its target's content, and goes with it.
    #[test]
    fn a_delete_takes_the_edits_of_what_it_removes() {
        let mut events = timeline();
        events.push(edit_of(5, 0));
        events.push(edit_of(6, 2));
        events.push(edit_of(7, 1));
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), true);
        assert_eq!(
            (ids(&removed), ids(&events)),
            (
                ["evt_0", "evt_1", "evt_3", "evt_4", "evt_5", "evt_7"]
                    .map(str::to_owned)
                    .to_vec(),
                ["evt_2", "evt_6"].map(str::to_owned).to_vec()
            )
        );
    }

    #[test]
    fn a_delete_without_cascade_still_takes_the_target_s_edits() {
        let mut events = timeline();
        events.push(edit_of(5, 0));
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), false);
        assert_eq!(
            ids(&removed),
            ["evt_0", "evt_5"].map(str::to_owned).to_vec()
        );
    }

    #[test]
    fn a_delete_of_a_missing_event_removes_nothing() {
        let mut events = timeline();
        let removed = remove(&mut events, &EventId::new("evt_9").unwrap(), true);
        assert_eq!((removed.len(), events.len()), (0, 5));
    }
}
