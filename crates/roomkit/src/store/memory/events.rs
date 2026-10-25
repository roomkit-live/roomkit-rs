//! The in-memory store's hard delete (RFC §14.1 `delete_event`, §10.3):
//! the event, its replies, and the edits and deletes of what it removes.

use std::collections::BTreeSet;
use std::mem;

use roomkit_core::model::{EventId, RoomEvent};
use roomkit_core::rules::edits;

/// Removes `event_id` from `events`, with what goes with it (RFC §10.3,
/// gap G-063):
///
/// - with `cascade_replies`, its replies, the events whose
///   `parent_event_id` names it, one level deep: the pipeline roots every
///   thread parent (RFC §10.1 step 8), so a reply names its thread's root,
///   and an event naming one of the replies goes nowhere with them;
/// - whatever `cascade_replies` says, the edits and deletes of the target
///   and of its replies: an edit holds its target's content, and a change
///   of an event that no longer exists changes nothing.
///
/// Returns the events removed, the target first, then its replies in index
/// order, then the changes in index order, an event both a reply and a
/// change among the replies; none when `events` does not hold the target.
pub(super) fn remove(
    events: &mut Vec<RoomEvent>,
    event_id: &EventId,
    cascade_replies: bool,
) -> Vec<RoomEvent> {
    let Some(at) = events.iter().position(|event| &event.id == event_id) else {
        return Vec::new();
    };
    let target = events.remove(at);
    let replies = if cascade_replies {
        take(events, |event| {
            event.parent_event_id.as_ref() == Some(event_id)
        })
    } else {
        Vec::new()
    };
    let gone: BTreeSet<&EventId> = replies
        .iter()
        .map(|reply| &reply.id)
        .chain([event_id])
        .collect();
    let changes = take(events, |event| changes_one_of(event, &gone));
    [target].into_iter().chain(replies).chain(changes).collect()
}

/// Takes the events of `events` that `belongs` holds for out of it, in index
/// order.
fn take(events: &mut Vec<RoomEvent>, belongs: impl Fn(&RoomEvent) -> bool) -> Vec<RoomEvent> {
    let (mut taken, left): (Vec<RoomEvent>, Vec<RoomEvent>) = mem::take(events)
        .into_iter()
        .partition(|event| belongs(event));
    *events = left;
    taken.sort_by_key(|event| event.index);
    taken
}

/// Whether `event` is an edit or a delete of one of the events `gone`.
fn changes_one_of(event: &RoomEvent, gone: &BTreeSet<&EventId>) -> bool {
    edits::mutation(event)
        .ok()
        .flatten()
        .is_some_and(|mutation| gone.contains(mutation.target_event_id()))
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

    /// RFC §10.3: the replies one level deep, the pipeline rooting every
    /// thread parent (§10.1 step 8): a reply to a reply stays.
    #[test]
    fn a_delete_takes_its_replies_one_level_deep() {
        let mut events = timeline();
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), true);
        assert_eq!(
            (ids(&removed), ids(&events)),
            (
                ["evt_0", "evt_1", "evt_3"].map(str::to_owned).to_vec(),
                ["evt_2", "evt_4"].map(str::to_owned).to_vec()
            )
        );
    }

    /// RFC §10.3, §14.1: the target, then its replies by index, then the
    /// changes by index, whatever their places in the timeline.
    #[test]
    fn a_delete_returns_the_target_then_the_replies_then_the_changes() {
        let mut events = vec![event(0, None), edit_of(1, 0), event(2, Some(0))];
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), true);
        assert_eq!(
            ids(&removed),
            ["evt_0", "evt_2", "evt_1"].map(str::to_owned).to_vec()
        );
    }

    /// RFC §10.3: an event both a reply and a change is removed once, among
    /// the replies, or among the changes when replies are not taken.
    #[test]
    fn a_reply_that_is_a_change_is_counted_once() {
        let edit_in_thread = || {
            let mut edit = edit_of(1, 0);
            edit.parent_event_id = Some(EventId::new("evt_0").unwrap());
            edit
        };
        let removed = |cascade_replies| {
            let mut events = vec![event(0, None), edit_in_thread(), event(2, Some(0))];
            ids(&remove(
                &mut events,
                &EventId::new("evt_0").unwrap(),
                cascade_replies,
            ))
        };
        assert_eq!(
            (removed(true), removed(false)),
            (
                ["evt_0", "evt_1", "evt_2"].map(str::to_owned).to_vec(),
                ["evt_0", "evt_1"].map(str::to_owned).to_vec()
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

        /// RFC §14.1 `get_event_by_idempotency_key`, §13.4, §22.1: a key is
        /// looked up from its channel, or within the whole room for none.
        #[tokio::test]
        async fn a_key_is_found_from_its_channel_or_within_the_room() {
            let store = InMemoryStore::new();
            store
                .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
                .await
                .unwrap();
            let mut keyed = event(0, None);
            keyed.idempotency_key = Some("key_1".to_owned());
            store.commit(keyed, counters(0), None).await.unwrap();
            let (sms, ws) = (
                ChannelId::new("sms").unwrap(),
                ChannelId::new("ws").unwrap(),
            );
            let mut found = Vec::new();
            for channel in [Some(&sms), Some(&ws), None] {
                let event = store
                    .get_event_by_idempotency_key(&room_id(), channel, "key_1")
                    .await
                    .unwrap();
                found.push(event.map(|event| event.id.to_string()));
            }
            assert_eq!(
                found,
                vec![Some("evt_0".to_owned()), None, Some("evt_0".to_owned())]
            );
        }

        /// `evt_{index}` from `channel`, carrying `key`.
        fn keyed(index: u64, channel: &str, key: &str) -> RoomEvent {
            let mut keyed = event(index, None);
            keyed.id = EventId::new(format!("evt_k{index}")).unwrap();
            keyed.source.channel_id = ChannelId::new(channel).unwrap();
            keyed.idempotency_key = Some(key.to_owned());
            keyed
        }

        /// RFC §14.3, §13.4: a key its channel holds in the room refuses
        /// another event with nothing written, the stored one kept; another
        /// channel's key is its own, and a blank key is no key.
        #[tokio::test]
        async fn a_key_its_channel_holds_refuses_another_event() {
            let store = store().await;
            store
                .commit(keyed(2, "sms", "SM1"), counters(2), None)
                .await
                .unwrap();
            let again = store
                .commit(keyed(3, "sms", "SM1"), counters(3), Some(edited("hello")))
                .await;
            let (room, target) = (
                store.get_room(&room_id()).await.unwrap().unwrap(),
                store.get_event(&EventId::new("evt_0").unwrap()).await,
            );
            let others = [
                store.commit(keyed(3, "ws", "SM1"), counters(3), None).await,
                store.commit(keyed(4, "sms", " "), counters(4), None).await,
                store.commit(keyed(5, "sms", " "), counters(5), None).await,
            ];
            assert_eq!(
                (
                    again,
                    room.event_count,
                    target.unwrap().unwrap().content,
                    others,
                ),
                (
                    Err(StoreError::IdempotencyKeyTaken {
                        room_id: room_id(),
                        channel_id: ChannelId::new("sms").unwrap(),
                        key: "SM1".to_owned(),
                    }),
                    3,
                    EventContent::text("hi"),
                    [Ok(()), Ok(()), Ok(())],
                )
            );
        }

        /// RFC §14.3: once stored, an event's `delivery_results` is written
        /// by the record of its deliveries alone: an update and the commit
        /// of an edit's target keep what the store holds.
        #[tokio::test]
        async fn an_update_and_a_change_s_commit_keep_the_delivery_results() {
            let store = InMemoryStore::new();
            store
                .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
                .await
                .unwrap();
            let mut delivered = event(0, None);
            delivered
                .delivery_results
                .insert("sms".to_owned(), "failed".into());
            store.commit(delivered, counters(0), None).await.unwrap();
            store
                .update_event(&EventId::new("evt_0").unwrap(), edited("hi again").updates)
                .await
                .unwrap();
            store
                .commit(event(1, None), counters(1), Some(edited("hello")))
                .await
                .unwrap();
            let stored = store
                .get_event(&EventId::new("evt_0").unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (stored.content, stored.delivery_results.get("sms").cloned()),
                (EventContent::text("hello"), Some("failed".into()))
            );
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
                ["evt_0", "evt_1", "evt_3", "evt_5", "evt_7"]
                    .map(str::to_owned)
                    .to_vec(),
                ["evt_2", "evt_4", "evt_6"].map(str::to_owned).to_vec()
            )
        );
    }

    /// RFC §10.3: the changes of the target and of its replies go, not a
    /// change of a change: an edit of the target's edit stays.
    #[test]
    fn a_delete_leaves_a_change_of_a_change() {
        let mut events = vec![event(0, None), edit_of(1, 0), edit_of(2, 1)];
        let removed = remove(&mut events, &EventId::new("evt_0").unwrap(), true);
        assert_eq!(
            (ids(&removed), ids(&events)),
            (
                ["evt_0", "evt_1"].map(str::to_owned).to_vec(),
                vec!["evt_2".to_owned()]
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
