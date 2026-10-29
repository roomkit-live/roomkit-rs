//! A room's timeline: commits, reads, updates and hard deletes (RFC §8.1,
//! §10.3, §13.4, §14.1, §14.3).

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, DeleteContent, EventContent, EventStatus, EventType, EventUpdates, MediaContent,
    Room, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit::{ConversationStore, EventFilter, EventQuery, StoreError, TargetUpdate};
use serde_json::json;

use super::{
    channel, counters, edit_of, event, event_id, event_stored, ids, reply, room_id, with_events,
    with_room,
};
use crate::Store;

/// The update an edit of `evt_0` writes, its text `text`.
fn edited(text: &str) -> TargetUpdate {
    let updates = EventUpdates {
        content: Some(TextContent::new(text).into()),
        ..EventUpdates::default()
    };
    TargetUpdate {
        event_id: event_id("evt_0"),
        updates,
    }
}

/// `room_1` holding `evt_0` and its reply `evt_1`.
async fn with_thread() -> Store {
    with_events(vec![event(0), reply(1, 0)]).await
}

/// An event is read back as it was committed, every field.
#[tokio::test]
async fn an_event_is_read_as_it_was_committed() {
    let mut committed = event(0);
    committed.status = EventStatus::Delivered;
    committed.addressed_to = Some(vec![channel("ai")]);
    committed.correlation_id = Some("crm_77".to_owned());
    committed.created_at = Timestamp::from_unix_millis(1_792_677_780_250).unwrap();
    committed
        .metadata
        .insert("tags".to_owned(), json!(["a", 1, null]));
    committed
        .channel_data
        .insert("segments".to_owned(), json!(2));
    let store = with_events(vec![committed.clone()]).await;
    assert_eq!(
        store.get_event(&event_id("evt_0")).await,
        Ok(Some(committed))
    );
}

#[tokio::test]
async fn an_index_the_room_holds_is_refused() {
    let store = with_events(vec![event(0)]).await;
    let again = store.commit(event(0), counters(0), None).await;
    assert_eq!(
        again,
        Err(StoreError::IndexTaken {
            room_id: room_id(),
            index: 0
        })
    );
}

/// A media its model refuses, built by struct literal past the check
/// `MediaContent::new` runs (RFC §6.4).
fn javascript_media() -> EventContent {
    MediaContent {
        url: "javascript:alert(1)".to_owned(),
        ..MediaContent::new("https://example.com/a.png", "image/png").unwrap()
    }
    .into()
}

/// The refusal of `javascript_media`, Python's `ValueError`.
fn javascript_refused() -> StoreError {
    StoreError::Value(
        "URL must be a link (http://, https://), a data: URI, or a reference <scheme>://... (two \
         letters of scheme at least, not file, ftp, javascript or vbscript, no '..' segment and \
         no backslash): \"javascript:alert(1)\""
            .to_owned(),
    )
}

/// A media whose link's scheme a struct literal wrote in capitals, past
/// the lower case `MediaContent::new` writes.
fn shouting_media(url: &str) -> EventContent {
    MediaContent {
        url: url.to_owned(),
        ..MediaContent::new("https://example.com/a.png", "image/png").unwrap()
    }
    .into()
}

/// The URL of the media `event` holds.
fn media_url(event: &RoomEvent) -> Option<&str> {
    if let EventContent::Media(media) = &event.content {
        Some(&media.url)
    } else {
        None
    }
}

/// A link's scheme a struct literal holds in capitals is written in lower
/// case by every write, a commit, a commit's update of its target and an
/// update, as Python's model writes it when built and reads it back (probe
/// `tasks/probes/step5-lot2-review-A/model_bounds_written.py`: a stored
/// `HTTPS://Example.com/a.png` reads `https://Example.com/a.png`).
#[tokio::test]
async fn a_link_s_scheme_is_written_in_lower_case() {
    let store = with_events(vec![event(0)]).await;
    let mut media = event(1);
    media.content = shouting_media("HTTPS://Example.com/a.png");
    store.commit(media, counters(1), None).await.unwrap();
    let target = TargetUpdate {
        event_id: event_id("evt_0"),
        updates: EventUpdates {
            content: Some(shouting_media("Http://Example.com/b.png")),
            ..EventUpdates::default()
        },
    };
    store
        .commit(edit_of(2, 0), counters(2), Some(target))
        .await
        .unwrap();
    let updates = EventUpdates {
        content: Some(shouting_media("DATA:image/png;base64,AAAA")),
        ..EventUpdates::default()
    };
    store
        .update_event(&event_id("evt_1"), updates)
        .await
        .unwrap();
    let events = store
        .list_events(&room_id(), &EventQuery::default())
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(media_url).collect::<Vec<_>>(),
        [
            Some("http://Example.com/b.png"),
            Some("data:image/png;base64,AAAA"),
            None
        ]
    );
}

/// A content its model refuses, which a struct literal can hold, is
/// refused by every write that would store it, nothing written: a commit,
/// a commit's update of its target, an update. Stored, it would fail every
/// later read of the timeline.
#[tokio::test]
async fn a_content_its_model_refuses_is_never_written() {
    let store = with_events(vec![event(0)]).await;
    let mut media = event(1);
    media.content = javascript_media();
    let edit = edit_of(1, 0);
    let target = TargetUpdate {
        event_id: event_id("evt_0"),
        updates: EventUpdates {
            content: Some(javascript_media()),
            ..EventUpdates::default()
        },
    };
    let updates = EventUpdates {
        content: Some(javascript_media()),
        ..EventUpdates::default()
    };
    let refused = [
        store.commit(media, counters(1), None).await,
        store.commit(edit, counters(1), Some(target)).await,
        store
            .update_event(&event_id("evt_0"), updates)
            .await
            .map(drop),
    ];
    assert_eq!(
        (
            refused,
            store.list_events(&room_id(), &EventQuery::default()).await
        ),
        (
            [
                Err(javascript_refused()),
                Err(javascript_refused()),
                Err(javascript_refused())
            ],
            Ok(vec![event(0)])
        )
    );
}

#[tokio::test]
async fn a_commit_to_a_missing_room_is_refused() {
    let store = crate::store().await;
    assert_eq!(
        store.commit(event(0), counters(0), None).await,
        Err(StoreError::RoomNotFound(room_id()))
    );
}

#[tokio::test]
async fn a_context_holds_the_latest_events_only() {
    let store = with_events((0..3).map(event).collect()).await;
    let context = store.load_context(&room_id(), 2).await.unwrap().unwrap();
    assert_eq!(
        context
            .recent_events
            .iter()
            .map(|event| event.index)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

/// RFC §14.1 `get_event_by_idempotency_key`, §13.4, §22.1: a key is
/// looked up from its channel, or within the whole room for none.
#[tokio::test]
async fn a_key_is_found_from_its_channel_or_within_the_room() {
    let mut keyed = event(0);
    keyed.idempotency_key = Some("key_1".to_owned());
    let store = with_events(vec![keyed]).await;
    let (sms, ws) = (channel("sms"), channel("ws"));
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

/// `evt_k{index}` from `channel`, carrying `key`.
fn keyed(index: u64, channel: &str, key: &str) -> RoomEvent {
    let mut keyed = event(index);
    keyed.id = event_id(&format!("evt_k{index}"));
    keyed.source.channel_id = ChannelId::new(channel).unwrap();
    keyed.idempotency_key = Some(key.to_owned());
    keyed
}

/// RFC §14.3, §13.4: a key its channel holds in the room refuses
/// another event with nothing written, the stored one kept; another
/// channel's key is its own, and a blank key is no key.
#[tokio::test]
async fn a_key_its_channel_holds_refuses_another_event() {
    let store = with_thread().await;
    store
        .commit(keyed(2, "sms", "SM1"), counters(2), None)
        .await
        .unwrap();
    let again = store
        .commit(keyed(3, "sms", "SM1"), counters(3), Some(edited("hello")))
        .await;
    let (room, target) = (
        store.get_room(&room_id()).await.unwrap().unwrap(),
        store.get_event(&event_id("evt_0")).await,
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
                channel_id: channel("sms"),
                key: "SM1".to_owned(),
            }),
            3,
            TextContent::new("hi").into(),
            [Ok(()), Ok(()), Ok(())],
        )
    );
}

/// RFC §13.4: a blank key is no key: an event committed with one is
/// stored with none, and a lookup of one finds nothing.
#[tokio::test]
async fn a_blank_key_is_stored_as_no_key_and_finds_nothing() {
    let store = with_thread().await;
    store
        .commit(keyed(2, "sms", "   "), counters(2), None)
        .await
        .unwrap();
    let stored = store.get_event(&event_id("evt_k2")).await.unwrap().unwrap();
    let found = store
        .get_event_by_idempotency_key(&room_id(), None, "   ")
        .await
        .unwrap();
    assert_eq!((stored.idempotency_key, found), (None, None));
}

/// RFC §14.3: once stored, an event's `delivery_results` is written
/// by the record of its deliveries alone: an update and the commit
/// of an edit's target keep what the store holds.
#[tokio::test]
async fn an_update_and_a_change_s_commit_keep_the_delivery_results() {
    let mut delivered = event(0);
    delivered
        .delivery_results
        .insert("sms".to_owned(), "failed".into());
    let store = with_events(vec![delivered]).await;
    store
        .update_event(&event_id("evt_0"), edited("hi again").updates)
        .await
        .unwrap();
    store
        .commit(event(1), counters(1), Some(edited("hello")))
        .await
        .unwrap();
    let stored = store.get_event(&event_id("evt_0")).await.unwrap().unwrap();
    assert_eq!(
        (stored.content, stored.delivery_results.get("sms").cloned()),
        (TextContent::new("hello").into(), Some("failed".into()))
    );
}

/// RFC §10.1 step 12: the target and the edit are one write.
#[tokio::test]
async fn a_commit_updates_its_target_in_the_same_write() {
    let store = with_thread().await;
    store
        .commit(event(2), counters(2), Some(edited("hello")))
        .await
        .unwrap();
    let target = store.get_event(&event_id("evt_0")).await.unwrap().unwrap();
    assert_eq!(target.content, TextContent::new("hello").into());
}

#[tokio::test]
async fn a_commit_whose_target_is_gone_writes_nothing() {
    let store = with_thread().await;
    store
        .delete_event(&room_id(), &event_id("evt_0"), false)
        .await
        .unwrap();
    let committed = store
        .commit(event(2), counters(2), Some(edited("hello")))
        .await;
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(
        (
            committed,
            room.event_count,
            store.get_event_count(&room_id(), None).await
        ),
        (Err(StoreError::EventNotFound(event_id("evt_0"))), 2, Ok(1))
    );
}

/// A commit's target is an event of its room.
#[tokio::test]
async fn a_commit_whose_target_is_another_room_s_is_refused() {
    let store = with_thread().await;
    let other = RoomId::new("room_2").unwrap();
    store.create_room(Room::new(other.clone())).await.unwrap();
    let mut elsewhere = event(0);
    elsewhere.id = event_id("evt_elsewhere");
    elsewhere.room_id = other;
    let committed = store
        .commit(elsewhere, counters(0), Some(edited("hello")))
        .await;
    assert_eq!(committed, Err(StoreError::EventNotFound(event_id("evt_0"))));
}

/// Gap G-064: a hard-deleted index is never given again.
#[tokio::test]
async fn a_hard_deleted_index_is_refused() {
    let store = with_thread().await;
    store
        .delete_event(&room_id(), &event_id("evt_1"), true)
        .await
        .unwrap();
    let again = store.commit(event(1), counters(1), None).await;
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
    let store = with_thread().await;
    store
        .delete_event(&room_id(), &event_id("evt_0"), true)
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
    let store = with_thread().await;
    store
        .commit(event_stored(2, EventStatus::Blocked), counters(2), None)
        .await
        .unwrap();
    let with_blocked = EventFilter {
        include_blocked: true,
        ..EventFilter::default()
    };
    let counts = (
        store.get_event_count(&room_id(), None).await,
        store
            .get_event_count(&room_id(), Some(EventFilter::default()))
            .await,
        store.get_event_count(&room_id(), Some(with_blocked)).await,
        store
            .list_events(&room_id(), &received())
            .await
            .map(|events| events.len()),
    );
    assert_eq!(counts, (Ok(3), Ok(2), Ok(3), Ok(2)));
}

#[tokio::test]
async fn an_update_writes_what_it_names_and_returns_the_event() {
    let store = with_thread().await;
    let updated = store
        .update_event(&event_id("evt_0"), edited("hello").updates)
        .await
        .unwrap();
    assert_eq!(
        (updated.content, updated.index),
        (TextContent::new("hello").into(), 0)
    );
}

#[tokio::test]
async fn an_update_of_a_missing_event_is_refused() {
    let store = with_thread().await;
    let updated = store
        .update_event(&event_id("evt_9"), EventUpdates::default())
        .await;
    assert_eq!(updated, Err(StoreError::EventNotFound(event_id("evt_9"))));
}

#[tokio::test]
async fn a_delete_in_another_room_removes_nothing() {
    let store = with_thread().await;
    let other = RoomId::new("room_2").unwrap();
    store.create_room(Room::new(other.clone())).await.unwrap();
    let removed = store
        .delete_event(&other, &event_id("evt_0"), true)
        .await
        .unwrap();
    assert_eq!(
        (removed.len(), store.get_event_count(&room_id(), None).await),
        (0, Ok(2))
    );
}

/// `evt_0`, its replies `evt_1` and `evt_3`, `evt_2` apart, and `evt_4`, a
/// reply to `evt_3`.
fn timeline() -> Vec<RoomEvent> {
    vec![event(0), reply(1, 0), event(2), reply(3, 0), reply(4, 3)]
}

/// The ids `delete_event` of `target` removes and those it leaves.
async fn deleting(
    events: Vec<RoomEvent>,
    target: &str,
    cascade_replies: bool,
) -> (Vec<String>, Vec<String>) {
    let store = with_events(events).await;
    let removed = store
        .delete_event(&room_id(), &event_id(target), cascade_replies)
        .await
        .unwrap();
    let left = store.list_events(&room_id(), &every_row()).await.unwrap();
    (ids(&removed), ids(&left))
}

fn owned(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| (*id).to_owned()).collect()
}

/// RFC §10.3: the replies one level deep, the pipeline rooting every
/// thread parent (§10.1 step 8): a reply to a reply stays.
#[tokio::test]
async fn a_delete_takes_its_replies_one_level_deep() {
    assert_eq!(
        deleting(timeline(), "evt_0", true).await,
        (
            owned(&["evt_0", "evt_1", "evt_3"]),
            owned(&["evt_2", "evt_4"])
        )
    );
}

/// RFC §10.3, §14.1: the target, then its replies by index, then the
/// changes by index, whatever their places in the timeline.
#[tokio::test]
async fn a_delete_returns_the_target_then_the_replies_then_the_changes() {
    let events = vec![event(0), edit_of(1, 0), reply(2, 0)];
    assert_eq!(
        deleting(events, "evt_0", true).await.0,
        owned(&["evt_0", "evt_2", "evt_1"])
    );
}

/// RFC §10.3: an event both a reply and a change is removed once, among
/// the replies, or among the changes when replies are not taken.
#[tokio::test]
async fn a_reply_that_is_a_change_is_counted_once() {
    let events = || {
        let mut edit_in_thread = edit_of(1, 0);
        edit_in_thread.parent_event_id = Some(event_id("evt_0"));
        vec![event(0), edit_in_thread, reply(2, 0)]
    };
    assert_eq!(
        (
            deleting(events(), "evt_0", true).await.0,
            deleting(events(), "evt_0", false).await.0
        ),
        (
            owned(&["evt_0", "evt_1", "evt_2"]),
            owned(&["evt_0", "evt_1"])
        )
    );
}

#[tokio::test]
async fn a_delete_without_cascade_takes_the_target_only() {
    let (removed, left) = deleting(timeline(), "evt_0", false).await;
    assert_eq!((removed, left.len()), (owned(&["evt_0"]), 4));
}

#[tokio::test]
async fn a_delete_of_a_reply_keeps_its_root() {
    assert_eq!(
        deleting(timeline(), "evt_3", true).await.0,
        owned(&["evt_3", "evt_4"])
    );
}

/// Gap G-063: an edit holds its target's content, and goes with it.
#[tokio::test]
async fn a_delete_takes_the_edits_of_what_it_removes() {
    let mut events = timeline();
    events.extend([edit_of(5, 0), edit_of(6, 2), edit_of(7, 1)]);
    assert_eq!(
        deleting(events, "evt_0", true).await,
        (
            owned(&["evt_0", "evt_1", "evt_3", "evt_5", "evt_7"]),
            owned(&["evt_2", "evt_4", "evt_6"])
        )
    );
}

/// RFC §10.3: the changes of the target and of its replies go, not a
/// change of a change: an edit of the target's edit stays.
#[tokio::test]
async fn a_delete_leaves_a_change_of_a_change() {
    let events = vec![event(0), edit_of(1, 0), edit_of(2, 1)];
    assert_eq!(
        deleting(events, "evt_0", true).await,
        (owned(&["evt_0", "evt_1"]), owned(&["evt_2"]))
    );
}

#[tokio::test]
async fn a_delete_without_cascade_still_takes_the_target_s_edits() {
    let mut events = timeline();
    events.push(edit_of(5, 0));
    assert_eq!(
        deleting(events, "evt_0", false).await.0,
        owned(&["evt_0", "evt_5"])
    );
}

#[tokio::test]
async fn a_delete_of_a_missing_event_removes_nothing() {
    let (removed, left) = deleting(timeline(), "evt_9", true).await;
    assert_eq!((removed.len(), left.len()), (0, 5));
}

/// RFC §14.1 `get_event`: an event is found whichever room holds it.
#[tokio::test]
async fn an_event_is_found_whichever_room_holds_it() {
    let store = with_room().await;
    let other = RoomId::new("room_2").unwrap();
    store.create_room(Room::new(other.clone())).await.unwrap();
    let mut elsewhere = event(0);
    elsewhere.room_id = other.clone();
    store
        .commit(elsewhere.clone(), counters(0), None)
        .await
        .unwrap();
    assert_eq!(
        store.get_event(&event_id("evt_0")).await,
        Ok(Some(elsewhere))
    );
}

/// RFC §10.3: an edit holds a version of the text its target said, so a
/// delete's commit marks the target's edits deleted with it, as the Python
/// implementation's stores do, whose histories withhold an edit by its own
/// mark; an edit of another event keeps its own.
#[tokio::test]
async fn a_delete_s_commit_marks_its_target_s_edits_deleted() {
    let store = with_events(vec![event(0), edit_of(1, 0), event(2), edit_of(3, 2)]).await;
    let mut delete = event(4);
    delete.r#type = EventType::Delete;
    delete.content = EventContent::Delete(DeleteContent::new(event_id("evt_0")));
    let updates = EventUpdates {
        metadata: Some([("deleted".to_owned(), json!(true))].into_iter().collect()),
        ..EventUpdates::default()
    };
    let deleted = TargetUpdate {
        event_id: event_id("evt_0"),
        updates,
    };
    store
        .commit(delete, counters(4), Some(deleted))
        .await
        .unwrap();
    let marks: Vec<Option<serde_json::Value>> = store
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .take(4)
        .map(|event| event.metadata.get("deleted").cloned())
        .collect();
    assert_eq!(marks, [Some(json!(true)), Some(json!(true)), None, None]);
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> EventQuery {
    EventQuery {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..EventQuery::default()
    }
}

/// Every event of a room's timeline the room received.
fn received() -> EventQuery {
    EventQuery {
        limit: u64::MAX,
        ..EventQuery::default()
    }
}
