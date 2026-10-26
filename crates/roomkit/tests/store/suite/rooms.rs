//! Rooms, their counters, their delivery cursor and their private state
//! (RFC §5.1, §13.5, §14.1).

use pretty_assertions::assert_eq;
use roomkit::model::{Room, RoomId, RoomStatus, RoomTimers, RoomUpdates, Timestamp};
use roomkit::{ConversationStore, RoomFilter, StoreError};
use serde_json::{Value, json};

use super::{counters, event, room_id, with_room};

#[tokio::test]
async fn a_room_id_cannot_be_created_twice() {
    let store = with_room().await;
    let again = store
        .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
        .await;
    assert_eq!(again, Err(StoreError::RoomExists(room_id())));
}

/// A room is read back as it was written, every field.
#[tokio::test]
async fn a_room_is_read_as_it_was_written() {
    let store = crate::store().await;
    let mut room = Room::new(room_id(), Timestamp::from_unix_millis(1_250).unwrap());
    room.organization_id = Some("org_1".to_owned());
    room.timers = RoomTimers {
        inactive_after_seconds: Some(60),
        closed_after_seconds: Some(120),
        last_activity_at: Some(Timestamp::from_unix_millis(2_500).unwrap()),
    };
    room.metadata.insert("crm".to_owned(), json!({ "id": 7 }));
    store.create_room(room.clone()).await.unwrap();
    assert_eq!(store.get_room(&room_id()).await, Ok(Some(room)));
}

#[tokio::test]
async fn a_commit_sets_the_room_s_counters() {
    let store = with_room().await;
    store.commit(event(0), counters(0), None).await.unwrap();
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!((room.event_count, room.latest_index), (1, Some(0)));
}

/// RFC §14.1 `advance_delivered_index`, §13.5, as the Python
/// implementation's stores answer it: a compare-and-set from the index
/// before, from any lower value when forced, never backwards, nothing
/// for a room the store does not hold; a commit and a room's update
/// leave the cursor as stored.
#[tokio::test]
async fn the_delivery_cursor_advances_by_compare_and_set_only() {
    let store = with_room().await;
    let room = room_id();
    let missing = RoomId::new("room_9").unwrap();
    let start = store.get_delivered_index(&room).await.unwrap();
    let mut moves = Vec::new();
    for (index, force) in [(0, false), (0, false), (2, false), (2, true), (1, true)] {
        moves.push(
            store
                .advance_delivered_index(&room, index, force)
                .await
                .unwrap(),
        );
    }
    store.commit(event(0), counters(0), None).await.unwrap();
    store
        .update_room(&room, RoomUpdates::default())
        .await
        .unwrap();
    assert_eq!(
        (
            start,
            moves,
            store.get_delivered_index(&room).await.unwrap(),
            store
                .get_room(&room)
                .await
                .unwrap()
                .unwrap()
                .delivered_index,
            store.get_delivered_index(&missing).await.unwrap(),
            store
                .advance_delivered_index(&missing, 0, true)
                .await
                .unwrap()
        ),
        (-1, vec![true, false, false, true, false], 2, 2, -1, false)
    );
}

#[tokio::test]
async fn a_room_update_writes_only_the_fields_it_names() {
    let store = with_room().await;
    store.commit(event(0), counters(0), None).await.unwrap();
    let mut updates = RoomUpdates::default();
    updates.status = Some(RoomStatus::Paused);
    let room = store.update_room(&room_id(), updates).await.unwrap();
    assert_eq!((room.status, room.event_count), (RoomStatus::Paused, 1));
}

#[tokio::test]
async fn an_update_of_a_missing_room_is_refused() {
    let store = crate::store().await;
    assert_eq!(
        store.update_room(&room_id(), RoomUpdates::default()).await,
        Err(StoreError::RoomNotFound(room_id()))
    );
}

#[tokio::test]
async fn a_commit_writes_the_room_updates_it_carries() {
    let store = with_room().await;
    let mut resumed = counters(0);
    resumed.room.status = Some(RoomStatus::Active);
    let mut paused = RoomUpdates::default();
    paused.status = Some(RoomStatus::Paused);
    store.update_room(&room_id(), paused).await.unwrap();
    store.commit(event(0), resumed, None).await.unwrap();
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(room.status, RoomStatus::Active);
}

/// RFC §14.1: the store keeps a room's private state apart from its
/// metadata, a write removing the metadata's copy under the key, a `None`
/// removing the key, and nothing written for a room it does not hold; a
/// context carries the register of authors (§10.1 step 12).
#[tokio::test]
async fn a_room_s_private_state_is_kept_apart() {
    let store = with_room().await;
    let room = room_id();
    let Value::Object(value) = json!({ "version": 3, "names": {} }) else {
        unreachable!()
    };
    let mut copied = RoomUpdates::default();
    copied.metadata = [("author_register".to_owned(), json!("a copy"))]
        .into_iter()
        .collect();
    store.update_room(&room, copied).await.unwrap();
    store
        .set_private_state(&room, "author_register", Some(value.clone()))
        .await
        .unwrap();
    let kept = store.get_private_state(&room, "author_register").await;
    let carried = store
        .load_context(&room, 0)
        .await
        .unwrap()
        .unwrap()
        .author_register;
    let in_metadata = store
        .get_room(&room)
        .await
        .unwrap()
        .unwrap()
        .metadata
        .contains_key("author_register");
    store
        .set_private_state(&room, "author_register", None)
        .await
        .unwrap();
    let removed = store.get_private_state(&room, "author_register").await;
    let missing = RoomId::new("room_9").unwrap();
    let unknown = store
        .set_private_state(&missing, "author_register", Some(value.clone()))
        .await;
    assert_eq!(
        (kept, carried, in_metadata, removed, unknown),
        (
            Ok(Some(value.clone())),
            Some(value),
            false,
            Ok(None),
            Ok(())
        )
    );
}

#[tokio::test]
async fn listing_rooms_with_timers_armed_skips_the_others() {
    let store = with_room().await;
    let mut timed = Room::new(RoomId::new("room_2").unwrap(), Timestamp::UNIX_EPOCH);
    timed.timers = RoomTimers {
        inactive_after_seconds: Some(60),
        ..RoomTimers::default()
    };
    store.create_room(timed).await.unwrap();
    let mut filter = RoomFilter::default();
    filter.timers_armed = true;
    let listed = store.list_rooms(&filter).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|room| room.id.as_str())
            .collect::<Vec<_>>(),
        ["room_2"]
    );
}

#[tokio::test]
async fn listing_rooms_without_a_filter_returns_them_oldest_first() {
    let store = crate::store().await;
    for (id, millis) in [("late", 2_000), ("early", 1_000)] {
        let created = Timestamp::from_unix_millis(millis).unwrap();
        store
            .create_room(Room::new(RoomId::new(id).unwrap(), created))
            .await
            .unwrap();
    }
    let listed = store.list_rooms(&RoomFilter::default()).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|room| room.id.as_str())
            .collect::<Vec<_>>(),
        ["early", "late"]
    );
}

#[tokio::test]
async fn listing_rooms_by_organization_and_status_skips_the_others() {
    let store = crate::store().await;
    for (id, organization, status) in [
        ("bank_active", Some("bank"), RoomStatus::Active),
        ("bank_closed", Some("bank"), RoomStatus::Closed),
        ("clinic_active", Some("clinic"), RoomStatus::Active),
        ("none_active", None, RoomStatus::Active),
    ] {
        let mut room = Room::new(RoomId::new(id).unwrap(), Timestamp::UNIX_EPOCH);
        room.organization_id = organization.map(str::to_owned);
        room.status = status;
        store.create_room(room).await.unwrap();
    }
    let mut filter = RoomFilter::default();
    filter.organization_id = Some("bank".to_owned());
    filter.status = Some(RoomStatus::Active);
    let listed = store.list_rooms(&filter).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|room| room.id.as_str())
            .collect::<Vec<_>>(),
        ["bank_active"]
    );
}
