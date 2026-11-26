//! Rooms, their counters, their delivery cursor and their private state
//! (RFC §5.1, §13.5, §14.1).

use crate::model::{Metadata, Room, RoomId, RoomStatus, RoomTimers, Timestamp};
use crate::store::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, FindRoomsOptions, ListEventsOptions,
    PatchRoomMetadataOptions,
};
use crate::{ConversationStore, StoreError};
use serde_json::{Value, json};

use super::{Held, Open, commit, event, room_id, with_events, with_room};

async fn a_room_id_cannot_be_created_twice<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let again = store.create_room(Room::new(room_id())).await;
    assert_eq!(again, Err(StoreError::RoomAlreadyExists(room_id())));
}

/// A room is read back as it was written, every field.
async fn a_room_is_read_as_it_was_written<T: Held>(open: Open<'_, T>) {
    let store = open().await;
    let mut room = Room::new(room_id());
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

async fn a_commit_sets_the_room_s_counters<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    commit(&store, event(0), None).await.unwrap();
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!((room.event_count, room.latest_index), (1, Some(0)));
}

/// RFC §14.1 `advance_delivered_index`, §13.5, as the Python
/// implementation's stores answer it: a compare-and-set from the index
/// before, from any lower value when forced, never backwards, nothing
/// for a room the store does not hold; a commit and a room's update, from
/// a copy read before the cursor moved, leave the cursor as stored.
async fn the_delivery_cursor_advances_by_compare_and_set_only<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let room = room_id();
    let missing = RoomId::new("room_9").unwrap();
    let stale = store.get_room(&room).await.unwrap().unwrap();
    let start = store.get_delivered_index(&room).await.unwrap();
    let mut moves = Vec::new();
    for (index, force) in [(0, false), (0, false), (2, false), (2, true), (1, true)] {
        let options = AdvanceDeliveredIndexOptions { force };
        moves.push(
            store
                .advance_delivered_index(&room, index, options)
                .await
                .unwrap(),
        );
    }
    commit(&store, event(0), None).await.unwrap();
    store.update_room(stale).await.unwrap();
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
                .advance_delivered_index(&missing, 0, AdvanceDeliveredIndexOptions { force: true })
                .await
                .unwrap()
        ),
        (-1, vec![true, false, false, true, false], 2, 2, -1, false)
    );
}

/// RFC §14.3, Python's `update_room`: the room is written whole, but for
/// the counters the store manages, which a copy read before a commit
/// does not rewind.
async fn a_room_update_writes_the_room_whole_but_its_counters<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let mut stale = store.get_room(&room_id()).await.unwrap().unwrap();
    commit(&store, event(0), None).await.unwrap();
    stale.status = RoomStatus::Paused;
    let returned = store.update_room(stale).await.unwrap();
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(
        (
            (returned.status, returned.event_count),
            (room.status, room.event_count, room.latest_index)
        ),
        ((RoomStatus::Paused, 1), (RoomStatus::Paused, 1, Some(0)))
    );
}

async fn an_update_of_a_missing_room_is_refused<T: Held>(open: Open<'_, T>) {
    let store = open().await;
    assert_eq!(
        store.update_room(Room::new(room_id())).await,
        Err(StoreError::RoomNotFound(room_id()))
    );
}

/// RFC §5.1, Python's `committed_state`: a commit that is the room's
/// activity resumes a paused room and moves its last activity; one that is
/// not leaves both.
async fn an_activity_commit_resumes_a_paused_room<T: Held>(open: Open<'_, T>) {
    let paused_at = Timestamp::from_unix_millis(1_000).unwrap();
    let mut states = Vec::new();
    for activity in [false, true] {
        let store = with_room(open).await;
        let mut room = store.get_room(&room_id()).await.unwrap().unwrap();
        room.status = RoomStatus::Paused;
        room.paused_at = Some(paused_at);
        store.update_room(room).await.unwrap();
        let options = CommitEventOptions {
            activity,
            ..CommitEventOptions::default()
        };
        store
            .commit_event(&room_id(), event(0), options)
            .await
            .unwrap();
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        states.push((
            room.status,
            room.paused_at.is_some(),
            room.timers.last_activity_at.is_some(),
        ));
    }
    assert_eq!(
        states,
        [
            (RoomStatus::Paused, true, false),
            (RoomStatus::Active, false, true)
        ]
    );
}

/// Python's `patch_room_metadata`: the keys `unset` names go, then the
/// patch is written, the other keys kept; `None` for a room the store does
/// not hold.
async fn a_metadata_patch_writes_its_keys_and_keeps_the_others<T: Held>(open: Open<'_, T>) {
    let store = open().await;
    let mut room = Room::new(room_id());
    room.metadata = metadata(json!({ "kept": 1, "gone": 2, "changed": 3 }));
    store.create_room(room).await.unwrap();
    let unset = PatchRoomMetadataOptions {
        unset: vec!["gone".to_owned()],
    };
    let patched = store
        .patch_room_metadata(&room_id(), metadata(json!({ "changed": 4 })), unset)
        .await
        .unwrap()
        .map(|room| room.metadata);
    let missing = store
        .patch_room_metadata(
            &RoomId::new("room_9").unwrap(),
            Metadata::new(),
            PatchRoomMetadataOptions::default(),
        )
        .await;
    assert_eq!(
        (patched, missing),
        (Some(metadata(json!({ "kept": 1, "changed": 4 }))), Ok(None))
    );
}

/// `value`, a JSON object, as metadata.
fn metadata(value: Value) -> Metadata {
    let Value::Object(metadata) = value else {
        unreachable!()
    };
    metadata
}

/// Python's `delete_room`: the room and what it holds go; whether the
/// store held it.
async fn a_room_s_deletion_takes_what_it_holds<T: Held>(open: Open<'_, T>) {
    let store = with_events(open, vec![event(0)]).await;
    let deleted = store.delete_room(&room_id()).await;
    let again = store.delete_room(&room_id()).await;
    assert_eq!(
        (
            deleted,
            again,
            store.get_room(&room_id()).await,
            store
                .list_events(&room_id(), ListEventsOptions::default())
                .await
                .map(|events| events.len())
        ),
        (Ok(true), Ok(false), Ok(None), Ok(0))
    );
}

/// Python: `tests/test_store_memory.py::TestObservationOperations::test_list_by_room`:
/// a room lists its own observations, not another room's. Both rooms are
/// created: roomkit-rs refuses an observation of a room it does not hold
/// (`RoomNotFound`, RMK-1058, `docs/architecture.md`), where Python's test
/// adds them to rooms it never created.
async fn a_room_lists_its_own_observations<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let other = RoomId::new("room_2").unwrap();
    store.create_room(Room::new(other.clone())).await.unwrap();
    for (id, room) in [("obs1", room_id()), ("obs2", other.clone())] {
        let observation =
            crate::model::Observation::new(id.parse().unwrap(), room, super::channel("ai1"), id);
        store.add_observation(observation).await.unwrap();
    }
    let listed = async |room: &RoomId| {
        let observations = store.list_observations(room).await.unwrap();
        observations
            .into_iter()
            .map(|observation| observation.id.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        (listed(&room_id()).await, listed(&other).await),
        (vec!["obs1".to_owned()], vec!["obs2".to_owned()])
    );
}

/// Python: `tests/test_store_memory.py::TestFindRooms::test_find_no_match`:
/// a status no room has finds none.
async fn finding_rooms_of_a_status_no_room_has_finds_none<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let closed = FindRoomsOptions {
        status: Some(RoomStatus::Closed),
        ..FindRoomsOptions::default()
    };
    assert_eq!(
        store.find_rooms(closed).await.map(|rooms| rooms.len()),
        Ok(0)
    );
}

/// Python: `tests/test_store_memory.py::TestDeleteRoomCleanup::test_tasks_cleaned_on_delete`
/// and `test_observations_cleaned_on_delete`: a room's deletion takes its
/// tasks and observations, a task read by its id included.
async fn a_room_s_deletion_takes_its_tasks_and_observations<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let task = crate::model::Task::new("t1".parse().unwrap(), room_id(), "Follow up");
    let observation = crate::model::Observation::new(
        "o1".parse().unwrap(),
        room_id(),
        super::channel("ai"),
        "noted",
    );
    store.add_task(task).await.unwrap();
    store.add_observation(observation).await.unwrap();

    store.delete_room(&room_id()).await.unwrap();

    assert_eq!(
        (
            store.get_task(&"t1".parse().unwrap()).await,
            store
                .list_tasks(&room_id(), crate::store::ListTasksOptions::default())
                .await
                .map(|tasks| tasks.len()),
            store
                .list_observations(&room_id())
                .await
                .map(|observations| observations.len()),
        ),
        (Ok(None), Ok(0), Ok(0))
    );
}

/// RFC §14.1: the store keeps a room's private state apart from its
/// metadata, a write removing the metadata's copy under the key, a `None`
/// removing the key, and nothing written for a room it does not hold.
async fn a_room_s_private_state_is_kept_apart<T: Held>(open: Open<'_, T>) {
    let store = with_room(open).await;
    let room = room_id();
    let Value::Object(value) = json!({ "version": 3, "names": {} }) else {
        unreachable!()
    };
    let copied = metadata(json!({ "author_register": "a copy" }));
    store
        .patch_room_metadata(&room, copied, PatchRoomMetadataOptions::default())
        .await
        .unwrap();
    store
        .set_private_state(&room, "author_register", Some(value.clone()))
        .await
        .unwrap();
    let kept = store.get_private_state(&room, "author_register").await;
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
        (kept, in_metadata, removed, unknown),
        (Ok(Some(value)), false, Ok(None), Ok(()))
    );
}

async fn listing_rooms_without_a_filter_returns_them_oldest_first<T: Held>(open: Open<'_, T>) {
    let store = open().await;
    for (id, millis) in [("late", 2_000), ("early", 1_000)] {
        let created = Timestamp::from_unix_millis(millis).unwrap();
        let room = Room {
            created_at: created,
            updated_at: created,
            ..Room::new(RoomId::new(id).unwrap())
        };
        store.create_room(room).await.unwrap();
    }
    let listed = store
        .list_rooms(crate::store::ListRoomsOptions::default())
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|room| room.id.as_str())
            .collect::<Vec<_>>(),
        ["early", "late"]
    );
}

/// Python's `find_rooms`: of an organization, in a status, holding every
/// key and value of the metadata filter, a page of them, by creation then
/// id.
async fn finding_rooms_by_organization_status_and_metadata_skips_the_others<T: Held>(
    open: Open<'_, T>,
) {
    let store = open().await;
    for (id, organization, status) in [
        ("bank_active", Some("bank"), RoomStatus::Active),
        ("bank_closed", Some("bank"), RoomStatus::Closed),
        ("clinic_active", Some("clinic"), RoomStatus::Active),
        ("none_active", None, RoomStatus::Active),
        ("bank_vip", Some("bank"), RoomStatus::Active),
    ] {
        let mut room = Room::new(RoomId::new(id).unwrap());
        room.organization_id = organization.map(str::to_owned);
        room.status = status;
        if id == "bank_vip" {
            room.metadata = metadata(json!({ "tier": "vip" }));
        }
        store.create_room(room).await.unwrap();
    }
    let found = async |options: FindRoomsOptions| {
        let rooms = store.find_rooms(options).await.unwrap();
        rooms
            .into_iter()
            .map(|room| room.id.to_string())
            .collect::<Vec<_>>()
    };
    let bank = || FindRoomsOptions {
        organization_id: Some("bank".to_owned()),
        status: Some(RoomStatus::Active),
        ..FindRoomsOptions::default()
    };
    let vip = FindRoomsOptions {
        metadata_filter: Some(metadata(json!({ "tier": "vip" }))),
        ..bank()
    };
    let second = FindRoomsOptions {
        limit: 1,
        offset: 1,
        ..bank()
    };
    assert_eq!(
        (found(bank()).await, found(vip).await, found(second).await),
        (
            vec!["bank_active".to_owned(), "bank_vip".to_owned()],
            vec!["bank_vip".to_owned()],
            vec!["bank_vip".to_owned()]
        )
    );
}

/// This module's checks, in order.
pub(super) async fn check<T: Held>(open: Open<'_, T>) {
    a_room_id_cannot_be_created_twice(open).await;
    a_room_is_read_as_it_was_written(open).await;
    a_commit_sets_the_room_s_counters(open).await;
    the_delivery_cursor_advances_by_compare_and_set_only(open).await;
    a_room_update_writes_the_room_whole_but_its_counters(open).await;
    an_update_of_a_missing_room_is_refused(open).await;
    an_activity_commit_resumes_a_paused_room(open).await;
    a_metadata_patch_writes_its_keys_and_keeps_the_others(open).await;
    a_room_s_deletion_takes_what_it_holds(open).await;
    a_room_lists_its_own_observations(open).await;
    finding_rooms_of_a_status_no_room_has_finds_none(open).await;
    a_room_s_deletion_takes_its_tasks_and_observations(open).await;
    a_room_s_private_state_is_kept_apart(open).await;
    listing_rooms_without_a_filter_returns_them_oldest_first(open).await;
    finding_rooms_by_organization_status_and_metadata_skips_the_others(open).await;
}
