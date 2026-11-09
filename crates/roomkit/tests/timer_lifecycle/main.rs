//! Public timer operations across store races and caller cancellation (RFC §5.1).
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod cancellation;
mod context;
mod store;
#[path = "../support/warnings.rs"]
mod warnings;

use roomkit::model::{
    ChannelId, ChannelType, EventSource, RoomEvent, RoomId, RoomStatus, RoomTimers, TextContent,
    Timestamp,
};
use roomkit::testing::{EventTap, idle};
use roomkit::{
    CheckAllTimersOptions, CheckRoomTimersOptions, ConversationStore, CreateRoomOptions, RoomKit,
    RoomKitError, RoomKitOptions,
};
use std::sync::atomic::Ordering;
use std::time::Duration;
use store::TimerStore;

const BOUND: Duration = Duration::from_secs(5);
fn id(name: &str) -> RoomId {
    RoomId::new(name).unwrap()
}

async fn room(kit: &RoomKit<TimerStore>, name: &str, closing: bool) {
    let mut room = kit
        .create_room(CreateRoomOptions {
            room_id: Some(id(name)),
            timers: Some(RoomTimers {
                inactive_after_seconds: Some(60),
                closed_after_seconds: closing.then_some(60),
                last_activity_at: Some(Timestamp::UNIX_EPOCH),
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    if closing {
        room.status = RoomStatus::Paused;
        room.paused_at = Some(Timestamp::UNIX_EPOCH);
        kit.store().store.update_room(room).await.unwrap();
    }
}

async fn reached(kit: &RoomKit<TimerStore>) {
    tokio::time::timeout(BOUND, kit.store().reached.notified())
        .await
        .unwrap();
}

// Python: tests/test_room_timers.py::test_a_commit_landing_before_the_transition_write_is_not_overwritten
#[tokio::test]
async fn a_commit_landing_before_the_transition_write_is_not_overwritten() {
    for closing in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::with_store(TimerStore::default()));
        room(&kit, "room", closing).await;
        let tap = EventTap::on(&kit, &["room_paused", "room_closed"]);
        kit.store().before_write.store(true, Ordering::SeqCst);
        let checking = {
            let kit = kit.clone();
            tokio::spawn(async move {
                kit.check_room_timers(&id("room"), CheckRoomTimersOptions::default())
                    .await
            })
        };
        reached(&kit).await;
        let mut event = RoomEvent::new(
            id("room"),
            EventSource::new(ChannelId::new("voice").unwrap(), ChannelType::VOICE),
            TextContent::new("hello"),
        );
        event.status = roomkit::model::EventStatus::Delivered;
        kit.store()
            .store
            .commit_event(
                &id("room"),
                event,
                roomkit::store::CommitEventOptions::default(),
            )
            .await
            .unwrap();
        let committed = kit
            .store()
            .store
            .get_room(&id("room"))
            .await
            .unwrap()
            .unwrap();
        kit.store().release.notify_one();
        let checked = tokio::time::timeout(BOUND, checking)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        idle(&kit).await;
        assert_eq!(checked, committed);
        assert_eq!(checked.status, RoomStatus::Active);
        assert_eq!(checked.event_count, 1);
        assert_eq!(checked.latest_index, Some(0));
        assert_eq!(tap.events(), Vec::<roomkit::FrameworkEvent>::new());
        assert_eq!(kit.store().conditional_writes.load(Ordering::SeqCst), 1);
        kit.close().await.unwrap();
    }
}

// Python: tests/test_resilience.py::TestRoomTimers::test_check_all_timers_goes_on_past_a_room_that_fails
#[tokio::test]
async fn a_sweep_passes_a_failed_or_deleted_room_and_checks_the_rest() {
    for deleted in [false, true] {
        let (warnings, _capture) = warnings::Warnings::capture();
        let kit = RoomKit::new(RoomKitOptions::with_store(TimerStore::default()));
        room(&kit, "bad", false).await;
        room(&kit, "good", false).await;
        if deleted {
            kit.store().delete_listed.store(true, Ordering::SeqCst);
        } else {
            *kit.store().fail_room.lock().unwrap() = Some(id("bad"));
        }
        let changed = kit
            .check_all_timers(CheckAllTimersOptions::default())
            .await
            .unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].id, id("good"));
        assert_eq!(changed[0].status, RoomStatus::Paused);
        assert_ne!(warnings.containing("bad"), Vec::<String>::new());
        kit.close().await.unwrap();
    }
}

// Python: tests/test_resilience.py::TestRoomTimers::test_the_kit_s_close_stops_a_sweep
#[tokio::test]
async fn the_kit_s_close_stops_a_sweep_already_listing_its_rooms() {
    let kit = RoomKit::new(RoomKitOptions::with_store(TimerStore::default()));
    room(&kit, "room", false).await;
    kit.store().after_listing.store(true, Ordering::SeqCst);
    let sweep = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.check_all_timers(CheckAllTimersOptions::default()).await })
    };
    reached(&kit).await;
    kit.close().await.unwrap();
    kit.store().release.notify_one();
    let result = tokio::time::timeout(BOUND, sweep).await.unwrap().unwrap();
    assert!(matches!(result, Err(RoomKitError::KitClosed)));
    assert_eq!(
        kit.store()
            .store
            .get_room(&id("room"))
            .await
            .unwrap()
            .unwrap()
            .status,
        RoomStatus::Active
    );
}

#[tokio::test]
async fn a_timer_check_cancelled_before_its_write_changes_nothing() {
    let kit = RoomKit::new(RoomKitOptions::with_store(TimerStore::default()));
    room(&kit, "room", false).await;
    kit.store().before_read.store(true, Ordering::SeqCst);
    let checking = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.check_room_timers(&id("room"), CheckRoomTimersOptions::default())
                .await
        })
    };
    reached(&kit).await;
    checking.abort();
    assert!(checking.await.unwrap_err().is_cancelled());
    kit.store().release.notify_one();
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
    assert_eq!(kit.store().conditional_writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        kit.store()
            .store
            .get_room(&id("room"))
            .await
            .unwrap()
            .unwrap()
            .status,
        RoomStatus::Active
    );
    kit.close().await.unwrap();
}
