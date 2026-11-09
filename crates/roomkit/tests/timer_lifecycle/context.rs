//! Hooks receive the stored context, including writes outside the room lock.
use super::*;
use roomkit::model::RoomContext;
use roomkit::{HookExecution, HookOptions, HookTrigger};
use std::future::ready;
use std::sync::{Arc, Mutex};

// Python: tests/test_room_timers.py::test_timer_hooks_read_activity_committed_after_the_transition_write
#[tokio::test]
async fn timer_hooks_read_activity_committed_after_the_transition_write() {
    let kit = RoomKit::new(RoomKitOptions::with_store(TimerStore::default()));
    room(&kit, "room", false).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let contexts = Arc::clone(&seen);
    kit.hook(
        HookTrigger::OnRoomPaused,
        move |_: RoomEvent, context: RoomContext| {
            contexts.lock().unwrap().push(context);
            ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "stored".into(),
            ..Default::default()
        },
    )
    .unwrap();
    kit.store().after_write.store(true, Ordering::SeqCst);
    let checking = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.check_room_timers(&id("room"), Default::default()).await })
    };
    reached(&kit).await;
    kit.store()
        .store
        .commit_event(
            &id("room"),
            RoomEvent::new(
                id("room"),
                EventSource::new(ChannelId::new("voice").unwrap(), ChannelType::VOICE),
                TextContent::new("hello"),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    kit.store().release.notify_one();
    let checked = tokio::time::timeout(BOUND, checking)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    idle(&kit).await;
    let stored = kit
        .store()
        .store
        .get_room(&id("room"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checked.status, RoomStatus::Active);
    assert_eq!(checked.event_count, stored.event_count);
    assert_eq!(checked.latest_index, stored.latest_index);
    let contexts = seen.lock().unwrap().clone();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].room, checked);
    assert_eq!(contexts[0].recent_events.len(), 1);
    assert_eq!(
        contexts[0].recent_events[0].source.channel_id,
        ChannelId::new("voice").unwrap()
    );
    kit.close().await.unwrap();
}
