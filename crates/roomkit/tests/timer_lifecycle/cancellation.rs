//! A cancelled caller finishes its timer gate without starting its own change.
use super::*;
use roomkit::model::RoomContext;
use roomkit::{HookExecution, HookOptions, HookTrigger};
use std::future::ready;
use std::sync::{Arc, Mutex};

// Python: tests/test_room_timers.py::test_a_transition_its_caller_left_is_reported_whole
#[tokio::test]
async fn a_transition_its_caller_left_is_reported_whole() {
    for entry in [Entry::Check, Entry::Inbound, Entry::Attach, Entry::Record] {
        reports_after_cancellation(entry).await;
    }
}

async fn reports_after_cancellation(entry: Entry) {
    let kit = RoomKit::new(RoomKitOptions::with_store(TimerStore::default()));
    room(&kit, "room", false).await;
    let mut stored = kit
        .store()
        .store
        .get_room(&id("room"))
        .await
        .unwrap()
        .unwrap();
    stored.timers.closed_after_seconds = Some(60);
    kit.store().store.update_room(stored).await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    for trigger in [HookTrigger::OnRoomPaused, HookTrigger::OnRoomClosed] {
        let seen = Arc::clone(&seen);
        kit.hook(
            trigger,
            move |_: RoomEvent, context: RoomContext| {
                seen.lock().unwrap().push(context);
                ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                name: format!("{trigger:?}"),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let tap = EventTap::on(&kit, &["room_paused", "room_closed"]);
    kit.store().after_write.store(true, Ordering::SeqCst);
    register_sources(&kit).await;
    let checking = {
        let kit = kit.clone();
        tokio::spawn(async move { entry.invoke(&kit).await })
    };
    reached(&kit).await;
    checking.abort();
    assert!(checking.await.unwrap_err().is_cancelled());
    kit.store().release.notify_one();
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
    let stored = kit
        .store()
        .store
        .get_room(&id("room"))
        .await
        .unwrap()
        .unwrap();
    let events = kit
        .store()
        .store
        .list_events(&id("room"), roomkit::store::ListEventsOptions::default())
        .await
        .unwrap();
    assert_eq!(tap.types(), ["room_paused", "room_closed"]);
    assert_eq!(events.len(), 2);
    assert_eq!(stored.event_count, 2);
    assert_eq!(stored.latest_index, Some(1));
    assert_eq!(stored.status, RoomStatus::Closed);
    let contexts = seen.lock().unwrap().clone();
    assert_eq!(contexts.len(), 2);
    for context in contexts {
        // Releasing the lock advances the delivery cursor after this snapshot.
        assert_eq!(context.room.status, stored.status);
        assert_eq!(context.room.paused_at, stored.paused_at);
        assert_eq!(context.room.closed_at, stored.closed_at);
        assert_eq!(context.room.event_count, stored.event_count);
        assert_eq!(context.room.latest_index, stored.latest_index);
        assert_eq!(context.recent_events, Vec::<RoomEvent>::new());
    }
    assert!(
        kit.store()
            .store
            .get_binding(&id("room"), &ChannelId::new("ws").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    kit.close().await.unwrap();
}

#[derive(Clone, Copy)]
enum Entry {
    Check,
    Inbound,
    Attach,
    Record,
}

impl Entry {
    async fn invoke(self, kit: &RoomKit<TimerStore>) -> Result<(), RoomKitError> {
        match self {
            Self::Check => kit
                .check_room_timers(&id("room"), CheckRoomTimersOptions::default())
                .await
                .map(drop),
            Self::Inbound => {
                let message = roomkit::model::InboundMessage::new(
                    ChannelId::new("sms").unwrap(),
                    "alice",
                    TextContent::new("too late"),
                );
                kit.process_inbound(
                    message,
                    roomkit::ProcessInboundOptions {
                        room_id: Some(id("room")),
                        ..Default::default()
                    },
                )
                .await
                .map(drop)
            }
            Self::Attach => kit
                .attach_channel(
                    &id("room"),
                    &ChannelId::new("ws").unwrap(),
                    roomkit::AttachChannelOptions::default(),
                )
                .await
                .map(drop),
            Self::Record => {
                let event = RoomEvent::new(
                    id("room"),
                    EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
                    TextContent::new("cancelled record"),
                );
                kit.commit_event(&id("room"), event, roomkit::CommitEventOptions::default())
                    .await
                    .map(drop)
            }
        }
    }
}

async fn register_sources(kit: &RoomKit<TimerStore>) {
    let sms = roomkit::testing::RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.store()
        .store
        .add_binding(roomkit::model::ChannelBinding::at(
            ChannelId::new("sms").unwrap(),
            id("room"),
            ChannelType::SMS,
            roomkit::model::ChannelCategory::Transport,
            roomkit::model::ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        ))
        .await
        .unwrap();
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
}
