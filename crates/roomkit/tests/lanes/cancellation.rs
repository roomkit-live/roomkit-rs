//! Caller cancellation while both the turn and the transport are held.

use std::future::ready;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, EventType, RoomEvent, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_core::rules::transcoding::text_of;

use super::{BOUND, Gated, SlowAgent, channel, every_row, message, room_id};

async fn setup() -> (RoomKit, SlowAgent, Gated) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let agent = SlowAgent::new();
    let target = Gated::new();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.register_channel(target.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai", "slow"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, agent, target)
}

fn observe(kit: &RoomKit) -> Arc<Mutex<Vec<String>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            observed.lock().unwrap().push(text_of(&event.content));
            ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "aftermath".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    seen
}

async fn dropped_call(direct: bool) {
    let (kit, agent, target) = setup().await;
    let observed = observe(&kit);
    let calling = kit.clone();
    let caller = tokio::spawn(async move {
        if direct {
            calling
                .send_event(
                    &room_id(),
                    &channel("sms"),
                    TextContent::new("Bonjour"),
                    roomkit::SendEventOptions::default(),
                )
                .await
                .unwrap();
        } else {
            calling
                .process_inbound(
                    message("sms", "alice", "Bonjour"),
                    ProcessInboundOptions {
                        room_id: Some(room_id()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
    });
    tokio::time::timeout(BOUND, agent.turning.notified())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, target.delivering.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(
        tokio::time::timeout(BOUND, caller)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    tokio::time::timeout(BOUND, async {
        while agent.log.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*agent.log.lock().unwrap(), ["dropped unfinished"]);
    assert_eq!(*target.delivered.lock().unwrap(), Vec::<String>::new());
    target.go.notify_one();
    agent.go.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert_eq!(*target.delivered.lock().unwrap(), ["Bonjour"]);
    assert_eq!(*observed.lock().unwrap(), ["Bonjour"]);
    assert_eq!(*agent.log.lock().unwrap(), ["dropped unfinished"]);
    let messages: Vec<_> = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.r#type == EventType::Message)
        .map(|e| text_of(&e.content))
        .collect();
    assert_eq!(messages, ["Bonjour"]);
    kit.close().await.unwrap();
}

// Python: tests/test_delivery_cancel_turns.py::test_a_cancel_returns_at_once_while_a_transport_still_delivers
#[tokio::test]
async fn dropping_inbound_cuts_its_turn_while_its_transport_still_delivers() {
    dropped_call(false).await;
}

// Python: tests/test_delivery_cancel_turns.py::test_a_cancelled_send_event_is_still_delivered
#[tokio::test]
async fn dropping_send_event_cuts_its_turn_and_keeps_its_transport_delivery() {
    dropped_call(true).await;
}
