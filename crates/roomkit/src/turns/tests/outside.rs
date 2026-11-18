//! Owner callbacks from a peer after the turn that started its work.

use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{ChannelType, EventSource, RoomEvent, RoomId, TextContent, Visibility};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::support::channel;
use crate::context::ExecutionContext;
use crate::realtime::{EphemeralEvent, EphemeralEventType};
use crate::testing::{RecordingTransport, RecordingTransportOptions};
use crate::turn::TurnKit;
use crate::turns::wire::{OwnerMessageEnvelope, PreviousTurn, RoomCallResult};
use crate::{
    AttachChannelOptions, CreateRoomOptions, RoomKit, RoomKitOptions, SubscribeRoomOptions,
};

async fn dispatch(kit: &RoomKit, message: Value) -> Option<RoomCallResult> {
    let port: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
    let envelope: OwnerMessageEnvelope =
        serde_json::from_value(json!({"message":message})).unwrap();
    kit.inner.turns.receive_owner_message(port, envelope).await
}

fn publication(turn: &str, executor: &str, frame: &Value) -> Value {
    json!({"type":"room_publication","turn_id":turn,"executor_id":executor,"room_id":"r","channel_id":"app","frame":frame})
}

fn call(turn: &str, executor: &str, key: &str) -> Value {
    json!({"type":"room_call","turn_id":turn,"executor_id":executor,"room_id":"r","channel_id":"app",
        "call_id":key,"verb":"update_room_metadata","arguments":{"room_id":"r","metadata":{key:2}}})
}

async fn owner() -> (RoomKit, RoomId) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("r").unwrap();
    let id = channel("app");
    kit.register_channel(RecordingTransport::new(
        id.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let created = kit
        .create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    kit.attach_channel(&room, &id, AttachChannelOptions::default())
        .await
        .unwrap();
    let mut trigger = RoomEvent::new(
        room.clone(),
        EventSource::new(id.clone(), ChannelType::SMS),
        TextContent::new("hello"),
    );
    trigger.chain_depth = 2;
    let first = PreviousTurn::after("first", None, Some(&trigger));
    trigger.chain_depth = 5;
    trigger.response_visibility = Some(Visibility::None);
    let next = PreviousTurn::after("next", Some(&first), Some(&trigger));
    {
        let mut records = kit.inner.turns.records();
        records.put(&id, &room, next);
        records.mark_room(&room, created.created_at);
        records.took(&id, &room, "next", "right");
    }
    (kit, room)
}

#[tokio::test]
async fn the_owner_runs_only_its_exact_last_workers_room_call() {
    let (kit, room) = owner().await;
    for (turn, executor, id) in [
        ("first", "right", "old"),
        ("next", "wrong", "foreign"),
        ("next", "right", "new"),
    ] {
        let result = dispatch(&kit, call(turn, executor, id)).await.unwrap();
        assert_eq!(result.call_id, id);
        if id == "new" {
            assert!(result.error.is_none(), "{result:?}");
        } else {
            assert_eq!(result.error.unwrap().kind, "PermissionError");
        }
    }
    let saved = kit
        .get_room(&room, crate::GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(saved.metadata.get("new"), Some(&Value::from(2)));
    assert!(!saved.metadata.contains_key("old"));
    assert!(!saved.metadata.contains_key("foreign"));
    // The same caller credentials must not allow work on a new incarnation.
    {
        let id = channel("app");
        let mut records = kit.inner.turns.records();
        records.pop_room(&room);
        let mut held = PreviousTurn::after("next", None, None);
        held.executor_id = Some("right".into());
        records.put(&id, &room, held);
        records.mark_room(&room, roomkit_core::model::Timestamp::UNIX_EPOCH);
    }
    let answer = dispatch(&kit, call("next", "right", "stale"))
        .await
        .unwrap();
    assert_eq!(answer.error.unwrap().kind, "ValueError");
    kit.close().await.unwrap();
}

#[tokio::test]
async fn publications_use_the_named_turns_scope_and_current_executor() {
    let (kit, room) = owner().await;
    let (events, mut received) = mpsc::unbounded_channel();
    kit.on("tool_call", move |event| {
        let events = events.clone();
        async move {
            events
                .send((event.data, ExecutionContext::current().turn_depth()))
                .unwrap();
        }
    })
    .unwrap();
    let (tx, mut notification) = mpsc::unbounded_channel();
    kit.subscribe_room(
        &room,
        move |event| {
            let tx = tx.clone();
            async move {
                tx.send(event).unwrap();
            }
        },
        SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    let framework = |id: &str, room_id: &str| json!({"type":"framework_event","event_type":"tool_call","room_id":room_id,"data":{"id":id}});
    for (turn, executor, frame) in [
        ("first", "wrong", framework("foreign", "r")),
        ("first", "right", framework("wrong room", "other")),
        ("first", "right", framework("kept", "r")),
    ] {
        assert!(
            dispatch(&kit, publication(turn, executor, &frame))
                .await
                .is_none()
        );
    }
    let (payload, depth) = tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload["id"], "kept");
    assert_eq!(depth, Some(3));
    let mut event = EphemeralEvent::new(room, EphemeralEventType::Custom, "app");
    event.channel_id = Some(channel("app"));
    for (turn, kind, user) in [
        ("missing", EphemeralEventType::Custom, "unknown"),
        ("next", EphemeralEventType::Custom, "scoped"),
        ("first", EphemeralEventType::TextDelta, "text"),
        ("first", EphemeralEventType::Custom, "allowed"),
    ] {
        event.user_id = user.to_owned();
        event.r#type = kind;
        let frame = json!({"type":"ephemeral","channel":"room:r","event":event.to_dict()});
        assert!(
            dispatch(&kit, publication(turn, "right", &frame))
                .await
                .is_none()
        );
    }
    let event = tokio::time::timeout(Duration::from_secs(2), notification.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.user_id, "allowed");
    assert!(event.room_created_at.is_some());
    kit.close().await.unwrap();
}
