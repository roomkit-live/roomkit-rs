//! Python: `tests/test_local_host_context.py`, through real local AI tools.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "host_context/store.rs"]
mod store;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use roomkit::model::{ChannelType, InboundMessage, Metadata, RoomContext, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AddMemberOptions, AttachChannelOptions, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, SendEventOptions,
    UpdateRoomMetadataOptions, current_turn_host,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiTool, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ToolHandler, ToolResult,
};
use serde_json::{Value, json};

async fn kit<S: ConversationStore>(store: S) -> RoomKit<S> {
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    for name in ["ops", "late"] {
        kit.register_channel(RecordingTransport::new(
            name.parse().unwrap(),
            ChannelType::WEBHOOK,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
    }
    kit.create_room(CreateRoomOptions {
        room_id: Some("r1".parse().unwrap()),
        metadata: json!({"nested": {"version": "before"}})
            .as_object()
            .unwrap()
            .clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &"r1".parse().unwrap(),
        &"ops".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.send_event(
        &"r1".parse().unwrap(),
        &"ops".parse().unwrap(),
        TextContent::new("earlier"),
        SendEventOptions::default(),
    )
    .await
    .unwrap();
    kit
}

async fn read<S: ConversationStore>(
    kit: &RoomKit<S>,
    streaming: bool,
    window: usize,
    handler: ToolHandler,
) {
    let mut calling = AiResponse::new("");
    calling.tool_calls.push(AiToolCall::new("read-1", "read"));
    let provider = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![calling, AiResponse::new("done")]),
        ..Default::default()
    });
    kit.register_channel(
        AiChannel::new(
            "agent".parse().unwrap(),
            provider,
            AiChannelOptions {
                max_context_events: window,
                tools: vec![AiTool::new("read", "Read the room").unwrap().into()],
                tool_handler: Some(handler),
                tool_search: Some(false),
                ..Default::default()
            },
        )
        .unwrap(),
    )
    .unwrap();
    kit.attach_channel(
        &"r1".parse().unwrap(),
        &"agent".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.process_inbound(
        InboundMessage::new(
            "ops".parse().unwrap(),
            "alice",
            TextContent::new("read now"),
        ),
        ProcessInboundOptions {
            room_id: Some("r1".parse().unwrap()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

async fn change_room<S: ConversationStore>(kit: &RoomKit<S>) {
    let room = "r1".parse().unwrap();
    kit.update_room_metadata(
        &room,
        json!({"nested": {"version": "after"}})
            .as_object()
            .unwrap()
            .clone(),
        UpdateRoomMetadataOptions::default(),
    )
    .await
    .unwrap();
    kit.attach_channel(
        &room,
        &"late".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.add_member(
        &room,
        &"ops".parse().unwrap(),
        &"bob".parse().unwrap(),
        AddMemberOptions {
            display_name: Some("Bob".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    kit.send_event(
        &room,
        &"agent".parse().unwrap(),
        TextContent::new("written during the tool"),
        SendEventOptions::default(),
    )
    .await
    .unwrap();
}

fn assert_fresh(before: &Metadata, fresh: &Metadata) {
    assert_eq!(
        before
            .get("room")
            .unwrap()
            .pointer("/metadata/nested")
            .unwrap(),
        &json!({"version": "before"})
    );
    assert_eq!(
        fresh
            .get("room")
            .unwrap()
            .pointer("/metadata/nested")
            .unwrap(),
        &json!({"version": "after"})
    );
    for (key, field, name) in [
        ("bindings", "channel_id", "late"),
        ("participants", "id", "bob"),
    ] {
        assert!(
            !before[key]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item[field] == name)
        );
        assert!(
            fresh[key]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item[field] == name)
        );
    }
    assert_eq!(
        fresh["recent_events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event.pointer("/content/body").unwrap().clone())
            .collect::<Vec<_>>(),
        vec![
            json!("earlier"),
            json!("read now"),
            json!("written during the tool")
        ]
    );
    assert_eq!(fresh["history_start_index"], 0);
    assert!(
        fresh
            .get("room")
            .unwrap()
            .get("created_at")
            .unwrap()
            .is_string()
    );
    assert_eq!(
        serde_json::from_value::<RoomContext>(Value::Object(fresh.clone()))
            .unwrap()
            .room
            .id
            .as_str(),
        "r1"
    );
    let mut keys = fresh.keys().map(String::as_str).collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "author_register",
            "bindings",
            "history_start_index",
            "participants",
            "recent_events",
            "room"
        ]
    );
}

#[tokio::test]
async fn the_host_rereads_room_members_bindings_and_history() {
    for streaming in [false, true] {
        for window in [0, 50] {
            let kit = kit(roomkit::InMemoryStore::new()).await;
            let weak = kit.downgrade();
            let seen = Arc::new(Mutex::new(None));
            let saved = Arc::clone(&seen);
            let handler = ToolHandler::new(move |_, _| {
                let (weak, saved) = (weak.clone(), Arc::clone(&saved));
                async move {
                    let host = current_turn_host().unwrap();
                    let before = host.room_context().await.unwrap();
                    change_room(&weak.upgrade().unwrap()).await;
                    let mut changed = host.room_context().await.unwrap();
                    changed["room"]["metadata"]["nested"]["version"] =
                        json!("caller changed its copy");
                    let fresh = host.room_context().await.unwrap();
                    *saved.lock().unwrap() = Some((before, changed, fresh));
                    Ok(ToolResult::Text("read".into()))
                }
            });
            read(&kit, streaming, window, handler).await;
            kit.close().await.unwrap();
            let (before, changed, fresh) = seen.lock().unwrap().take().unwrap();
            assert_eq!(
                changed["room"]["metadata"]["nested"]["version"],
                "caller changed its copy"
            );
            assert_fresh(&before, &fresh);
        }
    }
}

#[tokio::test]
async fn a_host_context_read_propagates_its_store_failure() {
    for streaming in [false, true] {
        for fault in [1, 2] {
            let kit = kit(store::ContextStore::default()).await;
            let weak = kit.downgrade();
            let seen = Arc::new(Mutex::new(None));
            let saved = Arc::clone(&seen);
            let handler = ToolHandler::new(move |_, _| {
                let (weak, saved) = (weak.clone(), Arc::clone(&saved));
                async move {
                    let kit = weak.upgrade().unwrap();
                    kit.store().fault.store(fault, Ordering::SeqCst);
                    let result = current_turn_host().unwrap().room_context().await;
                    kit.store().fault.store(0, Ordering::SeqCst);
                    *saved.lock().unwrap() = Some(result);
                    Ok(ToolResult::Text("read".into()))
                }
            });
            read(&kit, streaming, 50, handler).await;
            kit.close().await.unwrap();
            let error = seen.lock().unwrap().take().unwrap().unwrap_err();
            if fault == 1 {
                assert!(
                    matches!(&error, RoomKitError::RoomNotFound(room) if room.as_str() == "r1")
                );
                assert_eq!(error.to_string(), "room r1 not found");
            } else {
                assert!(matches!(error, RoomKitError::Store(_)));
                assert_eq!(error.to_string(), "the store failed");
                assert_eq!(
                    std::error::Error::source(&error).unwrap().to_string(),
                    "context store unavailable"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_room_context_reader_is_refused_after_its_turn() {
    for streaming in [false, true] {
        let kit = kit(roomkit::InMemoryStore::new()).await;
        let seen = Arc::new(Mutex::new(None));
        let saved = Arc::clone(&seen);
        let handler = ToolHandler::new(move |_, _| {
            let saved = Arc::clone(&saved);
            async move {
                let host = current_turn_host().unwrap();
                let result = host.room_context().await;
                *saved.lock().unwrap() = Some((host, result));
                Ok(ToolResult::Text("read".into()))
            }
        });
        read(&kit, streaming, 50, handler).await;
        let (host, result) = seen.lock().unwrap().take().unwrap();
        assert_eq!(result.unwrap()["room"]["id"], "r1");
        let error = host.room_context().await.unwrap_err();
        assert!(matches!(error, RoomKitError::Runtime(_)));
        assert_eq!(
            error.to_string(),
            "room_context needs a turn: after its turn ended, a host sends add_observation, post_status, send_event, update_room_metadata only (RFC §10.6.5)"
        );
        kit.close().await.unwrap();
    }
}
