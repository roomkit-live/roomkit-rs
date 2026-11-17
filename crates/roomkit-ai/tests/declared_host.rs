//! Declared callbacks reached through a real local turn (RFC §10.6.5).
//! Python: the declared-verb cases in `tests/test_turns_host.py`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "declared_host/close.rs"]
mod close;

use std::sync::{Arc, Mutex};

use roomkit::model::{ChannelType, InboundMessage, Metadata, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::turns::{DeclareHostVerbOptions, DeclaredVerb};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitError,
    RoomKitOptions, current_turn_host,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiTool, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ToolHandler, ToolResult, current_response_metadata,
    current_tool_actor_id, current_tool_call, current_tool_room_id,
};
use serde_json::{Value, json};

async fn kit(streaming: bool, handler: ToolHandler) -> RoomKit {
    let mut calling = AiResponse::new("");
    calling.tool_calls.push(AiToolCall::new("c1", "lookup"));
    let provider = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![calling, AiResponse::new("done")]),
        ..Default::default()
    });
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        provider,
        AiChannelOptions {
            tools: vec![AiTool::new("lookup", "Look up").unwrap().into()],
            tool_handler: Some(handler),
            tool_search: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        "web".parse().unwrap(),
        ChannelType::WEBHOOK,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some("r".parse().unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    for channel in ["web", "ai"] {
        kit.attach_channel(
            &"r".parse().unwrap(),
            &channel.parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}

#[tokio::test]
async fn a_declared_callback_reads_its_call_and_merges_its_record() {
    for streaming in [false, true] {
        let seen = Arc::new(Mutex::new(None));
        let saved = Arc::clone(&seen);
        let handler = ToolHandler::new(move |_, _| {
            let saved = Arc::clone(&saved);
            async move {
                let record = current_response_metadata().unwrap();
                record.insert("asked", json!("boards"));
                let arguments = json!({"page": "/boards"}).as_object().unwrap().clone();
                let answer = current_turn_host()
                    .unwrap()
                    .call("app.where", Some(arguments))
                    .await;
                *saved.lock().unwrap() = Some((
                    answer.map_err(|error| error.to_string()),
                    record.get("answered"),
                ));
                Ok(ToolResult::Text("handled".into()))
            }
        });
        let kit = kit(streaming, handler).await;
        kit.declare_host_verb("app.where", DeclaredVerb::new(|arguments| async move {
            let call = current_tool_call();
            let record = current_response_metadata();
            let answer = json!({
                "room": current_tool_room_id().map(|id| id.to_string()),
                "actor": current_tool_actor_id(),
                "channel": call.as_ref().and_then(|call| call.channel_id.as_ref()).map(ToString::to_string),
                "id": call.map(|call| call.tool_call_id),
                "asked": record.as_ref().and_then(|record| record.get("asked")),
                "arguments": arguments,
                "has_host": current_turn_host().is_some(),
            });
            if let Some(record) = record { record.insert("answered", json!(true)); }
            Ok(answer)
        }), DeclareHostVerbOptions::default()).unwrap();
        let output = kit
            .process_inbound(
                InboundMessage::new("web".parse().unwrap(), "alice", TextContent::new("go")),
                ProcessInboundOptions {
                    room_id: Some("r".parse().unwrap()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            Some((
                Ok(json!({
                    "room": "r", "actor": "alice", "channel": "ai", "id": "c1", "asked": "boards",
                    "arguments": {"page": "/boards"}, "has_host": false,
                })),
                Some(json!(true))
            ))
        );
        assert_eq!(output.response_metadata.get("asked"), Some(json!("boards")));
        assert_eq!(output.response_metadata.get("answered"), Some(json!(true)));
        kit.close().await.unwrap();
    }
}

#[tokio::test]
async fn declaration_refuses_reserved_empty_and_duplicate_names_then_closed_kits() {
    let kit: RoomKit = RoomKit::new(RoomKitOptions::default());
    let verb = DeclaredVerb::new(|_| async { Ok(Value::Null) });
    kit.declare_host_verb("app.twice", verb.clone(), DeclareHostVerbOptions::default())
        .unwrap();
    for name in [
        "",
        "send_event",
        "delegate",
        "wait_task",
        "post_status",
        "update_room_metadata",
        "add_observation",
        "room_tasks",
        "room_context",
        "room_vision",
        "app.twice",
    ] {
        assert!(matches!(
            kit.declare_host_verb(name, verb.clone(), DeclareHostVerbOptions::default()),
            Err(RoomKitError::Value(_))
        ));
    }
    kit.close().await.unwrap();
    for name in ["app.new", "app.twice", ""] {
        assert!(matches!(
            kit.declare_host_verb(name, verb.clone(), DeclareHostVerbOptions::default()),
            Err(RoomKitError::KitClosed)
        ));
    }
    assert_eq!(verb.call(Metadata::new()).await.unwrap(), Value::Null);
}

#[tokio::test]
async fn a_retained_child_does_not_lend_its_old_host_or_record_to_the_owner() {
    for streaming in [false, true] {
        let ready = Arc::new(tokio::sync::Notify::new());
        let child = Arc::new(Mutex::new(None));
        let saving = Arc::clone(&child);
        let waiting = Arc::clone(&ready);
        let handler = ToolHandler::new(move |_, _| {
            let saving = Arc::clone(&saving);
            let waiting = Arc::clone(&waiting);
            async move {
                let record = current_response_metadata().unwrap();
                record.insert("old", json!(true));
                let later = roomkit::keep_context(async move {
                    waiting.notified().await;
                    let host = current_turn_host().unwrap();
                    assert_eq!(
                        current_response_metadata().unwrap().get("old"),
                        Some(json!(true))
                    );
                    let answer = host.call("app.later", None).await.unwrap();
                    assert!(current_turn_host().is_some());
                    assert_eq!(
                        current_response_metadata().unwrap().get("old"),
                        Some(json!(true))
                    );
                    answer
                });
                *saving.lock().unwrap() = Some(tokio::spawn(later));
                Ok(ToolResult::Text("scheduled".into()))
            }
        });
        let kit = kit(streaming, handler).await;
        kit.declare_host_verb(
            "app.later",
            DeclaredVerb::new(|_| async {
                Ok(json!({
                    "host": current_turn_host().is_some(),
                    "record": current_response_metadata().is_some(),
                    "actor": current_tool_actor_id(),
                    "id": current_tool_call().map(|call| call.tool_call_id),
                }))
            }),
            DeclareHostVerbOptions {
                between_turns: true,
            },
        )
        .unwrap();
        kit.process_inbound(
            InboundMessage::new("web".parse().unwrap(), "alice", TextContent::new("go")),
            ProcessInboundOptions {
                room_id: Some("r".parse().unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        ready.notify_one();
        let child = child.lock().unwrap().take().unwrap();
        let answer = tokio::time::timeout(std::time::Duration::from_secs(2), child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            answer,
            json!({"host": false, "record": false, "actor": null, "id": ""})
        );
        kit.close().await.unwrap();
    }
}
