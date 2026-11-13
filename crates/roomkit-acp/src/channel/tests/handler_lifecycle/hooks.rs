//! Registered handler hooks work outside a turn, with their kit's telemetry.

use std::sync::Arc;

use roomkit::model::{HookResult, Metadata, RoomContext, RoomId};
use roomkit::telemetry::{MockTelemetryProvider, SpanKind};
use roomkit::{
    CreateRoomOptions, HookExecution, HookRegistration, HookTrigger, RoomKit, RoomKitOptions,
    ToolCallEvent,
};
use roomkit_ai::{
    ExternalToolHandler, FireBeforeHookOptions, OnToolResultOptions, ProcessToolCallOptions,
};
use serde_json::json;

use super::fixture::channel;

#[tokio::test]
async fn off_turn_handler_hooks_use_the_registered_kits_telemetry() {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::clone(&telemetry).into()),
        ..Default::default()
    });
    let (agent, _, service) = channel(0, false);
    kit.register_channel(agent).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for (trigger, mode) in [
        (HookTrigger::BeforeToolUse, HookExecution::Sync),
        (HookTrigger::OnToolCall, HookExecution::Async),
    ] {
        kit.turn_executor()
            .hooks()
            .register(HookRegistration::new(
                trigger,
                mode,
                |_: ToolCallEvent, _: RoomContext| async {
                    HookResult::allow(roomkit::model::hook::AllowOptions::default())
                },
            ))
            .unwrap();
    }
    let decision = service
        .process_tool_call(
            "Read",
            &Metadata::new(),
            ProcessToolCallOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(decision.approved);
    service
        .on_tool_result(
            "Read",
            &Metadata::new(),
            "read",
            OnToolResultOptions {
                room_id: Some(room),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(telemetry.get_spans(SpanKind::HookSync).len(), 1);
    assert_eq!(telemetry.get_spans(SpanKind::HookAsync).len(), 1);
    assert!(
        telemetry
            .get_spans(SpanKind::HookSync)
            .iter()
            .all(|span| span.parent_id.is_none())
    );
}

#[tokio::test]
async fn inherited_approval_helper_preserves_the_raw_decision() {
    let (agent, _, service) = channel(0, false);
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::BeforeToolUse,
            HookExecution::Sync,
            |_: ToolCallEvent, _: RoomContext| async {
                let mut result =
                    HookResult::block("", roomkit::model::hook::BlockOptions::default());
                result
                    .metadata
                    .insert("arguments".to_owned(), json!({"path":"other"}));
                result
            },
        ))
        .unwrap();
    let input = serde_json::from_value(json!({"path":"original"})).unwrap();
    let raw = service
        .state()
        .fire_before_hook(
            "Read",
            &input,
            FireBeforeHookOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await;
    assert!(!raw.allowed);
    assert_eq!(raw.reason.as_deref(), Some(""));
    assert_eq!(
        raw.arguments,
        Some(serde_json::from_value(json!({"path":"other"})).unwrap())
    );
    assert!(raw.detail.is_none());
    let adapted = service
        .process_tool_call(
            "Read",
            &input,
            ProcessToolCallOptions {
                room_id: Some(room),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!adapted.approved);
    assert_eq!(adapted.reason, "Tool 'Read' denied by pre-execution hook.");
    assert!(adapted.modified_input.is_none());
    kit.close().await.unwrap();
}
