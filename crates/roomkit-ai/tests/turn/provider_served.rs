//! A known provider outcome survives cancellation while its report runs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{HookResult, InboundMessage, TextContent};
use roomkit::{
    CancelOptions, HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, ToolCallEvent,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiToolCall, MockAiProvider, MockAiProviderOptions,
    ServedCall,
};
use serde_json::Value;
use tokio::sync::Notify;

use super::support::{Room, channel, room_id};

// Python: tests/test_external_call_cut.py::test_a_call_the_provider_ran_cut_while_reported_keeps_its_outcome[no-handler]
#[tokio::test]
async fn a_provider_call_cut_while_reported_keeps_its_one_original_outcome() {
    for streaming in [false, true] {
        for is_error in [false, true] {
            known_report_survives_cut(streaming, is_error).await;
        }
    }
}

async fn known_report_survives_cut(streaming: bool, is_error: bool) {
    let mut call = AiToolCall::new("c1", "lookup");
    call.served = Some(ServedCall {
        result: "known outcome".into(),
        is_error,
    });
    let mut response = AiResponse::new("");
    response.tool_calls.push(call);
    let model = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![response]),
        ..Default::default()
    });
    let agent = AiChannel::new(channel("assistant"), model, AiChannelOptions::default()).unwrap();
    let room = Room::with_agent(agent).await;
    let entered = Arc::new(Notify::new());
    let held = Arc::clone(&entered);
    room.kit
        .hook(
            HookTrigger::OnToolCall,
            move |_: ToolCallEvent, _| {
                held.notify_one();
                std::future::pending::<Result<HookResult<ToolCallEvent>, roomkit::HookError>>()
            },
            HookOptions {
                name: "hold".into(),
                timeout: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    room.kit
        .hook(
            HookTrigger::OnToolCall,
            move |event: ToolCallEvent, _| {
                capture.lock().unwrap().push(event);
                std::future::ready(())
            },
            HookOptions {
                name: "audit".into(),
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let framework = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&framework);
    room.kit
        .on("tool_call", move |event| {
            capture.lock().unwrap().push(event.data);
            std::future::ready(())
        })
        .unwrap();
    let sent = room
        .kit
        .process_inbound(
            InboundMessage::new(channel("sms"), "alice", TextContent::new("Go")),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    sent.delivery
        .unwrap()
        .cancel(CancelOptions::default())
        .await
        .unwrap();
    roomkit::testing::idle(&room.kit).await;
    room.kit.close().await.unwrap();
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    let observed = observed.first().unwrap();
    assert_eq!(observed.result, Some(Value::String("known outcome".into())));
    assert_eq!(observed.is_error, is_error);
    assert!(!observed.cancelled);
    let framework = framework.lock().unwrap();
    assert_eq!(framework.len(), 1);
    let framework = framework.first().unwrap();
    assert_eq!(framework.get("tool_call_id"), Some(&Value::from("c1")));
    assert_eq!(
        framework
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        is_error
    );
    assert_ne!(framework.get("cancelled"), Some(&Value::Bool(true)));
}
