//! Public callbacks share their right with provider reports and turn cuts.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{EventContent, EventType, HookResult, InboundMessage, TextContent};
use roomkit::{
    CancelOptions, ConversationStore, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, ToolCallEvent,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiTool, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ServedCall, ToolHandler, ToolResult, turn_report_claim,
};
use serde_json::json;
use tokio::sync::Notify;

use super::support::{Room, channel, room_id};

fn provider(streaming: bool, calls: Vec<AiToolCall>) -> MockAiProvider {
    let mut first = AiResponse::new("");
    first.tool_calls = calls;
    MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![first, AiResponse::new("done")]),
        ..MockAiProviderOptions::default()
    })
}

fn observers(room: &Room) -> Arc<Mutex<Vec<ToolCallEvent>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&seen);
    room.kit
        .hook(
            HookTrigger::OnToolCall,
            move |event: ToolCallEvent, _| {
                capture.lock().unwrap().push(event);
                std::future::ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
    seen
}

// Python: tests/tools/test_turn_report_claim_lifecycle.py::test_provider_occurrences_keep_independent_claims_under_one_id
#[tokio::test]
async fn provider_occurrences_keep_independent_claims_under_one_id() {
    for streaming in [false, true] {
        let calls = ["first", "second"]
            .into_iter()
            .map(|query| {
                let mut call = AiToolCall::new("c1", "lookup");
                call.arguments.insert("q".into(), json!(query));
                call.served = Some(ServedCall {
                    result: query.into(),
                    is_error: false,
                });
                call
            })
            .collect();
        let agent = AiChannel::new(
            channel("assistant"),
            provider(streaming, calls),
            AiChannelOptions::default(),
        )
        .unwrap();
        let room = Room::with_agent(agent).await;
        let reports = observers(&room);
        let claimed = Arc::new(Mutex::new(Vec::new()));
        let capture = Arc::clone(&claimed);
        room.kit
            .hook(
                HookTrigger::OnToolCall,
                move |event: ToolCallEvent, _| {
                    let claim =
                        turn_report_claim(&event.tool_call_id, event.channel_id.as_str()).unwrap();
                    capture.lock().unwrap().push((
                        event.arguments.get("q").cloned().unwrap(),
                        claim(),
                        claim(),
                    ));
                    std::future::ready(Ok(HookResult::allow(AllowOptions::default())))
                },
                HookOptions::default(),
            )
            .unwrap();
        room.says("go").await;
        roomkit::testing::idle(&room.kit).await;
        room.kit.close().await.unwrap();
        assert_eq!(
            *claimed.lock().unwrap(),
            [
                (json!("first"), true, false),
                (json!("second"), true, false)
            ]
        );
        assert!(reports.lock().unwrap().is_empty());
        let events = room
            .kit
            .store()
            .list_events(&room_id(), roomkit::store::ListEventsOptions::default())
            .await
            .unwrap();
        let ends: Vec<_> = events
            .iter()
            .filter(|event| event.r#type == EventType::ToolCallEnd)
            .map(|event| {
                let EventContent::ToolCall(content) = &event.content else {
                    panic!("tool end content");
                };
                (
                    content.arguments.get("q").cloned().unwrap(),
                    content.outcome,
                )
            })
            .collect();
        assert_eq!(
            ends,
            [
                (
                    json!("first"),
                    Some(roomkit::model::ToolCallOutcome::Served)
                ),
                (
                    json!("second"),
                    Some(roomkit::model::ToolCallOutcome::Served)
                )
            ]
        );
    }
}

// Python: tests/tools/test_turn_report_claim_lifecycle.py::test_a_claimed_local_call_keeps_its_end_row_when_cancelled
#[tokio::test]
async fn a_claimed_local_call_keeps_its_end_row_when_cancelled() {
    cancelled_call(true).await;
}

// Python: tests/tools/test_turn_report_claim_lifecycle.py::test_cancel_observer_keeps_its_turn_claim
#[tokio::test]
async fn cancel_observer_keeps_its_turn_claim() {
    cancelled_call(false).await;
}

async fn cancelled_call(preclaim: bool) {
    for streaming in [false, true] {
        let entered = Arc::new(Notify::new());
        let notify = Arc::clone(&entered);
        let retained = Arc::new(Mutex::new(Vec::new()));
        let capture = Arc::clone(&retained);
        let handler = ToolHandler::new(move |_, _| {
            let (notify, capture) = (Arc::clone(&notify), Arc::clone(&capture));
            async move {
                let claim = turn_report_claim("c1", "assistant").unwrap();
                if preclaim {
                    assert!(claim());
                }
                capture.lock().unwrap().push(claim);
                notify.notify_one();
                std::future::pending::<Result<ToolResult, roomkit_ai::ToolError>>().await
            }
        });
        let agent = AiChannel::new(
            channel("assistant"),
            provider(streaming, vec![AiToolCall::new("c1", "lookup")]),
            AiChannelOptions {
                tools: vec![AiTool::new("lookup", "Lookup").unwrap().into()],
                tool_handler: Some(handler),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let room = Room::with_agent(agent).await;
        let reports = observers(&room);
        let observed_claims = Arc::new(Mutex::new(Vec::new()));
        let observe = Arc::clone(&observed_claims);
        room.kit
            .hook(
                HookTrigger::OnToolCall,
                move |event: ToolCallEvent, _| {
                    let claim = turn_report_claim(&event.tool_call_id, event.channel_id.as_str());
                    observe.lock().unwrap().push(claim.map(|claim| claim()));
                    std::future::ready(())
                },
                HookOptions {
                    execution: HookExecution::Async,
                    ..HookOptions::default()
                },
            )
            .unwrap();
        let result = room
            .kit
            .process_inbound(
                InboundMessage::new(channel("sms"), "alice", TextContent::new("go")),
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    defer_delivery: true,
                    ..ProcessInboundOptions::default()
                },
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        result
            .delivery
            .unwrap()
            .cancel(CancelOptions::default())
            .await
            .unwrap();
        roomkit::testing::idle(&room.kit).await;
        room.kit.close().await.unwrap();
        {
            let reports = reports.lock().unwrap();
            assert_eq!(reports.len(), usize::from(!preclaim));
            assert!(reports.iter().all(|report| report.cancelled));
        }
        let expected = if preclaim { vec![] } else { vec![Some(false)] };
        assert_eq!(*observed_claims.lock().unwrap(), expected);
        {
            let retained = retained.lock().unwrap();
            assert_eq!(retained.len(), 1);
            assert!(!retained.first().unwrap()());
        }
        let events = room
            .kit
            .store()
            .list_events(&room_id(), roomkit::store::ListEventsOptions::default())
            .await
            .unwrap();
        let outcomes: Vec<_> = events
            .iter()
            .filter(|event| event.r#type == EventType::ToolCallEnd)
            .map(|event| {
                let EventContent::ToolCall(content) = &event.content else {
                    return None;
                };
                content.outcome
            })
            .collect();
        assert_eq!(outcomes, [Some(roomkit::model::ToolCallOutcome::Cancelled)]);
    }
}
