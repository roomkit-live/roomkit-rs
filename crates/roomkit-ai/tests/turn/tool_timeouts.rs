//! Per-turn bounds travel with the resolved tools and handler (RFC §21.6).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{EventContent, EventType, ToolCallOutcome};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiChannelTurnConfig, AiResponse, AiTool, AiToolCall, FinishReason,
    MockAiProvider, MockAiProviderOptions, ToolHandler, ToolResult,
};
use serde_json::{Value, json};

use super::support::{Room, channel};

type Bounds = BTreeMap<String, Option<Duration>>;

struct Cancelled(Option<Arc<AtomicBool>>);

impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(flag) = &self.0 {
            flag.store(true, Ordering::SeqCst);
        }
    }
}

async fn configured(
    streaming: bool,
    maps: Vec<Option<Bounds>>,
) -> (Room, MockAiProvider, Arc<AtomicBool>) {
    let mut response = AiResponse::new("");
    response.tool_calls = vec![AiToolCall::new("c1", "slow")];
    response.finish_reason = Some(FinishReason::ToolCalls);
    let model = MockAiProvider::new(MockAiProviderOptions {
        ai_responses: Some(vec![response, AiResponse::new("done")]),
        streaming,
        ..MockAiProviderOptions::default()
    });
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cancelled);
    let handler = ToolHandler::new(move |_name, _arguments| {
        let flag = Arc::clone(&flag);
        async move {
            let mut guard = Cancelled(Some(flag));
            tokio::time::sleep(Duration::from_millis(60)).await;
            guard.0.take();
            Ok(ToolResult::Text("finished".to_owned()))
        }
    });
    let maps = Mutex::new(maps.into_iter());
    let options = AiChannelOptions {
        tool_timeout: Some(Duration::from_millis(20)),
        config_provider: Some(Arc::new(move |_binding, _context| {
            let config = AiChannelTurnConfig {
                tools: Some(vec![AiTool::new("slow", "Answers slowly").unwrap()]),
                tool_handler: Some(handler.clone()),
                tool_timeouts: maps.lock().unwrap().next().unwrap(),
                ..AiChannelTurnConfig::default()
            };
            Box::pin(async move { Ok(Some(config)) })
        })),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), model.clone(), options).unwrap();
    (Room::with_agent(agent).await, model, cancelled)
}

async fn results(room: &Room) -> Vec<(Option<ToolCallOutcome>, Value)> {
    room.timeline()
        .await
        .into_iter()
        .filter_map(|event| {
            if event.r#type != EventType::ToolCallEnd {
                return None;
            }
            let EventContent::ToolCall(call) = event.content else {
                return None;
            };
            Some((call.outcome, call.result))
        })
        .collect()
}

// Python: tests/test_tool_timeout.py::test_a_turns_configuration_bounds_the_tools_it_declares
#[tokio::test]
async fn a_turns_configuration_bounds_the_tools_it_declares() {
    for streaming in [false, true] {
        let (room, model, cancelled) =
            configured(streaming, vec![Some([("slow".to_owned(), None)].into())]).await;
        room.says("Go.").await;
        assert!(!cancelled.load(Ordering::SeqCst));
        assert_eq!(model.calls().len(), 2);
        assert_eq!(
            results(&room).await,
            [(Some(ToolCallOutcome::Served), json!("finished"))]
        );
        room.kit.close().await.unwrap();
    }
}

// Python: tests/test_tool_timeout.py::test_a_turns_bounds_are_never_the_next_turns
#[tokio::test]
async fn a_turns_bounds_are_never_the_next_turns() {
    for streaming in [false, true] {
        let (room, model, cancelled) = configured(
            streaming,
            vec![Some([("slow".to_owned(), None)].into()), None],
        )
        .await;
        room.says("First.").await;
        assert!(!cancelled.load(Ordering::SeqCst));
        room.says("Again.").await;
        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(model.calls().len(), 4);
        assert_eq!(
            results(&room).await,
            [
                (Some(ToolCallOutcome::Served), json!("finished")),
                (
                    Some(ToolCallOutcome::Failed),
                    json!("{\"error\": \"Tool 'slow' failed (ToolTimeoutError)\"}")
                ),
            ]
        );
        room.kit.close().await.unwrap();
    }
}

// Python: tests/test_tool_timeout.py::test_a_turns_bound_that_is_not_positive_fails_the_turn
#[tokio::test]
async fn a_zero_turn_bound_fails_before_the_model_is_asked() {
    for streaming in [false, true] {
        let (room, model, _) = configured(
            streaming,
            vec![Some([("slow".to_owned(), Some(Duration::ZERO))].into())],
        )
        .await;
        let result = room.says("Go.").await;
        assert!(
            result
                .error
                .unwrap()
                .message
                .contains("must be positive or None")
        );
        assert_eq!(model.calls().len(), 0);
        assert_eq!(results(&room).await.len(), 0);
        room.kit.close().await.unwrap();
    }
}
