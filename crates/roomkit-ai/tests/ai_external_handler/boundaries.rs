//! Input refusal, full reports, and registration remain the public handler contract.
use super::*;
use roomkit::model::ToolCallOutcome;
use roomkit::{RoomKit, RoomKitOptions};

// Python: test_external_call_routing.py::test_a_call_written_unreadable_is_refused_before_the_handler
#[tokio::test]
async fn unreadable_arguments_never_reach_the_external_handler() {
    for streaming in [false, true] {
        let mut call = AiToolCall::new("c1", "remote");
        call.arguments
            .insert("raw".into(), serde_json::json!("[1,2]"));
        call.partial = true;
        call.garbled = true;
        let handler = Arc::new(Handler::default());
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model(streaming, call),
            AiChannelOptions {
                external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let output = turn(agent.clone()).await;
        let ends = ends(&output);
        assert_eq!(ends.len(), 1);
        let end = ends.first().unwrap();
        assert_eq!(end.outcome, Some(ToolCallOutcome::Refused));
        assert!(
            end.result
                .as_str()
                .unwrap()
                .contains("arguments unreadable")
        );
        assert_eq!(handler.decisions.load(Ordering::SeqCst), 0);
        assert!(handler.reports.lock().unwrap().is_empty());
        agent.close().await;
    }
}

// The report receives the full result, before the model/end row's bound.
#[tokio::test]
async fn reports_keep_the_original_result_before_it_is_bounded() {
    for streaming in [false, true] {
        let full = "complete result ".repeat(500);
        let handler = Arc::new(Handler {
            result: full.clone(),
            ..Handler::default()
        });
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model(streaming, AiToolCall::new("c1", "remote")),
            AiChannelOptions {
                external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                evict_threshold_tokens: 1,
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let output = turn(agent.clone()).await;
        assert_eq!(
            handler.reports.lock().unwrap().as_slice(),
            std::slice::from_ref(&full)
        );
        let ends = ends(&output);
        assert_eq!(ends.len(), 1);
        let end = ends.first().unwrap();
        assert_eq!(end.outcome, Some(ToolCallOutcome::Served));
        let bounded = end.result.as_str().unwrap();
        assert!(bounded.len() < full.len());
        assert!(bounded.contains("read_stored_result"));
        agent.close().await;
    }
}

// Python: TestExternalToolHandlerHookIntegration identity/reuse tests.
#[tokio::test]
async fn registration_names_the_handler_and_warns_when_reused() {
    let (warnings, _capture) = warnings::Warnings::capture();
    let handler = Arc::new(Handler::default());
    assert_eq!(handler.channel_id(), "");
    let kit = RoomKit::new(RoomKitOptions::default());
    for id in ["first", "second"] {
        let agent = AiChannel::new(
            id.parse().unwrap(),
            MockAiProvider::new(MockAiProviderOptions::default()),
            AiChannelOptions {
                external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        kit.register_channel(agent).unwrap();
        assert_eq!(handler.channel_id(), id);
    }
    assert!(
        warnings
            .containing("already wired")
            .iter()
            .any(|line| line.contains("already wired") && line.contains("first"))
    );
    kit.close().await.unwrap();
    assert_eq!(handler.starts.load(Ordering::SeqCst), 0);
    assert_eq!(handler.stops.load(Ordering::SeqCst), 0);
}
