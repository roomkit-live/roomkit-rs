//! Failure rendering is bounded and independent of the reporting handler.

use super::*;
use roomkit::model::hook::AllowOptions;
use serde_json::Value;

// Python: tests/test_refused_reports.py::test_an_acp_failure_reports_one_body_with_or_without_a_handler
#[tokio::test]
async fn an_acp_failure_reports_one_body_with_or_without_a_handler() {
    for raw in [
        Value::Null,
        json!([{"type":"image","mimeType":"image/png", "data":"A".repeat(600 * 1024)}]),
    ] {
        let without = tokio::time::timeout(PATIENCE, report(raw.clone(), false))
            .await
            .unwrap();
        let with = tokio::time::timeout(PATIENCE, report(raw, true))
            .await
            .unwrap();
        assert_eq!(with, without);
        assert!(with.len() < 1000);
    }
}

async fn report(raw: Value, with_handler: bool) -> String {
    let (channel, mut opened) = if with_handler {
        channel_with_handler(Arc::new(PolicyExternalToolHandler::default()))
    } else {
        let (channel, opened, _) = super::super::channel();
        (channel, opened)
    };
    let kit = room(channel).await;
    let reports = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&reports);
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Sync,
            move |event: ToolCallEvent, _: RoomContext| {
                assert!(event.is_error);
                observed.lock().unwrap().push(event.result.unwrap());
                async { HookResult::allow(AllowOptions::default()) }
            },
        ))
        .unwrap();
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = start_tool(&mut agent).await;
    let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{"sessionUpdate":"tool_call_update",
            "toolCallId":"read","status":"failed","rawOutput":raw}}});
    agent
        .writer
        .write_all(format!("{update}\n").as_bytes())
        .await
        .unwrap();
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(running.await.unwrap().error.is_none());
    close(&kit, &mut agent).await;
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 1);
    reports[0].as_str().unwrap().to_owned()
}
