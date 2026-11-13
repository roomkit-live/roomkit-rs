//! A completed call's report survives cancellation while its SYNC hook waits.

use super::*;
use tokio::sync::Notify;

// Python: tests/test_tool_call_reports.py::test_an_acp_call_whose_report_is_cut_is_still_reported_once[handler].
#[tokio::test]
async fn a_handler_report_cut_in_its_sync_hook_still_reports_once() {
    tokio::time::timeout(PATIENCE, cut_report()).await.unwrap();
}

async fn cut_report() {
    let (channel, mut opened) =
        channel_with_handler(Arc::new(PolicyExternalToolHandler::default()));
    let kit = room(channel).await;
    let held = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (entered, resume) = (Arc::clone(&held), Arc::clone(&release));
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Sync,
            move |_: ToolCallEvent, _: RoomContext| {
                let (entered, resume) = (Arc::clone(&entered), Arc::clone(&resume));
                async move {
                    entered.notify_one();
                    resume.notified().await;
                    HookResult::allow(roomkit::model::hook::AllowOptions::default())
                }
            },
        ))
        .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let reports = Arc::clone(&observed);
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            move |event: ToolCallEvent, _: RoomContext| {
                reports.lock().unwrap().push(event);
                async {}
            },
        ))
        .unwrap();
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = start_tool(&mut agent).await;
    let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{"sessionUpdate":"tool_call_update", "toolCallId":"read",
            "status":"completed","rawOutput":"original"}}});
    agent
        .writer
        .write_all(format!("{update}\n").as_bytes())
        .await
        .unwrap();
    held.notified().await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    release.notify_one();
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    close(&kit, &mut agent).await;
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert!(!observed[0].cancelled && !observed[0].is_error);
    assert_eq!(observed[0].result, Some(json!("original")));
}
