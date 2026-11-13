//! A custom handler delegates its result from a child task, then fails.

mod cancellation;
mod concurrent;
mod cut_report;
mod failure_body;
mod standalone;

use std::sync::{Arc, Mutex};

use roomkit::model::{ChannelId, HookResult, Metadata, RoomContext};
use roomkit::{
    HookExecution, HookRegistration, HookTrigger, ToolCallEvent, TurnRecord, keep_context,
};
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, OnToolResultOptions,
    PolicyExternalToolHandler, ProcessToolCallOptions, ToolDecision, ToolError,
};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use super::super::Piped;
use super::{Agent, PATIENCE, close, room, send, start_tool};
use crate::{AcpChannel, AcpChannelOptions};

fn channel_with_handler(
    handler: Arc<dyn ExternalToolHandler>,
) -> (
    AcpChannel<Piped>,
    mpsc::UnboundedReceiver<tokio::io::DuplexStream>,
) {
    let (agents, opened) = mpsc::unbounded_channel();
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Piped {
                agents,
                closed: Arc::default(),
                log: Arc::default(),
            }),
            external_tool_handler: Some(handler),
            ..AcpChannelOptions::default()
        },
    )
    .unwrap();
    (channel, opened)
}

struct ChildReport(Arc<PolicyExternalToolHandler>);

impl ExternalToolHandler for ChildReport {
    fn state(&self) -> &ExternalToolHandlerState {
        self.0.state()
    }
    fn process_tool_call<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        self.0.process_tool_call(name, input, options)
    }
    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        let (handler, name, input, result) = (
            Arc::clone(&self.0),
            name.to_owned(),
            input.clone(),
            result.to_owned(),
        );
        Box::pin(async move {
            tokio::spawn(keep_context(async move {
                handler
                    .on_tool_result(&name, &input, &result, options)
                    .await
            }))
            .await
            .unwrap()?;
            Err(ToolError::Failed("audit sink down".to_owned()))
        })
    }
}

// Python: test_acp_handler_report_context::test_a_child_task_report_then_failure_does_not_report_the_call_twice.
#[tokio::test]
async fn a_child_task_report_then_failure_does_not_report_the_call_twice() {
    let (agent_channel, mut opened) = channel_with_handler(Arc::new(ChildReport(Arc::new(
        PolicyExternalToolHandler::default(),
    ))));
    let kit = room(agent_channel).await;
    let reports = Arc::new(Mutex::new(Vec::new()));
    for mode in [HookExecution::Sync, HookExecution::Async] {
        let reports = Arc::clone(&reports);
        kit.turn_executor()
            .hooks()
            .register(HookRegistration::new(
                HookTrigger::OnToolCall,
                mode,
                move |event: ToolCallEvent, _: RoomContext| {
                    reports.lock().unwrap().push((mode, event));
                    async { HookResult::allow(roomkit::model::hook::AllowOptions::default()) }
                },
            ))
            .unwrap();
    }
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = start_tool(&mut agent).await;
    let notification = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{"sessionUpdate":"tool_call_update", "toolCallId":"read",
            "status":"completed","rawOutput":"read"}}});
    agent
        .writer
        .write_all(format!("{notification}\n").as_bytes())
        .await
        .unwrap();
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    let result = tokio::time::timeout(PATIENCE, running)
        .await
        .unwrap()
        .unwrap();
    assert!(result.error.is_none());
    close(&kit, &mut agent).await;
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 2, "exactly one SYNC and one ASYNC report");
    assert_eq!(
        reports
            .iter()
            .filter(|(mode, _)| *mode == HookExecution::Sync)
            .count(),
        1
    );
    assert!(
        reports
            .iter()
            .all(|(_, event)| event.result == Some(json!("read")))
    );
}
