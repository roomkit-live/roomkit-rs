//! An ACP permission request pending while its turn ends, or the agent withdraws it.

use roomkit::model::RoomId;
use roomkit_ai::OnToolCancelledOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

use super::*;

#[derive(Default)]
struct Waiting {
    policy: PolicyExternalToolHandler,
    asked: Notify,
    resume: Notify,
    withdrawn: AtomicBool,
    completed: AtomicBool,
    aborted: AtomicBool,
    abandoned: Notify,
}

struct Deciding<'a>(&'a Waiting);
impl Drop for Deciding<'_> {
    fn drop(&mut self) {
        if !self.0.completed.load(Ordering::SeqCst) {
            self.0.aborted.store(true, Ordering::SeqCst);
            self.0.abandoned.notify_one();
        }
    }
}

#[tokio::test]
async fn closing_the_connection_drops_its_pending_idle_permission() {
    tokio::time::timeout(PATIENCE, close_pending())
        .await
        .unwrap();
}

async fn close_pending() {
    let handler = Arc::new(Waiting::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Waiting>::clone(&handler));
    let kit = room(channel).await;
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(running.await.unwrap().error.is_none());
    let permission = json!({"jsonrpc":"2.0","id":"idle-permission","method":"session/request_permission","params":{
        "sessionId":"session-r1","toolCall":{"toolCallId":"read","title":"Read","rawInput":{}},
        "options":[{"optionId":"a","name":"Allow","kind":"allow_once"}]}});
    agent
        .writer
        .write_all(format!("{permission}\n").as_bytes())
        .await
        .unwrap();
    handler.asked.notified().await;
    close(&kit, &mut agent).await;
    handler.abandoned.notified().await;
    assert!(handler.aborted.load(Ordering::SeqCst));
    assert!(!handler.completed.load(Ordering::SeqCst));
    assert!(!handler.withdrawn.load(Ordering::SeqCst));
}

impl ExternalToolHandler for Waiting {
    fn state(&self) -> &ExternalToolHandlerState {
        self.policy.state()
    }
    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            let _deciding = Deciding(self);
            self.asked.notify_one();
            self.resume.notified().await;
            self.completed.store(true, Ordering::SeqCst);
            Ok(ToolDecision {
                reason: "turn ended".to_owned(),
                ..ToolDecision::new(false)
            })
        })
    }
    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        self.policy.on_tool_result(name, input, result, options)
    }
    fn on_tool_cancelled<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.withdrawn.store(true, Ordering::SeqCst);
            self.resume.notify_one();
            self.policy.on_tool_cancelled(name, input, options).await
        })
    }
}

// Python: test_acp_handler_cancellation::test_turn_cancellation_answers_a_pending_permission_cancelled.
#[tokio::test]
async fn turn_cancellation_answers_a_pending_permission_cancelled() {
    for announced in [true, false] {
        tokio::time::timeout(PATIENCE, check_pending(announced))
            .await
            .unwrap();
    }
}

fn permission(id: &str, call: &str) -> serde_json::Value {
    json!({"jsonrpc":"2.0","id":id,"method":"session/request_permission","params":{
        "sessionId":"session-r1","toolCall":{"toolCallId":call,"title":"Read","rawInput":{}},
        "options":[{"optionId":"a","name":"Allow","kind":"allow_once"},{"optionId":"r","name":"Reject","kind":"reject_once"}]}})
}

async fn write(agent: &mut Agent, message: &serde_json::Value) {
    agent
        .writer
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
}

/// ACP: a client that cancels a prompt MUST answer each of its pending
/// permission requests `cancelled`. The handler's decision is abandoned,
/// and the call, announced or not, is reported once, cancelled (RFC §6.4,
/// §9.3).
async fn check_pending(announced: bool) {
    let handler = Arc::new(Waiting::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Waiting>::clone(&handler));
    let kit = room(channel.clone()).await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for mode in [HookExecution::Sync, HookExecution::Async] {
        let observed = Arc::clone(&observed);
        kit.turn_executor()
            .hooks()
            .register(HookRegistration::new(
                HookTrigger::OnToolCall,
                mode,
                move |event: ToolCallEvent, _: RoomContext| {
                    observed.lock().unwrap().push((mode, event));
                    async { HookResult::allow(roomkit::model::hook::AllowOptions::default()) }
                },
            ))
            .unwrap();
    }
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = if announced {
        start_tool(&mut agent).await
    } else {
        agent.prompted(true).await
    };
    write(&mut agent, &permission("permission", "read")).await;
    handler.asked.notified().await;
    assert!(channel.cancel(&RoomId::new("r1").unwrap()).await.unwrap());
    assert_eq!(agent.read().await["method"], "session/cancel");
    let reply = agent.read().await;
    assert_eq!(reply["id"], "permission");
    assert_eq!(reply["result"]["outcome"], json!({"outcome": "cancelled"}));
    agent
        .answer(&prompt, json!({"stopReason":"cancelled"}))
        .await;
    let result = running.await.unwrap();
    assert!(result.error.is_none());
    handler.abandoned.notified().await;
    close(&kit, &mut agent).await;
    assert!(handler.withdrawn.load(Ordering::SeqCst));
    assert!(handler.aborted.load(Ordering::SeqCst));
    assert!(!handler.completed.load(Ordering::SeqCst));
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    for (mode, event) in &*observed {
        assert_eq!(*mode, HookExecution::Async);
        assert!(event.cancelled && !event.refused);
        assert_eq!(event.tool_call_id, "read");
    }
}

// Python: test_acp_handler_cancellation::test_an_agents_cancel_request_leaves_the_decision_to_the_handler.
/// The agent withdraws a pending permission request with
/// `$/cancel_request`: ACP lets a client ignore it, and RoomKit does. The
/// handler keeps deciding the request, and its decision is the answer the
/// agent gets.
#[tokio::test]
async fn an_agents_cancel_request_leaves_the_decision_to_the_handler() {
    tokio::time::timeout(PATIENCE, withdrawn_by_the_agent())
        .await
        .unwrap();
}

async fn withdrawn_by_the_agent() {
    let handler = Arc::new(Waiting::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Waiting>::clone(&handler));
    let kit = room(channel).await;
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    write(&mut agent, &permission("first", "read")).await;
    handler.asked.notified().await;
    let withdraw =
        json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":"first"}});
    write(&mut agent, &withdraw).await;
    // Taken in wire order: once the second request reaches the handler, the
    // notification before it was taken.
    write(&mut agent, &permission("second", "write")).await;
    handler.asked.notified().await;
    assert!(!handler.withdrawn.load(Ordering::SeqCst));
    assert!(!handler.aborted.load(Ordering::SeqCst));
    handler.resume.notify_one();
    handler.resume.notify_one();
    let mut answers = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let reply = agent.read().await;
        answers.insert(
            reply["id"].as_str().unwrap().to_owned(),
            reply["result"].clone(),
        );
    }
    let refused = json!({"outcome": {"outcome": "selected", "optionId": "r"}});
    assert_eq!(answers["first"], refused);
    assert_eq!(answers["second"], refused);
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(running.await.unwrap().error.is_none());
    close(&kit, &mut agent).await;
}
