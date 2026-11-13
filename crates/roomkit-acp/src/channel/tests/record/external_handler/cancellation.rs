//! Pending ACP approvals are owned by the connection, not the prompt task.

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

// Python: test_acp_handler_cancellation::test_turn_cancellation_leaves_approval_to_the_handlers_withdrawal.
#[tokio::test]
async fn turn_cancellation_leaves_approval_to_the_handlers_withdrawal() {
    for announced in [true, false] {
        tokio::time::timeout(PATIENCE, check_pending(announced))
            .await
            .unwrap();
    }
}

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
    let permission = json!({"jsonrpc":"2.0","id":"permission","method":"session/request_permission","params":{
        "sessionId":"session-r1","toolCall":{"toolCallId":"read","title":"Read","rawInput":{}},
        "options":[{"optionId":"a","name":"Allow","kind":"allow_once"},{"optionId":"r","name":"Reject","kind":"reject_once"}]}});
    agent
        .writer
        .write_all(format!("{permission}\n").as_bytes())
        .await
        .unwrap();
    handler.asked.notified().await;
    assert!(channel.cancel(&RoomId::new("r1").unwrap()).await.unwrap());
    assert_eq!(agent.read().await["method"], "session/cancel");
    agent
        .answer(&prompt, json!({"stopReason":"cancelled"}))
        .await;
    let result = running.await.unwrap();
    assert!(result.error.is_none());
    let withdrawn = handler.withdrawn.load(Ordering::SeqCst);
    if !announced {
        handler.resume.notify_one();
    }
    let reply = loop {
        let reply = agent.read().await;
        if reply["id"] == "permission" {
            break reply;
        }
        assert_eq!(reply["method"], "session/cancel");
    };
    close(&kit, &mut agent).await;
    assert_eq!(reply["result"]["outcome"]["optionId"], "r");
    assert_eq!(withdrawn, announced);
    assert!(handler.completed.load(Ordering::SeqCst));
    assert!(!handler.aborted.load(Ordering::SeqCst));
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), usize::from(announced));
    for (mode, event) in &*observed {
        assert_eq!(*mode, HookExecution::Async);
        assert!(event.cancelled && !event.refused);
        assert_eq!(event.tool_call_id, "read");
    }
}
