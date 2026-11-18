//! A wired channel reports the external agent's actual outcome on direct delivery.
use super::{PATIENCE, Stage, direct_prompt, fixture, notify};
use roomkit::model::{HookResult, Metadata, RoomContext, RoomId, Visibility};
use roomkit::{Channel, HookExecution, HookRegistration, HookTrigger, RoomKit, ToolCallEvent};
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, OnToolResultOptions,
    ProcessToolCallOptions, ToolDecision, ToolError,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

type Reports = Arc<Mutex<Vec<(HookExecution, ToolCallEvent)>>>;

#[derive(Default)]
struct BrokenPolicy(ExternalToolHandlerState);

impl ExternalToolHandler for BrokenPolicy {
    fn state(&self) -> &ExternalToolHandlerState {
        &self.0
    }
    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async {
            Err(ToolError::Raised {
                kind: "RuntimeError".into(),
                message: "policy unavailable".into(),
            })
        })
    }
    fn on_tool_result<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: &'a str,
        _: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async { Ok(()) }) // A channel-decided failure bypasses this handler.
    }
}

#[tokio::test]
async fn directly_wired_external_result_reports_what_the_agent_consumed() {
    tokio::time::timeout(PATIENCE, check(false)).await.unwrap();
}

#[tokio::test]
async fn directly_wired_external_permission_failure_only_reaches_observers() {
    tokio::time::timeout(PATIENCE, check(true)).await.unwrap();
}

async fn check(permission_failure: bool) {
    let (channel, state) = fixture::with_options(
        Stage::Prompt,
        crate::AcpChannelOptions {
            external_tool_handler: permission_failure
                .then(|| -> Arc<dyn ExternalToolHandler> { Arc::new(BrokenPolicy::default()) }),
            ..Default::default()
        },
    );
    let room = RoomId::new("r1").unwrap();
    let kit = RoomKit::new(roomkit::RoomKitOptions::default());
    kit.create_room(roomkit::CreateRoomOptions {
        room_id: Some(room),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.turn_executor().wire(&channel).unwrap();
    let seen = observe(&kit);
    let active = channel.clone();
    // A narrow response still runs the executor's reporting hooks.
    let visibility = permission_failure.then_some(Visibility::Intelligence);
    let turn = tokio::spawn(async move {
        direct_prompt(&active, super::test_input("r1").context.room, visibility).await
    });
    state.reached.notified().await;
    let session = "session-2";
    notify(&state, session, json!({"sessionUpdate":"tool_call","toolCallId":"read","title":"read","rawInput":{"key":"value"},"status":"in_progress"})).await;
    let status = if permission_failure {
        reject_permission(&state, session).await;
        "failed"
    } else {
        "completed"
    };
    notify(&state, session, json!({"sessionUpdate":"tool_call_update","toolCallId":"read","status":status,"rawOutput":"agent result"})).await;
    state.resume.notify_one();
    assert!(turn.await.unwrap().responded);
    channel.close().await;
    kit.close().await.unwrap();
    assert_reports(&seen.lock().unwrap(), permission_failure);
}

fn observe(kit: &RoomKit) -> Reports {
    let seen = Arc::new(Mutex::new(Vec::new()));
    for mode in [HookExecution::Sync, HookExecution::Async] {
        let reports = Arc::clone(&seen);
        kit.turn_executor()
            .hooks()
            .register(HookRegistration::new(
                HookTrigger::OnToolCall,
                mode,
                move |mut event: ToolCallEvent, ctx: RoomContext| {
                    assert!(roomkit::current_turn_host().is_none());
                    assert_eq!(event.room_id.as_ref(), Some(&ctx.room.id));
                    reports.lock().unwrap().push((mode, event.clone()));
                    async move {
                        event.result = Some(json!("hook replacement"));
                        HookResult::modify(event, roomkit::model::hook::ModifyOptions::default())
                    }
                },
            ))
            .unwrap();
    }
    seen
}

async fn reject_permission(state: &fixture::State, session: &str) {
    let client = state.client.lock().unwrap().clone().unwrap();
    let answer = client
        .request_permission(
            serde_json::from_value(json!({
                "sessionId":session,"toolCall":{"toolCallId":"read","title":"read"},
                "options":[{"optionId":"no","kind":"reject_once","name":"Reject"}]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(answer).unwrap()["outcome"]["optionId"],
        "no"
    );
}

fn assert_reports(reports: &[(HookExecution, ToolCallEvent)], permission_failure: bool) {
    let modes: Vec<_> = reports.iter().map(|(mode, _)| *mode).collect();
    if permission_failure {
        assert_eq!(modes, [HookExecution::Async]);
        let event = &reports[0].1;
        assert!(event.is_error && !event.cancelled && !event.refused);
        assert_eq!(
            event.error_detail.as_deref(),
            Some("RuntimeError: policy unavailable")
        );
        assert!(
            !event
                .result
                .as_ref()
                .unwrap()
                .to_string()
                .contains("policy unavailable")
        );
    } else {
        assert_eq!(modes, [HookExecution::Sync, HookExecution::Async]);
        assert!(
            reports
                .iter()
                .all(|(_, e)| e.result == Some(json!("agent result")) && !e.is_error)
        );
    }
    assert!(
        reports
            .iter()
            .all(|(_, e)| e.tool_call_id == "read" && e.arguments["key"] == "value")
    );
}
