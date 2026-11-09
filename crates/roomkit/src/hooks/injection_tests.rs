//! The generic SYNC invocation contract, apart from each caller's payload type.

#[path = "../../tests/support/warnings.rs"]
mod warnings;

use super::*;
use roomkit_core::model::{EventSource, InjectedEvent, Room, Task, TaskId, TextContent};

// Python: tests/test_hook_injection_contract.py::test_only_broadcast_keeps_injections_in_a_sync_chain
#[tokio::test]
async fn only_broadcast_keeps_injections_in_a_sync_chain() {
    let event = RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(ChannelId::new("system").unwrap(), ChannelType::SYSTEM),
        TextContent::new("cause"),
    );
    let context = RoomContext::new(Room::new(event.room_id.clone()));
    for trigger in [
        HookTrigger::BeforeBroadcast,
        HookTrigger::OnVerificationRequired,
        HookTrigger::BeforeDeliver,
        HookTrigger::OnTranscription,
        HookTrigger::BeforeTts,
        HookTrigger::BeforeToolUse,
        HookTrigger::OnToolCall,
        HookTrigger::AfterToolRound,
        HookTrigger::OnUserInputRequired,
        HookTrigger::BeforeAiGeneration,
    ] {
        for action in [HookAction::Allow, HookAction::Modify, HookAction::Block] {
            let mut answer = HookResult::new(action);
            answer.tasks = vec![Task::new(
                TaskId::new("review").unwrap(),
                event.room_id.clone(),
                "Review",
            )];
            answer.injected_events = vec![InjectedEvent::new(event.clone())];
            answer.metadata.insert("kept".to_owned(), true.into());
            if action == HookAction::Modify {
                let mut changed = event.clone();
                changed.content = TextContent::new("changed").into();
                answer.event = Some(changed);
            }
            if action == HookAction::Block {
                answer.reason = Some("held".to_owned());
            }
            let expected = answer.clone();
            let handler = Arc::new(Closure(move |_: RoomEvent, _: RoomContext| {
                let answer = answer.clone();
                async move { answer }
            }));
            let (warnings, _capturing) = warnings::Warnings::capture();
            let spec = HookSpec::new("notify", trigger, HookExecution::Sync);
            let ran = run_traced(
                &spec,
                handler,
                event.clone(),
                context.clone(),
                refused_content,
            )
            .await;
            let HookOutcome::Returned(result) = ran else {
                panic!("valid hook failed: {trigger:?}");
            };
            assert_eq!(result.action, expected.action);
            assert_eq!(result.event, expected.event);
            assert_eq!(result.reason, expected.reason);
            assert_eq!(result.metadata, expected.metadata);
            assert_eq!(result.tasks, expected.tasks);
            let keeps = trigger == HookTrigger::BeforeBroadcast;
            assert_eq!(
                result.injected_events.len(),
                usize::from(keeps),
                "{trigger:?}"
            );
            let lines = warnings.containing("notify");
            assert_eq!(lines.len(), usize::from(!keeps), "{trigger:?}");
            if !keeps {
                assert!(lines[0].starts_with("WARN"));
                assert!(
                    lines[0].contains(serde_json::to_value(trigger).unwrap().as_str().unwrap())
                );
                assert!(lines[0].contains("dropped"));
            }
        }
    }
}
