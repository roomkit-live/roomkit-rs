use super::*;
use roomkit::ConversationStore;
use roomkit::model::{EventContent, EventType, Metadata, ToolCallOutcome};
use roomkit_ai::{
    Agent, AgentOptions, AiError, AiTool, PlanTaskHandle, PlanUpdatedEvent, ToolResult,
};
use roomkit_core::ops::{Environment, SystemEnvironment};

// Python: test_planning_boundaries.py::test_task_and_unicode_title_limits_reach_the_actual_tool_result
#[tokio::test]
async fn public_limits_count_tasks_and_unicode_characters_without_refusing_the_served_call() {
    for streaming in [false, true] {
        for (count, length, accepted) in [(100, 500, true), (101, 1, false), (1, 501, false)] {
            let title = "é".repeat(length);
            let tasks = vec![json!({"title": title, "status": "pending"}); count];
            let model = model(
                vec![plan(json!(tasks)), AiResponse::new("Done.")],
                streaming,
            );
            let kit = setup(model.clone(), &["r"], RoomKitOptions::default()).await;
            say(&kit, "r", "Plan.").await;
            let calls = model.calls();
            assert_eq!(calls.len(), 2);
            let results = results(calls.last().unwrap());
            let [result] = results.as_slice() else {
                panic!("one result required")
            };
            assert!(!result.is_error);
            assert_eq!(result.outcome, Some(ToolCallOutcome::Served));
            let ToolResult::Text(text) = &result.result else {
                panic!("text result required")
            };
            assert_stored_result(&kit, text).await;
            let result: Value = serde_json::from_str(text).unwrap();
            if accepted {
                assert_eq!(
                    result,
                    json!({"status":"ok", "task_count":count, "pending":count, "in_progress":0, "completed":0, "blocked":0})
                );
            } else {
                let error = if count > 100 {
                    "Invalid plan: at most 100 tasks are allowed"
                } else {
                    "Invalid plan: task 0 title must be at most 500 characters"
                };
                assert_eq!(result, json!({"error": error}));
            }
            kit.close().await.unwrap();
        }
    }
}

#[expect(clippy::panic, reason = "test helper rejects malformed stored rows")]
async fn assert_stored_result(kit: &RoomKit, text: &str) {
    let rows = kit
        .store()
        .list_events(
            &"r".parse().unwrap(),
            roomkit::store::ListEventsOptions::default(),
        )
        .await
        .unwrap();
    let ends: Vec<_> = rows
        .iter()
        .filter(|event| event.r#type == EventType::ToolCallEnd)
        .collect();
    let [event] = ends.as_slice() else {
        panic!("one end row required")
    };
    let EventContent::ToolCall(call) = &event.content else {
        panic!("call content required")
    };
    assert_eq!(call.outcome, Some(ToolCallOutcome::Served));
    assert_eq!(call.result, json!(text));
}

#[test]
fn payload_defaults_are_independent_and_preserve_open_task_objects() {
    let before = SystemEnvironment.now();
    let mut event = PlanUpdatedEvent::new("r".parse().unwrap(), "agent".parse().unwrap());
    let other = PlanUpdatedEvent::new("r".parse().unwrap(), "agent".parse().unwrap());
    assert!(event.timestamp >= before && event.timestamp <= SystemEnvironment.now());
    let task: Metadata =
        serde_json::from_value(json!({"title":"Read", "status":"custom", "extra":{"id":1}}))
            .unwrap();
    event.tasks.push(PlanTaskHandle::new(task.clone()));
    assert_eq!(other.tasks, Vec::<PlanTaskHandle>::new());
    assert_eq!(task_values(&event.tasks), json!([task]));
    assert_eq!(event.clone(), event);
}

#[test]
fn payload_identifiers_do_not_apply_record_validation() {
    // Python: test_planning_boundaries.py::test_plan_event_keeps_identifiers_without_applying_record_validation
    for (room, channel) in [("", ""), ("room", "all"), ("room", "a b")] {
        let event = PlanUpdatedEvent::new(room.parse().unwrap(), channel.parse().unwrap());
        assert_eq!(event.room_id.as_str(), room);
        assert_eq!(event.channel_id.as_str(), channel);
    }
}

#[test]
fn both_public_constructors_refuse_a_host_collision_when_planning_is_enabled() {
    let options = || AiChannelOptions {
        enable_planning: true,
        tools: vec![AiTool::new("plan_tasks", "Host planner").unwrap().into()],
        ..Default::default()
    };
    let provider = model(vec![], false);
    assert!(
        matches!(AiChannel::new("agent".parse().unwrap(), provider.clone(), options()), Err(AiError::DuplicateTool { name }) if name == "plan_tasks")
    );
    assert!(
        matches!(Agent::new("agent".parse().unwrap(), provider, AgentOptions { ai: options(), ..Default::default() }), Err(AiError::DuplicateTool { name }) if name == "plan_tasks")
    );
}
