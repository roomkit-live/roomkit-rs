//! Public result carriers from Python's provider models (RFC §6.7).
use roomkit::model::{
    ChannelId, Confidence, Observation, ObservationId, RoomId, Task, TaskId, ToolCallOutcome,
};
use roomkit_ai::{AiResponse, AiToolResultPart};

// Python: tests/test_resilience.py::TestProviderABCEnhancements::test_ai_response_with_tasks
#[test]
fn ai_response_with_tasks_keeps_the_lists_and_its_equality_contract() {
    fn is_eq<T: Eq>() {}
    is_eq::<AiResponse>();
    let mut response = AiResponse::new("answer");
    assert!(response.tasks.is_empty() && response.observations.is_empty());
    let room = RoomId::new("r1").unwrap();
    let task = Task::new(TaskId::new("t1").unwrap(), room.clone(), "Task");
    let observation = Observation::new(
        ObservationId::new("o1").unwrap(),
        room,
        ChannelId::new("ai1").unwrap(),
        "Observation",
    );
    response.tasks.push(task.clone());
    response.observations.push(observation.clone());
    assert_eq!(response.tasks, [task]);
    assert_eq!(response.observations, [observation]);
    assert_eq!(response, response.clone());
    assert!(Confidence::new(f64::NAN).is_err());
}

// Python: providers/ai/base.py::AIToolResultPart defaults and rendering helpers.
#[test]
fn a_public_result_keeps_the_copy_separate_from_its_text() {
    let mut part = AiToolResultPart::new("c1", "lookup", "body");
    assert_eq!(part.structured_content, None);
    assert_eq!(part.outcome, None);
    part.outcome = Some(ToolCallOutcome::Served);
    part.structured_content = serde_json::json!({"page": 3}).as_object().cloned();
    assert_eq!(part.as_text(), "body");
    assert_eq!(part.split_for_message(), ("body".to_owned(), Vec::new()));
}
