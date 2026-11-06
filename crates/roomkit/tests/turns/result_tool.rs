//! A delegation that forces a tool of the caller's own (RFC §23.3):
//! Python's `ResultTool`, as the supervised flow forces `submit_verdict`.
//! The worker is the kit's AI channel over a scripted model.

use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, Metadata, TaskStatus};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DelegateOptions, ResultTool, RoomKit, RoomKitOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use serde_json::json;

use crate::support::{channel, room};

const REMINDER: &str = "You did not submit a verdict. Call submit_verdict now.";

/// `submit_verdict`, read as it was called; never called, a refusal.
fn verdict() -> ResultTool {
    ResultTool::new(
        json!({
            "name": "submit_verdict",
            "description": "Give your verdict.",
            "parameters": { "type": "object" },
        }),
        Arc::new(|arguments: &Metadata| arguments.clone()),
        Arc::new(|_role: &str, _last_output: &str, _attempts: u32| {
            let mut refused = Metadata::new();
            refused.insert("approved".to_owned(), json!(false));
            refused.insert("next_task".to_owned(), json!(null));
            refused
        }),
        REMINDER,
    )
}

/// A kit whose room `r1` binds an SMS transport, with the agent `boss`
/// answering with `model`.
async fn kit(model: &ScriptedProvider) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let boss = AiChannel::new(channel("boss"), model.clone(), AiChannelOptions::default());
    kit.register_channel(boss.unwrap()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

fn judged(retries: u32) -> DelegateOptions {
    DelegateOptions {
        wait: true,
        require_structured_result: true,
        max_result_retries: retries,
        result_tool: Some(verdict()),
        ..DelegateOptions::default()
    }
}

// Python: tests/tasks/test_structured_result.py::TestAnotherResultTool::test_the_given_tool_is_injected_and_its_call_captured
#[tokio::test]
async fn the_given_tool_is_injected_and_its_call_captured() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling(
            "",
            [(
                "c1",
                "submit_verdict",
                r#"{"approved": true, "feedback": "", "next_task": "write it up"}"#,
            )],
        ),
        ScriptedRound::answering("Done."),
    ]);
    let kit = kit(&model).await;

    let task = kit
        .delegate(&room(), &channel("boss"), "Judge it.", judged(2))
        .await
        .unwrap();
    let result = task.result().unwrap();
    let declared: Vec<Vec<String>> = model
        .requests()
        .iter()
        .map(|request| request.tools.iter().map(|tool| tool.name.clone()).collect())
        .collect();
    kit.close().await.unwrap();

    assert_eq!(
        (result.status, result.output.as_deref(), declared),
        (
            TaskStatus::Completed,
            Some(r#"{"approved": true, "feedback": "", "next_task": "write it up"}"#),
            // The re-read of stored results after it (RFC §21.5).
            vec![vec!["submit_verdict".to_owned(), "read_stored_result".to_owned()]; 2]
        )
    );
}

// Python: tests/tasks/test_structured_result.py::TestAnotherResultTool::test_its_reminder_and_its_missing_payload_are_used
#[tokio::test]
async fn its_reminder_and_its_missing_payload_are_used() {
    let model = ScriptedProvider::new([
        ScriptedRound::answering("Thinking."),
        ScriptedRound::answering("Still thinking."),
    ]);
    let kit = kit(&model).await;

    let task = kit
        .delegate(&room(), &channel("boss"), "Judge it.", judged(1))
        .await
        .unwrap();
    let result = task.result().unwrap();
    let reminded = model.requests().get(1).is_some_and(|request| {
        request
            .messages
            .last()
            .is_some_and(|message| format!("{:?}", message.content).contains(REMINDER))
    });
    kit.close().await.unwrap();

    assert_eq!(
        (result.output.as_deref(), reminded),
        (Some(r#"{"approved": false, "next_task": null}"#), true)
    );
}
