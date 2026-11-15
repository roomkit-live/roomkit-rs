use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::human_input::{HumanInputHandler, PendingInputEvent};
use roomkit::model::TextContent;
use roomkit::{AttachChannelOptions, HookExecution, HookOptions, HookTrigger, SendEventOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiTool, HumanInputToolHandler, HumanInputToolHandlerOptions,
    current_response_metadata,
};
use serde_json::json;

use super::support::{model, snapshot};

// Python: tests/test_ai_turn_context_human.py::test_a_human_input_observer_keeps_the_turn
#[tokio::test]
async fn a_human_input_observer_keeps_the_turn() {
    for streaming in [false, true] {
        let kit = super::lifetime::kit().await;
        let shared = HumanInputHandler::default();
        let human = HumanInputToolHandler::new(
            ["check"],
            HumanInputToolHandlerOptions {
                handler: Some(shared.clone()),
                tool_definitions: vec![AiTool::new("check", "Ask the person").unwrap()],
                ..HumanInputToolHandlerOptions::default()
            },
        )
        .unwrap();
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            model(streaming, true),
            AiChannelOptions {
                human_input_handler: Some(human),
                config_provider: Some(Arc::new(|_, _| {
                    current_response_metadata()
                        .unwrap()
                        .insert("configured", json!(true));
                    Box::pin(async { Ok(None) })
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        kit.register_channel(agent).unwrap();
        kit.attach_channel(
            &"r1".parse().unwrap(),
            &"ai1".parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        kit.hook(
            HookTrigger::OnUserInputRequired,
            move |event: PendingInputEvent, _| {
                captured.lock().unwrap().push((
                    snapshot("human"),
                    current_response_metadata().and_then(|record| record.get("configured")),
                ));
                shared.resolve(&event.pending_id, "yes");
                async {}
            },
            HookOptions {
                name: "human context".into(),
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            kit.send_event(
                &"r1".parse().unwrap(),
                &"sms1".parse().unwrap(),
                TextContent::new("go"),
                SendEventOptions {
                    chain_depth: 2,
                    ..SendEventOptions::default()
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        kit.close().await.unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        for (read, record) in &*seen {
            read.check("r1", None, 3);
            assert_eq!(read.room.as_ref().unwrap().id.as_str(), "r1");
            assert_eq!(record, &Some(json!(true)));
        }
    }
}
