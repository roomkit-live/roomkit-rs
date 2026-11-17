//! Direct registered reporting helpers preserve provider data outside a turn.
use super::*;
use roomkit::model::{HookResult, InboundMessage};
use roomkit::{HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, ToolCallEvent};
use roomkit_ai::{Agent, AgentOptions};
use serde_json::json;

// Python: test_tool_call_hook_chain.py::test_an_external_report_s_observers_see_the_provider_s_outcome
#[tokio::test]
async fn outside_a_turn_registered_agent_reports_keep_the_original_outcome() {
    for attempted in 0..3 {
        let handler = Arc::new(Handler::default());
        let agent = Agent::new(
            "ai".parse().unwrap(),
            MockAiProvider::new(MockAiProviderOptions::default()),
            AgentOptions {
                ai: AiChannelOptions {
                    external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                    ..AiChannelOptions::default()
                },
                ..AgentOptions::default()
            },
        )
        .unwrap();
        let kit = reports::room(agent).await;
        assert_eq!(handler.channel_id(), "ai");
        kit.hook(
            HookTrigger::OnToolCall,
            move |mut event: ToolCallEvent, _| {
                let result = match attempted {
                    0 => {
                        let mut result =
                            HookResult::allow(roomkit::model::hook::AllowOptions::default());
                        result.metadata.insert("result".into(), json!("REWRITTEN"));
                        result
                    }
                    1 => HookResult::block(
                        "restricted",
                        roomkit::model::hook::BlockOptions::default(),
                    ),
                    _ => {
                        event.result = Some(json!("MODIFIED"));
                        HookResult::modify(event, roomkit::model::hook::ModifyOptions::default())
                    }
                };
                std::future::ready(result)
            },
            HookOptions {
                name: "rewrite".into(),
                ..HookOptions::default()
            },
        )
        .unwrap();
        let observed = audit(&kit);
        handler
            .state
            .fire_on_tool_hook(
                "Read",
                &Metadata::new(),
                "file contents",
                FireOnToolHookOptions {
                    tool_call_id: "t1".into(),
                    room_id: Some("r".parse().unwrap()),
                    ..FireOnToolHookOptions::default()
                },
            )
            .await;
        bounded(roomkit::testing::idle(&kit)).await;
        let seen = observed.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen.first().unwrap().result, Some(json!("file contents")));
        assert!(!seen.first().unwrap().is_error);
        bounded(kit.close()).await.unwrap();
    }
}

// Python: test_refused_reports.py::test_a_handler_can_report_what_failed
#[tokio::test]
async fn a_custom_handler_can_report_its_private_failure_detail() {
    for streaming in [false, true] {
        let handler = Arc::new(Handler {
            detail: Some("disk full".into()),
            ..Handler::default()
        });
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model(streaming, AiToolCall::new("c1", "remote")),
            AiChannelOptions {
                external_tool_handler: Some(Arc::<Handler>::clone(&handler)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let kit = reports::room(agent).await;
        let observed = audit(&kit);
        bounded(kit.process_inbound(
            InboundMessage::new("sms".parse().unwrap(), "u", TextContent::new("go")),
            ProcessInboundOptions {
                room_id: Some("r".parse().unwrap()),
                ..ProcessInboundOptions::default()
            },
        ))
        .await
        .unwrap();
        bounded(roomkit::testing::idle(&kit)).await;
        let seen = observed.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen.first().unwrap().error_detail.as_deref(),
            Some("disk full")
        );
        assert!(seen.first().unwrap().is_error);
        bounded(kit.close()).await.unwrap();
    }
}

fn audit(kit: &roomkit::RoomKit) -> Arc<Mutex<Vec<ToolCallEvent>>> {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            capture.lock().unwrap().push(event);
            std::future::ready(())
        },
        HookOptions {
            name: "audit".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    observed
}
