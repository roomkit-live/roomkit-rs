//! A report under another channel never consumes the local occurrence's right.
use super::*;
use roomkit::model::InboundMessage;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, ToolCallEvent,
};
use roomkit_ai::{AiTool, PolicyExternalToolHandler, ToolHandler, ToolResult};

// Python: test_external_call_cut.py::test_another_channels_report_never_claims_the_turns_call
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_channels_same_id_does_not_claim_the_local_call() {
    for streaming in [false, true] {
        other_channel(streaming).await;
    }
}

async fn other_channel(streaming: bool) {
    let other = Arc::new(PolicyExternalToolHandler::default());
    let kit = RoomKit::new(RoomKitOptions::default());
    let agent = AiChannel::new(
        "other".parse().unwrap(),
        MockAiProvider::new(MockAiProviderOptions::default()),
        AiChannelOptions {
            external_tool_handler: Some(Arc::<PolicyExternalToolHandler>::clone(&other)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    kit.register_channel(agent).unwrap();
    let agent = local_agent(streaming, other);
    kit.register_channel(agent).unwrap();
    kit.register_channel(RecordingTransport::new(
        "sms".parse().unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room: roomkit::model::RoomId = "r".parse().unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for channel in ["ai", "sms"] {
        kit.attach_channel(
            &room,
            &channel.parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            capture
                .lock()
                .unwrap()
                .push((event.channel_id.to_string(), event.name, event.result));
            std::future::ready(())
        },
        HookOptions {
            name: "audit".into(),
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    bounded(kit.process_inbound(
        InboundMessage::new("sms".parse().unwrap(), "u", TextContent::new("go")),
        ProcessInboundOptions {
            room_id: Some(room),
            ..ProcessInboundOptions::default()
        },
    ))
    .await
    .unwrap();
    bounded(roomkit::testing::idle(&kit)).await;
    let mut seen = observed.lock().unwrap().clone();
    seen.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        seen,
        [
            (
                "ai".into(),
                "lookup".into(),
                Some(serde_json::json!("found"))
            ),
            (
                "other".into(),
                "Bash".into(),
                Some(serde_json::json!("ran"))
            )
        ]
    );
    bounded(kit.close()).await.unwrap();
}

fn local_agent(
    streaming: bool,
    other: Arc<PolicyExternalToolHandler>,
) -> AiChannel<MockAiProvider> {
    let mut response = AiResponse::new("");
    response.tool_calls.push(AiToolCall::new("c1", "lookup"));
    let provider = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![response, AiResponse::new("done")]),
        ..MockAiProviderOptions::default()
    });
    AiChannel::new(
        "ai".parse().unwrap(),
        provider,
        AiChannelOptions {
            tools: vec![AiTool::new("lookup", "Lookup").unwrap().into()],
            tool_handler: Some(ToolHandler::new(move |_, _| {
                let other = Arc::clone(&other);
                async move {
                    other
                        .on_tool_result(
                            "Bash",
                            &Metadata::new(),
                            "ran",
                            OnToolResultOptions {
                                tool_call_id: "c1".into(),
                                room_id: Some("r".parse().unwrap()),
                                ..OnToolResultOptions::default()
                            },
                        )
                        .await?;
                    Ok(ToolResult::Text("found".into()))
                }
            })),
            ..AiChannelOptions::default()
        },
    )
    .unwrap()
}
