//! Python: `test_local_turn_publication_errors`, through the real text tool loop.

#![expect(clippy::unwrap_used, reason = "a broken fixture fails its test")]
#[path = "../../roomkit/tests/local_presence_failures/backend.rs"]
#[expect(
    dead_code,
    reason = "the shared backend also exposes its original error identity to core tests"
)]
mod backend;
#[path = "../../roomkit/tests/local_presence_failures/diagnostics.rs"]
mod diagnostics;

use backend::Backend;
use roomkit::model::{ChannelType, InboundMessage, TextContent};
use roomkit::realtime::EphemeralEventType;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiTool, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ToolHandler, ToolResult,
};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn a_failed_tool_start_does_not_stop_the_tool_or_its_end() {
    for streaming in [false, true] {
        let (logs, _capturing) = diagnostics::Diagnostics::capture();
        let backend = Arc::new(Backend::new(1, EphemeralEventType::ToolCallStart));
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<Backend>::clone(&backend)),
            ..Default::default()
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let called = Arc::clone(&calls);
        let mut calling = AiResponse::new("");
        calling.tool_calls.push(AiToolCall::new("tc1", "search"));
        let provider = MockAiProvider::new(MockAiProviderOptions {
            streaming,
            ai_responses: Some(vec![calling, AiResponse::new("Done.")]),
            ..Default::default()
        });
        kit.register_channel(
            AiChannel::new(
                "agent".parse().unwrap(),
                provider,
                AiChannelOptions {
                    tools: vec![AiTool::new("search", "Search").unwrap().into()],
                    tool_handler: Some(ToolHandler::new(move |name, _| {
                        called.lock().unwrap().push(name);
                        async { Ok(ToolResult::Text("found".into())) }
                    })),
                    tool_search: Some(false),
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
        kit.register_channel(RecordingTransport::new(
            "ops".parse().unwrap(),
            ChannelType::WEBHOOK,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some("r1".parse().unwrap()),
            ..Default::default()
        })
        .await
        .unwrap();
        for channel in ["ops", "agent"] {
            kit.attach_channel(
                &"r1".parse().unwrap(),
                &channel.parse().unwrap(),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
        kit.process_inbound(
            InboundMessage::new("ops".parse().unwrap(), "alice", TextContent::new("go")),
            ProcessInboundOptions {
                room_id: Some("r1".parse().unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        roomkit::testing::idle(&kit).await;
        kit.close().await.unwrap();
        assert_eq!(*calls.lock().unwrap(), ["search"]);
        let events = backend.published.lock().unwrap();
        assert!(
            !events
                .iter()
                .any(|event| event.r#type == EphemeralEventType::ToolCallStart)
        );
        let ends: Vec<_> = events
            .iter()
            .filter(|event| event.r#type == EphemeralEventType::ToolCallEnd)
            .collect();
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0].data["tool_calls"][0]["status"], "completed");
        let failures = logs.containing("publishing a turn notification failed");
        assert_eq!(failures.len(), 1);
        assert!(failures[0].starts_with("ERROR "));
        assert!(failures[0].contains("realtime down"));
    }
}
