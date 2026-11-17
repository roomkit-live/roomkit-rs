use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiResponse, AiTool, AiToolCall, AiToolResultPart, MockAiProvider,
    MockAiProviderOptions, SkillRegistry, ToolHandler, ToolPolicy,
};
use serde_json::json;

pub(super) static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) struct Turn {
    pub(super) kit: RoomKit,
    model: MockAiProvider,
}

pub(super) fn call(arguments: serde_json::Value) -> AiToolCall {
    let mut call = AiToolCall::new("c1", "lookup");
    call.arguments = serde_json::from_value(arguments).unwrap();
    call
}

impl Turn {
    pub(super) async fn new(
        streaming: bool,
        calls: Vec<AiToolCall>,
        handler: ToolHandler,
        policy: Option<ToolPolicy>,
    ) -> Self {
        Self::with_skills(streaming, calls, handler, policy, None).await
    }

    pub(super) async fn with_skills(
        streaming: bool,
        calls: Vec<AiToolCall>,
        handler: ToolHandler,
        policy: Option<ToolPolicy>,
        skills: Option<SkillRegistry>,
    ) -> Self {
        let model = MockAiProvider::new(MockAiProviderOptions {
            streaming,
            ai_responses: Some(vec![
                AiResponse {
                    tool_calls: calls,
                    ..AiResponse::new("")
                },
                AiResponse::new("done"),
            ]),
            ..Default::default()
        });
        let definition = AiTool {
            parameters: json!({"type":"object","properties":{"query":{"type":"string"}}}),
            ..AiTool::new("lookup", "Synthetic lookup").unwrap()
        };
        let agent = AiChannel::new(
            ChannelId::new("ai").unwrap(),
            model.clone(),
            AiChannelOptions {
                tools: vec![definition.into()],
                tool_handler: Some(handler),
                tool_policy: policy,
                skills,
                tool_search: Some(false),
                ..Default::default()
            },
        )
        .unwrap();
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(RecordingTransport::new(
            ChannelId::new("sms").unwrap(),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        kit.register_channel(agent).unwrap();
        let room = RoomId::new("r1").unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        for name in ["sms", "ai"] {
            kit.attach_channel(
                &room,
                &ChannelId::new(name).unwrap(),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
        Self { kit, model }
    }

    pub(super) async fn run(&self) -> Vec<AiToolResultPart> {
        let result = self
            .kit
            .process_inbound(
                InboundMessage::new(
                    ChannelId::new("sms").unwrap(),
                    "synthetic-user",
                    TextContent::new("run"),
                ),
                ProcessInboundOptions {
                    room_id: Some(RoomId::new("r1").unwrap()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(result.error.is_none());
        self.kit.close().await.unwrap();
        let requests = self.model.calls();
        assert_eq!(requests.len(), 2);
        requests
            .get(1)
            .unwrap()
            .messages
            .iter()
            .flat_map(|message| message.tool_results())
            .cloned()
            .collect()
    }
}
