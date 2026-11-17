use std::sync::Arc;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventSource, Metadata, Room, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::{
    Agent, AgentOptions, AiChannel, AiChannelOptions, AiProvider, AiTool, AiToolHandle, Tool,
    ToolEntry, ToolFuture, ToolResult,
};

pub(crate) enum Door<P> {
    Channel(Box<AiChannel<P>>),
    Agent(Box<Agent<P>>),
}

impl<P: AiProvider> Door<P> {
    pub(crate) fn new(agent: bool, provider: P, options: AiChannelOptions) -> Self {
        let id = ChannelId::new("ai").unwrap();
        if agent {
            Self::Agent(Box::new(
                Agent::new(
                    id,
                    provider,
                    AgentOptions {
                        ai: options,
                        ..Default::default()
                    },
                )
                .unwrap(),
            ))
        } else {
            Self::Channel(Box::new(AiChannel::new(id, provider, options).unwrap()))
        }
    }

    pub(crate) fn tools(&self) -> Vec<AiToolHandle> {
        match self {
            Self::Channel(channel) => channel.extra_tools(),
            Self::Agent(agent) => agent.extra_tools(),
        }
    }

    pub(crate) async fn turn(&self, metadata: Metadata) -> ChannelOutput {
        let room = RoomId::new("r1").unwrap();
        let event = RoomEvent::new(
            room.clone(),
            EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
            EventContent::Text(TextContent::new("hello")),
        );
        let binding = ChannelBinding {
            category: ChannelCategory::Intelligence,
            metadata,
            ..ChannelBinding::new(ChannelId::new("ai").unwrap(), room.clone(), ChannelType::AI)
        };
        let context = RoomContext::new(Room::new(room));
        match self {
            Self::Channel(channel) => channel.on_event(event, binding, context).await.unwrap(),
            Self::Agent(agent) => agent.on_event(event, binding, context).await.unwrap(),
        }
    }

    pub(crate) async fn close(&self) {
        match self {
            Self::Channel(channel) => channel.close().await,
            Self::Agent(agent) => agent.close().await,
        }
    }
}

struct FakeTool {
    definition: AiTool,
}

impl Tool for FakeTool {
    fn definition(&self) -> AiTool {
        self.definition.clone()
    }
    fn handler(&self, name: String, _arguments: Metadata) -> ToolFuture {
        let answer = if name == self.definition.name {
            "object answer".to_owned()
        } else {
            format!("{{\"error\": \"Unknown tool: {name}\"}}")
        };
        Box::pin(async move { Ok(ToolResult::Text(answer)) })
    }
}

pub(crate) fn tool(name: &str) -> ToolEntry {
    let definition = AiTool {
        parameters: serde_json::json!({"type":"object","properties":{}}),
        ..AiTool::new(name, format!("Fake {name}")).unwrap()
    };
    ToolEntry::Tool(Arc::new(FakeTool { definition }))
}
