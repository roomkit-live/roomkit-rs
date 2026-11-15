use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use roomkit::ChannelError;
use roomkit::model::{
    ChannelBinding, ChannelType, EventSource, Room, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::memory::{IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use roomkit_ai::{
    AiResponse, AiToolCall, MockAiProvider, MockAiProviderOptions, current_tool_actor_id,
    current_tool_chain_depth, current_tool_room, current_tool_room_id,
};
use serde_json::json;

#[derive(Debug)]
pub(super) struct Seen {
    pub stage: &'static str,
    pub room: Option<Arc<Room>>,
    pub room_id: Option<String>,
    pub actor: Option<String>,
    pub depth: u32,
}

impl Seen {
    pub(super) fn check(&self, room: &str, actor: Option<&str>, depth: u32) {
        assert_eq!(self.room_id.as_deref(), Some(room), "stage {}", self.stage);
        assert_eq!(self.actor.as_deref(), actor);
        assert_eq!(self.depth, depth);
    }
}

pub(super) fn snapshot(stage: &'static str) -> Seen {
    Seen {
        stage,
        room: current_tool_room(),
        room_id: current_tool_room_id().map(|id| id.to_string()),
        actor: current_tool_actor_id(),
        depth: current_tool_chain_depth(),
    }
}

pub(super) fn event(room: &str, actor: Option<&str>) -> (RoomEvent, RoomContext) {
    let mut room = Room::new(RoomId::new(room).unwrap());
    room.organization_id = Some("acme".into());
    room.metadata.insert("tenant".into(), json!("acme"));
    let mut source = EventSource::new("sms1".parse().unwrap(), ChannelType::SMS);
    source.participant_id = actor.map(|id| id.parse().unwrap());
    let mut event = RoomEvent::new(room.id.clone(), source, TextContent::new("go"));
    event.chain_depth = 2;
    (event, RoomContext::new(room))
}

pub(super) fn binding(room: &str) -> ChannelBinding {
    let mut binding = ChannelBinding::new(
        "ai1".parse().unwrap(),
        room.parse().unwrap(),
        ChannelType::AI,
    );
    binding.metadata.insert(
        "tools".into(),
        json!([{"name":"check", "description":"Check the turn"}]),
    );
    binding
}

pub(super) fn model(streaming: bool, calls: bool) -> MockAiProvider {
    let mut responses = Vec::new();
    if calls {
        for id in ["tc1", "tc2"] {
            let mut response = AiResponse::new("");
            response.tool_calls.push(AiToolCall::new(id, "check"));
            responses.push(response);
        }
    }
    responses.push(AiResponse::new("done"));
    MockAiProvider::new(MockAiProviderOptions {
        ai_responses: Some(responses),
        streaming,
        ..MockAiProviderOptions::default()
    })
}

pub(super) struct Memory(pub Arc<Mutex<Vec<Seen>>>);

impl MemoryProvider for Memory {
    fn retrieve<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a RoomEvent,
        _: &'a RoomContext,
        _: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        self.0.lock().unwrap().push(snapshot("memory"));
        Box::pin(async { Ok(MemoryResult::default()) })
    }

    fn ingest<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a RoomEvent,
        _: IngestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + 'a>> {
        self.0.lock().unwrap().push(snapshot("ingest"));
        Box::pin(async { Ok(()) })
    }
}
