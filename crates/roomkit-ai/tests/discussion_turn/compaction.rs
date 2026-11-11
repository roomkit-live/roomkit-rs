//! Compaction keeps the answered input when a hook edits a later message.

use roomkit::model::hook::ModifyOptions;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
    EventContent, HookResult, InboundMessage, Metadata, RoomContext, RoomEvent, RoomId,
    TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, issue_mark, retire_mark,
};
use roomkit_ai::memory::{CompactOptions, MemoryProvider, MemoryResult, RetrieveOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContentPart, AiGenerationEvent, AiMessage, AiMessageContent,
    AiRole, AiTool, AiToolCallPart, AiToolResultPart, ToolResult, Tools,
};
use serde_json::Value;
use std::{
    collections::VecDeque,
    future::{Future, ready},
    pin::Pin,
    sync::{Arc, Mutex},
};
fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}
#[derive(Default)]
struct Memory {
    compacted: Mutex<Vec<Vec<AiMessage>>>,
    reactions: Mutex<VecDeque<Option<Vec<AiMessage>>>>,
}
impl MemoryProvider for Memory {
    fn retrieve<'a>(
        &'a self,
        _room: &'a RoomId,
        _event: &'a RoomEvent,
        context: &'a RoomContext,
        _options: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        Box::pin(ready(Ok(MemoryResult {
            events: context.recent_events.clone(),
            ..Default::default()
        })))
    }
    fn compact<'a>(
        &'a self,
        _room: &'a RoomId,
        history: &'a [AiMessage],
        _usage: &'a Metadata,
        _options: CompactOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<AiMessage>>, ChannelError>> + Send + 'a>>
    {
        self.compacted.lock().unwrap().push(history.to_vec());
        Box::pin(ready(Ok(self
            .reactions
            .lock()
            .unwrap()
            .pop_front()
            .flatten())))
    }
}
struct Deferred {
    agent: AiChannel<ScriptedProvider>,
    mark: Value,
}
impl Channel for Deferred {
    fn channel_id(&self) -> ChannelId {
        self.agent.channel_id()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn capabilities(&self) -> ChannelCapabilities {
        self.agent.capabilities()
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.agent.handle_inbound(message, context).await
    }
    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.agent.deliver(event, binding, context).await
    }
    async fn on_event(
        &self,
        mut event: RoomEvent,
        binding: ChannelBinding,
        mut context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let mut earlier = event.clone();
        earlier.id = roomkit::model::EventId::new("earlier").unwrap();
        earlier.index = 1;
        let mut follower = event.clone();
        follower.id = roomkit::model::EventId::new("later").unwrap();
        follower.index = 3;
        follower.content = EventContent::Text(TextContent::new("Later detail."));
        event.index = 2;
        event
            .metadata
            .insert("_discussion_turn".into(), self.mark.clone());
        context.recent_events = vec![earlier, follower];
        self.agent.on_event(event, binding, context).await
    }
}
#[tokio::test]
async fn editing_a_follower_does_not_choose_an_earlier_identical_question() {
    let memory = Arc::new(Memory::default());
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let tools = Tools::new().tool(AiTool::new("lookup", "Look it up").unwrap(), |_, _| async {
        Ok(ToolResult::Text("found".into()))
    });
    let agent = AiChannel::new(
        id("a"),
        model.clone(),
        AiChannelOptions {
            memory: Some(Arc::<Memory>::clone(&memory)),
            tools: tools.into(),
            ..Default::default()
        },
    )
    .unwrap();
    let mark = issue_mark(vec![]);
    let (kit, room) = setup(agent, &mark).await;
    kit.hook(
        HookTrigger::BeforeAiGeneration,
        |mut event: AiGenerationEvent, _context: RoomContext| async move {
            // Keep both identical questions exactly where they were. Change only the last message.
            let last = event.ai_context.messages.last_mut().unwrap();
            last.content = AiMessageContent::Text(format!("{} [checked]", last.content.text()));
            HookResult::modify(event, ModifyOptions::default())
        },
        HookOptions::default(),
    )
    .unwrap();
    kit.process_inbound(
        InboundMessage::new(id("sms"), "alice", TextContent::new("Same question.")),
        ProcessInboundOptions {
            room_id: Some(room),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    let asked = memory.compacted.lock().unwrap().clone();
    let requests = model.requests();
    kit.close().await.unwrap();
    retire_mark(&mark);
    assert_eq!(requests.len(), 2);
    assert_eq!(asked.len(), 1);
    assert_eq!(
        asked[0].len(),
        1,
        "the first identical question is history; actual requests: {requests:?}; memory: {asked:?}"
    );
    assert_eq!(asked[0][0].content.text(), "Same question.");
}

async fn setup(agent: AiChannel<ScriptedProvider>, mark: &Value) -> (RoomKit, RoomId) {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(Deferred {
        agent,
        mark: mark.clone(),
    })
    .unwrap();
    kit.register_channel(RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for channel in [id("sms"), id("a")] {
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, room)
}

#[tokio::test]
async fn ambiguous_input_protects_old_tools_and_shortens_only_new_results() {
    let summary = AiMessage::new(AiRole::Assistant, "Memory added this.");
    let memory = Arc::new(Memory {
        reactions: Mutex::new(VecDeque::from([Some(vec![summary.clone()]), Some(vec![])])),
        ..Default::default()
    });
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::calling("", [("c2", "lookup", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let long = "A whole result.\n".repeat(250);
    let result = long.clone();
    let tools = Tools::new().tool(AiTool::new("lookup", "Look it up").unwrap(), move |_, _| {
        let result = result.clone();
        async move { Ok(ToolResult::Text(result)) }
    });
    let agent = AiChannel::new(
        id("a"),
        model.clone(),
        AiChannelOptions {
            memory: Some(Arc::<Memory>::clone(&memory)),
            tools: tools.into(),
            ..Default::default()
        },
    )
    .unwrap();
    let left = vec![
        AiMessage::new(
            AiRole::Assistant,
            vec![AiContentPart::ToolCall(AiToolCallPart::new(
                "old", "lookup",
            ))],
        ),
        AiMessage::new(
            AiRole::Tool,
            vec![AiContentPart::ToolResult(AiToolResultPart::new(
                "old",
                "lookup",
                long.clone(),
            ))],
        ),
        AiMessage::new(AiRole::User, "Redacted question."),
        AiMessage::new(AiRole::User, "Later detail."),
    ];
    let mark = issue_mark(vec![]);
    let (kit, room) = setup(agent, &mark).await;
    let replacement = left.clone();
    kit.hook(
        HookTrigger::BeforeAiGeneration,
        move |mut event: AiGenerationEvent, _context: RoomContext| {
            let replacement = replacement.clone();
            async move {
                event.ai_context.messages = replacement;
                HookResult::modify(event, ModifyOptions::default())
            }
        },
        HookOptions::default(),
    )
    .unwrap();
    kit.process_inbound(
        InboundMessage::new(id("sms"), "alice", TextContent::new("Private question.")),
        ProcessInboundOptions {
            room_id: Some(room),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    let asked = memory.compacted.lock().unwrap().clone();
    let requests = model.requests();
    kit.close().await.unwrap();
    retire_mark(&mark);
    assert_eq!(asked, vec![vec![], vec![]]);
    assert_eq!(requests.len(), 3);
    let protected: Vec<_> = std::iter::once(summary).chain(left).collect();
    for request in &requests[1..] {
        assert_eq!(&request.messages[..protected.len()], &protected);
    }
    let results: Vec<_> = requests[2]
        .messages
        .iter()
        .flat_map(|m| match &m.content {
            AiMessageContent::Parts(parts) => parts.as_slice(),
            AiMessageContent::Text(_) => &[],
        })
        .filter_map(|part| {
            if let AiContentPart::ToolResult(result) = part {
                Some(result)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_text(), long);
    assert!(results[1].as_text().contains("evicted_c1"));
    assert_eq!(results[2].as_text(), long);
}
