//! Streamed text and the tool start are visible before the ACP prompt ends.

use super::{Agent, PATIENCE, channel, close, room_with_transport, send};
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventContent, EventType, InboundMessage,
    RoomContext, RoomEvent, ToolCallStatus,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{Channel, ChannelError, DeliveryStream, StreamItem, TurnRecord};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

struct Reading {
    inner: RecordingTransport,
    observed: tokio::sync::mpsc::UnboundedSender<StreamItem>,
}

impl Channel for Reading {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id().clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }
    fn supports_streaming_delivery(&self) -> bool {
        true
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.inner.handle_inbound(message, context).await
    }
    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.inner.deliver(event, binding, context).await
    }
    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        while let Some(item) = stream.next().await {
            self.observed.send(item?).unwrap();
        }
        Ok(ChannelOutput::empty())
    }
}

// Python: test_acp.py::test_framework_delivers_acp_text_before_prompt_finishes.
// CLI rendering belongs to the Python CLI; this verifies public transport delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_framework_delivers_acp_text_before_prompt_finishes() {
    tokio::time::timeout(PATIENCE, check()).await.unwrap();
}

async fn update(agent: &mut Agent, update: Value) {
    let notification = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1", "update":update}});
    agent
        .writer
        .write_all(format!("{notification}\n").as_bytes())
        .await
        .unwrap();
}

async fn first_items(observed: &mut mpsc::UnboundedReceiver<StreamItem>) {
    let mut text = String::new();
    loop {
        match observed.recv().await.unwrap() {
            StreamItem::Text(delta) => text.push_str(&delta),
            StreamItem::Row(event) if event.r#type == EventType::ToolCallStart => {
                let EventContent::ToolCall(call) = &event.content else {
                    panic!("tool start content");
                };
                assert_eq!(call.tool_id, "tool-stream");
                assert_eq!(call.tool_name, "Inspect files");
                assert_eq!(text, "First chunk");
                return;
            }
            _ => {}
        }
    }
}

async fn finish(agent: &mut Agent, prompt: &Value) {
    update(
        agent,
        json!({"sessionUpdate":"tool_call_update","toolCallId":"tool-stream",
        "status":"completed","rawOutput":{"files":3}}),
    )
    .await;
    update(agent, json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":" then second"}})).await;
    agent.answer(prompt, json!({"stopReason":"end_turn"})).await;
}

async fn check() {
    let (channel, mut opened, _) = channel();
    let (observed, mut items) = mpsc::unbounded_channel();
    let reader = Reading {
        inner: RecordingTransport::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        ),
        observed,
    };
    let kit = room_with_transport(channel, reader).await;
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let processing = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    start(&mut agent).await;
    first_items(&mut items).await;
    assert!(!processing.is_finished());
    finish(&mut agent, &prompt).await;
    let result = processing.await.unwrap();
    assert!(result.error.is_none());
    assert_end(&mut items);
    close(&kit, &mut agent).await;
}

async fn start(agent: &mut Agent) {
    update(agent, json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"First chunk"}})).await;
    update(agent, json!({"sessionUpdate":"tool_call","toolCallId":"tool-stream","title":"Inspect files","kind":"read","status":"in_progress","rawInput":{"glob":"src/**/*.py"}})).await;
}

fn assert_end(items: &mut mpsc::UnboundedReceiver<StreamItem>) {
    let mut ended = Vec::new();
    while let Ok(item) = items.try_recv() {
        if let StreamItem::Row(event) = item {
            if event.r#type == EventType::ToolCallEnd {
                ended.push(event);
            }
        }
    }
    assert_eq!(ended.len(), 1);
    let EventContent::ToolCall(call) = &ended[0].content else {
        panic!("tool end content");
    };
    assert_eq!(call.tool_id, "tool-stream");
    assert_eq!(call.tool_name, "Inspect files");
    assert_eq!(call.status, ToolCallStatus::Completed);
}
