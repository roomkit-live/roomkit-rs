//! An abandoned public reader leaves tool reports under the ACP turn's record.

use super::{Agent, PATIENCE, channel, close, observe, room_with_transport, send, start_tool};
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{Channel, ChannelError, DeliveryStream, StreamItem, TurnRecord};
use serde_json::json;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

struct Abandon {
    inner: RecordingTransport,
    foreign: TurnRecord,
}

impl Channel for Abandon {
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
            if matches!(item?, StreamItem::Text(_)) {
                break;
            }
        }
        tokio::spawn(self.foreign.clone().scope(async move {
            drop(stream);
        }))
        .await
        .unwrap();
        Ok(ChannelOutput::empty())
    }
}

// Python: tests/test_channels/test_acp_record.py::test_an_acp_callback_keeps_its_record_across_a_nested_local_turn[abandoned]
#[tokio::test]
async fn an_abandoned_acp_reader_reports_under_its_turns_record() {
    tokio::time::timeout(PATIENCE, check_abandoned())
        .await
        .unwrap();
}

async fn check_abandoned() {
    let (channel, mut agents, _) = channel();
    let foreign = TurnRecord::default();
    foreign.current().insert("foreign", json!(true));
    let reader = Abandon {
        inner: RecordingTransport::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        ),
        foreign: foreign.clone(),
    };
    let kit = room_with_transport(channel, reader).await;
    let mut observed = observe(&kit);
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let _unanswered = start_tool(&mut agent).await;
    let text = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Anchor"}}
    }});
    agent
        .writer
        .write_all(format!("{text}\n").as_bytes())
        .await
        .unwrap();
    running.await.unwrap();
    let (kind, record) = observed.recv().await.unwrap();
    assert_eq!(kind, "tool");
    assert_eq!(record.get("observed"), Some(json!(true)));
    assert!(record.get("acp").is_some());
    assert_eq!(record.get("foreign"), None);
    assert_eq!(foreign.current().to_metadata().len(), 1);
    assert_eq!(foreign.current().get("foreign"), Some(json!(true)));
    close(&kit, &mut agent).await;
    assert!(observed.try_recv().is_err());
}
