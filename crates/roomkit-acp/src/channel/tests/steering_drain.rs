//! Steering remains addressable until the response stream has drained.
//! Python: `tests/test_channels/test_acp_steering.py` (lazy stream boundary).
use super::{Agent, Ask, PATIENCE, Piped, TurnStream, channel, drained};
use crate::AcpChannel;
use futures_core::Stream;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, current_turn_host,
    model::{
        Cancel, ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
        InboundMessage, ResponseMetadata, RoomContext, RoomEvent, RoomId, TextContent,
    },
    testing::{RecordingTransport, RecordingTransportOptions},
};
use serde_json::json;
use std::{
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::io::AsyncWriteExt;

struct Reading {
    inner: AcpChannel<Piped>,
    result: Arc<Mutex<Option<usize>>>,
}
impl Channel for Reading {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    async fn handle_inbound(
        &self,
        m: InboundMessage,
        c: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.inner.handle_inbound(m, c).await
    }
    async fn deliver(
        &self,
        e: RoomEvent,
        b: ChannelBinding,
        c: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.inner.deliver(e, b, c).await
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let host = current_turn_host().unwrap();
        let record = ResponseMetadata::new();
        record.insert("acp", json!({"protocol_version":1}));
        let mut stream = TurnStream::new(
            Arc::clone(&self.inner.shared),
            Ask {
                standalone: false,
                room_id: context.room.id.clone(),
                event_id: event.id.to_string(),
                input: crate::channel::context::Input {
                    notes: None,
                    context,
                    trigger: event,
                    text: "hello".to_owned(),
                    blocks: Vec::new(),
                },
                record: record.clone(),
                host: Some(host.clone()),
                answers_room: true,
            },
        );
        let first = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(PATIENCE, async {
            while record.to_metadata()["acp"].get("prompt_returned") != Some(&json!(true)) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first,roomkit::ResponseItem::Text(t) if t=="first"));
        assert!(matches!(second,roomkit::ResponseItem::Text(t) if t=="second"));
        *self.result.lock().unwrap() = self
            .inner
            .steer(&Cancel::default().into(), host.turn_id())
            .await
            .unwrap();
        assert_eq!(
            drained(&mut stream).await,
            (vec!["third".to_owned()], false)
        );
        assert_eq!(
            self.inner
                .steer(&Cancel::default().into(), host.turn_id())
                .await
                .unwrap(),
            None
        );
        Ok(ChannelOutput::empty())
    }
    async fn close(&self) {
        self.inner.close().await;
    }
}

#[tokio::test]
async fn test_steering_reaches_a_finished_prompt_until_its_stream_ends() {
    let (channel, mut agents, _) = channel();
    let result = Arc::new(Mutex::new(None));
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(Reading {
        inner: channel,
        result: Arc::clone(&result),
    })
    .unwrap();
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
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
    for name in ["ws", "claude"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(name).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let running = tokio::spawn({
        let kit = Arc::clone(&kit);
        async move {
            kit.process_inbound(
                InboundMessage::new(
                    ChannelId::new("ws").unwrap(),
                    "alice",
                    TextContent::new("hello"),
                ),
                ProcessInboundOptions {
                    room_id: Some(room),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        }
    });
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let init = agent.read().await;
    agent
        .answer(&init, json!({"protocolVersion":1,"agentCapabilities":{}}))
        .await;
    let new = agent.read().await;
    agent.answer(&new, json!({"sessionId":"s1"})).await;
    let prompt = agent.read().await;
    assert_eq!(prompt["method"], "session/prompt");
    for text in ["first", "second", "third"] {
        let n = json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}}});
        agent
            .writer
            .write_all(format!("{n}\n").as_bytes())
            .await
            .unwrap();
    }
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(
        tokio::time::timeout(PATIENCE, running)
            .await
            .unwrap()
            .unwrap()
            .error
            .is_none()
    );
    assert_eq!(
        *result.lock().unwrap(),
        Some(1),
        "Python keeps the turn addressable until its terminal is consumed"
    );
    let cancel = agent.read().await;
    assert_eq!(cancel["method"], "session/cancel");
    assert_eq!(cancel["params"]["sessionId"], "s1");
    kit.close().await.unwrap();
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}
