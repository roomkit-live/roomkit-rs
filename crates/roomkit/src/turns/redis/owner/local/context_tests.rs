//! A channel's initial callback and its stream each inherit their owner's caller.
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use crate::stream::ResponseItem;
use crate::turns::context::{TurnContext, TurnScope};
use crate::turns::observe::Carried;
use crate::turns::pipe::pipe;
use crate::turns::wire::{Control, Frame, Item};
use crate::turns::{CarriedRequest, MemoryTurn};
use crate::{Channel, ChannelError, RedisTurnBackend, TurnExecutor};
use futures_core::Stream;

struct Reader(bool);

impl Stream for Reader {
    type Item = Result<ResponseItem, ChannelError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = (!std::mem::replace(&mut self.0, true)).then(|| {
            let scope = TurnScope::current();
            Ok(ResponseItem::Text(format!(
                "reader={},submitter={}",
                scope.contains("reader"),
                scope.contains("submitter")
            )))
        });
        Poll::Ready(item)
    }
}

struct Probe(ChannelId);

impl Channel for Probe {
    fn channel_id(&self) -> ChannelId {
        self.0.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    async fn handle_inbound(
        &self,
        msg: InboundMessage,
        ctx: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        Ok(crate::default_inbound_event(self, &msg, &ctx))
    }
    async fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        Ok(ChannelOutput::empty())
    }
    async fn on_event(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let scope = TurnScope::current();
        assert!(scope.contains("submitter"));
        assert!(!scope.contains("reader"));
        Ok(crate::current_turn_host()
            .unwrap()
            .respond_with_stream(Box::pin(Reader(false))))
    }
}

#[tokio::test]
async fn local_turn_inherits_submission_context_then_its_first_readers_context() {
    let backend = RedisTurnBackend::new(None, Default::default()).unwrap();
    let worker =
        TurnExecutor::new(|id| async move { Ok(Some(Probe(id))) }, Default::default()).unwrap();
    let lines = include_str!("../../../../../tests/fixtures/turns/python-case1.jsonl");
    let sample: serde_json::Value = serde_json::from_str(lines.lines().next().unwrap()).unwrap();
    let body = &sample["message"];
    let id: Arc<str> = Arc::from(body["turn_id"].as_str().unwrap());
    let request = CarriedRequest::from_wire(&body.to_string()).unwrap();
    let (frames, mut received) = pipe(id.clone(), Carried::Frame, Default::default());
    let (controls, incoming) = pipe(id.clone(), Carried::Control, Default::default());
    let owner = Arc::new(MemoryTurn::with_context(
        id,
        frames,
        controls,
        Default::default(),
        TurnContext::current().in_turn("submitter".into()),
    ));
    let turn = backend.take_here(&worker, request, &owner).unwrap();
    backend.serve_here(turn, &owner, incoming);
    let reads = tokio::time::timeout(Duration::from_secs(2), async {
        let mut text = Vec::new();
        loop {
            let (_, frame) = received.get().await.unwrap();
            match frame {
                Frame::Output { streams: true, .. } => {
                    TurnContext::current()
                        .in_turn("reader".into())
                        .scope(async {
                            owner.lend_stream_context();
                            owner
                                .controls
                                .put(&Control::Pull { count: 3, read: 0 })
                                .unwrap();
                        })
                        .await;
                }
                Frame::Item {
                    item: Item::Text { text: delta, .. },
                } => text.push(delta),
                Frame::Error { error } => panic!("local worker failed: {error:?}"),
                Frame::End => break,
                _ => {}
            }
        }
        text
    })
    .await
    .unwrap();
    assert_eq!(reads, ["reader=true,submitter=false"]);
    worker.close().await;
}
