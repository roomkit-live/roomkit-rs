//! A channel's initial callback and its stream each inherit their owner's caller.
use std::collections::BTreeSet;
use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use crate::stream::ResponseItem;
use crate::turns::context::{TurnContext, TurnScope};
use crate::turns::observe::{Carried, Observers};
use crate::turns::pipe::pipe;
use crate::turns::wire::{Control, Frame, Item};
use crate::turns::{CarriedRequest, MemoryTurn};
use crate::{
    Channel, ChannelError, RedisTurnBackend, RedisTurnBackendOptions, TurnExecutor,
    TurnExecutorOptions, TurnTimeouts,
};
use futures_core::Stream;

struct Reader(bool, Arc<AtomicBool>);

impl Drop for Reader {
    fn drop(&mut self) {
        self.1.store(true, Ordering::Release);
    }
}

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

struct Probe(ChannelId, Arc<AtomicBool>);

impl Channel for Probe {
    fn channel_id(&self) -> ChannelId {
        self.0.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn handle_inbound(
        &self,
        msg: InboundMessage,
        ctx: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(crate::default_inbound_event(self, &msg, &ctx)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    fn on_event(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let scope = TurnScope::current();
        assert!(scope.contains("submitter"));
        assert!(!scope.contains("reader"));
        ready(Ok(crate::current_turn_host().unwrap().respond_with_stream(
            Box::pin(Reader(false, Arc::clone(&self.1))),
        )))
    }
}

fn link() -> (
    CarriedRequest,
    Arc<MemoryTurn>,
    crate::turns::pipe::PipeReceiver<Control>,
    crate::turns::pipe::PipeReceiver<Frame>,
) {
    let lines = include_str!("../../../../../tests/fixtures/turns/python-case1.jsonl");
    let sample: serde_json::Value = serde_json::from_str(lines.lines().next().unwrap()).unwrap();
    let body = &sample["message"];
    let id: Arc<str> = Arc::from(body["turn_id"].as_str().unwrap());
    let request = CarriedRequest::from_wire(&body.to_string()).unwrap();
    let (frames, received) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
    let (controls, incoming) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
    let owner = Arc::new(MemoryTurn::with_context(
        id,
        frames,
        controls,
        TurnTimeouts::default(),
        TurnContext::current().in_turn("submitter".into()),
    ));
    (request, owner, incoming, received)
}

#[tokio::test]
async fn local_turn_inherits_submission_context_then_its_first_readers_context() {
    let backend = RedisTurnBackend::new(None, RedisTurnBackendOptions::default()).unwrap();
    let worker = TurnExecutor::new(
        |id| async move { Ok(Some(Probe(id, Arc::default()))) },
        TurnExecutorOptions::default(),
    )
    .unwrap();
    let (request, owner, incoming, mut received) = link();
    backend.submit_here(Arc::new(worker.clone()), request, &owner, incoming, || {});
    let reads = tokio::time::timeout(Duration::from_secs(2), async {
        let mut text = Vec::new();
        loop {
            let (_, frame) = received.get().await.unwrap();
            #[expect(
                clippy::wildcard_enum_match_arm,
                reason = "the context probe reads only stream text and ignores unrelated notifications"
            )]
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

#[tokio::test]
async fn a_backend_closed_after_placement_refuses_the_local_turn_before_handover() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    let backend = RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: format!("roomkit:placement:{}", uuid::Uuid::new_v4()),
            timeouts: TurnTimeouts {
                heartbeat_interval: Duration::from_millis(20),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let worker = TurnExecutor::new(
        |id| async move { Ok(Some(Probe(id, Arc::default()))) },
        TurnExecutorOptions {
            hosts: Some(["ai".to_owned()].into()),
            ..Default::default()
        },
    )
    .unwrap();
    backend.serve(worker).await.unwrap();
    let channel = "ai".parse().unwrap();
    let placed = backend
        .place(&channel, "r1", &[], None, &BTreeSet::new(), true)
        .await
        .unwrap();
    let crate::turns::redis::placement::Placed::Here(worker) = placed else {
        panic!("expected local worker")
    };
    backend.close().await.unwrap();

    let (request, owner, incoming, mut received) = link();
    backend.submit_here(Arc::clone(&worker), request, &owner, incoming, || {
        panic!("closed backend accepted a turn")
    });
    let (_, Frame::Error { error }) = received.get().await.unwrap() else {
        panic!("missing refusal")
    };
    assert_eq!(error.attributes["reason"], "backend_closed");
    assert!(matches!(received.get().await, Ok((2, Frame::End))));
    worker.close().await;
}

#[tokio::test]
async fn closing_after_admission_releases_the_reader_and_finishes_the_local_worker() {
    let backend = RedisTurnBackend::new(None, RedisTurnBackendOptions::default()).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let hook = Arc::clone(&dropped);
    let worker = TurnExecutor::new(
        move |id| {
            let signal = Arc::clone(&hook);
            async move { Ok(Some(Probe(id, signal))) }
        },
        TurnExecutorOptions::default(),
    )
    .unwrap();
    let (request, owner, incoming, mut received) = link();
    let (accepted, taken) = tokio::sync::oneshot::channel();
    backend.submit_here(
        Arc::new(worker.clone()),
        request,
        &owner,
        incoming,
        move || {
            accepted.send(()).unwrap();
        },
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        taken.await.unwrap();
        assert!(matches!(
            received.get().await,
            Ok((1, Frame::Accepted { .. }))
        ));
        assert!(matches!(
            received.get().await,
            Ok((2, Frame::Output { streams: true, .. }))
        ));
        backend.close().await.unwrap();
        assert_eq!(received.get().await.unwrap_err().reason(), "backend_closed");
        assert!(
            dropped.load(Ordering::Acquire),
            "the admitted worker's stream was not released"
        );
    })
    .await
    .unwrap();
    worker.close().await;
}
