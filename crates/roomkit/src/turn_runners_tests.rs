//! Contracts requiring access to the owner's private runner registry.

use std::collections::VecDeque;
use std::sync::Arc;

use futures_core::Stream;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId, TextContent,
};

use super::{Passes, Taken, Then, TurnRunner};
use crate::stream::{LoopEndMarker, LoopEndReason, ResponseItem};
use crate::testing::{EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions};
use crate::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, InMemoryStore,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};

#[derive(Clone, Copy)]
enum RunnerMode {
    Answered,
    Later,
    Passes,
}

struct CancelledRunner {
    mode: RunnerMode,
    cancel: bool,
}

impl TurnRunner<InMemoryStore> for CancelledRunner {
    fn take(
        &self,
        _kit: RoomKit,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Taken {
        let error = if self.cancel {
            RoomKitError::Cancelled
        } else {
            RoomKitError::Value("failed".to_owned())
        };
        match self.mode {
            RunnerMode::Answered => Taken::Answered(Box::pin(async { Err(error.into()) })),
            RunnerMode::Later => Taken::Later(Box::pin(async { Err(error) })),
            RunnerMode::Passes => Taken::Passes(Passes {
                first: Box::new((event, binding, context)),
                then: Then(Box::new(move |first| {
                    assert_eq!(first.answer, "first");
                    Box::pin(async { Err(error) })
                })),
            }),
        }
    }
}

// Python: tests/test_delivery_outcomes.py::TestTheCallerIsToldWhatHappened::test_an_agent_turn_its_strategy_cancels_is_no_error_of_the_call
async fn result_from(runner: CancelledRunner, streamed: bool) -> crate::InboundResult {
    let (kit, room, input, agent) = room_with_agent(streamed).await;
    kit.inner
        .turn_runners
        .take(&room, &agent, Arc::new(runner), 1)
        .unwrap();
    let result = kit
        .process_inbound(
            InboundMessage::new(input, "alice", TextContent::new("hello")),
            ProcessInboundOptions {
                room_id: Some(room),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    kit.close().await.unwrap();
    result
}

// Python: tests/test_delivery_outcomes.py::TestTheCallerIsToldWhatHappened::test_an_agent_turn_its_strategy_cancels_is_no_error_of_the_call
#[tokio::test]
async fn an_agent_turn_its_strategy_cancels_is_no_error_of_the_call() {
    for (mode, streamed) in runner_modes() {
        let result = result_from(CancelledRunner { mode, cancel: true }, streamed).await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(
            !result
                .delivery_results
                .contains_key(&ChannelId::new("agent").unwrap())
        );
    }
}

#[tokio::test]
async fn a_strategy_failure_is_still_reported() {
    for (mode, streamed) in runner_modes() {
        let result = result_from(
            CancelledRunner {
                mode,
                cancel: false,
            },
            streamed,
        )
        .await;
        let error = result.error.unwrap();
        assert_eq!(
            (error.error_type.as_str(), error.message.as_str()),
            ("ValueError", "failed")
        );
    }
}

async fn room_with_agent(streamed: bool) -> (RoomKit, RoomId, ChannelId, ChannelId) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("r1").unwrap();
    let input = ChannelId::new("input").unwrap();
    let agent = ChannelId::new("agent").unwrap();
    kit.register_channel(RecordingTransport::new(
        input.clone(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(FirstPassAgent {
        inner: EchoAgent::new(agent.clone(), "first", EchoAgentOptions::default()),
        streamed,
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for channel in [&input, &agent] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, room, input, agent)
}

fn runner_modes() -> [(RunnerMode, bool); 4] {
    [
        (RunnerMode::Answered, false),
        (RunnerMode::Later, false),
        (RunnerMode::Passes, false),
        (RunnerMode::Passes, true),
    ]
}

struct FirstPassAgent {
    inner: EchoAgent,
    streamed: bool,
}

impl Channel for FirstPassAgent {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id().clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
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

    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if !self.streamed {
            return self.inner.on_event(event, binding, context).await;
        }
        let stream = FirstPassStream(VecDeque::from([
            ResponseItem::Text("first".to_owned()),
            ResponseItem::LoopEnd(LoopEndMarker::new(LoopEndReason::Completed)),
        ]));
        Ok(crate::current_turn_host()
            .unwrap()
            .respond_with_stream(Box::pin(stream)))
    }
}

struct FirstPassStream(VecDeque<ResponseItem>);

impl Stream for FirstPassStream {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.get_mut().0.pop_front().map(Ok))
    }
}
