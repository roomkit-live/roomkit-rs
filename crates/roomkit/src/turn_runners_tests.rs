//! Contracts requiring access to the owner's private runner registry.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelType, InboundMessage, RoomContext, RoomEvent, RoomId,
    TextContent,
};

use super::{Taken, TurnRunner};
use crate::testing::{EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions};
use crate::{
    AttachChannelOptions, CreateRoomOptions, InMemoryStore, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions,
};

struct CancelledRunner {
    later: bool,
    cancel: bool,
}

impl TurnRunner<InMemoryStore> for CancelledRunner {
    fn take(
        &self,
        _kit: RoomKit,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Taken {
        let error = if self.cancel {
            RoomKitError::Cancelled
        } else {
            RoomKitError::Value("failed".to_owned())
        };
        if self.later {
            Taken::Later(Box::pin(async { Err(error) }))
        } else {
            Taken::Answered(Box::pin(async { Err(error.into()) }))
        }
    }
}

// Python: tests/test_delivery_outcomes.py::TestTheCallerIsToldWhatHappened::test_an_agent_turn_its_strategy_cancels_is_no_error_of_the_call
async fn result_from(runner: CancelledRunner) -> crate::InboundResult {
    let (kit, room, input, agent) = room_with_agent().await;
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
    for later in [false, true] {
        let result = result_from(CancelledRunner {
            later,
            cancel: true,
        })
        .await;
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
    for later in [false, true] {
        let result = result_from(CancelledRunner {
            later,
            cancel: false,
        })
        .await;
        let error = result.error.unwrap();
        assert_eq!(
            (error.error_type.as_str(), error.message.as_str()),
            ("ValueError", "failed")
        );
    }
}

async fn room_with_agent() -> (RoomKit, RoomId, ChannelId, ChannelId) {
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
    kit.register_channel(EchoAgent::new(
        agent.clone(),
        "unused",
        EchoAgentOptions::default(),
    ))
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
