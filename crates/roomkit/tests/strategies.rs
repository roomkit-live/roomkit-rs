//! A strategy's agents given live (RFC §19.7): what the shared scenarios,
//! whose agents are named by id, cannot give.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::Arc;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId,
};
use roomkit::orchestration::{StrategyAgent, StrategyFuture};
use roomkit::{
    Channel, ChannelError, CreateRoomOptions, ListBindingsOptions, Orchestration, RoomKit,
    RoomKitOptions, default_inbound_event,
};

/// An intelligence channel that answers nothing.
#[derive(Clone)]
struct Quiet(&'static str);

impl Channel for Quiet {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new(self.0).unwrap()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// A strategy whose agents are given live, set up nothing else.
struct Team(Vec<Quiet>);

impl Orchestration for Team {
    fn agents(&self) -> Vec<StrategyAgent> {
        self.0.iter().cloned().map(StrategyAgent::channel).collect()
    }

    fn install<'call>(
        &'call self,
        _kit: &'call RoomKit,
        _room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(async { Ok(()) })
    }
}

async fn bound(kit: &RoomKit, room: &str) -> Vec<String> {
    let room = RoomId::new(room).unwrap();
    let mut bound: Vec<String> = kit
        .list_bindings(&room, ListBindingsOptions::default())
        .await
        .unwrap()
        .into_iter()
        .map(|binding| binding.channel_id.as_str().to_owned())
        .collect();
    bound.sort();
    bound
}

// Python: tests/orchestration/test_framework_orchestration.py::TestFrameworkOrchestration::test_agents_registered_once
#[tokio::test]
async fn agents_registered_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(Quiet("shared")).unwrap();
    let team: Arc<dyn Orchestration> = Arc::new(Team(vec![Quiet("shared"), Quiet("other")]));

    for room in ["r1", "r2"] {
        let options = CreateRoomOptions {
            room_id: Some(RoomId::new(room).unwrap()),
            orchestration: Some(Some(Arc::clone(&team))),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
    }

    let held: Vec<String> = kit
        .list_channels()
        .iter()
        .map(|channel| channel.channel_id().as_str().to_owned())
        .collect();
    assert_eq!(held, ["shared", "other"]);
    assert_eq!(bound(&kit, "r1").await, ["other", "shared"]);
    assert_eq!(bound(&kit, "r2").await, ["other", "shared"]);
    kit.close().await.unwrap();
}
