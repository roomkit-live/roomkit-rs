//! The loop strategy (RFC §19.7.4) driven through the public API the way an
//! integrator does: live agents given to a `Loop`, installed with
//! `create_room(orchestration=...)`, a message in, the approved output
//! out. The shared scenarios (roomkit-specs
//! `conformance/scenarios/orchestration/a-loop-*`) name every agent by its
//! id; these give them live.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;

use roomkit::model::{ChannelId, ChannelType, EventContent, InboundMessage, RoomId, TextContent};
use roomkit::orchestration::{LoopOptions, StrategyAgent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, GetTimelineOptions, Loop, Orchestration,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{Agent, AgentOptions, MockAiProvider, MockAiProviderOptions};

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

/// An agent `name` of `role` answering `responses` in turn.
fn agent(name: &str, role: &str, responses: &[&str]) -> Agent<MockAiProvider> {
    let provider = MockAiProvider::new(MockAiProviderOptions {
        responses: responses.iter().map(|&text| text.to_owned()).collect(),
        ..MockAiProviderOptions::default()
    });
    let options = AgentOptions {
        role: Some(role.to_owned()),
        ..AgentOptions::default()
    };
    Agent::new(id(name), provider, options).unwrap()
}

/// A kit holding an SMS transport, and room `r1` binding it, installed
/// with `review`.
async fn room(review: Arc<Loop>) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let review: Arc<dyn Orchestration> = review;
    let options = CreateRoomOptions {
        room_id: Some(RoomId::new("r1").unwrap()),
        orchestration: Some(Some(review)),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.attach_channel(&room, &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

/// The texts of `r1`'s messages, by sender, in order.
async fn messages(kit: &RoomKit) -> Vec<(String, String)> {
    let room = RoomId::new("r1").unwrap();
    let timeline = kit
        .get_timeline(&room, GetTimelineOptions::default())
        .await
        .unwrap();
    timeline
        .into_iter()
        .filter_map(|event| {
            let EventContent::Text(text) = event.content else {
                return None;
            };
            Some((event.source.channel_id.to_string(), text.body))
        })
        .collect()
}

// Python: tests/orchestration/test_loop_strategy.py::TestExecuteLoop::test_revision_cycle,
// with the agents given live, as the integrator gives them.
// Python: tests/orchestration/test_room_installs.py::test_installing_a_strategy_again_in_its_room_replaces_its_own[loop]
#[tokio::test]
async fn a_loop_of_live_agents_revises_until_its_reviewer_approves() {
    let writer = agent("writer", "Writer", &["Draft one.", "Draft two."]);
    let editor = agent("editor", "Editor", &["Needs work.", "APPROVED"]);
    let options = LoopOptions {
        reviewer: Some(StrategyAgent::channel(editor)),
        ..LoopOptions::default()
    };
    let review = Arc::new(Loop::new(StrategyAgent::channel(writer), options).unwrap());
    let kit = room(Arc::clone(&review)).await;
    Orchestration::install(review.as_ref(), &kit, &RoomId::new("r1").unwrap())
        .await
        .unwrap();

    let message = InboundMessage::new(id("sms"), "u1", TextContent::new("Write it."));
    let result = kit
        .process_inbound(message, ProcessInboundOptions::default())
        .await
        .unwrap();

    assert!(result.error.is_none());
    assert_eq!(
        messages(&kit).await,
        [
            ("sms".to_owned(), "Write it.".to_owned()),
            ("writer".to_owned(), "Draft two.".to_owned()),
        ]
    );
    // The reviewer is the kit's, never the room's.
    assert!(kit.get_channel(&id("editor")).is_some());
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_strategy_loop.py::TestLoopInstall::test_skips_already_registered_reviewer
#[tokio::test]
async fn a_live_reviewer_the_kit_holds_is_not_registered_again() {
    // The kit holds an agent under the reviewer's id: the live one given
    // the loop is not registered over it.
    let editor = agent("editor", "Editor", &["APPROVED"]);
    let writer = agent("writer", "Writer", &["Draft."]);
    let options = LoopOptions {
        reviewers: vec![StrategyAgent::channel(agent("editor", "Editor", &[]))],
        ..LoopOptions::default()
    };
    let review = Loop::new(StrategyAgent::channel(writer), options).unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(editor).unwrap();
    let review: Arc<dyn Orchestration> = Arc::new(review);
    let options = CreateRoomOptions {
        room_id: Some(RoomId::new("r1").unwrap()),
        orchestration: Some(Some(review)),
        ..CreateRoomOptions::default()
    };

    kit.create_room(options).await.unwrap();

    let held: Vec<String> = kit
        .list_channels()
        .iter()
        .map(|channel| channel.channel_id().to_string())
        .collect();
    assert_eq!(held, ["editor", "writer"]);
    kit.close().await.unwrap();
}
