//! Strategy passes enter through the executor but do not ask speaking admission.

use roomkit::model::{EventContent, InboundMessage};
use roomkit::orchestration::SupervisorOptions;
use roomkit::orchestration::strategies::supervisor::WorkerStrategy;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    CreateRoomOptions, InstallStrategyOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
    Supervisor,
};
use roomkit_ai::{Agent, AgentOptions};

use super::*;

// Python: tests/test_speaking_strategy.py::test_strategy_passes_bypass_both_the_silent_policy_and_its_thinker
#[tokio::test]
async fn strategy_passes_bypass_both_the_silent_policy_and_its_thinker() {
    for streaming in [false, true] {
        for refine in [false, true] {
            strategy_case(streaming, refine).await;
        }
    }
}

async fn strategy_case(streaming: bool, refine: bool) {
    let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
        decisions: vec![SpeakMode::Silent.into()],
        ..MockSpeakPolicyOptions::default()
    }));
    let thinker = Arc::new(MockThinker::new(MockThinkerOptions::default()));
    let provider = MockAiProvider::new(MockAiProviderOptions {
        responses: if refine {
            vec!["Task.".into(), "Presented.".into()]
        } else {
            vec!["Presented.".into()]
        },
        streaming,
        ..MockAiProviderOptions::default()
    });
    let boss = Agent::new(
        id("boss"),
        provider.clone(),
        AgentOptions {
            ai: AiChannelOptions {
                speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
                thinker: Some(Arc::<MockThinker>::clone(&thinker)),
                ..AiChannelOptions::default()
            },
            ..AgentOptions::default()
        },
    )
    .unwrap();
    let worker = Agent::new(id("worker"), model(false), AgentOptions::default()).unwrap();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(boss).unwrap();
    kit.register_channel(worker).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &id("sms"), roomkit::AttachChannelOptions::default())
        .await
        .unwrap();
    let supervisor = Supervisor::new(
        id("boss"),
        vec![id("worker").into()],
        SupervisorOptions {
            strategy: Some(WorkerStrategy::Parallel),
            auto_delegate: true,
            refine_task: refine,
            ..SupervisorOptions::default()
        },
    )
    .unwrap();
    kit.install_strategy(
        &room,
        Arc::new(supervisor),
        InstallStrategyOptions::default(),
    )
    .await
    .unwrap();
    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        kit.process_inbound(
            InboundMessage::new(id("sms"), "alice", TextContent::new("Go.")),
            ProcessInboundOptions {
                room_id: Some(room),
                ..ProcessInboundOptions::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        answer
            .response_events
            .iter()
            .map(|event| if let EventContent::Text(text) = &event.content {
                text.body.as_str()
            } else {
                "unexpected content"
            })
            .collect::<Vec<_>>(),
        ["Presented."],
        "streaming={streaming}, refine={refine}"
    );
    assert_eq!(provider.calls().len(), if refine { 2 } else { 1 });
    assert_eq!(policy.turns().len(), 0);
    assert_eq!(thinker.calls().len(), 0);
    tokio::time::timeout(Duration::from_secs(3), kit.close())
        .await
        .unwrap()
        .unwrap();
}
