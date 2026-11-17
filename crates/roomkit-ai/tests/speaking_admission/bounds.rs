//! Decisions measured at the public hook, and turns never submitted to a policy.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::{EventType, InboundMessage};
use roomkit::{HookExecution, HookOptions, HookTrigger, ProcessInboundOptions};
use roomkit_ai::speaking::{
    SpeakDecision, SpeakDecisionEvent, SpeakPolicy, SpeakTurn, SpeakingFuture,
};

use super::*;

// Python: tests/test_speak_policy.py::test_every_decision_reaches_on_speak_decision
// Python: tests/test_speak_policy.py::test_always_speak_answers_and_reports_each_decision
// Python: tests/test_thinker.py::test_speak_decision_is_unchanged_without_a_thinker
#[tokio::test]
async fn a_policy_reports_its_real_decision_and_no_thought_without_a_thinker() {
    for silent in [false, true] {
        let policy: Arc<dyn SpeakPolicy> = if silent {
            Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
                decisions: vec![SpeakDecision {
                    reason: "side talk".into(),
                    judgments: [("addressed", 0.1)].into_iter().collect(),
                    ..SpeakMode::Silent.into()
                }],
                ..MockSpeakPolicyOptions::default()
            }))
        } else {
            Arc::new(roomkit_ai::AlwaysSpeak)
        };
        let model = model(false);
        let kit = kit::setup(
            false,
            AiChannelOptions {
                speak_policy: Some(policy),
                ..AiChannelOptions::default()
            },
            model.clone(),
        )
        .await;
        let seen = Arc::new(Mutex::new(Vec::<SpeakDecisionEvent>::new()));
        let reports = Arc::clone(&seen);
        kit.hook(
            HookTrigger::OnSpeakDecision,
            move |event: SpeakDecisionEvent, _: RoomContext| {
                reports.lock().unwrap().push(event);
                std::future::ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        kit.process_inbound(
            InboundMessage::new(id("sms"), "alice", TextContent::new("Hello")),
            ProcessInboundOptions {
                room_id: Some(RoomId::new("r1").unwrap()),
                ..ProcessInboundOptions::default()
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), roomkit::testing::idle(&kit))
            .await
            .unwrap();
        kit.close().await.unwrap();
        let reports = seen.lock().unwrap();
        assert_eq!(reports.len(), 1);
        let report = reports.first().unwrap();
        assert_eq!(
            (report.room_id.as_str(), report.channel_id.as_str()),
            ("r1", "ai")
        );
        assert_eq!(
            report.decision.mode,
            if silent {
                SpeakMode::Silent
            } else {
                SpeakMode::Speak
            }
        );
        assert_eq!(
            report.decision.reason,
            if silent { "side talk" } else { "always" }
        );
        assert_eq!(
            report.decision.judgments.get("addressed"),
            silent.then_some(0.1)
        );
        assert!(report.thought.is_none());
        assert!(!report.late && !report.asked_again);
        assert_eq!(model.calls().len(), usize::from(!silent));
    }
}

// Python: tests/test_speak_policy.py::test_a_decision_reports_how_long_the_policy_took
// Python: tests/test_speak_policy.py::test_a_policy_that_fails_or_is_late_does_not_silence_the_agent
#[tokio::test(start_paused = true)]
async fn the_hook_measures_the_policy_and_a_timeout_warns_then_speaks() {
    for late in [false, true] {
        let policy = MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![SpeakMode::Speak.into()],
            delay: if late { 1.0 } else { 0.05 },
            ..MockSpeakPolicyOptions::default()
        });
        let model = model(false);
        let kit = kit::setup(
            false,
            AiChannelOptions {
                speak_policy: Some(Arc::new(policy)),
                speak_timeout: Duration::from_millis(100),
                ..AiChannelOptions::default()
            },
            model.clone(),
        )
        .await;
        let seen = Arc::new(Mutex::new(Vec::<SpeakDecisionEvent>::new()));
        let reports = Arc::clone(&seen);
        kit.hook(
            HookTrigger::OnSpeakDecision,
            move |event: SpeakDecisionEvent, _: RoomContext| {
                reports.lock().unwrap().push(event);
                std::future::ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        let (warnings, _capture) = warnings::Warnings::capture();
        let result = kit
            .process_inbound(
                InboundMessage::new(id("sms"), "alice", TextContent::new("Nova ?")),
                ProcessInboundOptions {
                    room_id: Some(RoomId::new("r1").unwrap()),
                    ..ProcessInboundOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(result.error.is_none());
        tokio::time::timeout(Duration::from_secs(3), roomkit::testing::idle(&kit))
            .await
            .unwrap();
        let reports = seen.lock().unwrap().clone();
        assert_eq!(reports.len(), 1);
        let report = reports.first().unwrap();
        let expected = if late { 100.0 } else { 50.0 };
        assert!((report.duration_ms - expected).abs() <= 2.0, "{report:?}");
        assert!(!report.asked_again);
        assert_eq!(report.decision.reason, if late { "fallback" } else { "" });
        assert_eq!(warnings.containing("speaking").len(), usize::from(late));
        assert_eq!(model.calls().len(), 1);
        kit.close().await.unwrap();
    }
}

// Python: tests/test_edit_delete_authorization.py::TestNoTurnOnAChangeOrANotice::test_no_speak_policy_nor_thinker_is_asked_what_opens_no_turn
#[tokio::test]
async fn changes_notices_and_own_messages_consult_neither_policy_nor_thinker() {
    let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions::default()));
    let thinker = Arc::new(MockThinker::new(MockThinkerOptions::default()));
    let model = model(false);
    let channel = AiChannel::new(
        id("ai"),
        model.clone(),
        AiChannelOptions {
            speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
            thinker: Some(Arc::<MockThinker>::clone(&thinker)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    for kind in [
        EventType::Edit,
        EventType::Delete,
        EventType::System,
        EventType::ToolCallStart,
        EventType::ToolCallEnd,
        EventType::Message,
    ] {
        let (mut event, binding, context) = input("a notice");
        event.r#type = kind;
        if kind == EventType::Message {
            event.source.channel_id = id("ai");
        }
        assert!(
            !channel
                .on_event(event, binding, context)
                .await
                .unwrap()
                .responded
        );
    }
    assert_eq!(policy.turns().len(), 0);
    assert_eq!(thinker.calls().len(), 0);
    assert_eq!(model.calls(), Vec::new());
    channel.close().await;
}

#[derive(Default)]
struct Forgetful {
    rooms: Mutex<Vec<String>>,
    closed: AtomicUsize,
}
impl SpeakPolicy for Forgetful {
    fn decide<'a>(&'a self, _: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        Box::pin(std::future::ready(Ok(SpeakMode::Silent.into())))
    }
    fn forget_room(&self, room: &str) -> Result<(), roomkit::ChannelError> {
        self.rooms.lock().unwrap().push(room.into());
        Ok(())
    }
    fn close(&self) -> SpeakingFuture<'_, ()> {
        self.closed.fetch_add(1, Ordering::Release);
        Box::pin(std::future::ready(Ok(())))
    }
}

// Python: tests/test_speak_listening.py::test_the_channel_forgets_the_state_of_a_room_it_leaves_or_joins
#[tokio::test]
async fn the_channel_forgets_its_wrapped_policy_on_both_binding_boundaries() {
    let policy = Arc::new(Forgetful::default());
    let wrapped =
        roomkit_ai::AnswerOnly::new(Arc::<Forgetful>::clone(&policy), vec!["Sylvain"]).unwrap();
    let channel = AiChannel::new(
        id("ai"),
        model(false),
        AiChannelOptions {
            speak_policy: Some(Arc::new(wrapped)),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let (_, binding, _) = input("");
    channel
        .on_room_detached(RoomId::new("r1").unwrap())
        .await
        .unwrap();
    channel
        .on_room_attached(RoomId::new("r1").unwrap(), binding)
        .await
        .unwrap();
    assert_eq!(*policy.rooms.lock().unwrap(), ["r1", "r1"]);
    channel.close().await;
    assert_eq!(policy.closed.load(Ordering::Acquire), 0);
}

// Python: tests/test_channel_declaration.py::test_an_ai_channel_with_a_thinker_declares_it_thinks_while_listening
// Python: tests/orchestration/test_discussion_refusals.py::test_an_agent_that_thinks_while_it_listens_is_refused
#[tokio::test]
async fn a_declared_listener_cannot_be_installed_in_a_discussion() {
    for agent in [false, true] {
        let kit = kit::setup(
            agent,
            AiChannelOptions {
                speak_policy: Some(Arc::new(MockSpeakPolicy::new(
                    MockSpeakPolicyOptions::default(),
                ))),
                thinker: Some(Arc::new(MockThinker::new(MockThinkerOptions::default()))),
                ..AiChannelOptions::default()
            },
            model(false),
        )
        .await;
        let strategy = roomkit::orchestration::Discussion::new(
            vec![id("ai").into()],
            roomkit::orchestration::DiscussionOptions::default(),
        )
        .unwrap();
        let error = kit
            .install_strategy(
                &RoomId::new("r1").unwrap(),
                Arc::new(strategy),
                roomkit::InstallStrategyOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("ai thinks while it listens"),
            "{error}"
        );
        kit.close().await.unwrap();
    }
    let ordinary = AiChannel::new(id("ai"), model(false), AiChannelOptions::default()).unwrap();
    assert!(!ordinary.thinks_while_listening());
    assert!(!ordinary.declaration().thinks_while_listening);
}
