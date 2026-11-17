//! Python: `test_speaking_public_models.py` mock and optional-protocol cases.

use std::error::Error;
use std::sync::Arc;

use roomkit::model::{ChannelId, ChannelType, EventSource, RoomEvent, RoomId, TextContent};
use roomkit_ai::speaking::{
    AlwaysSpeak, MockSpeakPolicy, MockSpeakPolicyOptions, MockThinker, MockThinkerOptions,
    SpeakDecision, SpeakMode, SpeakPolicy, SpeakTurn, Thinker, Thought,
};
use roomkit_ai::{AiContext, SharedAiContext};

#[expect(clippy::unwrap_used, reason = "fixed test fixture identifiers")]
fn turn() -> SpeakTurn {
    SpeakTurn::new(RoomEvent::new(
        RoomId::new("r1").unwrap(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        TextContent::new("Hello"),
    ))
}

#[derive(Debug, thiserror::Error)]
#[error("unavailable")]
struct HostFailure;

// Python: test_speaking_public_models.py::test_mock_policy_records_before_failure_and_repeats_the_same_decision.
#[tokio::test]
async fn mock_policy_records_before_failure_and_repeats_the_same_decision() {
    let turn = turn();
    let decision = SpeakDecision {
        reason: "a reason".into(),
        ..SpeakMode::Offer.into()
    };
    let policy = MockSpeakPolicy::new(MockSpeakPolicyOptions {
        decisions: vec![decision.clone()],
        ..MockSpeakPolicyOptions::default()
    });
    assert_eq!(policy.decide(&turn).await.unwrap(), decision);
    assert_eq!(policy.decide(&turn).await.unwrap(), decision);
    decision.judgments.insert("heard", 0.75);
    assert_eq!(
        policy.decide(&turn).await.unwrap().judgments.get("heard"),
        Some(0.75)
    );
    assert_eq!(policy.turns(), [turn.clone(), turn.clone(), turn.clone()]);
    assert!(
        policy
            .turns()
            .first()
            .unwrap()
            .event
            .same_object(&turn.event)
    );

    let error = Arc::new(HostFailure);
    let scripted: Arc<dyn Error + Send + Sync> = Arc::<HostFailure>::clone(&error);
    let failed = MockSpeakPolicy::new(MockSpeakPolicyOptions {
        error: Some(scripted),
        ..MockSpeakPolicyOptions::default()
    });
    let raised = failed.decide(&turn).await.unwrap_err();
    assert!(std::ptr::eq(
        raised
            .source()
            .unwrap()
            .downcast_ref::<HostFailure>()
            .unwrap(),
        error.as_ref()
    ));
    assert_eq!(failed.turns(), std::slice::from_ref(&turn));
    assert_eq!(
        MockSpeakPolicy::new(MockSpeakPolicyOptions::default())
            .decide(&turn)
            .await
            .unwrap()
            .mode,
        SpeakMode::Speak
    );
}

// Python: test_speaking_public_models.py::test_mock_thinker_records_live_inputs_and_keeps_or_repeats_the_thought.
#[tokio::test]
async fn mock_thinker_records_live_inputs_and_keeps_or_repeats_the_thought() {
    let previous = Thought {
        text: "Previous".into(),
        ..Thought::default()
    };
    let context = SharedAiContext::new(AiContext::default());
    let thinker = MockThinker::new(MockThinkerOptions::default());
    assert_eq!(thinker.think(&previous, &context).await.unwrap(), previous);
    assert!(thinker.calls().first().unwrap().1.same_object(&context));
    context.update(|context| context.system_prompt = Some("Changed".into()));
    assert_eq!(
        thinker
            .calls()
            .first()
            .unwrap()
            .1
            .snapshot()
            .system_prompt
            .as_deref(),
        Some("Changed")
    );
    let next = Thought {
        text: "Next".into(),
        ..Thought::default()
    };
    let scripted = MockThinker::new(MockThinkerOptions {
        thoughts: vec![next.clone()],
        ..MockThinkerOptions::default()
    });
    assert_eq!(scripted.think(&previous, &context).await.unwrap(), next);
    assert_eq!(scripted.think(&previous, &context).await.unwrap(), next);
    let error = Arc::new(HostFailure);
    let cause: Arc<dyn Error + Send + Sync> = Arc::<HostFailure>::clone(&error);
    let failed = MockThinker::new(MockThinkerOptions {
        error: Some(cause),
        ..MockThinkerOptions::default()
    });
    let raised = failed.think(&previous, &context).await.unwrap_err();
    assert!(std::ptr::eq(
        raised
            .source()
            .unwrap()
            .downcast_ref::<HostFailure>()
            .unwrap(),
        error.as_ref()
    ));
    assert_eq!(failed.calls(), [(previous, context)]);
}

// Python: test_speaking_public_models.py::test_protocol_optional_hooks_and_answer_only_delegation
// (the base defaults only; AnswerOnly delegation has a separate witness).
#[tokio::test]
async fn optional_policy_and_thinker_lifecycle_hooks_do_nothing() {
    let policy = AlwaysSpeak;
    policy.forget_room("r1").unwrap();
    policy.close().await.unwrap();
    let decision = policy.decide(&turn()).await.unwrap();
    assert_eq!(decision.mode, SpeakMode::Speak);
    assert_eq!(decision.reason, "always");
    MockThinker::new(MockThinkerOptions::default())
        .close()
        .await
        .unwrap();
}
