//! Public channel admission and the context a listening thinker actually reads.
#![expect(clippy::unwrap_used, reason = "fixed native fixture")]

use std::sync::Arc;
use std::time::Duration;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventSource, Room, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit_ai::speaking::{
    MockSpeakPolicy, MockSpeakPolicyOptions, MockThinker, MockThinkerOptions, SpeakMode, Thought,
};
use roomkit_ai::{AiChannel, AiChannelOptions, MockAiProvider, MockAiProviderOptions};
use serde_json::json;

#[path = "speaking_admission/answering.rs"]
mod answering;
#[path = "speaking_admission/bounds.rs"]
mod bounds;
#[path = "speaking_admission/consumers.rs"]
mod consumers;
#[path = "speaking_admission/kit.rs"]
mod kit;
#[path = "speaking_admission/late.rs"]
mod late;
#[path = "speaking_admission/lifecycle.rs"]
mod lifecycle;
#[path = "speaking_admission/listening.rs"]
mod listening;
#[path = "speaking_admission/reports.rs"]
mod reports;
#[path = "speaking_admission/strategy.rs"]
mod strategy;
#[path = "speaking_admission/turns.rs"]
mod turns;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

fn id(value: &str) -> ChannelId {
    ChannelId::new(value).unwrap()
}

fn input(body: &str) -> (RoomEvent, ChannelBinding, RoomContext) {
    let room = RoomId::new("r1").unwrap();
    let mut event = RoomEvent::new(
        room.clone(),
        EventSource::new(id("sms"), ChannelType::SMS),
        TextContent::new(body),
    );
    event
        .metadata
        .insert("sender_name".into(), json!("Sylvain"));
    let mut ai = ChannelBinding::new(id("ai"), room.clone(), ChannelType::AI);
    ai.category = ChannelCategory::Intelligence;
    let sms = ChannelBinding::new(id("sms"), room.clone(), ChannelType::SMS);
    let mut context = RoomContext::new(Room::new(room));
    context.bindings = vec![ai.clone(), sms];
    context.recent_events = vec![event.clone()];
    (event, ai, context)
}

fn model(streaming: bool) -> MockAiProvider {
    MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["Yes?".into()],
        streaming,
        ..MockAiProviderOptions::default()
    })
}

// Python: tests/test_speaking_thinker_options.py::test_speaking_configuration_is_refused_before_the_turn_budget
#[test]
fn speaking_configuration_is_refused_before_the_turn_budget() {
    let error = AiChannel::new(
        id("ai"),
        model(false),
        AiChannelOptions {
            speak_timeout: Duration::ZERO,
            turn_budget_tokens: Some(0),
            ..AiChannelOptions::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.to_string(), "speak_timeout must be positive");
    let error = AiChannel::new(
        id("ai"),
        model(false),
        AiChannelOptions {
            thinker: Some(Arc::new(MockThinker::new(MockThinkerOptions::default()))),
            turn_budget_tokens: Some(0),
            ..AiChannelOptions::default()
        },
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("a thinker needs a speak_policy"));
}

// Python: tests/test_thinker.py::test_a_silent_turn_is_thought_about_from_its_context
#[tokio::test]
async fn a_silent_turn_is_thought_about_from_its_context() {
    for streaming in [false, true] {
        let model = model(streaming);
        let thinker = Arc::new(MockThinker::new(MockThinkerOptions {
            thoughts: vec![Thought {
                text: "Paul has the figures.".into(),
                ..Thought::default()
            }],
            ..MockThinkerOptions::default()
        }));
        let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![SpeakMode::Silent.into()],
            ..MockSpeakPolicyOptions::default()
        }));
        let channel = AiChannel::new(
            id("ai"),
            model.clone(),
            AiChannelOptions {
                system_prompt: Some("You are Nova.".into()),
                speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
                thinker: Some(Arc::<MockThinker>::clone(&thinker)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let (event, binding, context) = input("Paul, do you have the numbers?");
        let output = channel.on_event(event, binding, context).await.unwrap();
        assert!(!output.responded);
        assert_eq!(model.calls(), Vec::new());
        let calls = thinker.calls();
        assert_eq!(calls.len(), 1);
        let (previous, context) = calls.first().unwrap();
        assert_eq!(*previous, Thought::default());
        let context = context.snapshot();
        assert!(context.system_prompt.unwrap().contains("You are Nova."));
        assert!(
            context
                .messages
                .last()
                .unwrap()
                .text()
                .contains("Paul, do you have the numbers?")
        );
        assert!(context.messages.last().unwrap().text().contains("Sylvain"));
        assert_eq!(policy.turns().len(), 1);
        assert_eq!(
            policy.turns().first().unwrap().thought,
            Some(Thought::default())
        );
        channel.close().await;
    }
}

// Python: tests/test_thinker.py::test_with_something_to_say_in_time_the_agent_raises_its_hand
// Python: tests/test_thinker.py::test_the_thinker_reads_the_one_person_by_name
#[tokio::test]
async fn an_in_time_thought_reasks_the_policy_then_joins_the_answer_notes() {
    for streaming in [false, true] {
        let model = model(streaming);
        let price = Thought {
            text: "They look for the licence price; I know it.".into(),
            want_to_say: vec!["It costs 1,200 $ a year.".into()],
            urgent: false,
        };
        let thinker = Arc::new(MockThinker::new(MockThinkerOptions {
            thoughts: vec![price.clone()],
            ..MockThinkerOptions::default()
        }));
        let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![SpeakMode::Silent.into(), SpeakMode::Offer.into()],
            ..MockSpeakPolicyOptions::default()
        }));
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
        let (event, binding, context) = input("We are looking for the licence price.");
        let output = tokio::time::timeout(
            Duration::from_secs(2),
            channel.on_event(event, binding, context),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.responded);
        assert_eq!(policy.turns().len(), 2);
        assert_eq!(policy.turns().last().unwrap().thought, Some(price));
        let calls = model.calls();
        assert_eq!(calls.len(), 1);
        let text = calls.first().unwrap().messages.last().unwrap().text();
        assert!(text.contains("It costs 1,200 $ a year."));
        assert!(text.contains("Offer, in one short sentence"));
        assert!(!text.contains("Sylvain"));
        assert!(
            thinker
                .calls()
                .first()
                .unwrap()
                .1
                .snapshot()
                .messages
                .last()
                .unwrap()
                .text()
                .contains("Sylvain")
        );
        channel.close().await;
    }
}
