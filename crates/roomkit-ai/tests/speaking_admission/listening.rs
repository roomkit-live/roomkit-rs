//! Real listening and audience policies composed with the channel's room thinker.

use std::sync::Mutex;

use roomkit::model::InboundMessage;
use roomkit::{HookExecution, HookOptions, HookTrigger, ProcessInboundOptions};
use roomkit_ai::classifiers::{Script, ScriptedAnswer};
use roomkit_ai::speaking::SpeakingFuture;
use roomkit_ai::{
    AnswerOnly, ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions, MockClassifier,
    MockClassifierOptions, SharedAiContext, SpeakDecisionEvent, SpeakPolicy, SpeakTurn, Thinker,
};
use tokio::sync::Notify;

use super::*;

fn script(values: &[(&str, f64)]) -> Script {
    values
        .iter()
        .map(|(key, value)| ((*key).to_owned(), ScriptedAnswer::Number(*value)))
        .collect()
}

// Python: tests/test_speak_listening.py::test_a_voice_only_listened_to_neither_starts_nor_lifts_the_state
#[tokio::test]
async fn an_unanswered_speaker_cannot_change_the_real_classifier_listening_state() {
    let classifier = Arc::new(MockClassifier::new(MockClassifierOptions {
        answers: vec![script(&[("listen_request", 0.9), ("lift", 0.9)])],
        ..MockClassifierOptions::default()
    }));
    let inner = Arc::new(
        ClassifierSpeakPolicy::new(
            Arc::<MockClassifier>::clone(&classifier),
            "Nova",
            ClassifierSpeakPolicyOptions::default(),
        )
        .unwrap(),
    );
    let policy = AnswerOnly::new(Arc::<ClassifierSpeakPolicy>::clone(&inner), ["Sylvain"]).unwrap();
    let (event, _, _) = input("Stay quiet, everyone.");
    let mut turn = SpeakTurn::new(event.clone());
    turn.people = vec!["Sylvain".into()];
    turn.speakers.insert(event.id.as_str(), "TV".into());
    let result = policy.decide(&turn).await.unwrap();
    assert!(result.r#final);
    assert_eq!(classifier.calls().len(), 0);
    assert!(
        inner.state(&turn).unwrap().snapshot()["agent"]
            .get("listening_only")
            .is_none()
    );
}

#[derive(Default)]
struct Held {
    entered: Notify,
    release: Notify,
    calls: Mutex<usize>,
}
impl Thinker for Held {
    fn think<'a>(&'a self, _: &'a Thought, _: &'a SharedAiContext) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            let first = {
                let mut calls = self.calls.lock().unwrap();
                *calls += 1;
                *calls == 1
            };
            if first {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(Thought {
                text: "The printer is broken.".into(),
                want_to_say: vec!["Check the printer.".into()],
                urgent: false,
            })
        })
    }
}

// Python: tests/test_speak_listening.py::test_a_listening_room_thinks_without_holding_the_turn_back
#[tokio::test]
async fn a_real_listening_policy_thinks_then_offers_late_and_answers_the_next_question() {
    for streaming in [false, true] {
        listening_case(streaming).await;
    }
}

async fn listening_case(streaming: bool) {
    let classifier = Arc::new(MockClassifier::new(MockClassifierOptions {
        answers: vec![
            script(&[("listen_request", 0.9)]),
            script(&[("directness", 0.0)]),
            script(&[("answers", 0.9)]),
            script(&[("directness", 3.0), ("asked_me", 0.9)]),
        ],
        ..MockClassifierOptions::default()
    }));
    let policy = ClassifierSpeakPolicy::new(
        Arc::<MockClassifier>::clone(&classifier),
        "Nova",
        ClassifierSpeakPolicyOptions::default(),
    )
    .unwrap();
    let thinker = Arc::new(Held::default());
    let provider = model(streaming);
    let kit = kit::setup(
        false,
        AiChannelOptions {
            speak_policy: Some(Arc::new(policy)),
            thinker: Some(Arc::<Held>::clone(&thinker)),
            think_wait: Duration::from_secs(5),
            ..AiChannelOptions::default()
        },
        provider.clone(),
    )
    .await;
    let reports = Arc::new(Mutex::new(Vec::new()));
    let late = Arc::new(Notify::new());
    let (seen, finished) = (Arc::clone(&reports), Arc::clone(&late));
    kit.hook(
        HookTrigger::OnSpeakDecision,
        move |event: SpeakDecisionEvent, _: RoomContext| {
            if event.late {
                finished.notify_one();
            }
            seen.lock().unwrap().push(event);
            std::future::ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    for text in ["Only listen.", "My printer does not work either."] {
        tokio::time::timeout(Duration::from_secs(1), say(&kit, text))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), thinker.entered.notified())
        .await
        .unwrap();
    assert_eq!(provider.calls(), Vec::new());
    thinker.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), late.notified())
        .await
        .unwrap();
    assert_eq!(provider.calls(), Vec::new());
    tokio::time::timeout(
        Duration::from_secs(3),
        say(&kit, "What do you think, Nova?"),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_reports(&reports.lock().unwrap());
    assert_eq!(classifier.calls().len(), 4);
    assert_eq!(provider.calls().len(), 1);
}

fn assert_reports(reports: &[SpeakDecisionEvent]) {
    assert_eq!(
        reports
            .iter()
            .map(|r| (
                r.decision.mode,
                r.decision.reason.as_str(),
                r.asked_again,
                r.late
            ))
            .collect::<Vec<_>>(),
        [
            (SpeakMode::Silent, "asked to listen", false, false),
            (SpeakMode::Silent, "listening", false, false),
            (
                SpeakMode::Offer,
                "listening, has something to add",
                true,
                true
            ),
            (SpeakMode::Speak, "asked while listening", false, false),
        ]
    );
}

async fn say(kit: &roomkit::RoomKit, text: &str) {
    let result = kit
        .process_inbound(
            InboundMessage::new(id("sms"), "alice", TextContent::new(text)),
            ProcessInboundOptions {
                room_id: Some(RoomId::new("r1").unwrap()),
                ..ProcessInboundOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(result.error.is_none());
}
